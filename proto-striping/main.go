// PROTOTYPE — throwaway code for netacc issue #13.
// 验证：同一 peer 两条不同传输的连接上各开一条流、手工条带化是否可行，
// 并粗测聚合吞吐 vs 单路径基线（loopback，无真实 QoS，只看机制可行性与 CPU 并行度）。
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"
	msmux "github.com/multiformats/go-multistream"
)

const protoID = "/proto/striping/1.0.0"

var total = 256 << 20 // 256 MiB
var chunk = 64 << 10  // 64 KiB

// phase 一轮测试的接收侧统计；B 的 stream handler 始终指向 cur。
type phase struct {
	name   string
	expect int64
	left   atomic.Int64
	done   chan struct{}
	mu     sync.Mutex
	byConn map[string]*connStat
}

type connStat struct {
	bytes int64
	start time.Time
	end   time.Time
}

var cur atomic.Pointer[phase]

func newPhase(name string, expect int64) *phase {
	p := &phase{name: name, expect: expect, done: make(chan struct{}), byConn: map[string]*connStat{}}
	p.left.Store(expect)
	cur.Store(p)
	return p
}

func handler(st network.Stream) {
	p := cur.Load()
	key := transportOf(st.Conn().RemoteMultiaddr().String()) + "/" + st.Conn().ID()
	p.mu.Lock()
	cs := p.byConn[key]
	if cs == nil {
		cs = &connStat{start: time.Now()}
		p.byConn[key] = cs
	}
	p.mu.Unlock()
	n, _ := io.Copy(io.Discard, st)
	p.mu.Lock()
	cs.bytes = n
	cs.end = time.Now()
	p.mu.Unlock()
	if p.left.Add(-1) == 0 {
		close(p.done)
	}
}

func (p *phase) report() {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("=== [%s] B 侧逐连接接收 ===\n", p.name)
	for k, cs := range p.byConn {
		d := cs.end.Sub(cs.start)
		fmt.Printf("  %-16s %6.1f MiB in %8v -> %7.1f MiB/s\n", k, float64(cs.bytes)/(1<<20), d.Round(time.Millisecond), float64(cs.bytes)/(1<<20)/d.Seconds())
	}
}

func transportOf(addr string) string {
	switch {
	case strings.Contains(addr, "quic"):
		return "QUIC"
	case strings.Contains(addr, "/ws"):
		return "WS"
	default:
		return "TCP"
	}
}

func main() {
	ctx := context.Background()

	hostB, err := libp2p.New(libp2p.ListenAddrStrings(
		"/ip4/127.0.0.1/tcp/0",
		"/ip4/127.0.0.1/udp/0/quic-v1",
	))
	must(err)
	defer hostB.Close()

	hostA, err := libp2p.New(libp2p.NoListenAddrs)
	must(err)
	defer hostA.Close()
	hostB.SetStreamHandler(protoID, handler)

	var tcpAddr, quicAddr ma.Multiaddr
	for _, a := range hostB.Addrs() {
		switch transportOf(a.String()) {
		case "TCP":
			tcpAddr = a
		case "QUIC":
			quicAddr = a
		}
	}
	fmt.Println("B addrs:", hostB.Addrs())
	fmt.Println("B peer:", hostB.ID())

	// ---- 建两条连接：QUIC 走 swarm（peerstore 只给 quic 地址），TCP 走 transport 层直拨 ----
	hostA.Peerstore().AddAddr(hostB.ID(), quicAddr, peerstore.PermanentAddrTTL)
	must(hostA.Connect(ctx, peer.AddrInfo{ID: hostB.ID()}))
	quicConn := waitConn(hostA, hostB.ID(), "quic")
	fmt.Println("swarm conn:", quicConn.RemoteMultiaddr())

	sw := hostA.Network().(*swarm.Swarm)
	tpt := sw.TransportForDialing(tcpAddr)
	tcc, err := tpt.Dial(ctx, tcpAddr, hostB.ID())
	must(err)
	fmt.Println("direct-dialed conn:", tcc.RemoteMultiaddr())

	// ---- baseline: 单条 transport 直拨 TCP 连接 ----
	pb := newPhase("baseline TCP", 1)
	ms0, err := tcc.OpenStream(ctx)
	must(err)
	must(msmux.SelectProtoOrFail(protoID, ms0))
	buf := make([]byte, chunk)
	start := time.Now()
	for sent := 0; sent < total; sent += chunk {
		must(writeFull(ms0, buf))
	}
	must(ms0.Close())
	lastSend = time.Since(start)
	d0 := reportSend("baseline TCP(direct)")
	pb.report()

	// ---- striped: QUIC(swarm 托管) + TCP(transport 层直拨) ----
	ps := newPhase("striped QUIC+TCP", 2)
	sq, err := quicConn.NewStream(ctx)
	must(err)
	must(msmux.SelectProtoOrFail(protoID, sq))
	sq.SetProtocol(protoID)
	ms, err := tcc.OpenStream(ctx)
	must(err)
	must(msmux.SelectProtoOrFail(protoID, ms))

	start = time.Now()
	ws := []io.Writer{sq, ms}
	cls := []func() error{sq.CloseWrite, ms.Close}
	for sent, i := 0, 0; sent < total; sent, i = sent+chunk, i+1 {
		must(writeFull(ws[i%2], buf))
	}
	must(cls[0]())
	must(cls[1]())
	lastSend = time.Since(start)
	d1 := reportSend("striped QUIC+TCP")
	ps.report()

	fmt.Printf("\n加速比: %.2fx\n", float64(d0)/float64(d1))
}

var lastSend time.Duration

func reportSend(name string) time.Duration {
	fmt.Printf("\n[%s] 发送 %d MiB in %v -> %.1f MiB/s\n", name, total>>20, lastSend.Round(time.Millisecond), float64(total)/(1<<20)/lastSend.Seconds())
	return lastSend
}

func waitConn(h host.Host, p peer.ID, kind string) network.Conn {
	for i := 0; i < 300; i++ {
		for _, c := range h.Network().ConnsToPeer(p) {
			if strings.Contains(c.RemoteMultiaddr().String(), kind) {
				return c
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("no conn of kind " + kind)
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		os.Exit(1)
	}
}
