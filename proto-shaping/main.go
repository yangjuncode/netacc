// PROTOTYPE — throwaway code for netacc issue #15.
// 验证 transport 装饰器方案：包装 TCP 传输，给每个对端 peer 的连接流套共享限速桶；
// 原生 relay.New 零改动；两个客户端经中继打流，观察按 peer 的 max-min 公平收敛。
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/tcpreuse"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
)

// ---------- 限速注册表：按对端 peerID 一个共享桶 + 周期 progressive filling ----------

const mib = 1 << 20

var totalR = 200.0 * mib // 中继总容量 200 MiB/s

type bucket struct {
	lim    *rate.Limiter
	cur    atomic.Int64  // 本周期已读字节
	waited atomic.Bool   // 本周期是否发生过令牌等待（= 被限速，需求≥当前配额）
	demand float64       // 需求估计（字节/秒）
	share  float64
}

var reg = struct {
	sync.Mutex
	m map[peer.ID]*bucket
}{m: map[peer.ID]*bucket{}}

func bucketFor(p peer.ID) *bucket {
	reg.Lock()
	defer reg.Unlock()
	b := reg.m[p]
	if b == nil {
		b = &bucket{lim: rate.NewLimiter(rate.Limit(totalR), 256<<10)}
		b.share = totalR
		reg.m[p] = b
	}
	return b
}

func allocator() {
	tick := time.NewTicker(200 * time.Millisecond)
	for range tick.C {
		reg.Lock()
		type ent struct {
			p peer.ID
			b *bucket
		}
		var es []ent
		for p, b := range reg.m {
			if b.waited.Swap(false) {
				b.demand = totalR // 被限速过 → 需求视为不封顶，交给 progressive filling 均分
			} else {
				d := float64(b.cur.Swap(0)) / 0.2
				b.demand = 0.5*b.demand + 0.5*d // 未被限速 → 观测速率即需求，EWMA 平滑
			}
			es = append(es, ent{p, b})
		}
		// progressive filling：需求从小到大冻结
		sort.Slice(es, func(i, j int) bool { return es[i].b.demand < es[j].b.demand })
		remain := totalR
		left := len(es)
		for _, e := range es {
			share := remain / float64(left)
			if e.b.demand < share {
				e.b.share = e.b.demand // 需求小的冻结在需求
			} else {
				e.b.share = share
			}
			remain -= e.b.share
			left--
			e.b.lim.SetLimit(rate.Limit(e.b.share))
		}
		reg.Unlock()
	}
}

// ---------- transport 装饰器 ----------

type limStream struct {
	network.MuxedStream
	b *bucket
}

func (s *limStream) Read(p []byte) (int, error) {
	if len(p) > 128<<10 {
		p = p[:128<<10]
	}
	rsv := s.b.lim.ReserveN(time.Now(), len(p))
	if !rsv.OK() {
		return 0, fmt.Errorf("rate reserve failed")
	}
	if d := rsv.Delay(); d > 0 {
		s.b.waited.Store(true)
		time.Sleep(d)
	}
	n, err := s.MuxedStream.Read(p)
	s.b.cur.Add(int64(n))
	return n, err
}

type shapedConn struct {
	transport.CapableConn
}

func wrapStream(s network.MuxedStream, err error, p peer.ID) (network.MuxedStream, error) {
	if err != nil {
		return nil, err
	}
	return &limStream{MuxedStream: s, b: bucketFor(p)}, nil
}

func (c *shapedConn) AcceptStream() (network.MuxedStream, error) {
	s, err := c.CapableConn.AcceptStream()
	return wrapStream(s, err, c.RemotePeer())
}

func (c *shapedConn) OpenStream(ctx context.Context) (network.MuxedStream, error) {
	s, err := c.CapableConn.OpenStream(ctx)
	return wrapStream(s, err, c.RemotePeer())
}

type shapingListener struct {
	transport.Listener
}

func (l *shapingListener) Accept() (transport.CapableConn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &shapedConn{CapableConn: c}, nil
}

type shapingTpt struct {
	transport.Transport
}

func (t *shapingTpt) Listen(a ma.Multiaddr) (transport.Listener, error) {
	l, err := t.Transport.Listen(a)
	if err != nil {
		return nil, err
	}
	return &shapingListener{Listener: l}, nil
}

func (t *shapingTpt) Dial(ctx context.Context, a ma.Multiaddr, p peer.ID) (transport.CapableConn, error) {
	c, err := t.Transport.Dial(ctx, a, p)
	if err != nil {
		return nil, err
	}
	return &shapedConn{CapableConn: c}, nil
}

func newShapingTCP(u transport.Upgrader, rcmgr network.ResourceManager, shared *tcpreuse.ConnMgr) (transport.Transport, error) {
	inner, err := tcp.NewTCPTransport(u, rcmgr, shared)
	if err != nil {
		return nil, err
	}
	return &shapingTpt{Transport: inner}, nil
}

// ---------- 测试拓扑 ----------

const protoID = "/proto/shaping/1.0.0"

var recv = struct {
	sync.Mutex
	m map[peer.ID]*atomic.Int64
}{m: map[peer.ID]*atomic.Int64{}}

func counterFor(p peer.ID) *atomic.Int64 {
	recv.Lock()
	defer recv.Unlock()
	c := recv.m[p]
	if c == nil {
		c = &atomic.Int64{}
		recv.m[p] = c
	}
	return c
}

func main() {
	ctx := context.Background()
	go allocator()

	// 中继 R：装饰器传输 + 原生 relay 服务
	hostR, err := libp2p.New(
		libp2p.NoTransports,
		libp2p.Transport(newShapingTCP),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	must(err)
	defer hostR.Close()
	_, err = relay.New(hostR, relay.WithInfiniteLimits())
	must(err)
	fmt.Println("R:", hostR.ID(), hostR.Addrs())

	// 目的端 B：reservation 到 R，收流计数
	hostB, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	must(err)
	defer hostB.Close()
	hostB.SetStreamHandler(protoID, func(s network.Stream) {
		c := counterFor(s.Conn().RemotePeer())
		buf := make([]byte, 64<<10)
		for {
			n, err := s.Read(buf)
			c.Add(int64(n))
			if err != nil {
				return
			}
		}
	})
	rsv, err := relayclient.Reserve(ctx, hostB, peer.AddrInfo{ID: hostR.ID(), Addrs: hostR.Addrs()})
	must(err)
	fmt.Println("B reserved on R, expiration:", rsv.Expiration)

	// B 的中继地址：<R-addr>/p2p/<R>/p2p-circuit/p2p/<B>
	var circuitAddr ma.Multiaddr
	for _, a := range hostR.Addrs() {
		circuitAddr = a.Encapsulate(ma.StringCast("/p2p/" + hostR.ID().String() + "/p2p-circuit/p2p/" + hostB.ID().String()))
	}
	fmt.Println("circuit addr:", circuitAddr)

	// 两个客户端
	hostA1 := mustHost()
	hostA2 := mustHost()
	for _, h := range []host.Host{hostA1, hostA2} {
		h.Peerstore().AddAddr(hostB.ID(), circuitAddr, time.Hour)
	}

	// B 侧按 500ms 窗口打印每 peer 接收速率
	stopPrint := make(chan struct{})
	go func() {
		prev := map[peer.ID]int64{}
		t := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-stopPrint:
				return
			case <-t.C:
				recv.Lock()
				line := "  "
				for p, c := range recv.m {
					cur := c.Load()
					line += fmt.Sprintf("%s…: %.0f MiB/s  ", p.String()[len(p.String())-6:], float64(cur-prev[p])/(0.5*mib))
					prev[p] = cur
				}
				recv.Unlock()
				fmt.Println(line)
			}
		}
	}()

	// Phase 1：仅 A1 打流 4s —— 预期吃满 ~200 MiB/s
	fmt.Println("\n=== Phase 1: 仅 A1 打流 4s（单用户应吃满 R=200MiB/s）===")
	stop1 := blast(ctx, hostA1, hostB.ID())
	time.Sleep(4 * time.Second)
	stop1()
	time.Sleep(300 * time.Millisecond)

	// Phase 2：A1+A2 同时打流 5s —— 预期收敛到各 ~100 MiB/s
	fmt.Println("\n=== Phase 2: A1+A2 同时打流 5s（应收敛各 ~100MiB/s）===")
	s1 := blast(ctx, hostA1, hostB.ID())
	s2 := blast(ctx, hostA2, hostB.ID())
	time.Sleep(5 * time.Second)
	s1()
	s2()
	close(stopPrint)
	time.Sleep(300 * time.Millisecond)
	fmt.Println("\ndone")
}

func blast(ctx context.Context, h host.Host, dst peer.ID) func() {
	st, err := h.NewStream(ctx, dst, protoID)
	must(err)
	stop := make(chan struct{})
	var once sync.Once
	go func() {
		buf := make([]byte, 64<<10)
		for {
			select {
			case <-stop:
				st.Close()
				return
			default:
				if _, err := st.Write(buf); err != nil {
					return
				}
			}
		}
	}()
	return func() { once.Do(func() { close(stop) }) }
}

func mustHost() host.Host {
	h, err := libp2p.New()
	must(err)
	return h
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		os.Exit(1)
	}
}
