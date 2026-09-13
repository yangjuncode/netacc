package netaccrelay

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
)

// 集成测试拓扑（移植自 prototype/shaping-transport）：
//
//	A1 ──┐                    ┌── 计数（按源 peer 统计收到字节）
//	     ├─→ R（本组件中继）──→ B（reservation 到 R，收流计数）
//	A2 ──┘                    └──
//
// A→B 数据在中继上走 A↔R 连接上的 hop 流，读侧被 A 的共享桶限速。
const itProto = "/netaccrelay/itest/1.0.0"

// TestFairSharingIntegration 端到端验证公平带宽分配：
// 单用户打流时独占总容量；第二用户加入后 ~1s 量级收敛到大致均分。
func TestFairSharingIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const mib = 1 << 20
	const total = 64 * mib // 中继总容量 R = 64 MiB/s

	// 先建两端与两个源端，拿到 peerID 后再按白名单起中继
	hostB := mustHost(t, "/ip4/127.0.0.1/tcp/0")
	hostA1 := mustHost(t, "/ip4/127.0.0.1/tcp/0")
	hostA2 := mustHost(t, "/ip4/127.0.0.1/tcp/0")

	alloc := NewAllocator(total, WithRecomputeInterval(100*time.Millisecond))
	hostR, err := libp2p.New(
		libp2p.NoTransports,
		TCPTransport(alloc),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	comp, err := New(hostR,
		WithAllocator(alloc),
		WithWhitelist(hostB.ID(), hostA1.ID(), hostA2.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer comp.Close()
	defer hostR.Close()
	t.Logf("R: %s %v", hostR.ID(), hostR.Addrs())

	// B 收流并按远端 peer 计数
	recv := &recvCounters{m: map[peer.ID]*atomic.Int64{}}
	hostB.SetStreamHandler(itProto, func(s network.Stream) {
		c := recv.forPeer(s.Conn().RemotePeer())
		buf := make([]byte, 64<<10)
		for {
			n, err := s.Read(buf)
			c.Add(int64(n))
			if err != nil {
				return
			}
		}
	})
	defer hostB.Close()

	// B 向 R 做 reservation（B 在白名单内 → AllowReserve 放行）
	rsv, err := relayclient.Reserve(ctx, hostB, peer.AddrInfo{ID: hostR.ID(), Addrs: hostR.Addrs()})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("B reserved on R, expiration: %v", rsv.Expiration)

	// B 的中继地址：<R-addr>/p2p/<R>/p2p-circuit/p2p/<B>
	var circuitAddr ma.Multiaddr
	for _, a := range hostR.Addrs() {
		circuitAddr = a.Encapsulate(ma.StringCast(
			"/p2p/" + hostR.ID().String() + "/p2p-circuit/p2p/" + hostB.ID().String()))
	}
	for _, h := range []host.Host{hostA1, hostA2} {
		h.Peerstore().AddAddr(hostB.ID(), circuitAddr, time.Hour)
		defer h.Close()
	}

	// Phase 1：仅 A1 打流 2.5s——单用户应吃掉大部分容量
	stop1 := blast(t, ctx, hostA1, hostB.ID())
	time.Sleep(2500 * time.Millisecond)
	before := recv.forPeer(hostA1.ID()).Load()
	time.Sleep(1 * time.Second)
	after := recv.forPeer(hostA1.ID()).Load()
	stop1()
	solo := float64(after-before) / 1e0 // 最近 1s 的速率（字节/秒）
	t.Logf("Phase 1: A1 单用户速率 %.1f MiB/s（R=%d MiB/s）", solo/mib, total/mib)
	if solo < 0.3*total {
		t.Fatalf("单用户应能占用大部分总容量，实测 %.1f MiB/s", solo/mib)
	}
	time.Sleep(500 * time.Millisecond)

	// Phase 2：A1+A2 同打——跳过前 ~1.5s 收敛期，量后 2s 看是否均分
	s1 := blast(t, ctx, hostA1, hostB.ID())
	s2 := blast(t, ctx, hostA2, hostB.ID())
	time.Sleep(1500 * time.Millisecond)
	c1 := recv.forPeer(hostA1.ID()).Load()
	c2 := recv.forPeer(hostA2.ID()).Load()
	time.Sleep(2 * time.Second)
	d1 := recv.forPeer(hostA1.ID()).Load() - c1
	d2 := recv.forPeer(hostA2.ID()).Load() - c2
	s1()
	s2()

	rate1 := float64(d1) / 2 / mib
	rate2 := float64(d2) / 2 / mib
	sum := rate1 + rate2
	t.Logf("Phase 2 收敛后：A1=%.1f MiB/s, A2=%.1f MiB/s（合计 %.1f，R=%d）",
		rate1, rate2, sum, total/mib)
	if sum <= 0 {
		t.Fatal("Phase 2 无流量")
	}
	for name, r := range map[string]float64{"A1": rate1, "A2": rate2} {
		frac := r / sum
		if frac < 0.35 || frac > 0.65 {
			t.Fatalf("%s 份额 %.2f 超出 [0.35, 0.65]，未收敛到均分（A1=%.1f A2=%.1f MiB/s）",
				name, frac, rate1, rate2)
		}
	}
}

// TestACLBlocksStranger 验证白名单 ACL：非白名单 peer 的 RESERVE 被拒绝。
func TestACLBlocksStranger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	hostB := mustHost(t, "/ip4/127.0.0.1/tcp/0")
	defer hostB.Close()
	stranger := mustHost(t, "/ip4/127.0.0.1/tcp/0")
	defer stranger.Close()

	alloc := NewAllocator(64 << 20)
	hostR, err := libp2p.New(
		libp2p.NoTransports,
		TCPTransport(alloc),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostR.Close()
	comp, err := New(hostR, WithAllocator(alloc), WithWhitelist(hostB.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer comp.Close()

	if _, err := relayclient.Reserve(ctx, hostB, peer.AddrInfo{ID: hostR.ID(), Addrs: hostR.Addrs()}); err != nil {
		t.Fatalf("白名单内 peer 的 RESERVE 应成功：%v", err)
	}
	if _, err := relayclient.Reserve(ctx, stranger, peer.AddrInfo{ID: hostR.ID(), Addrs: hostR.Addrs()}); err == nil {
		t.Fatal("非白名单 peer 的 RESERVE 应被 ACL 拒绝")
	}
}

// ---------- 测试辅助 ----------

type recvCounters struct {
	sync.Mutex
	m map[peer.ID]*atomic.Int64
}

func (r *recvCounters) forPeer(p peer.ID) *atomic.Int64 {
	r.Lock()
	defer r.Unlock()
	c := r.m[p]
	if c == nil {
		c = &atomic.Int64{}
		r.m[p] = c
	}
	return c
}

func mustHost(t *testing.T, addrs ...string) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings(addrs...))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// blast 经中继电路向 dst 开流并持续写满；返回停止函数。
func blast(t *testing.T, ctx context.Context, h host.Host, dst peer.ID) func() {
	t.Helper()
	st, err := h.NewStream(ctx, dst, itProto)
	if err != nil {
		t.Fatal(fmt.Errorf("开电路流失败: %w", err))
	}
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
