package netacc_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
)

// openPairOpt 同 openPair，但 OpenStream 带逐调用选项。
func openPairOpt(t *testing.T, agga *netacc.Aggregator, aggb *netacc.Aggregator, hbID peer.ID, opts ...netacc.OpenOption) (sa, sb *netacc.Stream) {
	t.Helper()
	ctx := context.Background()
	type res struct {
		s   *netacc.Stream
		err error
	}
	ach := make(chan res, 1)
	go func() {
		s, err := aggb.Accept(ctx)
		ach <- res{s, err}
	}()
	sa, err := agga.OpenStream(ctx, hbID, opts...)
	if err != nil {
		t.Fatalf("OpenStream 失败: %v", err)
	}
	r := <-ach
	if r.err != nil {
		t.Fatalf("Accept 失败: %v", r.err)
	}
	return sa, r.s
}

// TestOptionMinPathsDefault 验收标准 3（构造默认）：
// New(..., WithMinPaths(2)) 后 OpenStream 不带选项也补挂到 2 条。
func TestOptionMinPathsDefault(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha, netacc.WithMinPaths(2)), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb) // 无逐调用选项 → 用构造默认
	defer sa.Close()
	defer sb.Close()
	if n := len(sa.Paths()); n < 2 {
		t.Fatalf("构造默认 WithMinPaths(2) 应补挂到 ≥2 条，得到 %d", n)
	}
}

// TestOptionMinPathsOverride 验收标准 3（逐调用覆盖）：
// 构造默认 WithMinPaths(3)，OpenStream(WithMinPaths(1)) 应覆盖成 1
// （若默认生效则补挂到 3，覆盖生效则保持 1）。
func TestOptionMinPathsOverride(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha, netacc.WithMinPaths(3)), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPairOpt(t, agga, aggb, hb.ID(), netacc.WithMinPaths(1))
	defer sa.Close()
	defer sb.Close()
	if n := len(sa.Paths()); n != 1 {
		t.Fatalf("逐调用 WithMinPaths(1) 应覆盖构造默认 3，得到 %d 条路径", n)
	}
	// 反向覆盖：构造默认 1（默认），逐调用提到 2
	ha2, hb2 := newTestHost(t), newTestHost(t)
	agga2, aggb2 := netacc.New(ha2), netacc.New(hb2)
	defer agga2.Close()
	defer aggb2.Close()
	connectHosts(t, ha2, hb2)
	sa2, sb2 := openPairOpt(t, agga2, aggb2, hb2.ID(), netacc.WithMinPaths(2))
	defer sa2.Close()
	defer sb2.Close()
	if n := len(sa2.Paths()); n < 2 {
		t.Fatalf("逐调用 WithMinPaths(2) 应补挂到 ≥2，得到 %d", n)
	}
}

// TestOptionMinPathsError 验收标准 4：peerstore 无对端地址可补挂时，
// WithMinPaths(2) 的 OpenStream 返回 errors.Is(ErrMinPaths) 的错误；
// 且建了一半的聚合流已被关闭（路由表不留残）。
func TestOptionMinPathsError(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ach := make(chan error, 1)
	go func() {
		s, err := aggb.Accept(ctx)
		if err == nil {
			s.Close()
		}
		ach <- err
	}()

	// 清掉 peerstore 里的对端地址：握手走既有 swarm 连接没问题，
	// 但 attachMinPaths 取不到任何可直拨地址 → 门槛无法满足
	ha.Peerstore().ClearAddrs(hb.ID())
	sa, err := agga.OpenStream(ctx, hb.ID(), netacc.WithMinPaths(2))
	if err == nil {
		sa.Close()
		t.Fatal("无地址可补挂时 WithMinPaths(2) 应失败")
	}
	if !errors.Is(err, netacc.ErrMinPaths) {
		t.Fatalf("错误应可用 errors.Is(err, ErrMinPaths) 判定，得到: %v", err)
	}
	if err := <-ach; err != nil {
		t.Fatalf("Accept 应成功（握手本身没问题，失败在建后补挂）: %v", err)
	}
}

// TestOptionHandshakeRejected 验收：对端未跑本协议时握手失败，
// 错误可用 errors.Is(err, ErrHandshake) 判定（「对端拒绝」语义化）。
func TestOptionHandshakeRejected(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t) // hb 上无 Aggregator
	agga := netacc.New(ha)
	defer agga.Close()
	connectHosts(t, ha, hb)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, err := agga.OpenStream(ctx, hb.ID())
	if err == nil {
		s.Close()
		t.Fatal("对端未跑 /netacc/agg/1.0.0，OpenStream 应报错")
	}
	if !errors.Is(err, netacc.ErrHandshake) {
		t.Fatalf("握手失败应可用 errors.Is(err, ErrHandshake) 判定，得到: %v", err)
	}
}

// TestOptionRemovePathErrors 验收：RemovePath 的错误语义——
// 未知 path_id → ErrUnknownPath；摘除最后一条 → ErrLastPath；
// 已关闭的流 → net.ErrClosed（标准语义）。
func TestOptionRemovePathErrors(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sb.Close()

	if err := sa.RemovePath(99); !errors.Is(err, netacc.ErrUnknownPath) {
		t.Fatalf("未知 path_id 应返回 ErrUnknownPath，得到: %v", err)
	}
	if err := sa.RemovePath(0); !errors.Is(err, netacc.ErrLastPath) {
		t.Fatalf("摘除最后一条路径应返回 ErrLastPath，得到: %v", err)
	}
	if err := sa.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sa.RemovePath(0); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("已关闭流上 RemovePath 应返回 net.ErrClosed，得到: %v", err)
	}
}

// TestOptionClosedAggregator 验收：Aggregator.Close 后
// OpenStream 返回 ErrClosed。
func TestOptionClosedAggregator(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga := netacc.New(ha)
	connectHosts(t, ha, hb)
	if err := agga.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := agga.OpenStream(context.Background(), hb.ID())
	if !errors.Is(err, netacc.ErrClosed) {
		t.Fatalf("Close 后 OpenStream 应返回 ErrClosed，得到: %v", err)
	}
}

// TestOptionReorderBufferOverride 验收：WithReorderBuffer 同样支持
// 逐调用覆盖——OpenStream 传入的上下界只作用于本条流。
func TestOptionReorderBufferOverride(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPairOpt(t, agga, aggb, hb.ID(), netacc.WithReorderBuffer(4<<10, 8<<10))
	defer sa.Close()
	defer sb.Close()
	// 初始重排缓冲容量 = minBuf（无任何指标时恒取下界，规格 §6）
	if st := sa.Stats(); st.RecvBufCap != 4<<10 {
		t.Fatalf("逐调用 WithReorderBuffer 应把初始容量压到 4KiB，得到 %d", st.RecvBufCap)
	}
	// 构造默认下界为 1MiB（WithReorderBuffer 文档值）
	if st := sb.Stats(); st.RecvBufCap != 1<<20 {
		t.Fatalf("对端流不受本侧 OpenOption 影响，容量应为构造默认 1MiB，得到 %d", st.RecvBufCap)
	}
}
