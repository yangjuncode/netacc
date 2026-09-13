package netacc_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	ma "github.com/multiformats/go-multiaddr"

	"github.com/yangjuncode/netacc"
)

// TestPolicyHookReceivesSnapshot 验收：WithPolicy 替换策略钩子后，
// 每条聚合流周期把观测快照喂给自定义 Policy；快照字段合理。
func TestPolicyHookReceivesSnapshot(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	var calls atomic.Int64
	var lastSnap atomic.Value
	pol := netacc.PolicyFunc(func(snap netacc.PolicySnapshot) netacc.PolicyDecision {
		calls.Add(1)
		lastSnap.Store(snap)
		return netacc.PolicyDecision{} // 不动
	})
	agga, aggb := netacc.New(ha, netacc.WithPolicy(pol)), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	eventually(t, "自定义策略被周期调用", func() bool { return calls.Load() >= 3 })
	snap := lastSnap.Load().(netacc.PolicySnapshot)
	if snap.Peer != hb.ID() || snap.StreamID != sa.ID() {
		t.Fatalf("快照标识不符: peer=%v id=%x", snap.Peer, snap.StreamID)
	}
	if len(snap.Paths) != 1 {
		t.Fatalf("快照应有 1 条路径，得到 %+v", snap.Paths)
	}
	// peerstore 有 hb 地址 → 候选非空且剔除中继形态
	if len(snap.CandidateAddrs) == 0 {
		t.Fatal("候选地址集不应为空（peerstore 有对端地址）")
	}
	for _, a := range snap.CandidateAddrs {
		if netacc.PathTransportOf(a) == netacc.TransportRelay {
			t.Fatalf("候选地址不应含中继形态: %s", a)
		}
	}
}

// TestPolicyAutoAddStallE2E 验收（host 级）：对端应用停滞（不 Read）
// 使发送端窗口顶满、待发积压——默认策略自动经 peerstore 地址直拨
// 补出新路径（AutoAdded 标记），恢复读取后全量数据到达。
func TestPolicyAutoAddStallE2E(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb) // 默认自动策略
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	const total = 3 << 20 // > 窗口+发送缓冲，确保积压持续
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(bytes.Repeat([]byte("s"), total))
		wdone <- err
	}()

	// sb 不读：窗口顶满 → 积压 → 默认策略补路径
	eventually(t, "默认策略自动补出路径", func() bool { return len(sa.Paths()) >= 2 })
	if n := len(sa.Paths()); n > 4 {
		t.Fatalf("自动加路径应受上限约束，得到 %d 条", n)
	}
	var autoSeen bool
	for _, pi := range sa.Paths() {
		if pi.AutoAdded {
			autoSeen = true
			if pi.Transport != netacc.TransportTCP {
				t.Fatalf("peerstore 仅 TCP 地址，自动补挂应为 TCP，得到 %s", pi.Transport)
			}
		}
	}
	if !autoSeen {
		t.Fatal("应存在 AutoAdded 路径")
	}
	eventually(t, "sb 侧同步看到补挂路径", func() bool { return len(sb.Paths()) >= 2 })

	// 恢复读取 → 全量数据到达（吞吐回升）
	got := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(io.Discard, io.LimitReader(sb, int64(total)))
		got <- n
	}()
	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("Write 失败: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("恢复读取后 Write 未完成")
	}
	if n := <-got; n != int64(total) {
		t.Fatalf("数据未完整到达: %d/%d", n, total)
	}
}

// TestPolicyDisabledNoAutoAdd 验收：WithPolicy(nil) 关闭自动化——
// 同样的积压场景下不做任何自动补挂，路径数保持 1；手动口不受影响。
func TestPolicyDisabledNoAutoAdd(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha, netacc.WithPolicy(nil)), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(bytes.Repeat([]byte("n"), 3<<20))
		wdone <- err
	}()
	// 等待时长明显超过触发窗口（默认 ~0.7s）：保持 1 条路径
	time.Sleep(2 * time.Second)
	if n := len(sa.Paths()); n != 1 {
		t.Fatalf("关闭自动化后不应补挂路径，得到 %d 条", n)
	}

	// 手动口仍可用
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, addrWith(t, hb, "/tcp/"))); err != nil {
		t.Fatalf("关闭策略后手动 AddPath 失败: %v", err)
	}
	eventually(t, "手动加路径生效", func() bool { return len(sa.Paths()) == 2 })
	go func() { _, _ = io.Copy(io.Discard, sb) }()
	if err := <-wdone; err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
}

// TestStreamPolicyOverride 验收：OpenStream 逐调用覆盖策略——
// WithStreamPolicy 注入的自定义策略生效（决策被执行补出路径），
// 未覆盖的流仍用 Aggregator 默认。
func TestStreamPolicyOverride(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	addAddr := mustMultiaddr(t, addrWith(t, hb, "/tcp/"))
	// 自定义策略：首拍即要求补挂给定地址（借 it 验证钩子被替换并执行）
	var saw atomic.Int64
	pol := netacc.PolicyFunc(func(snap netacc.PolicySnapshot) netacc.PolicyDecision {
		saw.Add(1)
		return netacc.PolicyDecision{Add: []ma.Multiaddr{addAddr}}
	})
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	ctx := context.Background()
	ach := make(chan *netacc.Stream, 1)
	go func() {
		if s, err := aggb.Accept(ctx); err == nil {
			ach <- s
		}
	}()
	sa, err := agga.OpenStream(ctx, hb.ID(), netacc.WithStreamPolicy(pol))
	if err != nil {
		t.Fatalf("OpenStream 失败: %v", err)
	}
	defer sa.Close()
	select {
	case sb := <-ach:
		defer sb.Close()
	case <-time.After(15 * time.Second):
		t.Fatal("Accept 超时")
	}
	eventually(t, "逐调用自定义策略被执行", func() bool {
		return saw.Load() > 0 && len(sa.Paths()) >= 2
	})
}
