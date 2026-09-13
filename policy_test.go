package netacc

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

// mustMA 解析 multiaddr 字符串（内部测试版）。
func mustMA(s string) ma.Multiaddr {
	m, err := ma.NewMultiaddr(s)
	if err != nil {
		panic(err)
	}
	return m
}

// testStreamID 生成一个确定性 agg_stream_id（首字节区分各测试）。
func testStreamID(b byte) [16]byte {
	var id [16]byte
	id[0] = b
	return id
}

// pipePolicyStream 建一对未启动的管道聚合流（ca 给 sa、cb 给 sb，
// 可传入包装过的 conn 注入受限信号），便于在 start 前注入策略
// seam（候选地址 / 拨号动作）。
func pipePolicyStream(t *testing.T, cfg streamConfig, ca, cb pathConn) (sa, sb *Stream) {
	t.Helper()
	id := testStreamID(24)
	return newStreamFull(id, ca, cfg, nil, "", true),
		newStreamFull(id, cb, cfg, nil, "", false)
}

// policyCfg 返回开启默认策略、快速节拍的测试配置。
func policyCfg(bufCap int) streamConfig {
	cfg := testCfg(bufCap)
	cfg.telemetryInterval = 20 * time.Millisecond
	cfg.policy = DefaultPolicy()
	cfg.policyCooldown = 50 * time.Millisecond
	return cfg
}

// ---------- 默认策略 Decide 纯函数单测 ----------

// snapPath 构造一条 PolicyPath 快照项。
func snapPath(id uint64, rate float64, auto bool, lowFor time.Duration) PolicyPath {
	return PolicyPath{
		PathInfo:    PathInfo{ID: id, EstRate: rate, AutoAdded: auto},
		LowShareFor: lowFor,
	}
}

// TestDefaultPolicyAddDecision 验收：积压持续超阈值 + 有候选 →
// 按偏好序给出前 policyAddTryN 个地址；任一前置条件不满足则不动。
func TestDefaultPolicyAddDecision(t *testing.T) {
	pol := DefaultPolicy()
	now := time.Now()
	addrs := []ma.Multiaddr{
		mustMA("/ip4/1.2.3.4/tcp/4001"),
		mustMA("/ip4/1.2.3.4/tcp/4002/ws"),
		mustMA("/ip4/1.2.3.4/udp/4003/quic-v1"),
		mustMA("/ip4/1.2.3.4/udp/4004/quic-v1"),
	}
	base := PolicySnapshot{
		Now:            now,
		Paths:          []PolicyPath{snapPath(0, 1<<20, false, 0)},
		BacklogFor:     policyAddMinBacklog,
		CandidateAddrs: addrs,
	}
	dec := pol.Decide(base)
	if len(dec.Add) != policyAddTryN {
		t.Fatalf("应按序给出前 %d 个候选，得到 %v", policyAddTryN, dec.Add)
	}
	for i := range dec.Add {
		if !dec.Add[i].Equal(addrs[i]) {
			t.Fatalf("候选顺序被打乱: %v", dec.Add)
		}
	}
	// 无积压 → 不补
	d := pol.Decide(PolicySnapshot{Now: now, Paths: base.Paths, CandidateAddrs: addrs})
	if len(d.Add) != 0 {
		t.Fatal("无积压不应补路径")
	}
	// 积压不足阈值 → 不补
	s := base
	s.BacklogFor = policyAddMinBacklog - time.Millisecond
	if d := pol.Decide(s); len(d.Add) != 0 {
		t.Fatal("积压未持续够阈值不应补路径")
	}
	// 冷却/退避期内 → 不补
	s = base
	s.NextAutoAddAt = now.Add(time.Second)
	if d := pol.Decide(s); len(d.Add) != 0 {
		t.Fatal("冷却期内不应补路径")
	}
	// 路径数到顶 → 不补
	s = base
	s.Paths = []PolicyPath{snapPath(0, 1<<20, false, 0), snapPath(2, 1<<20, true, 0),
		snapPath(4, 1<<20, true, 0), snapPath(6, 1<<20, true, 0)}
	if d := pol.Decide(s); len(d.Add) != 0 {
		t.Fatal("路径数到顶不应再补")
	}
	// 无候选 → 不补
	s = base
	s.CandidateAddrs = nil
	if d := pol.Decide(s); len(d.Add) != 0 {
		t.Fatal("无候选地址不应补路径")
	}
}

// TestDefaultPolicyRemoveDecision 验收保守摘除：只摘自动补挂且份额
// 长期垫底的路径，且摘除后至少保留 policyRemoveKeepPaths 条；
// 手动路径不摘、有积压不摘。
func TestDefaultPolicyRemoveDecision(t *testing.T) {
	pol := DefaultPolicy()
	now := time.Now()
	lowAuto := snapPath(4, 0, true, policyRemoveMinLowFor)
	paths := []PolicyPath{
		snapPath(0, 1<<20, false, 0), // 首路径，健康
		snapPath(2, 1<<20, true, 0),  // 自动补挂但活跃
		lowAuto,                      // 自动补挂 + 长期垫底 → 应被摘
	}
	dec := pol.Decide(PolicySnapshot{Now: now, Paths: paths})
	if len(dec.Remove) != 1 || dec.Remove[0] != 4 {
		t.Fatalf("应摘除垫底自动路径 4，得到 %v", dec.Remove)
	}
	// 垫底但非自动补挂（手动加的路径）→ 不摘
	manual := snapPath(4, 0, false, policyRemoveMinLowFor)
	d := pol.Decide(PolicySnapshot{Now: now, Paths: []PolicyPath{paths[0], paths[1], manual}})
	if len(d.Remove) != 0 {
		t.Fatal("手动路径即使垫底也不应被默认策略摘除")
	}
	// 垫底时长不足 → 不摘
	fresh := snapPath(4, 0, true, policyRemoveMinLowFor-time.Millisecond)
	d = pol.Decide(PolicySnapshot{Now: now, Paths: []PolicyPath{paths[0], paths[1], fresh}})
	if len(d.Remove) != 0 {
		t.Fatal("垫底时长不足不应摘除")
	}
	// 摘除后会跌破保留数（3→2 允许，2→1 不允许）
	d = pol.Decide(PolicySnapshot{Now: now, Paths: []PolicyPath{paths[0], lowAuto}})
	if len(d.Remove) != 0 {
		t.Fatal("摘除后只剩 1 条时不应摘除")
	}
	// 有积压（供给不足）时不摘
	d = pol.Decide(PolicySnapshot{Now: now, Paths: paths, BacklogFor: time.Second})
	if len(d.Remove) != 0 {
		t.Fatal("积压期不应摘路径")
	}
}

// ---------- 流级执行层测试（pipe + seam） ----------

// TestPolicyAutoAddPipe 验收：单路径受限（限速注入）+ 持续写 →
// 默认策略自动补挂路径，路径数+1、新路径被标记 AutoAdded 并分担流量。
// 拨号动作经 policyDialFn seam 注入管道（替代真实 transport 直拨）。
func TestPolicyAutoAddPipe(t *testing.T) {
	cfg := policyCfg(256 << 10)
	c0a, c0b := net.Pipe()
	// path0 限速 ~200KiB/s：单独供不上写入需求 → 积压持续
	sa, sb := pipePolicyStream(t, cfg,
		newRateDelayConn(c0a, 200<<10, 20*time.Millisecond, 8), c0b)
	defer sa.Close()
	defer sb.Close()

	var dials atomic.Int64
	var secondWritten atomic.Int64
	sa.policyAddrsFn = func() []ma.Multiaddr {
		return []ma.Multiaddr{mustMA("/ip4/127.0.0.1/tcp/9999")}
	}
	sa.policyDialFn = func(ctx context.Context, addr ma.Multiaddr) (uint64, error) {
		dials.Add(1)
		sa.mu.Lock()
		pid := sa.nextPathID
		sa.nextPathID += 2
		sa.mu.Unlock()
		c2a, c2b := net.Pipe()
		nc := countConn{c2a, &secondWritten, &atomic.Int64{}}
		if err := sa.attachPath(&path{id: pid, conn: nc, dialed: true}, nil); err != nil {
			_ = c2a.Close()
			_ = c2b.Close()
			return 0, err
		}
		if err := sb.attachPath(&path{id: pid, conn: c2b}, nil); err != nil {
			return 0, err
		}
		return pid, nil
	}
	sa.start()
	sb.start()

	const total = 1 << 20 // 1MiB：单路径 ~5s，补路径后远快于此
	rdone := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(io.Discard, io.LimitReader(sb, int64(total)))
		rdone <- n
	}()
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(bytes.Repeat([]byte("p"), total))
		wdone <- err
	}()

	eventuallyCond(t, "自动策略拨号补挂", func() bool { return dials.Load() >= 1 })
	eventuallyCond(t, "sa 路径数收敛到 2", func() bool { return len(sa.Paths()) == 2 })
	var autoSeen bool
	for _, pi := range sa.Paths() {
		if pi.AutoAdded {
			autoSeen = true
		}
	}
	if !autoSeen {
		t.Fatal("自动补挂的路径应标记 AutoAdded")
	}
	// 吞吐回升：全部数据应在远小于单路径所需的时间内送达
	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("Write 失败: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("补路径后 Write 未在时限内完成（吞吐未回升）")
	}
	if n := <-rdone; n != int64(total) {
		t.Fatalf("数据未完整到达: %d/%d", n, total)
	}
	if secondWritten.Load() == 0 {
		t.Fatal("新补挂的路径未承载任何数据")
	}
}

// TestPolicyDialBackoff 验收：自动拨号连续失败时按指数退避——
// 尝试次数有界且间隔递增，不会在积压期无限重试。
func TestPolicyDialBackoff(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	cfg := policyCfg(8 << 10)
	sa := newStreamFull(testStreamID(25), c1, cfg, nil, "", true)
	defer sa.Close()

	var mu sync.Mutex
	var attempts []time.Time
	sa.policyAddrsFn = func() []ma.Multiaddr {
		return []ma.Multiaddr{mustMA("/ip4/127.0.0.1/tcp/1")}
	}
	sa.policyDialFn = func(ctx context.Context, addr ma.Multiaddr) (uint64, error) {
		mu.Lock()
		attempts = append(attempts, time.Now())
		mu.Unlock()
		return 0, context.DeadlineExceeded // 恒失败
	}
	sa.start()

	sink := frameSink(t, c2)
	recvFrame(t, sink) // 初始窗口通告
	// 对端通告 8KiB 窗口后不再回 ACK → 在途顶满、发送端积压持续
	if _, err := c2.Write(appendAckFrame(nil, 0, 0, 0, 8<<10, 1, nil)); err != nil {
		t.Fatal(err)
	}
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(bytes.Repeat([]byte("b"), 64<<10))
		wdone <- err
	}()

	time.Sleep(600 * time.Millisecond)
	mu.Lock()
	n := len(attempts)
	ts := append([]time.Time(nil), attempts...)
	mu.Unlock()
	// 退避序列 50ms→100→200→400：600ms 内约 4 次；
	// 无退避（固定 50ms）会 ~12 次。上界 + 间隔递增双重断言。
	if n == 0 {
		t.Fatal("积压下应发生自动拨号尝试")
	}
	if n > 6 {
		t.Fatalf("退避失效：600ms 内尝试 %d 次过多", n)
	}
	for i := 2; i < n; i++ {
		prev := ts[i-1].Sub(ts[i-2])
		cur := ts[i].Sub(ts[i-1])
		if cur < prev {
			t.Fatalf("退避间隔应递增: 第%d次间隔 %v < 第%d次 %v", i, cur, i-1, prev)
		}
	}
	_ = sa.Close()
	<-wdone // Write 随 Close 返回错误，收掉 goroutine
}

// TestPolicyRemoveLowShare 验收：自动补挂且份额长期垫底的路径被
// 默认策略摘除（两侧同步），健康/手动路径不动。
func TestPolicyRemoveLowShare(t *testing.T) {
	cfg := policyCfg(16 << 10)
	c0a, c0b := net.Pipe()
	sa, sb := pipePolicyStream(t, cfg, c0a, c0b)
	defer sa.Close()
	defer sb.Close()
	sa.policyAddrsFn = func() []ma.Multiaddr { return nil }

	// 挂上 p2（健康）与 p4（自动补挂、垫底）两条管道路径
	c2a, c2b := net.Pipe()
	c4a, c4b := net.Pipe()
	if err := sa.attachPath(&path{id: 2, conn: c2a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sa.attachPath(&path{id: 4, conn: c4a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 4, conn: c4b}, nil); err != nil {
		t.Fatal(err)
	}
	sa.start()
	sb.start()
	eventuallyPaths(t, sa, 3)

	// 注入指标：p0/p2 健康（对端观测速率高），p4 垫底且已持续超阈
	now := time.Now()
	sa.mu.Lock()
	for _, p := range sa.paths {
		p.attachedAt = now.Add(-3 * policyPathWarmup)
	}
	sa.pathsByID[0].peerRate, sa.pathsByID[0].peerRateAt = float64(4<<20), now
	sa.pathsByID[2].peerRate, sa.pathsByID[2].peerRateAt = float64(4<<20), now
	p4 := sa.pathsByID[4]
	p4.auto = true
	p4.lowSince = now.Add(-2 * policyRemoveMinLowFor) // 已垫底很久
	sa.mu.Unlock()

	sa.runPolicy() // 同步执行一拍评估
	eventuallyPaths(t, sa, 2)
	eventuallyPaths(t, sb, 2) // PATH_DROP 带内同步
	for _, pi := range sa.Paths() {
		if pi.ID == 4 {
			t.Fatal("垫底自动路径应被摘除")
		}
	}
}

// TestPolicyCustomFuncExecuted 验收可插拔钩子：自定义 PolicyFunc
// 收到观测快照（字段合理），其 Add 决策被框架执行。
func TestPolicyCustomFuncExecuted(t *testing.T) {
	cfg := policyCfg(16 << 10)
	var calls atomic.Int64
	var lastSnap atomic.Value
	// 注意同一 Policy 实例被 sa/sb 两流共享（sb 侧无候选 seam）：
	// 只记录带候选地址的快照（即 sa 的），断言才有确定性。
	cfg.policy = PolicyFunc(func(snap PolicySnapshot) PolicyDecision {
		calls.Add(1)
		if len(snap.CandidateAddrs) > 0 {
			lastSnap.Store(snap)
			return PolicyDecision{Add: snap.CandidateAddrs[:1]}
		}
		return PolicyDecision{}
	})
	c0a, c0b := net.Pipe()
	sa, sb := pipePolicyStream(t, cfg, c0a, c0b)
	defer sa.Close()
	defer sb.Close()

	wantAddr := mustMA("/ip4/9.9.9.9/tcp/443")
	sa.policyAddrsFn = func() []ma.Multiaddr { return []ma.Multiaddr{wantAddr} }
	var gotAddr atomic.Value
	sa.policyDialFn = func(ctx context.Context, addr ma.Multiaddr) (uint64, error) {
		gotAddr.Store(addr.String())
		sa.mu.Lock()
		pid := sa.nextPathID
		sa.nextPathID += 2
		sa.mu.Unlock()
		c2a, c2b := net.Pipe()
		if err := sa.attachPath(&path{id: pid, conn: c2a, dialed: true}, nil); err != nil {
			return 0, err
		}
		if err := sb.attachPath(&path{id: pid, conn: c2b}, nil); err != nil {
			return 0, err
		}
		return pid, nil
	}
	sa.start()
	sb.start()

	eventuallyCond(t, "自定义策略被调用且见到候选", func() bool {
		return calls.Load() > 0 && lastSnap.Load() != nil
	})
	snap := lastSnap.Load().(PolicySnapshot)
	if len(snap.Paths) < 1 || snap.Paths[0].ID != 0 {
		t.Fatalf("快照路径集不符: %+v", snap.Paths)
	}
	if len(snap.CandidateAddrs) != 1 {
		t.Fatalf("快照候选地址不符: %+v", snap.CandidateAddrs)
	}
	eventuallyCond(t, "自定义决策被执行（拨号）", func() bool { return gotAddr.Load() != nil })
	if got := gotAddr.Load().(string); got != wantAddr.String() {
		t.Fatalf("拨号地址 %s 不是策略给出的 %s", got, wantAddr)
	}
	eventuallyCond(t, "sa 路径数收敛到 2", func() bool { return len(sa.Paths()) == 2 })
}

// TestPolicyNilDisables 验收：policy=nil 时积压也不触发自动行为。
func TestPolicyNilDisables(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	cfg := testCfg(8 << 10)
	cfg.telemetryInterval = 20 * time.Millisecond
	// cfg.policy 保持 nil
	sa := newStreamOnPath(testStreamID(26), c1, cfg)
	defer sa.Close()

	sink := frameSink(t, c2)
	recvFrame(t, sink)
	if _, err := c2.Write(appendAckFrame(nil, 0, 0, 0, 8<<10, 1, nil)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_, _ = sa.Write(bytes.Repeat([]byte("z"), 64<<10))
		close(done)
	}()
	time.Sleep(700 * time.Millisecond)
	if n := len(sa.Paths()); n != 1 {
		t.Fatalf("policy=nil 时路径数应保持 1，得到 %d", n)
	}
	_ = sa.Close()
	<-done
}

// ---------- chimera：首路径管道 + 真实 Aggregator 拨号 ----------

// chimeraPair 建一对「首路径为管道、agg 为真实 Aggregator」的聚合流：
// 首路径可用 rateDelayConn 等 double 注入受限信号，自动策略的补挂
// 走真实 transport 直拨（peerstore 取 hb 地址）。sa 为发起方
// （agg=agga/peer=hb），sb 为接收方（agg=aggb/peer=ha）。
func chimeraPair(t *testing.T, cfg streamConfig, ca, cb pathConn) (sa, sb *Stream) {
	t.Helper()
	ha, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("创建 ha 失败: %v", err)
	}
	hb, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("创建 hb 失败: %v", err)
	}
	t.Cleanup(func() { ha.Close(); hb.Close() })
	agga, aggb := New(ha), New(hb)
	t.Cleanup(func() { agga.Close(); aggb.Close() })
	ha.Peerstore().AddAddrs(hb.ID(), hb.Addrs(), peerstore.PermanentAddrTTL)

	sa = newStreamFull(testStreamID(27), ca, cfg, agga, hb.ID(), true)
	sb = newStreamFull(testStreamID(27), cb, cfg, aggb, ha.ID(), false)
	agga.register(sa)
	aggb.register(sb)
	sa.start()
	sb.start()
	return sa, sb
}

// TestPolicyAutoAddEndToEnd 验收标准 2 的主验证：path0 注入
// 「传输受限」信号（rateDelayConn 限速，等效 netem 整类降速），
// 默认策略经 peerstore 地址 + 真实 transport 直拨补出 TCP 路径，
// 聚合吞吐回升（全量数据在单路径不可能的时限内送达）。
func TestPolicyAutoAddEndToEnd(t *testing.T) {
	cfg := policyCfg(256 << 10)
	c0a, c0b := net.Pipe()
	sa, sb := chimeraPair(t, cfg,
		newRateDelayConn(c0a, 200<<10, 20*time.Millisecond, 8), c0b)
	defer sa.Close()
	defer sb.Close()

	const total = 2 << 20 // 2MiB：单走限速路径 ~10s，补出 TCP 后应远快
	rdone := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(io.Discard, io.LimitReader(sb, int64(total)))
		rdone <- n
	}()
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(bytes.Repeat([]byte("e"), total))
		wdone <- err
	}()

	eventuallyCond(t, "自动补出第二条路径", func() bool { return len(sa.Paths()) >= 2 })
	var added *PathInfo
	for _, pi := range sa.Paths() {
		if pi.AutoAdded {
			pi := pi
			added = &pi
		}
	}
	if added == nil {
		t.Fatal("新路径应标记 AutoAdded")
	}
	if added.Transport != TransportTCP {
		t.Fatalf("peerstore 仅 TCP 地址，补挂路径应为 TCP，得到 %s", added.Transport)
	}
	eventuallyCond(t, "sb 侧同步看到新路径", func() bool { return len(sb.Paths()) >= 2 })

	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("Write 失败: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("补路径后 2MiB 未在 8s 内写完（单路径需 ~10s，吞吐未回升）")
	}
	if n := <-rdone; n != int64(total) {
		t.Fatalf("数据未完整到达: %d/%d", n, total)
	}
}

// TestPolicyCandidateFiltering 验收候选地址集构造：peerstore 对端
// 地址经 §3.3 偏好排序、剔除中继形态，且未被在途路径占用的地址
// 排在前面（UDP 类受限时优先换传输/换地址，而非同址再拨）。
func TestPolicyCandidateFiltering(t *testing.T) {
	cfg := policyCfg(16 << 10)
	c0a, c0b := net.Pipe()
	sa, sb := chimeraPair(t, cfg, c0a, c0b)
	defer sa.Close()
	defer sb.Close()

	hbID := sa.peer
	// 往 peerstore 里追加 QUIC 与中继形态地址（peerstore 不校验
	// 可达性，只测候选构造逻辑）
	quicAddr := mustMA("/ip4/127.0.0.1/udp/9999/quic-v1")
	relayAddr := mustMA("/ip4/127.0.0.1/tcp/8888/p2p/" + hbID.String() + "/p2p-circuit")
	sa.agg.host.Peerstore().AddAddrs(hbID, []ma.Multiaddr{quicAddr, relayAddr},
		peerstore.PermanentAddrTTL)

	sa.mu.Lock()
	got := sa.policyCandidatesLocked()
	sa.mu.Unlock()
	if len(got) == 0 {
		t.Fatal("候选地址集不应为空")
	}
	for _, a := range got {
		if PathTransportOf(a) == TransportRelay {
			t.Fatalf("中继形态地址应被剔除: %s", a)
		}
	}
	// TCP（P0）应排在 QUIC（P1）之前
	var sawTCP, sawQUIC bool
	for i, a := range got {
		switch PathTransportOf(a) {
		case TransportQUIC:
			sawQUIC = true
			for _, prev := range got[:i] {
				if PathTransportOf(prev) == TransportTCP {
					sawTCP = true
				}
			}
		}
	}
	if !sawQUIC || !sawTCP {
		t.Fatalf("候选应先 TCP 后 QUIC: %v", got)
	}
	// path0 为管道（远端地址不匹配任何候选）→ 全部候选按未占用排前。
	// 在真实占用场景由 e2e 覆盖；此处验证排序稳定可读即可。
}

// TestPolicyManualCoexist 验收：自动补挂与手动 AddPath/RemovePath
// 共存不打架——自动路径可被手动摘除，手动加路径不受策略干扰。
func TestPolicyManualCoexist(t *testing.T) {
	cfg := policyCfg(256 << 10)
	c0a, c0b := net.Pipe()
	sa, sb := chimeraPair(t, cfg,
		newRateDelayConn(c0a, 200<<10, 20*time.Millisecond, 8), c0b)
	defer sa.Close()
	defer sb.Close()

	wdone := make(chan struct{})
	go func() {
		_, _ = sa.Write(bytes.Repeat([]byte("m"), 1<<20))
		close(wdone)
	}()
	rdone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(sb, 1<<20))
		close(rdone)
	}()

	eventuallyCond(t, "自动补挂路径", func() bool { return len(sa.Paths()) >= 2 })
	autoID := uint64(0)
	found := false
	for _, pi := range sa.Paths() {
		if pi.AutoAdded {
			autoID, found = pi.ID, true
		}
	}
	if !found {
		t.Fatal("应有自动补挂路径")
	}
	// 先等数据传输完成（积压清零、策略静默），再做手动操作——
	// 否则积压期策略可能继续补挂，与「收敛回 1 条」的断言竞态。
	select {
	case <-wdone:
	case <-time.After(8 * time.Second):
		t.Fatal("Write 未完成")
	}
	<-rdone
	n0 := len(sa.Paths())
	// 手动摘除自动补挂的路径
	if err := sa.RemovePath(autoID); err != nil {
		t.Fatalf("手动摘除自动路径失败: %v", err)
	}
	eventuallyCond(t, "摘除后路径数-1", func() bool { return len(sa.Paths()) == n0-1 })
	for _, pi := range sa.Paths() {
		if pi.ID == autoID {
			t.Fatal("被手动摘除的路径不应存在")
		}
	}
	// 手动加路径与自动策略并存（验证手动口不被干扰、path_id 不冲突）
	addrs := sa.agg.host.Peerstore().Addrs(sa.peer)
	if len(addrs) == 0 {
		t.Fatal("peerstore 应仍有对端地址")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, addrs[0]); err != nil {
		t.Fatalf("手动 AddPath 失败: %v", err)
	}
	eventuallyCond(t, "手动加路径生效", func() bool { return len(sa.Paths()) >= n0 })
}

// ---------- PING 节流验证（验收标准 4） ----------

// frameTypeConn 包装 conn 统计发出帧类型（本库每次 Write 恰为一帧，
// 首字节 varint 即类型——类型值均 <128，单字节可读）。
type frameTypeConn struct {
	net.Conn
	mu sync.Mutex
	n  map[frameType]int
}

func (c *frameTypeConn) Write(b []byte) (int, error) {
	if len(b) > 0 {
		c.mu.Lock()
		c.n[frameType(b[0])]++
		c.mu.Unlock()
	}
	return c.Conn.Write(b)
}

func (c *frameTypeConn) count(ft frameType) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[ft]
}

// TestPingThrottled 验收标准 4：稳态数据期 PING 不常态化占用
// 带宽——收端无 RTT 样本时只按 pingMinInterval 节流探测，拿到
// 应答样本后即停；全程 PING 帧数远小于 tick 数。
func TestPingThrottled(t *testing.T) {
	c1, c2 := net.Pipe()
	cfg := testCfg(64 << 10)
	cfg.telemetryInterval = 50 * time.Millisecond // 1.3s ≈ 26 拍
	cntA := &frameTypeConn{Conn: c1, n: map[frameType]int{}}
	cntB := &frameTypeConn{Conn: c2, n: map[frameType]int{}}
	sa := newStreamOnPath(testStreamID(28), cntA, cfg)
	sb := newStreamOnPath(testStreamID(28), cntB, cfg)
	defer sa.Close()
	defer sb.Close()

	stop := make(chan struct{})
	go func() {
		buf := bytes.Repeat([]byte("d"), 64<<10)
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := sa.Write(buf); err != nil {
					return
				}
			}
		}
	}()
	go func() { _, _ = io.Copy(io.Discard, sb) }()
	time.Sleep(1300 * time.Millisecond)
	close(stop)

	// 收端 b：无 RTT 样本 → 每 ≥500ms 探测一次，拿到应答即停 →
	// 全程 ~1-3 条；发送端 a：ACK 回显持续供样本 → 基本不探。
	if n := cntB.count(framePing); n > 5 {
		t.Fatalf("收端 PING 应被节流（≥500ms 一条），1.3s 内发了 %d 条", n)
	}
	if n := cntA.count(framePing); n > 3 {
		t.Fatalf("发送端健康期不应探测，发了 %d 条 PING", n)
	}
}
