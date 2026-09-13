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

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc/internal/pb"
)

// ---------- 调度器单测（伪造指标断言选路决策） ----------

// metricPath 构造一条带伪造指标的路径供调度器单测。
// rate/srtt 为 0 表示该指标无样本（冷启动）。
func metricPath(id uint64, rate float64, srtt time.Duration, inflight int, now time.Time) *path {
	p := &path{id: id, inflight: inflight, attachedAt: now}
	if rate > 0 {
		p.estRate, p.hasRate, p.lastRateAt = rate, true, now
	}
	if srtt > 0 {
		p.srtt, p.minRTT, p.hasRTT, p.lastRTTAt = srtt, srtt, true, now
	}
	return p
}

// TestPickDataMinDrain 验收：最短排空时间优先——(inflight+len)/est_rate
// 最小者中选；快路径积压深到反超慢路径时改选慢路径（带宽比例分流）。
func TestPickDataMinDrain(t *testing.T) {
	now := time.Now()
	fast := metricPath(0, 8<<20, 10*time.Millisecond, 0, now)  // 8MiB/s
	slow := metricPath(2, 1<<20, 100*time.Millisecond, 0, now) // 1MiB/s
	sched := minDrainScheduler{}
	paths := []*path{fast, slow}

	if p := sched.pickData(paths, 64<<10, now); p != fast {
		t.Fatal("应选排空时间更小的快路径")
	}
	// 快路径积压 1MiB → drain≈(1MiB+64KiB)/8MiB≈135ms > 慢路径 64ms
	fast.inflight = 1 << 20
	if p := sched.pickData(paths, 64<<10, now); p != slow {
		t.Fatal("快路径积压反超后应选慢路径")
	}
	// 同分确定性打破：inflight 并列取 path_id 小者，避免抖动
	fast.inflight, slow.inflight = 0, 0
	fast.estRate, slow.estRate = 1<<20, 1<<20 // 等速率 → drain 只取决于 inflight
	if p := sched.pickData(paths, 64<<10, now); p != fast {
		t.Fatal("同分应取 path_id 小者")
	}
}

// TestPickDataInflightCap 验收：inflight ≥ k·est_rate·srtt 的路径本轮
// 跳过（慢路径堆积硬顶）；全部顶满返回 nil。
func TestPickDataInflightCap(t *testing.T) {
	now := time.Now()
	fast := metricPath(0, 8<<20, 10*time.Millisecond, 0, now)
	slow := metricPath(2, 1<<20, 100*time.Millisecond, 0, now)
	sched := minDrainScheduler{}

	// fast 的 drain 本来更小，但 inflight 顶满 → 必须跳过
	fast.inflight = int(fast.inflightCap(now))
	if p := sched.pickData([]*path{fast, slow}, 64<<10, now); p != slow {
		t.Fatal("inflight 顶满的路径应被跳过")
	}
	slow.inflight = int(slow.inflightCap(now))
	if p := sched.pickData([]*path{fast, slow}, 64<<10, now); p != nil {
		t.Fatal("全部路径顶满应返回 nil（本轮不发，等 ACK）")
	}
}

// TestPickDataColdStart 验收：est_rate 未知时按名义速率均等分摊——
// inflight 少者优先，自然在 cold 路径间轮转；cold 路径也受
// minInflightCap 硬顶约束。
func TestPickDataColdStart(t *testing.T) {
	now := time.Now()
	a := metricPath(0, 0, 0, 0, now)
	b := metricPath(2, 0, 0, 0, now)
	sched := minDrainScheduler{}

	if p := sched.pickData([]*path{a, b}, 64<<10, now); p != a {
		t.Fatal("全 cold 同分应取 path_id 小者")
	}
	a.inflight += 64 << 10
	if p := sched.pickData([]*path{a, b}, 64<<10, now); p != b {
		t.Fatal("a 积压后应轮到 b（冷启动均等分摊）")
	}
	// cold 路径顶到 minInflightCap 也被跳过
	b.inflight = minInflightCap
	if p := sched.pickData([]*path{a, b}, 64<<10, now); p != a {
		t.Fatal("b 顶满应回选 a")
	}
}

// TestPickAckLowestRTT 验收：控制帧回送选 srtt 最小的存活路径；
// 无样本路径排后，全无样本退化为首条。
func TestPickAckLowestRTT(t *testing.T) {
	now := time.Now()
	a := metricPath(0, 0, 50*time.Millisecond, 0, now)
	b := metricPath(2, 0, 5*time.Millisecond, 0, now)
	c := metricPath(4, 0, 0, 0, now) // 无 RTT 样本
	sched := minDrainScheduler{}

	if p := sched.pickAck([]*path{a, b, c}, now); p != b {
		t.Fatal("ACK 应走 srtt 最小路径")
	}
	for _, p := range []*path{a, b, c} {
		p.hasRTT = false
	}
	if p := sched.pickAck([]*path{a, b, c}, now); p != a {
		t.Fatal("全无 RTT 样本应退化为首条路径")
	}
}

// ---------- 指标采样单测 ----------

// TestNoteRTTEWMA 验收：RFC 6298 EWMA 维护 srtt/rttvar + min_rtt。
func TestNoteRTTEWMA(t *testing.T) {
	now := time.Now()
	p := &path{}
	p.noteRTT(100*time.Millisecond, now)
	if !p.hasRTT || p.srtt != 100*time.Millisecond ||
		p.rttvar != 50*time.Millisecond || p.minRTT != 100*time.Millisecond {
		t.Fatalf("首样本初始化错误: srtt=%v rttvar=%v min=%v", p.srtt, p.rttvar, p.minRTT)
	}
	p.noteRTT(50*time.Millisecond, now)
	// RFC 6298：rttvar = 3/4·50 + 1/4·|100-50| = 50ms；
	//           srtt = 7/8·100 + 1/8·50 = 93.75ms；min_rtt 取小
	if p.srtt != 93750*time.Microsecond || p.rttvar != 50*time.Millisecond ||
		p.minRTT != 50*time.Millisecond {
		t.Fatalf("EWMA 错误: srtt=%v rttvar=%v min=%v", p.srtt, p.rttvar, p.minRTT)
	}
	// 负样本（时钟回拨/伪造回显）丢弃
	p.noteRTT(-time.Millisecond, now)
	if p.srtt != 93750*time.Microsecond {
		t.Fatal("负样本不应被采纳")
	}
}

// TestOnDeliveredRateSample 验收：BBR 式 delivery-rate——
// 每 ≥max(srtt, 下限) 出一个 delivered/Δt 样本，间隔内只记账。
func TestOnDeliveredRateSample(t *testing.T) {
	now := time.Now()
	p := &path{attachedAt: now, lastRateAt: now}
	p.srtt, p.hasRTT = 20*time.Millisecond, true
	p.inflight = 192 << 10 // 模拟已发未确认

	// 窗口内交付但未到采样间隔 → 不出样本
	p.onDelivered(64<<10, now.Add(10*time.Millisecond))
	if p.hasRate {
		t.Fatal("未到采样间隔不应出样本")
	}
	// 间隔满 → sample = 192KiB/30ms ≈ 6.4MiB/s，首样本直接采纳
	p.onDelivered(128<<10, now.Add(30*time.Millisecond))
	want := float64(192<<10) / 0.03
	if !p.hasRate || p.estRate != want {
		t.Fatalf("est_rate 应为 %v，得到 %v", want, p.estRate)
	}
	if p.inflight != 0 {
		t.Fatalf("inflight 应核销为 0，得到 %d", p.inflight)
	}
	// 回落样本走 EWMA：sample=0 时 estRate 只降 1/8
	p.onDelivered(0, now.Add(60*time.Millisecond)) // d==0：不算样本只推窗
	p.onDelivered(96<<10, now.Add(80*time.Millisecond))
	// d=96KiB, dt=50ms → sample≈1.9MiB/s < estRate → EWMA 降 1/8
	if p.estRate >= want {
		t.Fatal("回落样本应把 est_rate 拉低")
	}
}

// TestEffRateFusion 验收：effRate 优先取对端 TELEMETRY 观测
// （TTL 内的 ground truth），缺失/过期回退本地采样，全过期回 0
// （调度按冷启动处理）。
func TestEffRateFusion(t *testing.T) {
	now := time.Now()
	p := metricPath(0, 1<<20, 10*time.Millisecond, 0, now)
	p.peerRate, p.peerRateAt = float64(4<<20), now
	if got := p.effRate(now); got != float64(4<<20) {
		t.Fatalf("对端观测在 TTL 内应优先: %v", got)
	}
	p.peerRateAt = now.Add(-peerRateTTL - time.Millisecond) // 观测过期
	if got := p.effRate(now); got != float64(1<<20) {
		t.Fatalf("对端观测过期应回退本地: %v", got)
	}
	p.lastRateAt = now.Add(-p.staleAfter() - time.Millisecond) // 本地也过期
	if got := p.effRate(now); got != 0 {
		t.Fatalf("全过期应为 0（冷启动），得到 %v", got)
	}
}

// TestInflightCapValue 验收：cap = k·est_rate·srtt，无指标取下界。
func TestInflightCapValue(t *testing.T) {
	now := time.Now()
	p := metricPath(0, 10<<20, 10*time.Millisecond, 0, now)
	want := int64(inflightCapK * float64(10<<20) * (10 * time.Millisecond).Seconds())
	if got := p.inflightCap(now); got != want {
		t.Fatalf("cap 应为 %d，得到 %d", want, got)
	}
	cold := metricPath(0, 0, 0, 0, now)
	if got := cold.inflightCap(now); got != minInflightCap {
		t.Fatalf("无指标应取下界 %d，得到 %d", minInflightCap, got)
	}
}

// ---------- 流级集成测试 ----------

// eventuallyCond 轮询等待 cond 成立（内部测试版 eventually）。
func eventuallyCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// TestSRTTFromAckEcho 验收：人造 ACK 带 ts_echo+ts_path 回显往返 →
// 对应路径的 srtt/min_rtt 收敛到注入的往返延迟量级。
func TestSRTTFromAckEcho(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	var id [16]byte
	s := newStreamOnPath(id, c1, testCfg(16<<10))
	defer s.Close()

	sink := frameSink(t, c2)
	recvFrame(t, sink) // 初始窗口通告

	// 对端先通告窗口，发送端才放 DATA
	if _, err := c2.Write(appendAckFrame(nil, 0, 0, 0, 16<<10, 1, nil)); err != nil {
		t.Fatal(err)
	}
	var seq uint64 = 2
	for _, delay := range []time.Duration{30, 40, 50} {
		if _, err := s.Write(bytes.Repeat([]byte("r"), 4<<10)); err != nil {
			t.Fatalf("Write 失败: %v", err)
		}
		data := recvFrame(t, sink)
		if len(data.payload) == 0 {
			t.Fatal("应收到 DATA 帧")
		}
		time.Sleep(delay * time.Millisecond)
		// 回 ACK：cum 确认该帧、ts_echo 回显其 send_ts、ts_path=path0
		cum := data.off + uint64(len(data.payload))
		if _, err := c2.Write(appendAckFrame(nil, cum, data.sendTs, 1, 16<<10, seq, nil)); err != nil {
			t.Fatal(err)
		}
		seq++
	}
	eventuallyCond(t, "path0 采到 RTT 样本", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.pathsByID[0].hasRTT
	})
	s.mu.Lock()
	p := s.pathsByID[0]
	srtt, minRTT := p.srtt, p.minRTT
	s.mu.Unlock()
	// 样本 ≈ 注入延迟（30/40/50ms）+ 管道延迟：srtt 收敛到几十 ms
	if srtt < 20*time.Millisecond || srtt > 90*time.Millisecond {
		t.Fatalf("srtt 应收敛到注入延迟量级，得到 %v", srtt)
	}
	if minRTT < 20*time.Millisecond || minRTT > srtt {
		t.Fatalf("min_rtt 应 ≤ srtt 且不低于最小注入延迟: min=%v srtt=%v", minRTT, srtt)
	}
}

// TestPingColdStartRTT 验收：attachPath 的冷启动 PING 经同路径
// 应答回来 → 该路径获得首个 RTT 样本（无需等数据往返）。
func TestPingColdStartRTT(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 7
	cfg := testCfg(16 << 10)
	sa := newStreamOnPath(id, c0a, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()

	if err := sa.attachPath(&path{id: 2, conn: c2a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}
	eventuallyCond(t, "sa path2 经 PING 采到 RTT", func() bool {
		sa.mu.Lock()
		defer sa.mu.Unlock()
		return sa.pathsByID[2].hasRTT
	})
	eventuallyCond(t, "sb path2 经 PING 采到 RTT", func() bool {
		sb.mu.Lock()
		defer sb.mu.Unlock()
		return sb.pathsByID[2].hasRTT
	})
}

// TestTelemetryAppliesPeerRate 验收：收到对端 TELEMETRY 帧 →
// 对应路径 peerRate 生效并参与 effRate；未知 path_id 忽略。
func TestTelemetryAppliesPeerRate(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	var id [16]byte
	s := newStreamOnPath(id, c1, testCfg(16<<10))
	defer s.Close()

	sink := frameSink(t, c2)
	recvFrame(t, sink) // 初始窗口通告

	body, err := proto.Marshal(&pb.Telemetry{Rates: []*pb.PathRate{
		{PathId: 0, RateBps: 5 << 20},
		{PathId: 99, RateBps: 9 << 20}, // 不认识的 path_id 应被忽略
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Write(appendCtrlFrame(nil, frameTelemetry, body)); err != nil {
		t.Fatal(err)
	}
	eventuallyCond(t, "path0 peerRate 生效", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.pathsByID[0].peerRate == float64(5<<20)
	})
	s.mu.Lock()
	p := s.pathsByID[0]
	eff := p.effRate(time.Now())
	s.mu.Unlock()
	if eff != float64(5<<20) {
		t.Fatalf("effRate 应被 TELEMETRY 校准到 5MiB/s，得到 %v", eff)
	}
}

// TestTelemetryReport 验收端到端闭环：收端按周期回报各路径到达
// 速率 → 发送端 effRate 被校准（含 TELEMETRY 帧体编解码链路）。
func TestTelemetryReport(t *testing.T) {
	cfg := testCfg(64 << 10)
	cfg.telemetryInterval = 20 * time.Millisecond
	sa, sb := pipeStream(t, cfg)
	defer sa.Close()
	defer sb.Close()

	go func() { _, _ = io.Copy(io.Discard, sb) }()
	if _, err := sa.Write(bytes.Repeat([]byte("t"), 256<<10)); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	eventuallyCond(t, "sa path0 peerRate 被回报校准", func() bool {
		sa.mu.Lock()
		defer sa.mu.Unlock()
		p := sa.pathsByID[0]
		return p.peerRate > 0 && p.effRate(time.Now()) > 0
	})
}

// rateDelayConn 是测试 double：把 conn 包装成「有界发送队列 + 后台
// 按令牌桶限速 + 固定传播时延」的瓶颈链路，近似 netem 的 tbf+delay。
// Write 入队即返回（与真实路径上内核缓冲吸收写同构）——在途量可以
// 积累到 BDP/cap 量级，调度器的带宽比例分流才有发挥空间；队列满时
// Write 阻塞，即瓶颈链路的回压。限速思路复用 relay 包的令牌桶。
type rateDelayConn struct {
	net.Conn
	lim   *rate.Limiter
	delay time.Duration
	q     chan qitem // 有界发送队列（≈链路缓冲），满则回压
	done  chan struct{}
	once  sync.Once
}

// qitem 是一条待发送数据及其出队时刻（传播时延到期时刻）。
// 限速与传播时延分开建模：限速决定发送速率（串行化），时延是
// 入队后须等待的固定时间（可与后续帧的发送重叠，流水线化）。
type qitem struct {
	b  []byte
	at time.Time
}

func newRateDelayConn(c net.Conn, bps float64, delay time.Duration, qLen int) *rateDelayConn {
	rc := &rateDelayConn{
		Conn:  c,
		lim:   rate.NewLimiter(rate.Limit(bps), maxFramePayload+16),
		delay: delay,
		q:     make(chan qitem, qLen),
		done:  make(chan struct{}),
	}
	go rc.drainer()
	return rc
}

func (c *rateDelayConn) Write(b []byte) (int, error) {
	cp := make([]byte, len(b))
	copy(cp, b)
	select {
	case c.q <- qitem{b: cp, at: time.Now().Add(c.delay)}:
		return len(b), nil
	case <-c.done:
		return 0, net.ErrClosed
	}
}

// drainer 逐条消费发送队列：先按令牌桶等份额（串行化发送），再睡到
// 传播时延到期，最后写底层 conn。底层写失败或 Close 即退出。
func (c *rateDelayConn) drainer() {
	defer c.closeOnce()
	for {
		select {
		case it := <-c.q:
			if err := c.lim.WaitN(context.Background(), len(it.b)); err != nil {
				return
			}
			if d := time.Until(it.at); d > 0 {
				time.Sleep(d)
			}
			if _, err := c.Conn.Write(it.b); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *rateDelayConn) closeOnce() {
	c.once.Do(func() { close(c.done) })
}

func (c *rateDelayConn) Close() error {
	c.closeOnce()
	return c.Conn.Close()
}

// TestPipeHeterogeneousSplit 验收：两路径一快一慢（~8MiB/s vs
// ~1MiB/s，各加 50ms 固定时延让 BDP 越过 minInflightCap 地板——
// 否则两条路径 cap 都钉死在保底值上，分流退化成 1:1），稳态吞吐
// 明显偏向快路径。理论分流比 ≈ 速率比 8:1，受保底在途量与冷启动
// 均摊影响会压低慢路径份额，容差取 ≥2.5:1（不写死精确比例）。
func TestPipeHeterogeneousSplit(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 9
	cfg := testCfg(1 << 20)
	cfg.telemetryInterval = 20 * time.Millisecond

	var wFast, wSlow atomic.Int64
	fastConn := countConn{newRateDelayConn(c0a, 8<<20, 50*time.Millisecond, 16), &wFast, &atomic.Int64{}}
	slowConn := countConn{newRateDelayConn(c2a, 1<<20, 50*time.Millisecond, 8), &wSlow, &atomic.Int64{}}

	sa := newStreamOnPath(id, fastConn, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()
	if err := sa.attachPath(&path{id: 2, conn: slowConn, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}

	total := 3 << 20 // 3MiB
	rdone := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(io.Discard, io.LimitReader(sb, int64(total)))
		rdone <- n
	}()
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(bytes.Repeat([]byte("x"), total))
		wdone <- err
	}()
	if err := <-wdone; err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	if n := <-rdone; n != int64(total) {
		t.Fatalf("数据未完整到达: %d/%d", n, total)
	}
	f, sl := wFast.Load(), wSlow.Load()
	if sl == 0 {
		t.Fatal("慢路径完全未被使用")
	}
	if ratio := float64(f) / float64(sl); ratio < 2.5 {
		t.Fatalf("分流未明显偏向快路径: fast=%d slow=%d ratio=%.2f", f, sl, ratio)
	}
	t.Logf("分流比 fast:slow = %d:%d ≈ %.1f:1", f, sl, float64(f)/float64(sl))
}

// TestWindowCapGrowsWithBDP 验收窗口联动（规格 §6）：收端重排缓冲
// 容量按 Σ到达速率·max_srtt 重算超过初始下界，发送端经 ACK 看到
// 更大的通告窗口。
func TestWindowCapGrowsWithBDP(t *testing.T) {
	c0a, c0b := net.Pipe()
	var id [16]byte
	id[0] = 11
	// min<max 才让容量浮动；限速 4MiB/s + 25ms 时延 → 收端
	// estBDP ≈ 100KiB，明显超过 64KiB 下界
	cfg := streamConfig{
		minBuf:            64 << 10,
		maxBuf:            8 << 20,
		sendBufCap:        4 << 20,
		telemetryInterval: 20 * time.Millisecond,
	}
	sa := newStreamOnPath(id, newRateDelayConn(c0a, 4<<20, 25*time.Millisecond, 16), cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()

	rdone := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(io.Discard, io.LimitReader(sb, 1<<20))
		rdone <- n
	}()
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(bytes.Repeat([]byte("w"), 1<<20))
		wdone <- err
	}()

	eventuallyCond(t, "收端重排缓冲容量超过下界", func() bool {
		sb.mu.Lock()
		defer sb.mu.Unlock()
		return sb.rbuf.cap > 64<<10
	})
	eventuallyCond(t, "发送端看到更大的通告窗口", func() bool {
		sa.mu.Lock()
		defer sa.mu.Unlock()
		return sa.advWindow > 64<<10
	})
	if err := <-wdone; err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	if n := <-rdone; n != 1<<20 {
		t.Fatalf("数据未完整到达: %d", n)
	}
}
