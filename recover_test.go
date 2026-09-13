package netacc

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------- 测试 double ----------

// gatedConn 门控写方向：门开着时透传；门关上后 Write 仍返回成功
// 但数据被吞——模拟「写不报错但对端永远收不到」的半死路径
// （对端读侧卡死/中间盒静默丢包，只能靠 ACK 静默暴露）。
// 读方向不受影响：对端回送的 ACK 帧照常到达。
type gatedConn struct {
	net.Conn
	open atomic.Bool
}

func (c *gatedConn) Write(b []byte) (int, error) {
	if !c.open.Load() {
		return len(b), nil // 吞掉：写「成功」但永不到达
	}
	return c.Conn.Write(b)
}

// frameRange 是一个 DATA 帧在流内覆盖的字节区间 [off,end)。
type frameRange struct{ off, end uint64 }

// offLogConn 记录每个写出 DATA 帧的流内字节区间（Write 边界即
// 帧边界：发送泵/ACK/控制帧各自单独一次 Write；DATA 帧头前两
// 个 varint 为 type=1 与 offset）。用于断言「某区间先走 A 路径、
// 后又在 B 路径上被重发」——偏移全局唯一分配，重现即重发。
type offLogConn struct {
	net.Conn
	mu  sync.Mutex
	frs []frameRange
}

func (c *offLogConn) Write(b []byte) (int, error) {
	if len(b) > 2 && b[0] == byte(frameData) {
		if r, ok := parseDataRange(b); ok {
			c.mu.Lock()
			c.frs = append(c.frs, r)
			c.mu.Unlock()
		}
	}
	return c.Conn.Write(b)
}

func (c *offLogConn) ranges() []frameRange {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]frameRange(nil), c.frs...)
}

// parseDataRange 解出 DATA 帧线格式里的 [off, off+payload_len)。
func parseDataRange(b []byte) (frameRange, bool) {
	_, n := binary.Uvarint(b) // type
	if n <= 0 {
		return frameRange{}, false
	}
	off, m := binary.Uvarint(b[n:])
	if m <= 0 {
		return frameRange{}, false
	}
	n += m
	_, m = binary.Uvarint(b[n:]) // send_ts
	if m <= 0 {
		return frameRange{}, false
	}
	n += m
	pl, m := binary.Uvarint(b[n:]) // payload_len
	if m <= 0 {
		return frameRange{}, false
	}
	return frameRange{off, off + pl}, true
}

// rangesOverlap 报告两组字节区间是否存在任何重叠。
func rangesOverlap(a, b []frameRange) bool {
	for _, x := range a {
		for _, y := range b {
			if x.off < y.end && y.off < x.end {
				return true
			}
		}
	}
	return false
}

// holdConn 扣押首个 off==holdOff 的 DATA 帧不写到底层，直到
// release 补投——后续同偏移的重发帧照常透传（迟到的是原拷贝，
// 重发副本不被扣押）。用于精确制造「中间一跳迟到」的乱序时序。
type holdConn struct {
	net.Conn
	mu      sync.Mutex
	holdOff uint64
	held    []byte
}

func (c *holdConn) Write(b []byte) (int, error) {
	if len(b) > 0 && b[0] == byte(frameData) {
		c.mu.Lock()
		if c.held == nil {
			if r, ok := parseDataRange(b); ok && r.off == c.holdOff {
				c.held = append([]byte(nil), b...)
				c.mu.Unlock()
				return len(b), nil // 扣押：写「成功」但暂不投递
			}
		}
		c.mu.Unlock()
	}
	return c.Conn.Write(b)
}

// release 把扣押的帧补写到底层（迟到的原拷贝；收端按偏移去重）。
func (c *holdConn) release() {
	c.mu.Lock()
	b := c.held
	c.held = nil
	c.mu.Unlock()
	if len(b) > 0 {
		_, _ = c.Conn.Write(b)
	}
}

// ---------- 帧收集 helper ----------

// typedFrame 带帧类型的解码结果（frameSink 丢弃了类型，这里保留）。
type typedFrame struct {
	ft frameType
	f  frame
}

// typedFrameSink 同 frameSink，但保留帧类型供过滤。
func typedFrameSink(t *testing.T, conn net.Conn) <-chan typedFrame {
	t.Helper()
	ch := make(chan typedFrame, 64)
	go func() {
		fr := newFrameReader(conn)
		for {
			ft, f, err := fr.next()
			if err != nil {
				close(ch)
				return
			}
			ch <- typedFrame{ft, f}
		}
	}()
	return ch
}

// recvTyped 带超时取下一帧。
func recvTyped(t *testing.T, ch <-chan typedFrame) typedFrame {
	t.Helper()
	select {
	case tf, ok := <-ch:
		if !ok {
			t.Fatal("帧流意外结束")
		}
		return tf
	case <-time.After(5 * time.Second):
		t.Fatal("等帧超时")
		return typedFrame{}
	}
}

// collectData 在 d 时长内收集 channel 里的 DATA 帧。
func collectData(ch <-chan typedFrame, d time.Duration) []frame {
	deadline := time.After(d)
	var fs []frame
	for {
		select {
		case tf, ok := <-ch:
			if !ok {
				return fs
			}
			if tf.ft == frameData {
				fs = append(fs, tf.f)
			}
		case <-deadline:
			return fs
		}
	}
}

// ---------- 洞计算单测 ----------

// TestFirstHole 单测：SACK 减法——已收区间不重发，只补真正缺失的洞。
func TestFirstHole(t *testing.T) {
	cases := []struct {
		name     string
		off, end uint64
		ranges   []byteRange
		wantA    uint64
		wantB    uint64 // a>=b 表示全覆盖
	}{
		{"无 ranges 整段是洞", 10, 20, nil, 10, 20},
		{"整段被一条 range 覆盖", 10, 20, []byteRange{{0, 30}}, 20, 20},
		{"前缀覆盖补后缀", 10, 20, []byteRange{{0, 15}}, 15, 20},
		{"后缀覆盖补前缀", 10, 20, []byteRange{{15, 40}}, 10, 15},
		{"中段覆盖补前缀（首洞优先）", 10, 30, []byteRange{{15, 25}}, 10, 15},
		{"段前无关 range 忽略", 10, 20, []byteRange{{0, 5}}, 10, 20},
		{"段后无关 range 忽略", 10, 20, []byteRange{{30, 40}}, 10, 20},
		{"多条 range 恰好铺满", 10, 30, []byteRange{{10, 18}, {18, 30}}, 30, 30},
		{"边界相接不算洞", 10, 20, []byteRange{{10, 20}}, 20, 20},
	}
	for _, c := range cases {
		a, b := firstHole(c.off, c.end, normalizeRanges(c.ranges))
		if a != c.wantA || b != c.wantB {
			t.Errorf("%s: firstHole(%d,%d)=[%d,%d)，期望 [%d,%d)",
				c.name, c.off, c.end, a, b, c.wantA, c.wantB)
		}
	}
	// coveredBy 与 firstHole 的一致性抽查
	if !coveredBy([]byteRange{{0, 30}}, 10, 20) {
		t.Error("coveredBy 应判全覆盖")
	}
	if coveredBy([]byteRange{{0, 15}}, 10, 20) {
		t.Error("部分覆盖不应判 covered")
	}
}

// TestNormalizeRanges 单测：乱序/重叠输入被排序合并。
func TestNormalizeRanges(t *testing.T) {
	rs := normalizeRanges([]byteRange{{30, 40}, {10, 20}, {15, 35}, {50, 60}})
	want := []byteRange{{10, 40}, {50, 60}}
	if len(rs) != len(want) {
		t.Fatalf("合并结果错误: %+v", rs)
	}
	for i := range want {
		if rs[i] != want[i] {
			t.Fatalf("合并结果错误: %+v", rs)
		}
	}
}

// ---------- 调度降权单测 ----------

// TestPickDataSuspectPenalty 验收：可疑路径的 effRate 按
// 1/suspectPenalty 折算参与排空时间比较——更快但被标可疑的
// 路径应让位于稍慢但健康的路径；同时 inflightCap 同步收缩。
func TestPickDataSuspectPenalty(t *testing.T) {
	now := time.Now()
	fast := metricPath(0, 32<<20, 50*time.Millisecond, 0, now) // 32MiB/s
	mid := metricPath(2, 10<<20, 50*time.Millisecond, 0, now)  // 10MiB/s
	sched := minDrainScheduler{}
	paths := []*path{fast, mid}

	if p := sched.pickData(paths, 64<<10, now); p != fast {
		t.Fatal("正常态应选排空更快的 fast")
	}
	capBefore := fast.inflightCap(now)
	if capBefore <= minInflightCap {
		t.Fatalf("测试参数应使 cap 越过下界，得到 %d", capBefore)
	}

	fast.suspect = true // 标可疑：32MiB/s ÷ 4 = 8MiB/s < mid 10MiB/s
	if p := sched.pickData(paths, 64<<10, now); p != mid {
		t.Fatal("可疑路径降权后应让位给健康路径")
	}
	if got := fast.inflightCap(now); got >= capBefore {
		t.Fatalf("可疑路径 inflightCap 应收缩: before=%d after=%d", capBefore, got)
	}

	// 全部可疑时仍需发得出（降权是偏好不是禁发，否则单路径死锁）
	mid.suspect = true
	if p := sched.pickData(paths, 64<<10, now); p == nil {
		t.Fatal("全可疑也不应返回 nil（保底可发）")
	}
}

// TestSuspectAfterAndRTO 单测：可疑判据 k·srtt 下界 + 路径 RTO
// （srtt+4·rttvar，下界 pathRTOFloor）。
func TestSuspectAfterAndRTO(t *testing.T) {
	wan := &path{srtt: 300 * time.Millisecond, rttvar: 100 * time.Millisecond, hasRTT: true}
	if got := wan.suspectAfter(); got != 600*time.Millisecond {
		t.Fatalf("suspectAfter 应为 2·srtt=600ms，得到 %v", got)
	}
	if got := wan.rto(); got != pathRTOFloor {
		t.Fatalf("公式值 700ms 低于下界时应取 pathRTOFloor=%v，得到 %v", pathRTOFloor, got)
	}
	wan2 := &path{srtt: 300 * time.Millisecond, rttvar: 250 * time.Millisecond, hasRTT: true}
	if got := wan2.rto(); got != 1300*time.Millisecond {
		t.Fatalf("rto 应为 srtt+4·rttvar=1300ms，得到 %v", got)
	}
	// loopback 级 RTT：公式值 << 下界 → 取下界防误标/误摘
	lo := &path{srtt: time.Millisecond, rttvar: time.Millisecond, hasRTT: true}
	if got := lo.suspectAfter(); got != suspectFloor {
		t.Fatalf("小 srtt 应取 suspectFloor=%v，得到 %v", suspectFloor, got)
	}
	if got := lo.rto(); got != pathRTOFloor {
		t.Fatalf("小 RTT 应取 pathRTOFloor=%v，得到 %v", pathRTOFloor, got)
	}
	// 无样本路径：srtt=0 → 同样走下界
	cold := &path{}
	if got := cold.suspectAfter(); got != suspectFloor {
		t.Fatalf("无 RTT 样本应取 suspectFloor，得到 %v", got)
	}
}

// ---------- 软失效集成测试 ----------

// TestSoftFailResendAndDrop 验收标准 2 + RTO 摘除：
// path2 先是健康路径（有真实指标），随后 sa→sb 正向被门吞
// （写成功但对端永远收不到）——
//  1. 在途段超 k·srtt 无 ACK/SACK 覆盖 → path2 被标 suspect
//     （Paths() 可观测 = 降权生效的可观测钩子）；
//  2. 被吞区间经机会重发搬到 path0 上重发（偏移全局唯一，
//     path2 分到的区间重现于 path0 即换路重传的直接证据）；
//  3. 可疑态持续无活性证据超路径 RTO → 硬失效摘除，两侧收敛；
//  4. 全程数据不丢。
func TestSoftFailResendAndDrop(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 21
	cfg := testCfg(1 << 20)
	cfg.telemetryInterval = 20 * time.Millisecond // 快 tick：软失效扫描周期

	gate := &gatedConn{Conn: c2a}
	gate.open.Store(true)
	// path0 限速 512KiB/s：一是压低它的实测 estRate，否则快车道
	// 冷启动后永远赢、path2 分不到帧；二是让「机会重发后吞吐
	// 降级到剩余路径速率」成为可观测事实。
	// offLog 在 gate 外侧：记的是「分配到 path2」的全部 DATA
	// 区间（含被吞的）——被吞区间即待重发的洞。
	log0 := &offLogConn{Conn: newRateDelayConn(c0a, 512<<10, 5*time.Millisecond, 32)}
	log2 := &offLogConn{Conn: gate}

	sa := newStreamOnPath(id, log0, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()
	if err := sa.attachPath(&path{id: 2, conn: log2, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}

	want1 := bytes.Repeat([]byte("phase1-"), 16<<10) // 112KiB
	want2 := bytes.Repeat([]byte("phase2-"), 48<<10) // 336KiB
	total := int64(len(want1) + len(want2))
	rdone := make(chan []byte, 1)
	go func() {
		buf := &bytes.Buffer{}
		_, _ = io.Copy(buf, io.LimitReader(sb, total))
		rdone <- buf.Bytes()
	}()

	// 第一段：双路径都在分流（path2 攒下真实指标与被吞前基线）
	if _, err := sa.Write(want1); err != nil {
		t.Fatalf("phase1 Write 失败: %v", err)
	}
	eventuallyCond(t, "path2 分到数据", func() bool {
		return len(log2.ranges()) > 0
	})
	eventuallyCond(t, "phase1 全部确认", func() bool {
		sa.mu.Lock()
		defer sa.mu.Unlock()
		return sa.cumAcked >= uint64(len(want1))
	})

	// 关门：sa→sb 正向吞帧（半死路径），记下日志分界
	gate.open.Store(false)
	n0 := len(log0.ranges())

	if _, err := sa.Write(want2); err != nil {
		t.Fatalf("phase2 Write 失败: %v", err)
	}

	// 1) 可疑标记可观测（降权随 suspect 生效，见调度器折算）
	eventuallyCond(t, "path2 被标可疑", func() bool {
		for _, pi := range sa.Paths() {
			if pi.ID == 2 && pi.Suspect {
				return true
			}
		}
		return false
	})
	// 2) 机会重发：分到 path2 却未被覆盖的区间（关门前后都算），
	//    最终重现在 path0 的写出日志上——偏移全局唯一分配，
	//    重现即换路重传
	eventuallyCond(t, "被吞区间经 path0 换路重发", func() bool {
		return rangesOverlap(log0.ranges()[n0:], log2.ranges())
	})
	// 4) 数据完整到达（在途/被吞段全部经剩余路径兜回）
	got := <-rdone
	if !bytes.Equal(got, append(want1, want2...)) {
		t.Fatal("软失效恢复后数据不一致")
	}
	// 3) 可疑态持续无进展 → RTO 到期摘除 → 两侧收敛到单路径
	eventuallyPaths(t, sa, 1)
	eventuallyPaths(t, sb, 1) // PATH_DROP 带内同步摘除

	// 流在剩余路径上继续可用（吞吐自动降级到剩余路径）
	want3 := []byte("after-drop")
	if _, err := sa.Write(want3); err != nil {
		t.Fatalf("摘除后 Write 失败: %v", err)
	}
	got3 := make([]byte, len(want3))
	if _, err := io.ReadFull(sb, got3); err != nil {
		t.Fatalf("摘除后 Read 失败: %v", err)
	}
	if !bytes.Equal(got3, want3) {
		t.Fatal("摘除后数据不一致")
	}
}

// TestSuspectPathRecovers 验收恢复语义：路径被标可疑后若能重新
// 给出活性证据（PING 应答 → 新 RTT 样本），可疑态应清除、路径
// 不摘除、继续参与调度——「先降权观察，RTO 到期才摘除」的
// 前半段。
func TestSuspectPathRecovers(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 26
	cfg := testCfg(1 << 20)
	cfg.telemetryInterval = 20 * time.Millisecond

	gate := &gatedConn{Conn: c2a}
	gate.open.Store(true)
	// path0 限速：不限速时其实测 estRate 碾压冷启动的 path2，
	// path2 分不到在途段就无从超时，测试退化成单路径
	sa := newStreamOnPath(id, newRateDelayConn(c0a, 512<<10, 5*time.Millisecond, 32), cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()
	if err := sa.attachPath(&path{id: 2, conn: gate, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}
	rdone := make(chan []byte, 1)
	// 1MiB 传输量保证关门后仍有大量待发数据持续分到 path2 被吞
	// ——小流量会在关门前就全部发完并确认，无从超时
	want := bytes.Repeat([]byte("recover-"), 128<<10) // 1MiB
	go func() {
		buf := &bytes.Buffer{}
		_, _ = io.Copy(buf, io.LimitReader(sb, int64(len(want))))
		rdone <- buf.Bytes()
	}()
	if _, err := sa.Write(want); err != nil {
		t.Fatal(err)
	}
	// 等 path2 名下确有在途段（门外的猎物）再关门——它要负责
	// 让软失效判据超时而把路径标记为可疑
	eventuallyCond(t, "path2 名下有在途段", func() bool {
		sa.mu.Lock()
		defer sa.mu.Unlock()
		p2 := sa.pathsByID[2]
		return p2 != nil && p2.inflight > 0
	})
	gate.open.Store(false)
	suspectOf := func(id uint64) bool {
		for _, pi := range sa.Paths() {
			if pi.ID == id {
				return pi.Suspect
			}
		}
		return false
	}
	eventuallyCond(t, "path2 被标可疑", func() bool { return suspectOf(2) })

	// 重开门：sa→sb 正向恢复——可疑路径上的 PING 探测得到应答
	// → 新 RTT 样本 → 可疑态清除（恢复活性），路径保留不摘除
	gate.open.Store(true)
	eventuallyCond(t, "path2 恢复活性清除可疑", func() bool {
		return !suspectOf(2)
	})
	// 清除后仍双路径、流数据完整
	if got := <-rdone; !bytes.Equal(got, want) {
		t.Fatal("恢复过程中数据不一致")
	}
	if len(sa.Paths()) != 2 {
		t.Fatal("恢复后路径不应被摘除")
	}
}

// ---------- SACK 消费：重发只补缺失区间 ----------

// TestResendSkipsSACKedRanges 验收：硬失效（杀路径）后重发只补
// 对端真正缺失的洞——已被最新 SACK ranges 覆盖的区间不重发，
// 部分覆盖的段按 offset+len 重切分只发剩余尾巴。
// 测试脚本直接扮演对端（手工 ACK/收帧），时序完全确定：
// 关掉软失效扫描 tick（telemetryInterval 调大），只剩 dropPath
// 重注入一条路径，断言逐帧精确。
func TestResendSkipsSACKedRanges(t *testing.T) {
	c1a, c1b := net.Pipe()
	c3a, c3b := net.Pipe()
	defer c1b.Close()
	defer c3b.Close()
	var id [16]byte
	id[0] = 22
	cfg := testCfg(1 << 20)
	cfg.telemetryInterval = 10 * time.Second // 本测只验 SACK 消费，关掉软失效扫描

	s := newStreamOnPath(id, c1a, cfg)
	defer s.Close()
	sink0 := typedFrameSink(t, c1b)
	sink2 := typedFrameSink(t, c3b)
	// 初始窗口通告（建流即发的 ACK）
	if tf := recvTyped(t, sink0); tf.ft != frameAck {
		t.Fatalf("首帧应为窗口通告 ACK，得到类型 %d", tf.ft)
	}
	if err := s.attachPath(&path{id: 2, conn: c3a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	// 冷启动 PING 会落在 sink2 上，随帧流排干即可

	// 对端通告大窗口 → 发送端放行
	if _, err := c1b.Write(appendAckFrame(nil, 0, 0, 0, 4<<20, 1, nil)); err != nil {
		t.Fatal(err)
	}
	// 写 4 个满帧（256KiB）：冷启动 + minInflightCap 硬顶 →
	// 交替分配，path0={F1,F3}，path2={F2,F4}
	const total = 256 << 10
	if _, err := s.Write(bytes.Repeat([]byte("d"), total)); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	var on2 []frameRange
	got := 0
	deadline := time.Now().Add(5 * time.Second)
	for got < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("只收到 %d/4 个 DATA 帧", got)
		}
		select {
		case tf, ok := <-sink0:
			if !ok {
				t.Fatal("path0 帧流意外结束")
			}
			if tf.ft == frameData {
				got++
			}
		case tf, ok := <-sink2:
			if !ok {
				t.Fatal("path2 帧流意外结束")
			}
			if tf.ft == frameData {
				on2 = append(on2, frameRange{tf.f.off, tf.f.off + uint64(len(tf.f.payload))})
				got++
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	// 交替分配：path2 应恰好背走 F2[64k,128k) 与 F4[192k,256k)
	if len(on2) != 2 || on2[0] != (frameRange{64 << 10, 128 << 10}) ||
		on2[1] != (frameRange{192 << 10, 256 << 10}) {
		t.Fatalf("path2 上的帧分布与预期不符: %+v", on2)
	}

	// 对端回 ACK：cum 只认 F1；SACK 声称已收到 F4 的前半
	// [192k,224k)——F2 与 F4 的后半 [224k,256k) 是真洞
	if _, err := c1b.Write(appendAckFrame(nil, 64<<10, 0, 0, 4<<20, 2,
		[]byteRange{{192 << 10, 224 << 10}})); err != nil {
		t.Fatal(err)
	}
	eventuallyCond(t, "SACK ranges 已应用", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.peerRanges) == 1 && s.peerRanges[0] == (byteRange{192 << 10, 224 << 10})
	})

	// 杀 path2 → 其在途段（F2 整段 + F4）搬回重发池
	if err := c3a.Close(); err != nil {
		t.Fatal(err)
	}
	eventuallyPaths(t, s, 1)

	// 重发落在唯一存活的 path0 上，第一个补洞帧应是 F2 全段
	// [64k,128k)。注意时序细节：path0 自己还有在途未确认的
	// F3（128k,192k），加上 F2 的重发恰好顶到 minInflightCap
	// 硬顶——须先补一条 cum ACK 释放在途量，F4 尾巴才发得出。
	var resent []frameRange
	dl := time.Now().Add(5 * time.Second)
	for len(resent) < 1 {
		if time.Now().After(dl) {
			t.Fatal("F2 重发帧未出现")
		}
		select {
		case tf, ok := <-sink0:
			if !ok {
				t.Fatal("path0 帧流意外结束")
			}
			if tf.ft == frameData {
				resent = append(resent, frameRange{tf.f.off, tf.f.off + uint64(len(tf.f.payload))})
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	if resent[0] != (frameRange{64 << 10, 128 << 10}) {
		t.Fatalf("首个重发帧应为 F2 全段 [64k,128k)，得到 %+v", resent[0])
	}

	// 对端收下 F2 重发 + 早前 F3 → cum 推进到 192k，释放在途；
	// 堆里仍持有 F4 前半 → SACK 继续上报 [192k,224k)
	if _, err := c1b.Write(appendAckFrame(nil, 192<<10, 0, 0, 4<<20, 3,
		[]byteRange{{192 << 10, 224 << 10}})); err != nil {
		t.Fatal(err)
	}
	// 第二帧应是 F4 按洞边界重切分的尾巴 [224k,256k)；
	// SACK 已覆盖的 [192k,224k) 不得重发。
	for len(resent) < 2 {
		if time.Now().After(dl) {
			t.Fatalf("F4 尾巴重发帧未出现: 已收 %+v", resent)
		}
		select {
		case tf, ok := <-sink0:
			if !ok {
				t.Fatal("path0 帧流意外结束")
			}
			if tf.ft == frameData {
				resent = append(resent, frameRange{tf.f.off, tf.f.off + uint64(len(tf.f.payload))})
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	if resent[1] != (frameRange{224 << 10, 256 << 10}) {
		t.Fatalf("第二重发帧应为 F4 尾巴 [224k,256k)，得到 %+v", resent[1])
	}

	// 收尾：cum 覆盖全部 → 静默期内不应再有任何 DATA
	//（SACK 已收区间不重发 = 零冗余；归属存活路径的 F3 也未重发）
	if _, err := c1b.Write(appendAckFrame(nil, total, 0, 0, 4<<20, 4, nil)); err != nil {
		t.Fatal(err)
	}
	if fs := collectData(sink0, 300*time.Millisecond); len(fs) != 0 {
		t.Fatalf("SACK 已覆盖区间被重发/出现冗余帧: %+v", fs)
	}
}

// ---------- #19 遗留边角：收端没拿到的段靠软失效补回 ----------

// TestUncoveredSegResentOnLivePath 验收 #19 遗留边角的修复：
// 归属存活路径的段只要既不被 cum 覆盖、也不出现在最新 SACK
// ranges 里，发送端就会超时重发——不依赖路径死亡。
//
// 背景：诚实对端受窗口背压约束时，收端重排缓冲的 cap 丢段本不可达
// （unacked ≤ window 保证到达不越界）；会撞上满堆被丢的只有窗口
// 豁免的重发/迟到副本——被丢段永不进 SACK，若无本机制，洞首段
// 就此停摆。本测试用脚本化对端精确复现这个「对端没拿到」的状态：
// cum 停在 64k、SACK 只报 [128k,192k)——等价于 F2 的到达副本被
// cap 检查丢弃、F3 已入堆。
//
// 断言两条：
//  1. off=64k 的 F2 被反复重发（对端没有 → 补洞，直到覆盖）；
//  2. off=128k 的 F3 全程只发一次（SACK 已覆盖 → 防伪重传）。
func TestUncoveredSegResentOnLivePath(t *testing.T) {
	c1a, c1b := net.Pipe()
	defer c1b.Close()
	var id [16]byte
	id[0] = 23
	cfg := testCfg(1 << 20)
	cfg.telemetryInterval = 20 * time.Millisecond

	logged := &offLogConn{Conn: c1a}
	s := newStreamOnPath(id, logged, cfg)
	defer s.Close()
	sink := typedFrameSink(t, c1b)
	if tf := recvTyped(t, sink); tf.ft != frameAck {
		t.Fatalf("首帧应为窗口通告 ACK，得到类型 %d", tf.ft)
	}
	// 对端通告大窗口 → 发送端放行
	if _, err := c1b.Write(appendAckFrame(nil, 0, 0, 0, 4<<20, 1, nil)); err != nil {
		t.Fatal(err)
	}
	const total = 192 << 10
	if _, err := s.Write(bytes.Repeat([]byte("d"), total)); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	// 收 3 个首发帧 F1/F2/F3，记下末帧时间戳用于 ACK 回显
	var lastTs uint64
	for n := 0; n < 3; {
		if tf := recvTyped(t, sink); tf.ft == frameData {
			lastTs = tf.f.sendTs
			n++
		}
	}
	// 对端视图：F1 按序交付（cum=64k）、F3 已入堆（SACK），
	// F2 的到达副本被缓冲 cap 丢弃（既不进 cum 也不进 SACK）。
	// ACK 回显 F3 的 ts（ts_path=1 即对端视角的 path0）——等价于
	// 「F3 的到达副本被收下」，给 path0 一个真实 RTT 样本。
	if _, err := c1b.Write(appendAckFrame(nil, 64<<10, lastTs, 1, 4<<20, 2,
		[]byteRange{{128 << 10, 192 << 10}})); err != nil {
		t.Fatal(err)
	}
	eventuallyCond(t, "SACK 已应用", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.peerRanges) == 1
	})

	countAt := func(off uint64) int {
		n := 0
		for _, r := range logged.ranges() {
			if r.off == off {
				n++
			}
		}
		return n
	}
	// F2 逾期未覆盖 → path0 标可疑 → 同路径超时重发，反复直到被覆盖
	eventuallyCond(t, "F2 经软失效同路径重发", func() bool {
		return countAt(64<<10) >= 2
	})
	// 给足两个可疑周期再确认：SACK 已覆盖的 F3 始终只发了一次
	time.Sleep(2 * suspectFloor)
	if n := countAt(128 << 10); n != 1 {
		t.Fatalf("SACK 已覆盖的 F3 不应重发，却写了 %d 次", n)
	}

	// 排干积在 sink 里的重发帧，取最近一帧的 ts 回显（等价于
	// 对端最终收下了重发副本）
	for {
		select {
		case tf := <-sink:
			if tf.ft == frameData {
				lastTs = tf.f.sendTs
			}
		default:
			goto drained
		}
	}
drained:
	// 对端最终收下重发的 F2 → cum 全覆盖 → 静默期内不再重发。
	// 先等一拍结算：覆盖瞬间仍可能有刚发出的在途重发副本落地。
	if _, err := c1b.Write(appendAckFrame(nil, total, lastTs, 1, 4<<20, 3, nil)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if fs := collectData(sink, 200*time.Millisecond); len(fs) != 0 {
		t.Fatalf("覆盖后不应再有重发/冗余帧: %+v", fs)
	}
}

// TestDelayedFrameSamePathResend 验收同路径超时重发（调研 §4
// 策略 a 兜底）：单路径上中间一跳被扣押迟到 → 其段逾期未覆盖
// → 重发副本补洞推进；迟到的原拷贝随后到达，收端按偏移去重无害。
func TestDelayedFrameSamePathResend(t *testing.T) {
	ca, cb := net.Pipe()
	var id [16]byte
	id[0] = 25
	cfg := testCfg(1 << 20)
	cfg.telemetryInterval = 20 * time.Millisecond

	hold := &holdConn{Conn: ca, holdOff: 64 << 10} // 扣押 F2 的首个拷贝
	logged := &offLogConn{Conn: hold}
	sa := newStreamOnPath(id, logged, cfg)
	sb := newStreamOnPath(id, cb, cfg)
	defer sa.Close()
	defer sb.Close()

	want := bytes.Repeat([]byte("late-hop!"), 24<<10) // 216KiB → F1..F4
	rdone := make(chan []byte, 1)
	go func() {
		buf := &bytes.Buffer{}
		_, _ = io.Copy(buf, io.LimitReader(sb, int64(len(want))))
		rdone <- buf.Bytes()
	}()
	if _, err := sa.Write(want); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}

	// F2[64k,128k) 的原拷贝被押；其段逾期未覆盖 → 同路径重发
	eventuallyCond(t, "F2 同路径重发", func() bool {
		n := 0
		for _, r := range logged.ranges() {
			if r.off == 64<<10 {
				n++
			}
		}
		return n >= 2
	})

	// 数据经重发副本先行完整到达；再补投迟到的原拷贝（去重无害）
	got := <-rdone
	if !bytes.Equal(got, want) {
		t.Fatal("同路径重发后数据不一致")
	}
	hold.release()
}

// ---------- 硬失效：在途段换路重传 + 吞吐降级到剩余路径 ----------

// TestPathKillResendOnSurviving 验收标准 1 补强：传输中杀掉
// path2，其「在路上」的在途段（rateDelayConn 发送队列里滞留
// 未投递的部分）按原字节偏移经 path0 重发到达；path2 写量
// 冻结、path0 接管全部剩余流量 = 吞吐自动降级到剩余路径。
func TestPathKillResendOnSurviving(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 24
	cfg := testCfg(1 << 20)
	cfg.telemetryInterval = 20 * time.Millisecond

	log0 := &offLogConn{Conn: c0a}
	// path2 传播时延 200ms：Write 入队即返回（已记入日志）但送达
	// 在 200ms 之后——杀掉瞬间这些「已发出、在路上、未确认」的段
	// 全部成为待换路重发的在途段。
	log2 := &offLogConn{Conn: newRateDelayConn(c2a, 4<<20, 200*time.Millisecond, 32)}

	sa := newStreamOnPath(id, log0, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()
	if err := sa.attachPath(&path{id: 2, conn: log2, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}

	total := 1 << 20
	want := bytes.Repeat([]byte("kill-mid"), total/8)
	rdone := make(chan []byte, 1)
	go func() {
		buf := &bytes.Buffer{}
		_, _ = io.Copy(buf, io.LimitReader(sb, int64(total)))
		rdone <- buf.Bytes()
	}()
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(want)
		wdone <- err
	}()

	// 等 path2 把数据真正「压在路上」（在传播时延内滞留）再杀：
	// 此刻其名下必有未确认在途段
	eventuallyCond(t, "path2 在路上有数据", func() bool {
		return len(log2.ranges()) > 0
	})
	n0 := len(log0.ranges())
	m2 := len(log2.ranges())
	// 关 sa 侧底层 conn：drainer 写失败退出 + recvLoop EOF →
	// dropPath 把 path2 名下在途段重注入剩余路径
	_ = log2.Conn.Close()
	_ = c2b.Close()

	// 数据全部到达：未确认段经 path0 换路重传补齐
	got := <-rdone
	if !bytes.Equal(got, want) {
		t.Fatal("杀路径后数据丢失或不保序")
	}
	if err := <-wdone; err != nil {
		t.Fatalf("杀路径后 Write 失败: %v", err)
	}
	// 断言 1：path2 分到过且未确认的区间，确有部分在杀后
	// 重现在 path0 上（偏移唯一分配 → 重现即换路重传）
	eventuallyCond(t, "path2 在途段经 path0 重发", func() bool {
		return rangesOverlap(log0.ranges()[n0:], log2.ranges())
	})
	// 断言 2：path2 写量冻结（吞吐降级到 path0），两侧收敛
	if got := len(log2.ranges()); got != m2 {
		t.Fatalf("path2 死后仍在写: kill 时 %d 帧，现在 %d 帧", m2, got)
	}
	eventuallyPaths(t, sa, 1)
	eventuallyPaths(t, sb, 1)
}
