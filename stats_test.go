package netacc

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// errTestKill 是测试里模拟路径传输致死的死因。
var errTestKill = errors.New("test: 模拟路径死亡")

// recvEvent 从订阅通道取一条事件，带超时兜底防测试挂死。
func recvEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("事件通道意外关闭")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("等待事件超时")
		return Event{}
	}
}

// TestStatsSnapshot 验收标准 1：跑一段已知流量后，Stats() 的聚合
// 与逐路径字段量级正确——收发字节数与实写一致、速率/RTT 有样本、
// 缓冲字段非零且方向对。
func TestStatsSnapshot(t *testing.T) {
	sa, sb := pipeStream(t, testCfg(64<<10))
	defer sa.Close()
	defer sb.Close()

	want := bytes.Repeat([]byte("stats-"), 32<<10) // 192KiB
	go func() { _, _ = io.Copy(sb, sb) }()         // b 侧 echo 回 a

	if _, err := sa.Write(want); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("Read 失败: %v", err)
	}

	st := sa.Stats()
	if st.ID != sa.ID() || st.Peer != sa.Peer() {
		t.Fatal("Stats 的 ID/Peer 与流不符")
	}
	if len(st.Paths) != 1 {
		t.Fatalf("应有 1 条路径，得到 %d", len(st.Paths))
	}
	// 发送方向：已装帧 = 写入量；echo 全回后累积确认应推进到等量
	if st.SentBytes != uint64(len(want)) {
		t.Fatalf("SentBytes 应为 %d，得到 %d", len(want), st.SentBytes)
	}
	// 末尾 ACK 与 ReadFull 完成之间有竞态，进轮询断言
	eventuallyCond(t, "AckedBytes 与路径 Delivered 推进到全量", func() bool {
		st := sa.Stats()
		return st.AckedBytes == uint64(len(want)) &&
			st.Paths[0].Delivered == uint64(len(want))
	})
	// 接收方向：对端 echo 回来的量经重排缓冲交付
	if st.RecvCum != uint64(len(want)) {
		t.Fatalf("RecvCum 应为 %d，得到 %d", len(want), st.RecvCum)
	}
	if st.RecvBufCap != 64<<10 || st.SendBufCap != 2*(64<<10) {
		t.Fatalf("缓冲容量与配置不符: recv=%d send=%d", st.RecvBufCap, st.SendBufCap)
	}
	// 全量已读走 + 全部确认后，占用与在途应收尾到 0
	if st.RecvBuffered != 0 || st.RecvReady != 0 || st.PendingBytes != 0 {
		t.Fatalf("收尾后缓冲应清空: %+v", st)
	}

	// 逐路径：发出去的字节被确认（上面已轮询）+ 收端到达字节 =
	// echo 回包量
	p := sa.Stats().Paths[0]
	if p.ID != 0 || !p.Dialed {
		t.Fatalf("首路径标识错误: %+v", p.PathInfo)
	}
	if p.RxBytes != uint64(len(want)) {
		t.Fatalf("路径 RxBytes 应为 %d，得到 %d", len(want), p.RxBytes)
	}
	if p.ResentSegs != 0 || p.ResentBytes != 0 || st.LostSegs != 0 {
		t.Fatalf("无损管道不应有重传: %+v", st)
	}
	// 有流量后 RTT/速率应有实测样本（est_rate 有保鲜期，断言须
	// 在样本新鲜的时间窗内取到新快照——st 是旧快照，别复用）
	eventuallyCond(t, "路径采到 RTT 与速率样本", func() bool {
		cur := sa.Stats()
		ps := cur.Paths[0]
		return ps.SRTT > 0 && ps.MinRTT > 0 && ps.EstRate > 0 && cur.TxRateBps > 0
	})
}

// TestStatsResendCount 验收：路径死亡时其名下在途未确认段改判
// 换路重发（LostSegs），随后由存活路径实际重发出去
// （ResentSegs/ResentBytes，全流与逐路径口径都 > 0）。
//
// 确定性做法：直接在 mu 下伪造一段记在 path2 名下的在途段
// （off 接在 sentOff 之后、字节内容真实——对端把它当正常数据
// 收下，不依赖条带调度恰好把在途量分到 path2 的时序运气）。
func TestStatsResendCount(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 5
	cfg := testCfg(64 << 10)
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

	fake := []byte("resent-bytes")
	sa.mu.Lock()
	p2 := sa.pathsByID[2]
	sa.unackedSegs = append(sa.unackedSegs, sendSeg{off: sa.sentOff, data: fake, path: p2})
	p2.inflight += len(fake)
	sa.sentOff += uint64(len(fake)) // 这些字节视同已下发
	sa.mu.Unlock()

	sa.dropPath(p2, errTestKill, true) // 等价于传输致死：摘除+改判重发
	sa.mu.Lock()
	lost := sa.lostSegs
	sa.mu.Unlock()
	if lost != 1 {
		t.Fatalf("LostSegs 应为 1，得到 %d", lost)
	}

	// 待重发段经存活路径（path0）实际重发：对端应读到原字节
	got := make([]byte, len(fake))
	if _, err := io.ReadFull(sb, got); err != nil {
		t.Fatalf("对端读重发数据失败: %v", err)
	}
	if !bytes.Equal(got, fake) {
		t.Fatalf("重发数据不一致: %q != %q", got, fake)
	}
	eventuallyCond(t, "重传计数记账", func() bool {
		st := sa.Stats()
		return st.ResentSegs >= 1 && st.ResentBytes >= uint64(len(fake)) &&
			len(st.Paths) == 1 && st.Paths[0].ResentSegs >= 1
	})
}

// TestSubscribeAddRemove 验收标准 2：挂路径收 PathAdded，
// 摘除收 PathRemoved（含摘除原因与摘除前快照）。
func TestSubscribeAddRemove(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 6
	cfg := testCfg(16 << 10)
	sa := newStreamOnPath(id, c0a, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()

	ch, unsub := sa.Subscribe(8)
	defer unsub()

	if err := sa.attachPath(&path{id: 2, conn: c2a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}

	ev := recvEvent(t, ch)
	if ev.Type != EventPathAdded || ev.PathID != 2 || ev.Cause != nil {
		t.Fatalf("期望 PathAdded(id=2)，得到 %+v", ev)
	}
	if !ev.Info.Dialed || ev.At.IsZero() {
		t.Fatalf("事件快照/时刻错误: %+v", ev)
	}

	// 主动摘除：Removed 且 Cause 为 nil（非传输错误）
	if err := sa.RemovePath(2); err != nil {
		t.Fatalf("RemovePath 失败: %v", err)
	}
	ev = recvEvent(t, ch)
	if ev.Type != EventPathRemoved || ev.PathID != 2 {
		t.Fatalf("期望 PathRemoved(id=2)，得到 %+v", ev)
	}
	if ev.Cause != nil {
		t.Fatalf("主动摘除的 Cause 应为 nil，得到 %v", ev.Cause)
	}
	if ev.Info.ID != 2 {
		t.Fatalf("Removed 事件应带摘除前快照: %+v", ev.Info)
	}
}

// TestSubscribeKillCause 验收：传输错误致死的路径，Removed 事件
// 的 Cause 保留底层死因（errors.Is 可判定）。
//
// 用 dropPath 直调做确定性验证：net.Pipe 双侧同死时对端也会
// dropPath 并发 PATH_DROP 回来，本侧事件 Cause 是「本地传输错
// 还是远端通知（nil）」取决于竞态——两种取值都合法，e2e 无丢
// 包语义由 TestPipePathKillNoLoss 覆盖，这里只钉死 Cause 透传。
func TestSubscribeKillCause(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	defer c2b.Close()
	var id [16]byte
	id[0] = 7
	cfg := testCfg(16 << 10)
	sa := newStreamOnPath(id, c0a, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()

	ch, unsub := sa.Subscribe(8)
	defer unsub()
	p2 := &path{id: 2, conn: c2a, dialed: true}
	if err := sa.attachPath(p2, nil); err != nil {
		t.Fatal(err)
	}
	recvEvent(t, ch) // 排掉 PathAdded

	sa.dropPath(p2, errTestKill, false)
	ev := recvEvent(t, ch)
	if ev.Type != EventPathRemoved || ev.PathID != 2 {
		t.Fatalf("期望 PathRemoved(id=2)，得到 %+v", ev)
	}
	if !errors.Is(ev.Cause, errTestKill) {
		t.Fatalf("Removed 事件应透传死因，得到 %v", ev.Cause)
	}
}

// TestSubscribeDegraded 验收：「指标曾有效但失联」的路径在有发送
// 需求时被判疑似降级，订阅者收 PathDegraded；失联期间只报一次。
func TestSubscribeDegraded(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	// 排干 c2：sendLoop 的初始窗口通告是同步写，net.Pipe 无人读
	// 会把发送泵永久堵住，onTick 根本不会运行
	frameSink(t, c2)
	var id [16]byte
	id[0] = 8
	cfg := testCfg(16 << 10)
	cfg.telemetryInterval = 20 * time.Millisecond
	s := newStreamOnPath(id, c1, cfg)
	defer s.Close()

	ch, unsub := s.Subscribe(4)
	defer unsub()

	// 伪造「指标曾有效但失联 + 有发送需求」：hasRate 过期 +
	// unackedSegs 非空（needSend 恒真——段不被确认就一直存在）
	s.mu.Lock()
	p := s.paths[0]
	p.hasRate, p.estRate = true, 1<<20
	p.lastRateAt = time.Now().Add(-time.Hour)
	p.hasRTT, p.srtt, p.minRTT = true, time.Second, time.Second
	p.lastRTTAt = time.Now().Add(-time.Hour)
	s.unackedSegs = append(s.unackedSegs, sendSeg{off: 0, data: []byte("x"), path: p})
	s.mu.Unlock()

	ev := recvEvent(t, ch)
	if ev.Type != EventPathDegraded || ev.PathID != 0 {
		t.Fatalf("期望 PathDegraded(id=0)，得到 %+v", ev)
	}
	if ev.Info.ID != 0 {
		t.Fatalf("事件应带路径快照: %+v", ev.Info)
	}

	// 失联状态持续期间不应刷屏：一个 telemetry 周期内无第二条
	select {
	case ev := <-ch:
		t.Fatalf("失联期间不应重复发降级事件，得到 %+v", ev)
	case <-time.After(3 * cfg.telemetryInterval):
	}
}

// TestSubscribeSlowNotBlocking 验收：慢订阅者不拖死数据面——
// 缓冲打满后事件被丢弃，Write/Read 照常完成；且恢复消费后
// 事件经 Dropped 字段告知漏报数。
func TestSubscribeSlowNotBlocking(t *testing.T) {
	c0a, c0b := net.Pipe()
	var id [16]byte
	id[0] = 9
	cfg := testCfg(16 << 10)
	sa := newStreamOnPath(id, c0a, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()

	// 缓冲 1：第一条事件占住，其后事件在无人消费期间全丢
	ch, unsub := sa.Subscribe(1)
	defer unsub()

	c2a, c2b := net.Pipe()
	c4a, c4b := net.Pipe()
	defer c2b.Close()
	defer c4b.Close()
	// attach/摘/再 attach 三个事件：Added(2) 入队，Removed(2)
	// 与 Added(4) 被丢弃（dropped 计 2）。sb 侧同步挂接对应
	// 路径，保证后续 echo 的条带流量在两条路径上都能流通。
	if err := sa.attachPath(&path{id: 2, conn: c2a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sa.RemovePath(2); err != nil {
		t.Fatalf("RemovePath 失败: %v", err)
	}
	if err := sa.attachPath(&path{id: 4, conn: c4a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 4, conn: c4b}, nil); err != nil {
		t.Fatal(err)
	}

	// 丢弃期间数据面照常：echo 全量打通
	go func() { _, _ = io.Copy(sb, sb) }()
	want := bytes.Repeat([]byte("slow-sub"), 8<<10)
	if _, err := sa.Write(want); err != nil {
		t.Fatalf("慢订阅者不应阻塞 Write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("慢订阅者不应阻塞 Read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("echo 数据不一致")
	}

	// 通道里只成功入队了第一条（缓冲 1），Dropped=0
	ev := recvEvent(t, ch)
	if ev.Type != EventPathAdded || ev.PathID != 2 || ev.Dropped != 0 {
		t.Fatalf("首条事件应为 PathAdded(2) 且 Dropped=0: %+v", ev)
	}
	// 缓冲腾空后再来一条事件应投递成功，并携带期间丢弃数
	c6a, c6b := net.Pipe()
	defer c6b.Close()
	if err := sa.attachPath(&path{id: 6, conn: c6a, dialed: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sb.attachPath(&path{id: 6, conn: c6b}, nil); err != nil {
		t.Fatal(err)
	}
	ev = recvEvent(t, ch)
	if ev.Type != EventPathAdded || ev.PathID != 6 || ev.Dropped == 0 {
		t.Fatalf("恢复投递的事件应携带丢弃计数: %+v", ev)
	}
}

// TestSubscribeClose 验收：流终结时订阅通道被关闭（range 收尾）；
// 退订函数幂等，退订后不再收事件。
func TestSubscribeClose(t *testing.T) {
	sa, sb := pipeStream(t, testCfg(8<<10))
	defer sb.Close()

	ch, unsub := sa.Subscribe(4)
	if err := sa.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	// 流已关 → 通道被关闭
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("流关闭后通道应已关闭（不应再收到事件）")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("流关闭后订阅通道未被关闭")
	}
	unsub() // 幂等，不应 panic

	// 已关闭的流上订阅：返回立即关闭的通道
	ch2, _ := sa.Subscribe(4)
	if _, ok := <-ch2; ok {
		t.Fatal("已关闭流上的订阅通道应直接为关闭态")
	}
}

// TestEventTypeString 事件类型名（日志/断言可读性）。
func TestEventTypeString(t *testing.T) {
	for _, tc := range []struct {
		t    EventType
		want string
	}{
		{EventPathAdded, "path_added"},
		{EventPathRemoved, "path_removed"},
		{EventPathDegraded, "path_degraded"},
		{EventType(99), "unknown"},
	} {
		if tc.t.String() != tc.want {
			t.Fatalf("EventType(%d).String() = %q，期望 %q", tc.t, tc.t.String(), tc.want)
		}
	}
}
