package netacc

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// countConn 包装 net.Conn 统计读/写字节数，用于断言条带确实分摊到多条路径。
type countConn struct {
	net.Conn
	wr *atomic.Int64
	rd *atomic.Int64
}

func (c countConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.wr.Add(int64(n))
	return n, err
}

func (c countConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.rd.Add(int64(n))
	return n, err
}

// eventuallyPaths 轮询等待路径数收敛到 n，防异步摘除造成测试抖动。
func eventuallyPaths(t *testing.T, s *Stream, n int) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if len(s.Paths()) == n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("路径数未收敛到 %d，当前 %d", n, len(s.Paths()))
}

// TestPipeStripedTwoPaths 验收：双路径下写方向条带分摊到两条路径，
// 且读方向数据保序完整。
func TestPipeStripedTwoPaths(t *testing.T) {
	var w0, w2 atomic.Int64 // a 侧两条路径各自的写出字节数
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 1
	cfg := testCfg(16 << 10)
	sa := newStreamOnPath(id, countConn{c0a, &w0, &atomic.Int64{}}, cfg)
	sb := newStreamOnPath(id, c0b, cfg)
	defer sa.Close()
	defer sb.Close()

	// 直接挂第二条路径（管道注入，绕过 PATH_ATTACH 协议——
	// 协议本身由 host 级测试覆盖）
	if err := sa.attachPath(&path{id: 2, conn: countConn{c2a, &w2, &atomic.Int64{}}, dialed: true}, nil); err != nil {
		t.Fatalf("挂第二条路径失败: %v", err)
	}
	if err := sb.attachPath(&path{id: 2, conn: c2b}, nil); err != nil {
		t.Fatalf("对端挂第二条路径失败: %v", err)
	}
	if n := len(sa.Paths()); n != 2 {
		t.Fatalf("sa 应有 2 条路径，得到 %d", n)
	}
	eventuallyPaths(t, sb, 2)

	want := bytes.Repeat([]byte("stripe-me"), 40<<10) // ~360KiB，跨多帧
	go func() {
		_, _ = io.Copy(sb, sb) // echo 回 sa
	}()
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(want)
		wdone <- err
	}()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if err := <-wdone; err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("双路径 echo 数据不一致")
	}
	if w0.Load() == 0 || w2.Load() == 0 {
		t.Fatalf("条带未分摊：path0 写 %d 字节，path2 写 %d 字节", w0.Load(), w2.Load())
	}
}

// TestPipePathKillNoLoss 验收：传输中杀掉一条路径，流不中断、
// 在途未确认数据经剩余路径重发不丢失，且两侧路径集都收敛摘除。
func TestPipePathKillNoLoss(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 2
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

	want := bytes.Repeat([]byte("survive-kill"), 32<<10) // ~384KiB
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(want)
		wdone <- err
	}()

	// 收到一部分数据后杀掉第二条路径（ abrupt：直接关底层管道 ）
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sb, got[:32<<10]); err != nil {
		t.Fatalf("Read 前缀失败: %v", err)
	}
	_ = c2a.Close() // sa 侧路径 conn 死亡 → sa 摘除 + PATH_DROP → sb 摘除
	_ = c2b.Close()

	if _, err := io.ReadFull(sb, got[32<<10:]); err != nil {
		t.Fatalf("杀路径后 Read 失败: %v", err)
	}
	if err := <-wdone; err != nil {
		t.Fatalf("杀路径后 Write 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("杀路径后数据丢失或不保序")
	}
	eventuallyPaths(t, sa, 1)
	eventuallyPaths(t, sb, 1)
	if sa.Paths()[0].ID != 0 || sb.Paths()[0].ID != 0 {
		t.Fatal("收敛后应只剩首条路径")
	}
}

// TestPipeRemovePath 验收：RemovePath 主动摘除并在对端同步摘除
// （PATH_DROP 带内通知），其余路径不受影响。
func TestPipeRemovePath(t *testing.T) {
	c0a, c0b := net.Pipe()
	c2a, c2b := net.Pipe()
	var id [16]byte
	id[0] = 3
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
	eventuallyPaths(t, sb, 2)

	if err := sa.RemovePath(2); err != nil {
		t.Fatalf("RemovePath 失败: %v", err)
	}
	eventuallyPaths(t, sa, 1)
	eventuallyPaths(t, sb, 1) // PATH_DROP 带内到达

	// 未知 id 与「最后一条路径」都应报错
	if err := sa.RemovePath(2); err == nil {
		t.Fatal("对已删 path_id 再 RemovePath 应报错")
	}
	if err := sa.RemovePath(0); err == nil {
		t.Fatal("摘除最后一条路径应被拒绝")
	}

	// 剩余路径照常收发
	if _, err := sa.Write([]byte("still-alive")); err != nil {
		t.Fatalf("摘除后 Write 失败: %v", err)
	}
	got := make([]byte, 11)
	if _, err := io.ReadFull(sb, got); err != nil {
		t.Fatalf("摘除后 Read 失败: %v", err)
	}
	if string(got) != "still-alive" {
		t.Fatalf("摘除后数据不一致: %q", got)
	}
}

// TestStaleAckDropped 回归测试（#19 实测死锁）：多路径下 ACK 会跨
// 路径超车——生成早的 ACK 晚到时不得覆盖较新的通告窗口，否则发送端
// 视角的窗口被回退成过期值（如 win=0）永久停摆。按 seq 只应用最新。
func TestStaleAckDropped(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	var id [16]byte
	s := newStreamOnPath(id, c1, testCfg(16<<10))
	defer s.Close()

	sink := frameSink(t, c2)
	recvFrame(t, sink) // 初始窗口通告

	// 注入新 ACK（seq=9，窗口大）再注入旧 ACK（seq=3，窗口 0）——
	// 模拟旧帧在慢路径上晚到
	if _, err := c2.Write(appendAckFrame(nil, 0, 0, 16384, 9, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Write(appendAckFrame(nil, 0, 0, 0, 3, nil)); err != nil {
		t.Fatal(err)
	}
	// 等两帧都被处理：用第三轮合法 ACK(seq=10) 做栅栏——它必然在
	// 前两帧之后被应用
	if _, err := c2.Write(appendAckFrame(nil, 0, 0, 8192, 10, nil)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		w, seq := s.advWindow, s.peerAckSeq
		s.mu.Unlock()
		if seq == 10 {
			if w != 8192 {
				t.Fatalf("过期 ACK 覆盖了新窗口: advWindow=%d", w)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ACK 未被处理: seq=%d win=%d", seq, w)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestReserveRemotePathID 单测分侧命名空间校验：奇偶位 + 拒绝复用。
func TestReserveRemotePathID(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	var id [16]byte
	// 接收方视角：remoteParity=0（对端为发起方，偶数命名空间），
	// 握手路径已占用对端 id 0。
	s := newStreamFull(id, c1, testCfg(4<<10), nil, "", false)
	defer s.Close()

	if !s.reserveRemotePathID(2) {
		t.Fatal("对端偶数 id 2 应被接受")
	}
	if s.reserveRemotePathID(2) {
		t.Fatal("同一 id 不应被重复接受")
	}
	if s.reserveRemotePathID(3) {
		t.Fatal("奇数 id 属本侧命名空间，应被拒绝")
	}
	if s.reserveRemotePathID(0) {
		t.Fatal("握手路径 id 0 已占用，不应再接受")
	}
}
