package netacc

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// testCfg 返回小容量配置，便于在测试中快速触发窗口背压。
func testCfg(bufCap int) streamConfig {
	return streamConfig{minBuf: bufCap, maxBuf: bufCap, sendBufCap: 2 * bufCap}
}

// pipeStream 建一对挂在 net.Pipe 两端的 Stream。
func pipeStream(t *testing.T, cfg streamConfig) (a, b *Stream) {
	t.Helper()
	c1, c2 := net.Pipe()
	var id [16]byte
	id[0] = 1
	return newStreamOnPath(id, c1, cfg), newStreamOnPath(id, c2, cfg)
}

// frameSink 在后台持续解码 conn 上的帧并投递到 channel（喂测试断言）。
func frameSink(t *testing.T, conn net.Conn) <-chan frame {
	t.Helper()
	ch := make(chan frame, 64)
	go func() {
		fr := newFrameReader(conn)
		for {
			_, f, err := fr.next()
			if err != nil {
				close(ch)
				return
			}
			ch <- f
		}
	}()
	return ch
}

// recvFrame 从 frameSink 取一帧，带超时兜底防测试挂死。
func recvFrame(t *testing.T, ch <-chan frame) frame {
	t.Helper()
	select {
	case f, ok := <-ch:
		if !ok {
			t.Fatal("帧流意外结束")
		}
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("等待帧超时")
		return frame{}
	}
}

// TestPipeEcho 双 Stream 挂 net.Pipe：数据跨窗口多轮打通，
// 写出量大于接收窗口容量以强制走窗口背压路径。
func TestPipeEcho(t *testing.T) {
	sa, sb := pipeStream(t, testCfg(16<<10))
	defer sa.Close()
	defer sb.Close()

	want := bytes.Repeat([]byte("netacc-frames"), 8192) // ~104KiB > 窗口 16KiB
	go func() {
		// b 侧 echo
		_, _ = io.Copy(sb, sb)
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
		t.Fatal("echo 数据不一致")
	}
}

// TestPipeOutOfOrder 验收标准 2 的 Stream 级验证：
// 对端以乱序注入 DATA 帧，Read 必须输出保序字节流。
func TestPipeOutOfOrder(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	var id [16]byte
	s := newStreamOnPath(id, c1, testCfg(16<<10))
	defer s.Close()

	sink := frameSink(t, c2) // 排干 s 发出的通告/ACK
	recvFrame(t, sink)       // 初始窗口通告

	// 逆序注入四段：[30,40) [20,30) [10,20) [0,10)
	payload := []byte("0123456789abcdefghij0123456789ABCDEFGHIJ")
	for off := 30; off >= 0; off -= 10 {
		if _, err := c2.Write(appendDataFrame(nil, uint64(off), 1, payload[off:off+10])); err != nil {
			t.Fatalf("注入 DATA 帧失败: %v", err)
		}
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(s, got); err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("乱序注入后输出不保序: %q", got)
	}
	// 排干 ACK：ackCh cap1 去抖，条数不保证与 DATA 一一对应；
	// 只需见到一条 cum=40 的 ACK 即证明补洞后累积确认推进。
	for {
		if f := recvFrame(t, sink); f.cum == uint64(len(payload)) {
			break
		}
	}
}

// TestAckFields 验收标准 4：ACK 帧携带时间戳回显与窗口通告。
func TestAckFields(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	var id [16]byte
	cap := 16 << 10
	s := newStreamOnPath(id, c1, testCfg(cap))
	defer s.Close()

	sink := frameSink(t, c2)

	// 初始窗口通告：window = 满容量，ts_echo = 0（未收过数据）
	ann := recvFrame(t, sink)
	if ann.window != uint64(cap) || ann.cum != 0 || ann.tsEcho != 0 {
		t.Fatalf("初始通告字段错误: %+v", ann)
	}

	// 对端发一帧带 ts=7777 的 DATA
	data := []byte("hello-ack")
	if _, err := c2.Write(appendDataFrame(nil, 0, 7777, data)); err != nil {
		t.Fatalf("写 DATA 失败: %v", err)
	}
	ack := recvFrame(t, sink)
	if ack.tsEcho != 7777 {
		t.Fatalf("ACK 应回显 DATA 时间戳 7777，得到 %d", ack.tsEcho)
	}
	if ack.cum != uint64(len(data)) {
		t.Fatalf("ACK cum 应为 %d，得到 %d", len(data), ack.cum)
	}
	if ack.window != uint64(cap-len(data)) {
		t.Fatalf("ACK window 应为 %d，得到 %d", cap-len(data), ack.window)
	}
	if len(ack.ranges) != 0 {
		t.Fatalf("顺序到达不应有 SACK ranges，得到 %+v", ack.ranges)
	}

	// 乱序注入 [100,110)：cum 不动，ranges 应报告该洞后段
	if _, err := c2.Write(appendDataFrame(nil, 100, 8888, make([]byte, 10))); err != nil {
		t.Fatalf("写 DATA 失败: %v", err)
	}
	ack = recvFrame(t, sink)
	if ack.cum != uint64(len(data)) || len(ack.ranges) != 1 ||
		ack.ranges[0] != (byteRange{100, 110}) || ack.tsEcho != 8888 {
		t.Fatalf("乱序 ACK 字段错误: %+v", ack)
	}
}

// TestWindowBackpressure 验收标准 3：重排缓冲满 → 通告窗口收 0 →
// Write 被背压停发；对端 Read 释放窗口后 Write 继续。
func TestWindowBackpressure(t *testing.T) {
	cap := 8 << 10
	sa, sb := pipeStream(t, streamConfig{minBuf: cap, maxBuf: cap, sendBufCap: 2 * cap})
	defer sa.Close()
	defer sb.Close()

	total := 6 * cap // 写出量远大于窗口+发送缓冲
	wdone := make(chan int, 1)
	go func() {
		n, err := sa.Write(bytes.Repeat([]byte{0x5a}, total))
		if err != nil {
			t.Errorf("Write 失败: %v", err)
		}
		wdone <- n
	}()

	// 窗口满 + 发送缓冲满后 Write 必须停住（100ms 内不应完成）
	select {
	case n := <-wdone:
		t.Fatalf("窗口满时 Write 不应完成，却返回 n=%d", n)
	case <-time.After(100 * time.Millisecond):
	}

	// 对端读走数据 → 窗口打开 → Write 推进直至完成
	got := make([]byte, total)
	if _, err := io.ReadFull(sb, got); err != nil {
		t.Fatalf("对端 Read 失败: %v", err)
	}
	select {
	case n := <-wdone:
		if n != total {
			t.Fatalf("Write 应返回 %d，得到 %d", total, n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("对端读空后 Write 仍未完成")
	}
}

// TestWriteDeadlineWhileBlocked 写阻塞在窗口背压上时，
// SetWriteDeadline 到期必须返回 net.Error timeout（回归 net.Conn 语义）。
func TestWriteDeadlineWhileBlocked(t *testing.T) {
	cap := 8 << 10
	sa, _ := pipeStream(t, streamConfig{minBuf: cap, maxBuf: cap, sendBufCap: cap})
	defer sa.Close()

	// 对端不 Read：窗口很快收 0，发送缓冲随即占满
	if err := sa.SetWriteDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline 失败: %v", err)
	}
	start := time.Now()
	n, err := sa.Write(bytes.Repeat([]byte{0x7b}, 16*cap))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("期望 net.Error timeout，得到 n=%d err=%v", n, err)
	}
	if n <= 0 || n >= 16*cap {
		t.Fatalf("超时应返回部分写入进度，得到 n=%d", n)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("Write 超时响应过慢")
	}
}

// TestReadDeadlinePipe 读侧 deadline：无数据时 Read 超时返回 net.Error。
func TestReadDeadlinePipe(t *testing.T) {
	sa, _ := pipeStream(t, testCfg(8<<10))
	defer sa.Close()

	if err := sa.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline 失败: %v", err)
	}
	_, err := sa.Read(make([]byte, 1))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("期望 net.Error timeout，得到: %v", err)
	}
}

// TestCloseEOF 对端 Close → 本端读尽残余数据后得到 io.EOF。
func TestCloseEOF(t *testing.T) {
	sa, sb := pipeStream(t, testCfg(8<<10))
	defer sb.Close()

	want := []byte("bye")
	done := make(chan struct{})
	go func() {
		_, _ = sa.Write(want)
		time.Sleep(20 * time.Millisecond) // 让数据先落地再关
		_ = sa.Close()
		close(done)
	}()

	got := make([]byte, len(want))
	if _, err := io.ReadFull(sb, got); err != nil {
		t.Fatalf("Read 残余数据失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("残余数据不一致")
	}
	if _, err := sb.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("对端关闭后 Read 应返回 io.EOF，得到 %v", err)
	}
	<-done
}
