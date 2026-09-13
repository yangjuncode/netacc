package netacc_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/yangjuncode/netacc"
)

// newTestHost 建一个监听 127.0.0.1 随机 TCP 端口的 libp2p host。
func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("创建 host 失败: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// connectHosts 让 a 与 b 建立 swarm 连接（peerstore 喂地址后 Connect）。
func connectHosts(t *testing.T, a, b host.Host) {
	t.Helper()
	a.Peerstore().AddAddrs(b.ID(), b.Addrs(), peerstore.PermanentAddrTTL)
	if err := a.Connect(context.Background(), peer.AddrInfo{ID: b.ID()}); err != nil {
		t.Fatalf("连接 host 失败: %v", err)
	}
}

// openPair 建一对聚合流：a 侧 OpenStream，b 侧 Accept。
func openPair(t *testing.T, agga *netacc.Aggregator, aggb *netacc.Aggregator, hb host.Host) (sa, sb *netacc.Stream) {
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

	sa, err := agga.OpenStream(ctx, hb.ID())
	if err != nil {
		t.Fatalf("OpenStream 失败: %v", err)
	}
	r := <-ach
	if r.err != nil {
		t.Fatalf("Accept 失败: %v", r.err)
	}
	return sa, r.s
}

// TestOpenAcceptHandshakeID 验收：两端握手协商出一致的 128bit agg_stream_id。
func TestOpenAcceptHandshakeID(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)

	if sa.ID() == ([16]byte{}) {
		t.Fatal("agg_stream_id 不应为全零")
	}
	if sa.ID() != sb.ID() {
		t.Fatalf("两端 agg_stream_id 不一致: %x != %x", sa.ID(), sb.ID())
	}
	if sa.Peer() != hb.ID() || sb.Peer() != ha.ID() {
		t.Fatal("Peer() 返回的对端 ID 不正确")
	}
}

// TestEcho 验收：聚合流实现 net.Conn 语义，单路径透传 echo 双向打通。
func TestEcho(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	var _ net.Conn = sa // 编译期确认 net.Conn 实现

	// b 侧做 echo 服务
	go func() {
		defer sb.Close()
		_, _ = io.Copy(sb, sb)
	}()

	// a→b→a echo：多写几轮，验证流式收发
	msgs := [][]byte{
		[]byte("hello netacc"),
		make([]byte, 64<<10), // 64KiB 全零块，验证大块透传
		[]byte("再见"),
	}
	msgs[1][0] = 0xAB
	msgs[1][len(msgs[1])-1] = 0xCD
	for i, want := range msgs {
		if _, err := sa.Write(want); err != nil {
			t.Fatalf("第 %d 轮 Write 失败: %v", i, err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(sa, got); err != nil {
			t.Fatalf("第 %d 轮 Read 失败: %v", i, err)
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("第 %d 轮 echo 数据不一致 at %d: %x != %x", i, j, got[j], want[j])
			}
		}
	}

	// Close：写端关闭后对端读到 EOF
	if err := sa.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
}

// TestReadDeadline 验收：SetReadDeadline 生效，超时返回 net.Error timeout。
func TestReadDeadline(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, _ := openPair(t, agga, aggb, hb)

	if err := sa.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline 失败: %v", err)
	}
	_, err := sa.Read(make([]byte, 1))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("期望超时错误，得到: %v", err)
	}

	// 同时验证 SetDeadline / LocalAddr / RemoteAddr 可用
	if err := sa.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetDeadline 失败: %v", err)
	}
	if sa.LocalAddr() == nil || sa.RemoteAddr() == nil {
		t.Fatal("LocalAddr/RemoteAddr 不应为 nil")
	}
}

// TestOpenStreamToPeerWithoutNetacc：对端未加载本协议时 OpenStream 必须报错
// （握手失败而不是静默拿到一条坏流）。
func TestOpenStreamToPeerWithoutNetacc(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t) // hb 上没有 Aggregator
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
}

// TestAcceptContextCancel：无入流时 Accept 应响应 ctx 取消。
func TestAcceptContextCancel(t *testing.T) {
	h := newTestHost(t)
	agg := netacc.New(h)
	defer agg.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := agg.Accept(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望 context.DeadlineExceeded，得到: %v", err)
	}
}

// TestDoubleOpen 两条聚合流并行存在、各自独立（同一 Aggregator 多次 OpenStream）。
func TestDoubleOpen(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa1, sb1 := openPair(t, agga, aggb, hb)
	sa2, sb2 := openPair(t, agga, aggb, hb)

	if sa1.ID() == sa2.ID() {
		t.Fatal("两条聚合流的 agg_stream_id 应不同（128bit 随机）")
	}
	if _, err := sa2.Write([]byte("x")); err != nil {
		t.Fatalf("第二条流 Write 失败: %v", err)
	}
	if _, err := sb2.Read(make([]byte, 1)); err != nil {
		t.Fatalf("第二条流 Read 失败: %v", err)
	}
	_ = sb1
}
