package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/yangjuncode/netacc"
)

// ---------- 测试辅助 ----------

// fakeOpener 满足 StreamOpener：OpenStream 委给 fn。
// 构造校验测试里 fn 不会被调用，可为 nil。
type fakeOpener struct {
	fn func(ctx context.Context, p peer.ID, opts ...netacc.OpenOption) (*netacc.Stream, error)
}

func (f fakeOpener) OpenStream(ctx context.Context, p peer.ID, opts ...netacc.OpenOption) (*netacc.Stream, error) {
	return f.fn(ctx, p, opts...)
}

// fakeAccepter 满足 StreamAccepter：Accept 委给 fn。
type fakeAccepter struct {
	fn func(ctx context.Context) (*netacc.Stream, error)
}

func (f fakeAccepter) Accept(ctx context.Context) (*netacc.Stream, error) {
	return f.fn(ctx)
}

// pipeOpener 返回 open 函数：每次调用造一条 net.Pipe，客户端侧
// 交给 tunnel，另一端塞进 out channel 由测试扮演「聚合流对端」。
func pipeOpener(out chan<- net.Conn) func(context.Context) (net.Conn, error) {
	return func(context.Context) (net.Conn, error) {
		a, b := net.Pipe()
		out <- b
		return a, nil
	}
}

// chanAccepter 返回 accept 函数：从 ch 取测试喂入的流；ctx 取消时
// 返回 ctx.Err()——与 Aggregator.Accept 的取消语义一致。
func chanAccepter(ch <-chan net.Conn) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		select {
		case st := <-ch:
			return st, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// echoUpstream 起一个「先发 banner 再 echo」的上游 TCP 服务，
// 返回其地址；测试结束自动关闭。banner 验证「上游→流」方向，
// echo 验证「流→上游→流」回路。
func echoUpstream(t *testing.T, banner string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("起上游监听失败: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				if banner != "" {
					if _, err := c.Write([]byte(banner)); err != nil {
						return
					}
				}
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

// runClient 后台跑 Client.Run 并返回结果通道；测试结束自动 cancel
// 并断言 ctx 取消下 Run 返回 nil。
func runClient(t *testing.T, cli *Client) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cli.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("ctx 取消下 Run 应返回 nil，得到: %v", err)
		}
	})
	return done
}

// runServer 同 runClient 的 server 版。
func runServer(t *testing.T, srv *Server) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("ctx 取消下 Run 应返回 nil，得到: %v", err)
		}
	})
	return done
}

// waitClientAddr 等 Client.Run 把 listener 就位后返回实际监听地址。
func waitClientAddr(t *testing.T, cli *Client) net.Addr {
	t.Helper()
	for i := 0; i < 300; i++ {
		if a := cli.Addr(); a != nil {
			return a
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Client 监听迟迟未就绪")
	return nil
}

// recvStream 从 channel 收一条流，超时即失败；what 是失败信息前缀。
func recvStream(t *testing.T, ch <-chan net.Conn, what string) net.Conn {
	t.Helper()
	select {
	case st := <-ch:
		return st
	case <-time.After(3 * time.Second):
		t.Fatalf("%s：超时未等到聚合流", what)
		return nil
	}
}

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

// ---------- 构造参数校验 ----------

// TestNewClientValidation：nil opener、空 peer.ID、缺/非法监听端口
// 都应在 NewClient 阶段报错；port=0（内核分配）合法。
func TestNewClientValidation(t *testing.T) {
	opener := fakeOpener{}
	target := peer.ID("serverPeer")

	if _, err := NewClient(nil, target, WithListenPort(8080)); err == nil {
		t.Fatal("nil StreamOpener 应报错")
	}
	if _, err := NewClient(opener, "", WithListenPort(8080)); err == nil {
		t.Fatal("空 peer.ID 应报错")
	}
	if _, err := NewClient(opener, target); err == nil {
		t.Fatal("缺 WithListenPort 应报错")
	}
	for _, p := range []int{-1, -8080, 65536, 1 << 20} {
		if _, err := NewClient(opener, target, WithListenPort(p)); err == nil {
			t.Fatalf("非法端口 %d 应报错", p)
		}
	}
	for _, p := range []int{0, 1, 8080, 65535} {
		if _, err := NewClient(opener, target, WithListenPort(p)); err != nil {
			t.Fatalf("合法端口 %d 不应报错: %v", p, err)
		}
	}
}

// TestNewServerValidation：nil accepter、缺/非法 upstream 都应在
// NewServer 阶段报错。
func TestNewServerValidation(t *testing.T) {
	accepter := fakeAccepter{}

	if _, err := NewServer(nil, WithUpstream("127.0.0.1:80")); err == nil {
		t.Fatal("nil StreamAccepter 应报错")
	}
	if _, err := NewServer(accepter); err == nil {
		t.Fatal("缺 WithUpstream 应报错")
	}
	bad := []string{
		"", "127.0.0.1", "127.0.0.1:", ":8080", // 缺段
		"127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:-1", // 端口越界
		"127.0.0.1:abc", "a:b:c", // 非数字端口/形态错误
	}
	for _, addr := range bad {
		if _, err := NewServer(accepter, WithUpstream(addr)); err == nil {
			t.Fatalf("非法上游 %q 应报错", addr)
		}
	}
	for _, addr := range []string{"127.0.0.1:80", "example.com:443", "[::1]:8080"} {
		if _, err := NewServer(accepter, WithUpstream(addr)); err != nil {
			t.Fatalf("合法上游 %q 不应报错: %v", addr, err)
		}
	}
}

// TestClientOpenOptionsPassthrough：WithOpenOptions 的逐调用选项与
// target peer 应原样透传给 StreamOpener.OpenStream（走公开构造，
// 覆盖函数适配层）。
func TestClientOpenOptionsPassthrough(t *testing.T) {
	type call struct {
		p    peer.ID
		opts []netacc.OpenOption
	}
	got := make(chan call, 1)
	opener := fakeOpener{fn: func(_ context.Context, p peer.ID, opts ...netacc.OpenOption) (*netacc.Stream, error) {
		got <- call{p, opts}
		return nil, errors.New("建流失败（本测试只关心透传参数）")
	}}

	target := peer.ID("serverPeer")
	cli, err := NewClient(opener, target, WithListenPort(0),
		WithOpenOptions(netacc.WithMinPaths(2), netacc.WithHandshakeTimeout(5*time.Second)))
	if err != nil {
		t.Fatalf("NewClient 失败: %v", err)
	}
	runClient(t, cli)

	conn, err := net.Dial("tcp", waitClientAddr(t, cli).String())
	if err != nil {
		t.Fatalf("拨本地监听失败: %v", err)
	}
	defer conn.Close()

	select {
	case c := <-got:
		if c.p != target {
			t.Fatalf("OpenStream 的 peer 应为 %s，得到 %s", target, c.p)
		}
		if len(c.opts) != 2 {
			t.Fatalf("应透传 2 个 OpenOption，得到 %d", len(c.opts))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TCP 连接未触发 OpenStream")
	}
}

// ---------- Client 行为 ----------

// TestClientTCPToStream：一条本地 TCP 连接触发一次建流，
// TCP↔聚合流 双向字节透传正确。
func TestClientTCPToStream(t *testing.T) {
	streams := make(chan net.Conn, 4)
	cli := newClient(pipeOpener(streams), 0)
	runClient(t, cli)

	conn, err := net.Dial("tcp", waitClientAddr(t, cli).String())
	if err != nil {
		t.Fatalf("拨本地监听失败: %v", err)
	}
	defer conn.Close()

	st := recvStream(t, streams, "TCP 连接应触发建流")

	// TCP → 聚合流
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("TCP 写失败: %v", err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(st, got); err != nil {
		t.Fatalf("聚合流读失败: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("TCP→流方向数据错误: %q", got)
	}

	// 聚合流 → TCP
	if _, err := st.Write([]byte("world")); err != nil {
		t.Fatalf("流写失败: %v", err)
	}
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("TCP 读失败: %v", err)
	}
	if string(got) != "world" {
		t.Fatalf("流→TCP方向数据错误: %q", got)
	}
}

// TestClientConcurrentStreams：两条并发 TCP 连接各开一条聚合流，
// 数据互不串流。
func TestClientConcurrentStreams(t *testing.T) {
	streams := make(chan net.Conn, 4)
	cli := newClient(pipeOpener(streams), 0)
	runClient(t, cli)

	addr := waitClientAddr(t, cli).String()
	conns := make([]net.Conn, 2)
	sts := make([]net.Conn, 2)
	for i := range conns {
		var err error
		conns[i], err = net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("第 %d 条 TCP 拨号失败: %v", i, err)
		}
		defer conns[i].Close()
		sts[i] = recvStream(t, streams, fmt.Sprintf("第 %d 条连接应触发建流", i))
	}
	if sts[0] == sts[1] {
		t.Fatal("两条 TCP 连接应对应两条不同的聚合流")
	}

	// 两条连接各发各的数据，各自独立回程
	for i := range conns {
		msg := []byte{'A' + byte(i), 'x'}
		if _, err := conns[i].Write(msg); err != nil {
			t.Fatalf("连接 %d 写失败: %v", i, err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(sts[i], got); err != nil {
			t.Fatalf("流 %d 读失败: %v", i, err)
		}
		if got[0] != msg[0] {
			t.Fatalf("连接 %d 数据串流: %q", i, got)
		}

		reply := []byte{'a' + byte(i)}
		if _, err := sts[i].Write(reply); err != nil {
			t.Fatalf("流 %d 写失败: %v", i, err)
		}
		one := make([]byte, 1)
		if _, err := io.ReadFull(conns[i], one); err != nil {
			t.Fatalf("连接 %d 读失败: %v", i, err)
		}
		if one[0] != reply[0] {
			t.Fatalf("连接 %d 回程数据错误: %q", i, one)
		}
	}
}

// TestClientOpenStreamFailure：OpenStream 失败只断开该条 TCP 连接，
// 监听循环继续服务后续连接。
func TestClientOpenStreamFailure(t *testing.T) {
	streams := make(chan net.Conn, 4)
	var calls atomic.Int32
	open := func(context.Context) (net.Conn, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("建流失败（对端不在线）")
		}
		a, b := net.Pipe()
		streams <- b
		return a, nil
	}
	cli := newClient(open, 0)
	runClient(t, cli)
	addr := waitClientAddr(t, cli).String()

	// 第一条：open 失败 → TCP 连接应被关闭
	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("第一条拨号失败: %v", err)
	}
	defer c1.Close()
	_ = c1.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := c1.Read(make([]byte, 1)); err == nil {
		t.Fatal("OpenStream 失败后 TCP 连接应被关闭")
	}

	// 第二条：正常建流并完成一次往返，证明监听循环没被拖死
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("第二条拨号失败: %v", err)
	}
	defer c2.Close()
	st := recvStream(t, streams, "第二条连接应触发建流")
	if _, err := c2.Write([]byte("ok")); err != nil {
		t.Fatalf("第二条连接写失败: %v", err)
	}
	got := make([]byte, 2)
	if _, err := io.ReadFull(st, got); err != nil || string(got) != "ok" {
		t.Fatalf("第二条连接转发异常: got=%q err=%v", got, err)
	}
}

// TestClientCancelClosesConns：ctx 取消后 Run 返回 nil，
// 活动 TCP 连接与聚合流都被主动关闭，Addr 回到 nil。
func TestClientCancelClosesConns(t *testing.T) {
	streams := make(chan net.Conn, 4)
	cli := newClient(pipeOpener(streams), 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cli.Run(ctx) }()

	conn, err := net.Dial("tcp", waitClientAddr(t, cli).String())
	if err != nil {
		t.Fatalf("拨本地监听失败: %v", err)
	}
	defer conn.Close()
	st := recvStream(t, streams, "TCP 连接应触发建流")
	defer st.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ctx 取消下 Run 应返回 nil，得到: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ctx 取消后 Run 未及时返回")
	}

	// 活动连接应已被主动关闭：TCP 侧读出错/EOF，聚合流侧读写报错
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("ctx 取消后 TCP 连接应被关闭")
	}
	if _, err := st.Read(make([]byte, 1)); err == nil {
		t.Fatal("ctx 取消后聚合流应被关闭")
	}
	if a := cli.Addr(); a != nil {
		t.Fatalf("Run 退出后 Addr 应为 nil，得到 %v", a)
	}
}

// TestClientEOFPropagates：聚合流一侧 EOF 后整条映射终结
// （不支持半关闭：TCP 侧也被关闭）。
func TestClientEOFPropagates(t *testing.T) {
	streams := make(chan net.Conn, 4)
	cli := newClient(pipeOpener(streams), 0)
	runClient(t, cli)

	conn, err := net.Dial("tcp", waitClientAddr(t, cli).String())
	if err != nil {
		t.Fatalf("拨本地监听失败: %v", err)
	}
	defer conn.Close()
	st := recvStream(t, streams, "TCP 连接应触发建流")

	// 对端关掉聚合流 → bridge 终结 → TCP 连接也应被关闭
	if err := st.Close(); err != nil {
		t.Fatalf("关闭流端失败: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("聚合流 EOF 后 TCP 连接应被关闭（不支持半关闭）")
	}
}

// ---------- Server 行为 ----------

// TestServerStreamToUpstream：被 Accept 的聚合流桥接到固定上游——
// banner 验证「上游→流」，echo 验证「流→上游→流」回路。
func TestServerStreamToUpstream(t *testing.T) {
	upstream := echoUpstream(t, "BANNER\n")
	acceptCh := make(chan net.Conn, 4)
	srv := newServer(chanAccepter(acceptCh), upstream)
	runServer(t, srv)

	a, b := net.Pipe()
	defer b.Close()
	acceptCh <- a // server 侧持有 a；测试侧持有 b 扮演 client 端流

	// banner：上游→流方向
	banner := make([]byte, len("BANNER\n"))
	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(b, banner); err != nil {
		t.Fatalf("读上游 banner 失败: %v", err)
	}
	if string(banner) != "BANNER\n" {
		t.Fatalf("banner 内容错误: %q", banner)
	}

	// echo：流→上游→流
	if _, err := b.Write([]byte("ping")); err != nil {
		t.Fatalf("流写失败: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(b, got); err != nil {
		t.Fatalf("读 echo 失败: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("echo 数据错误: %q", got)
	}
}

// TestServerConcurrentStreams：多条聚合流各自拨上游、互不干扰。
func TestServerConcurrentStreams(t *testing.T) {
	upstream := echoUpstream(t, "")
	acceptCh := make(chan net.Conn, 4)
	srv := newServer(chanAccepter(acceptCh), upstream)
	runServer(t, srv)

	var wgCh = make(chan net.Conn, 2)
	for i := 0; i < 2; i++ {
		a, b := net.Pipe()
		acceptCh <- a
		wgCh <- b
	}
	for i := 0; i < 2; i++ {
		b := <-wgCh
		defer b.Close()
		msg := []byte{'0' + byte(i)}
		_ = b.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := b.Write(msg); err != nil {
			t.Fatalf("流 %d 写失败: %v", i, err)
		}
		got := make([]byte, 1)
		if _, err := io.ReadFull(b, got); err != nil {
			t.Fatalf("流 %d 读 echo 失败: %v", i, err)
		}
		if got[0] != msg[0] {
			t.Fatalf("流 %d echo 数据错误: %q", i, got)
		}
	}
}

// TestServerUpstreamDialFailure：上游拨不通只关本条聚合流，
// Accept 循环继续（第二条流同样走到拨号并失败），Run 不退出。
func TestServerUpstreamDialFailure(t *testing.T) {
	// 拿一个「曾经可用、现已关闭」的端口保证拨号被拒
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("起临时监听失败: %v", err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()

	acceptCh := make(chan net.Conn, 4)
	srv := newServer(chanAccepter(acceptCh), dead)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("ctx 取消下 Run 应返回 nil，得到: %v", err)
		}
	}()

	// 连喂两条流：都应被「拨上游失败 → 关闭」，证明 Accept 循环存活
	for i := 0; i < 2; i++ {
		a, b := net.Pipe()
		acceptCh <- a
		_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := b.Read(make([]byte, 1)); err == nil {
			t.Fatalf("第 %d 条流：上游拨失败后聚合流应被关闭", i)
		}
		_ = b.Close()
	}

	// Run 应仍在运行（没有因拨号失败退出）
	select {
	case err := <-done:
		t.Fatalf("上游拨号失败不应让 Run 退出: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestServerCancelClosesStreams：ctx 取消后 Run 返回 nil，
// 活动聚合流与上游连接都被主动关闭。
func TestServerCancelClosesStreams(t *testing.T) {
	upstream := echoUpstream(t, "")
	acceptCh := make(chan net.Conn, 4)
	srv := newServer(chanAccepter(acceptCh), upstream)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	a, b := net.Pipe()
	defer b.Close()
	acceptCh <- a

	// 先做一次 echo 往返，确认桥已建立、上游连接已在活动集中
	_ = b.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := b.Write([]byte("x")); err != nil {
		t.Fatalf("流写失败: %v", err)
	}
	if _, err := io.ReadFull(b, make([]byte, 1)); err != nil {
		t.Fatalf("读 echo 失败（桥未建立）: %v", err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ctx 取消下 Run 应返回 nil，得到: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ctx 取消后 Run 未及时返回")
	}

	// 聚合流端应已被关闭（pipe 端点关闭后读写立即报错）
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Fatal("ctx 取消后聚合流应被关闭")
	}
}

// TestServerEOFPropagates：上游一侧 EOF 后整条映射终结
// （不支持半关闭：聚合流侧也被关闭）。
func TestServerEOFPropagates(t *testing.T) {
	// 上游一连上就主动关闭，制造「上游 EOF」
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("起上游监听失败: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close() // 立即关 → 聚合流侧应随之被关
		}
	}()

	acceptCh := make(chan net.Conn, 4)
	srv := newServer(chanAccepter(acceptCh), ln.Addr().String())
	runServer(t, srv)

	a, b := net.Pipe()
	defer b.Close()
	acceptCh <- a

	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Fatal("上游 EOF 后聚合流应被关闭（不支持半关闭）")
	}
}

// ---------- 端到端（真实 Aggregator + libp2p）----------

// TestTunnelEndToEnd 真实聚合流端到端：
// TCP → client → 聚合流 → server → 上游 echo → 原路返回。
func TestTunnelEndToEnd(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	upstream := echoUpstream(t, "")

	// server：*Aggregator 直接满足 StreamAccepter
	srv, err := NewServer(aggb, WithUpstream(upstream))
	if err != nil {
		t.Fatalf("NewServer 失败: %v", err)
	}
	runServer(t, srv)

	// client：*Aggregator 直接满足 StreamOpener；port=0 走内核分配
	cli, err := NewClient(agga, hb.ID(), WithListenPort(0))
	if err != nil {
		t.Fatalf("NewClient 失败: %v", err)
	}
	runClient(t, cli)

	conn, err := net.Dial("tcp", waitClientAddr(t, cli).String())
	if err != nil {
		t.Fatalf("拨本地监听失败: %v", err)
	}
	defer conn.Close()

	want := []byte("端到端 hello")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("TCP 写失败: %v", err)
	}
	got := make([]byte, len(want))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("读 echo 失败: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("端到端 echo 数据错误: %q != %q", got, want)
	}
}
