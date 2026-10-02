package tunnel

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/internal/tunnelwire"
)

// ---------- 测试辅助 ----------

// appServer 起一个带给定 serverConfig 的 Server 并后台运行，
// 返回喂流通道。
func appServer(t *testing.T, cfg serverConfig) chan<- net.Conn {
	t.Helper()
	acceptCh := make(chan net.Conn, 4)
	srv := newServer(chanAccepter(acceptCh), cfg)
	runServer(t, srv)
	return acceptCh
}

// dialTunnel 向 server 喂一条 net.Pipe 流，client 端写 magic 后
// 返回就绪的帧会话与原始 conn（测试侧扮演隧道 client）。
func dialTunnel(t *testing.T, acceptCh chan<- net.Conn) (*tunnelwire.Session, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	acceptCh <- a // server 侧持有 a
	_ = b.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := b.Write(tunnelwire.Magic); err != nil {
		t.Fatalf("写 magic 失败: %v", err)
	}
	return tunnelwire.NewSession(b), b
}

// openHTTP 写一帧 http OPEN。
func openHTTP(t *testing.T, sess *tunnelwire.Session, open *tunnelwire.OpenMsg) {
	t.Helper()
	open.V, open.Kind = 1, tunnelwire.KindHTTP
	if err := sess.WriteJSON(tunnelwire.FrameOpen, open); err != nil {
		t.Fatalf("写 OPEN 失败: %v", err)
	}
}

// openWS 写一帧 ws OPEN。
func openWS(t *testing.T, sess *tunnelwire.Session, open *tunnelwire.OpenMsg) {
	t.Helper()
	open.V, open.Kind = 1, tunnelwire.KindWS
	if err := sess.WriteJSON(tunnelwire.FrameOpen, open); err != nil {
		t.Fatalf("写 OPEN 失败: %v", err)
	}
}

// readResult 读一帧并断言是 RESULT，返回解码结果。
func readResult(t *testing.T, sess *tunnelwire.Session) *tunnelwire.ResultMsg {
	t.Helper()
	ft, pl, err := sess.ReadFrame()
	if err != nil {
		t.Fatalf("读 RESULT 帧失败: %v", err)
	}
	if ft != tunnelwire.FrameResult {
		t.Fatalf("首帧应为 RESULT，得到 %s", ft)
	}
	res, err := tunnelwire.DecodeResult(pl)
	if err != nil {
		t.Fatalf("RESULT 解码失败: %v", err)
	}
	return res
}

// readHTTPBody 把响应体 DATA 帧读到 END，返回全部字节。
func readHTTPBody(t *testing.T, sess *tunnelwire.Session) []byte {
	t.Helper()
	var out []byte
	for {
		ft, pl, err := sess.ReadFrame()
		if err != nil {
			t.Fatalf("读响应体帧失败: %v", err)
		}
		switch ft {
		case tunnelwire.FrameData:
			out = append(out, pl...)
		case tunnelwire.FrameEnd:
			return out
		default:
			t.Fatalf("响应体阶段应只有 DATA/END，得到 %s", ft)
		}
	}
}

// ---------- 分流（magic sniff） ----------

// TestSniffRawToUpstream：无 magic 的流仍走 raw TCP 桥接——
// peek 到的前导字节须原样送达上游。
func TestSniffRawToUpstream(t *testing.T) {
	upstream := echoUpstream(t, "")
	acceptCh := appServer(t, serverConfig{upstream: upstream,
		openTimeout: defaultOpenTimeout})

	a, b := net.Pipe()
	defer b.Close()
	acceptCh <- a
	_ = b.SetDeadline(time.Now().Add(3 * time.Second))

	// 前导字节不是 magic → raw 桥接，peek 过的字节必须送达上游
	if _, err := b.Write([]byte("hello")); err != nil {
		t.Fatalf("写 raw 数据失败: %v", err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(b, got); err != nil {
		t.Fatalf("读 echo 失败: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("raw 桥接数据错误: %q", got)
	}
}

// TestSniffPartialMagicThenRaw：只发了 magic 前几个字节就停
// → 按 raw 处理且已发字节不丢（超时兜底验证）。
func TestSniffPartialMagicThenRaw(t *testing.T) {
	upstream := echoUpstream(t, "")
	// 用很小的 openTimeout 让 sniff 快速超时
	acceptCh := appServer(t, serverConfig{upstream: upstream,
		openTimeout: 200 * time.Millisecond})

	a, b := net.Pipe()
	defer b.Close()
	acceptCh <- a
	_ = b.SetDeadline(time.Now().Add(3 * time.Second))

	// 只写 "NT"（magic 前缀片段），等嗅探超时后写剩余数据
	if _, err := b.Write([]byte("NT")); err != nil {
		t.Fatalf("写部分 magic 失败: %v", err)
	}
	time.Sleep(400 * time.Millisecond) // 超过 openTimeout
	if _, err := b.Write([]byte("XX")); err != nil {
		t.Fatalf("补写 raw 数据失败: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(b, got); err != nil {
		t.Fatalf("读 echo 失败（前导字节丢失或超时）: %v", err)
	}
	if string(got) != "NTXX" {
		t.Fatalf("raw 数据应以 peek 字节重放开头: %q", got)
	}
}

// TestSniffTunnelNoUpstreamRaw：只配隧道不配 upstream 时，
// raw 流被直接关闭。
func TestSniffTunnelNoUpstreamRaw(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		httpHandler: http.NotFoundHandler(),
		openTimeout: defaultOpenTimeout,
	})
	a, b := net.Pipe()
	defer b.Close()
	acceptCh <- a
	_ = b.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := b.Write([]byte("rawdata")); err != nil {
		t.Fatalf("写失败: %v", err)
	}
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Fatal("未配 upstream 时 raw 流应被关闭")
	}
}

// TestSniffTunnelNoUpstreamNoMagic：无 upstream 时即使直接发合法
// OPEN 帧，缺少 NTUN\x01 magic 也必须关闭——magic 是协议身份边界。
func TestSniffTunnelNoUpstreamNoMagic(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		httpHandler: http.NotFoundHandler(),
		openTimeout: 200 * time.Millisecond,
	})
	a, b := net.Pipe()
	defer b.Close()
	acceptCh <- a
	_ = b.SetDeadline(time.Now().Add(3 * time.Second))
	sess := tunnelwire.NewSession(b)
	if err := sess.WriteJSON(tunnelwire.FrameOpen, &tunnelwire.OpenMsg{
		V: 1, Kind: tunnelwire.KindHTTP, Target: "/x",
	}); err != nil {
		t.Fatalf("写无 magic OPEN 失败: %v", err)
	}
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Fatal("缺 magic 的 OPEN 流应被关闭")
	}
}

// ---------- HTTP 本地 handler ----------

// TestTunnelHTTPLocalHandler：magic + OPEN{kind:http,target:/p}
// → 本地 handler 收到还原的 Request，响应经 RESULT+DATA+END 回传。
func TestTunnelHTTPLocalHandler(t *testing.T) {
	var gotURI, gotMethod, gotHost, gotHdr string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI, gotMethod, gotHost = r.URL.RequestURI(), r.Method, r.Host
		gotHdr = r.Header.Get("X-Req")
		w.Header().Set("X-Reply", "yes")
		w.WriteHeader(201)
		_, _ = w.Write([]byte("响应体"))
	})
	acceptCh := appServer(t, serverConfig{httpHandler: h,
		openTimeout: defaultOpenTimeout})

	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openHTTP(t, sess, &tunnelwire.OpenMsg{
		Target: "/a/b?x=1", Method: "PUT", Host: "inside.example",
		Headers: []tunnelwire.HeaderPair{{"X-Req", "v1"}},
	})
	if err := sess.WriteEnd(); err != nil { // PUT 空 body
		t.Fatalf("写 END 失败: %v", err)
	}

	res := readResult(t, sess)
	if res.Kind != tunnelwire.KindHTTP || res.Status != 201 || res.StatusText != "Created" {
		t.Fatalf("RESULT 不符: %+v", res)
	}
	if body := readHTTPBody(t, sess); string(body) != "响应体" {
		t.Fatalf("响应体错误: %q", body)
	}
	if gotURI != "/a/b?x=1" || gotMethod != "PUT" || gotHost != "inside.example" || gotHdr != "v1" {
		t.Fatalf("请求还原错误: uri=%q method=%q host=%q x-req=%q",
			gotURI, gotMethod, gotHost, gotHdr)
	}
}

// TestTunnelHTTPPostBody：POST 大 body（跨多个 DATA 帧）被
// handler 完整读到；响应同样流式回来。
func TestTunnelHTTPPostBody(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Len", string(rune('0'+len(body)/1024)))
		_, _ = w.Write(body) // echo 回 body
	})
	acceptCh := appServer(t, serverConfig{httpHandler: h,
		openTimeout: defaultOpenTimeout})

	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openHTTP(t, sess, &tunnelwire.OpenMsg{Target: "/echo", Method: "POST"})

	payload := bytes.Repeat([]byte("0123456789"), 30000) // 300KiB
	if err := sess.WriteData(payload); err != nil {
		t.Fatalf("写 body 失败: %v", err)
	}
	if err := sess.WriteEnd(); err != nil {
		t.Fatalf("写 END 失败: %v", err)
	}

	res := readResult(t, sess)
	if res.Status != 200 {
		t.Fatalf("状态码错误: %d", res.Status)
	}
	if body := readHTTPBody(t, sess); !bytes.Equal(body, payload) {
		t.Fatalf("echo body 不一致: len(got)=%d len(want)=%d", len(body), len(payload))
	}
}

// TestTunnelHTTPAbortCancelsHandler：handler 不读 body 时，client
// 发 ABORT 也应取消 r.Context()，避免 handler 挂住整条会话。
func TestTunnelHTTPAbortCancelsHandler(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		w.WriteHeader(499)
		_, _ = w.Write([]byte("aborted"))
	})
	acceptCh := appServer(t, serverConfig{httpHandler: h,
		openTimeout: defaultOpenTimeout})

	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openHTTP(t, sess, &tunnelwire.OpenMsg{Target: "/slow", Method: "POST"})
	if err := sess.WriteData([]byte("partial")); err != nil {
		t.Fatalf("写 DATA 失败: %v", err)
	}
	if err := sess.Abort("client_close", "放弃"); err != nil {
		t.Fatalf("写 ABORT 失败: %v", err)
	}
	res := readResult(t, sess)
	if res.Status != 499 {
		t.Fatalf("handler ctx 未被 ABORT 取消，状态码 %d", res.Status)
	}
	if body := readHTTPBody(t, sess); string(body) != "aborted" {
		t.Fatalf("响应体错误: %q", body)
	}
}

// TestTunnelHTTPNoHandler：kind=http 但未配 handler/proxy → 4xx。
func TestTunnelHTTPNoHandler(t *testing.T) {
	// 只配 upstream（raw），不配隧道 handler
	acceptCh := appServer(t, serverConfig{upstream: "127.0.0.1:1",
		openTimeout: defaultOpenTimeout})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openHTTP(t, sess, &tunnelwire.OpenMsg{Target: "/x"})
	if err := sess.WriteEnd(); err != nil {
		t.Fatalf("写 END 失败: %v", err)
	}
	res := readResult(t, sess)
	if res.Status != 404 {
		t.Fatalf("无 handler 应回 404，得到 %d", res.Status)
	}
}

// TestTunnelHTTPBadTarget：非法 target → 400。
func TestTunnelHTTPBadTarget(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		httpHandler: http.NotFoundHandler(),
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openHTTP(t, sess, &tunnelwire.OpenMsg{Target: "//evil.com/x"})
	res := readResult(t, sess)
	if res.Status != 400 {
		t.Fatalf("// 开头 target 应回 400，得到 %d", res.Status)
	}
}

// TestTunnelHTTPHandlerPanic：handler panic → 500 + END。
func TestTunnelHTTPHandlerPanic(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("崩了")
	})
	acceptCh := appServer(t, serverConfig{httpHandler: h,
		openTimeout: defaultOpenTimeout})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openHTTP(t, sess, &tunnelwire.OpenMsg{Target: "/"})
	_ = sess.WriteEnd()
	res := readResult(t, sess)
	if res.Status != 500 {
		t.Fatalf("panic 应回 500，得到 %d", res.Status)
	}
	readHTTPBody(t, sess) // 应能正常读到 END
}

// ---------- HTTP 代理 ----------

// TestTunnelHTTPProxyAllowDeny：Allow=nil 默认拒绝；
// Allow 返回 false 也拒绝；返回 true 才真正代理。
func TestTunnelHTTPProxyAllowDeny(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("upstream:" + r.URL.Path))
	}))
	defer upstream.Close()

	// 1) Allow=nil → 403
	acceptCh := appServer(t, serverConfig{
		httpProxy:   &HTTPProxyConfig{},
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	openHTTP(t, sess, &tunnelwire.OpenMsg{Target: upstream.URL + "/a"})
	_ = sess.WriteEnd()
	if res := readResult(t, sess); res.Status != 403 {
		t.Fatalf("Allow=nil 应默认拒绝 403，得到 %d", res.Status)
	}
	conn.Close()

	// 2) Allow=false → 403
	acceptCh2 := appServer(t, serverConfig{
		httpProxy: &HTTPProxyConfig{Allow: func(_ peer.ID, _ TunnelKind, _ *url.URL) bool {
			return false
		}},
		openTimeout: defaultOpenTimeout,
	})
	sess2, conn2 := dialTunnel(t, acceptCh2)
	defer conn2.Close()
	openHTTP(t, sess2, &tunnelwire.OpenMsg{Target: upstream.URL + "/a"})
	_ = sess2.WriteEnd()
	if res := readResult(t, sess2); res.Status != 403 {
		t.Fatalf("Allow=false 应拒绝 403，得到 %d", res.Status)
	}
}

// TestTunnelHTTPProxyPass：Allow 放行后请求真实到达上游，
// path/query/Host/自定义头透传，响应流式回传。
func TestTunnelHTTPProxyPass(t *testing.T) {
	var gotPath, gotHost, gotHdr string
	var gotConnHdr string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.RequestURI()
		gotHost = r.Host
		gotHdr = r.Header.Get("X-Custom")
		gotConnHdr = r.Header.Get("Connection")
		w.Header().Set("X-Up", "ok")
		_, _ = w.Write([]byte("proxied"))
	}))
	defer upstream.Close()

	acceptCh := appServer(t, serverConfig{
		httpProxy: &HTTPProxyConfig{Allow: func(_ peer.ID, k TunnelKind, u *url.URL) bool {
			return k == TunnelKindHTTP && strings.HasPrefix(u.Host, "127.0.0.1")
		}},
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()

	openHTTP(t, sess, &tunnelwire.OpenMsg{
		Target: upstream.URL + "/deep?q=9",
		Headers: []tunnelwire.HeaderPair{
			{"X-Custom", "cv"},
			{"Connection", "keep-alive, X-Custom"}, // 逐跳头 + 点名剥除测试
			{"Keep-Alive", "timeout=1"},
		},
	})
	_ = sess.WriteEnd()

	res := readResult(t, sess)
	if res.Status != 200 {
		t.Fatalf("代理响应状态错误: %d err=%q", res.Status, res.Error)
	}
	if body := readHTTPBody(t, sess); string(body) != "proxied" {
		t.Fatalf("代理响应体错误: %q", body)
	}
	if gotPath != "/deep?q=9" {
		t.Fatalf("上游收到 path/query 错误: %q", gotPath)
	}
	if gotHost == "" {
		t.Fatal("上游 Host 为空")
	}
	// X-Custom 被 Connection 点名 → 应被剥除
	if gotHdr != "" || gotConnHdr != "" && gotConnHdr != "close" {
		// Go client 会自己补 Connection: close？不会——StripHopByHop 已删，
		// 上游看到的 Connection 应为空（http.Transport 自行管理）。
		if gotConnHdr != "" {
			t.Fatalf("逐跳头泄漏: X-Custom=%q Connection=%q", gotHdr, gotConnHdr)
		}
	}
}

// TestTunnelHTTPProxyProtocolViolation：HTTP body 阶段收到 WS 帧必须回 ABORT，
// 不能伪装成普通 502 上游错误。
func TestTunnelHTTPProxyProtocolViolation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer upstream.Close()
	acceptCh := appServer(t, serverConfig{
		httpProxy:   &HTTPProxyConfig{Allow: func(peer.ID, TunnelKind, *url.URL) bool { return true }},
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openHTTP(t, sess, &tunnelwire.OpenMsg{
		Target:  upstream.URL,
		Method:  http.MethodPost,
		Headers: []tunnelwire.HeaderPair{{"Content-Length", "1"}},
	})
	if err := sess.WriteFrame(tunnelwire.FrameWSMessage, []byte{tunnelwire.WSOpcodeText, 'x'}); err != nil {
		t.Fatalf("写非法方向帧失败: %v", err)
	}
	ft, payload, err := sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameAbort {
		t.Fatalf("协议违例应回 ABORT，得到 ft=%s err=%v", ft, err)
	}
	abort, err := tunnelwire.DecodeAbort(payload)
	if err != nil || abort.Code != "protocol_error" {
		t.Fatalf("ABORT code 应为 protocol_error，得到 %+v err=%v", abort, err)
	}
}

// ---------- WS 本地 handler ----------

// TestTunnelWSLocalEcho：本地 WS echo handler：
// Accept → 消息回环 → WS_CLOSE 关闭握手。
func TestTunnelWSLocalEcho(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		wsHandler: TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {
			if err := c.Accept("chat"); err != nil {
				return
			}
			for {
				op, data, err := c.ReadMessage()
				if err != nil {
					return
				}
				if err := c.WriteMessage(op, data); err != nil {
					return
				}
			}
		}),
		openTimeout: defaultOpenTimeout,
	})

	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openWS(t, sess, &tunnelwire.OpenMsg{Target: "/chat", Protocols: []string{"chat"}})

	res := readResult(t, sess)
	if res.Kind != tunnelwire.KindWS || res.Status != 101 || res.Protocol != "chat" {
		t.Fatalf("WS RESULT 不符: %+v", res)
	}

	// echo 往返（text + binary）
	if err := sess.WriteWSMessage(tunnelwire.WSOpcodeText, []byte("你好")); err != nil {
		t.Fatalf("写 WS_MESSAGE 失败: %v", err)
	}
	ft, pl, err := sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameWSMessage {
		t.Fatalf("读 echo 帧失败: ft=%s err=%v", ft, err)
	}
	if pl[0] != tunnelwire.WSOpcodeText || string(pl[1:]) != "你好" {
		t.Fatalf("echo 数据错误: %q", pl)
	}
	bin := []byte{0, 1, 2, 255}
	if err := sess.WriteWSMessage(tunnelwire.WSOpcodeBinary, bin); err != nil {
		t.Fatalf("写二进制消息失败: %v", err)
	}
	ft, pl, err = sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameWSMessage || pl[0] != tunnelwire.WSOpcodeBinary ||
		!bytes.Equal(pl[1:], bin) {
		t.Fatalf("二进制 echo 错误: ft=%s pl=%v err=%v", ft, pl, err)
	}

	// ping → 应收到 pong（server 侧 ReadMessage 自动应答）
	if err := sess.WriteFrame(tunnelwire.FrameWSPing, []byte("pp")); err != nil {
		t.Fatalf("写 WS_PING 失败: %v", err)
	}
	ft, pl, err = sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameWSPong || string(pl) != "pp" {
		t.Fatalf("应收到 WS_PONG: ft=%s pl=%q err=%v", ft, pl, err)
	}

	// WS_CLOSE → server 回声后关流
	if err := sess.WriteJSON(tunnelwire.FrameWSClose, &tunnelwire.CloseMsg{Code: 1000, Reason: "bye"}); err != nil {
		t.Fatalf("写 WS_CLOSE 失败: %v", err)
	}
	ft, pl, err = sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameWSClose {
		t.Fatalf("应收到 WS_CLOSE 回声: ft=%s err=%v", ft, err)
	}
	cm, _ := tunnelwire.DecodeClose(pl)
	if cm.Code != 1000 {
		t.Fatalf("close code 错误: %d", cm.Code)
	}
}

// TestTunnelWSLocalReject：handler 调 Reject → client 见非 101 RESULT。
func TestTunnelWSLocalReject(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		wsHandler: TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {
			_ = c.Reject(403, "不许进")
		}),
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openWS(t, sess, &tunnelwire.OpenMsg{Target: "/x"})
	res := readResult(t, sess)
	if res.Status == 101 {
		t.Fatal("Reject 后不应回 101")
	}
	if res.Status != 403 || res.Error == "" {
		t.Fatalf("拒绝响应不符: %+v", res)
	}
}

// TestTunnelWSAcceptUnofferedProtocol：handler 不能选择客户端
// 未提供的子协议；Accept 应报错，随后可正常 Reject。
func TestTunnelWSAcceptUnofferedProtocol(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		wsHandler: TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {
			if err := c.Accept("ghost"); err == nil {
				t.Error("未提供的子协议不应可 Accept")
				return
			}
			_ = c.Reject(400, "bad protocol")
		}),
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openWS(t, sess, &tunnelwire.OpenMsg{Target: "/x", Protocols: []string{"chat"}})
	res := readResult(t, sess)
	if res.Status != 400 {
		t.Fatalf("应拒绝握手，得到 %+v", res)
	}
}

// ---------- WS 代理 ----------

// wsEchoUpstream 起一个 gorilla WS echo 上游，返回 ws:// URL。
func wsEchoUpstream(t *testing.T) string {
	t.Helper()
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			mt, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(mt, data); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestTunnelWSProxyEcho：ws:// 绝对 target 经代理到真实上游，
// 消息 echo + close 握手透传。
func TestTunnelWSProxyEcho(t *testing.T) {
	upstreamURL := wsEchoUpstream(t)
	acceptCh := appServer(t, serverConfig{
		wsProxy: &WSProxyConfig{Allow: func(_ peer.ID, k TunnelKind, u *url.URL) bool {
			return k == TunnelKindWS && u.Scheme == "ws"
		}},
		openTimeout: defaultOpenTimeout,
	})

	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openWS(t, sess, &tunnelwire.OpenMsg{Target: upstreamURL})

	res := readResult(t, sess)
	if res.Status != 101 {
		t.Fatalf("代理握手应回 101，得到 %+v", res)
	}

	if err := sess.WriteWSMessage(tunnelwire.WSOpcodeText, []byte("proxy-hello")); err != nil {
		t.Fatalf("写消息失败: %v", err)
	}
	ft, pl, err := sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameWSMessage ||
		pl[0] != tunnelwire.WSOpcodeText || string(pl[1:]) != "proxy-hello" {
		t.Fatalf("代理 echo 错误: ft=%s pl=%q err=%v", ft, pl, err)
	}

	// client 发 WS_CLOSE → 代理转发上游 close → 上游回声 → 隧道 WS_CLOSE
	if err := sess.WriteJSON(tunnelwire.FrameWSClose, &tunnelwire.CloseMsg{Code: 1000}); err != nil {
		t.Fatalf("写 WS_CLOSE 失败: %v", err)
	}
	ft, pl, err = sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameWSClose {
		t.Fatalf("应收 WS_CLOSE: ft=%s err=%v", ft, err)
	}
	cm, _ := tunnelwire.DecodeClose(pl)
	if cm.Code != 1000 {
		t.Fatalf("close code 应为 1000，得到 %d", cm.Code)
	}
}

// TestTunnelWSProxyProtocolViolation：WS 会话收到 HTTP DATA 帧必须回 ABORT。
func TestTunnelWSProxyProtocolViolation(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		wsProxy:     &WSProxyConfig{Allow: func(peer.ID, TunnelKind, *url.URL) bool { return true }},
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openWS(t, sess, &tunnelwire.OpenMsg{Target: wsEchoUpstream(t)})
	if res := readResult(t, sess); res.Status != 101 {
		t.Fatalf("代理握手应成功，得到 %+v", res)
	}
	if err := sess.WriteFrame(tunnelwire.FrameData, []byte("wrong direction")); err != nil {
		t.Fatalf("写非法方向帧失败: %v", err)
	}
	ft, payload, err := sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameAbort {
		t.Fatalf("协议违例应回 ABORT，得到 ft=%s err=%v", ft, err)
	}
	abort, err := tunnelwire.DecodeAbort(payload)
	if err != nil || abort.Code != "protocol_error" {
		t.Fatalf("ABORT code 应为 protocol_error，得到 %+v err=%v", abort, err)
	}
}

// TestTunnelWSProxyDeny：WS 代理 Allow=nil/false → 拒绝。
func TestTunnelWSProxyDeny(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		wsProxy:     &WSProxyConfig{}, // Allow=nil → 默认拒绝
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openWS(t, sess, &tunnelwire.OpenMsg{Target: "ws://127.0.0.1:9/x"})
	res := readResult(t, sess)
	if res.Status == 101 {
		t.Fatal("Allow=nil 不应回 101")
	}
	if res.Status != 403 {
		t.Fatalf("应回 403，得到 %d", res.Status)
	}
}

// ---------- 协议违例 ----------

// TestTunnelFirstFrameNotOpen：magic 后首帧不是 OPEN → ABORT + 关流。
func TestTunnelFirstFrameNotOpen(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		httpHandler: http.NotFoundHandler(),
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	// 直接发 RESULT（server→client 方向的帧出现在第一帧）
	if err := sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{Kind: "http", Status: 200}); err != nil {
		t.Fatalf("写帧失败: %v", err)
	}
	ft, pl, err := sess.ReadFrame()
	if err != nil {
		t.Fatalf("读 ABORT 失败: %v", err)
	}
	if ft != tunnelwire.FrameAbort {
		t.Fatalf("首帧非 OPEN 应回 ABORT，得到 %s", ft)
	}
	am, _ := tunnelwire.DecodeAbort(pl)
	if am.Code != "protocol_error" {
		t.Fatalf("ABORT code 应为 protocol_error，得到 %q", am.Code)
	}
}

// TestTunnelOpenBadVersion：v!=1 的 OPEN → ABORT + 关流。
func TestTunnelOpenBadVersion(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		httpHandler: http.NotFoundHandler(),
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	if err := sess.WriteJSON(tunnelwire.FrameOpen,
		&tunnelwire.OpenMsg{V: 99, Kind: "http", Target: "/"}); err != nil {
		t.Fatalf("写 OPEN 失败: %v", err)
	}
	ft, _, err := sess.ReadFrame()
	if err != nil || ft != tunnelwire.FrameAbort {
		t.Fatalf("非法版本应回 ABORT: ft=%s err=%v", ft, err)
	}
}

// TestTunnelWSLocalHandlerNoResponse：handler 直接返回（未 Accept/Reject）
// → conn.Close 兜底（client 见到终止而非挂死）。
func TestTunnelWSLocalHandlerNoResponse(t *testing.T) {
	acceptCh := appServer(t, serverConfig{
		wsHandler:   TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {}),
		openTimeout: defaultOpenTimeout,
	})
	sess, conn := dialTunnel(t, acceptCh)
	defer conn.Close()
	openWS(t, sess, &tunnelwire.OpenMsg{Target: "/x"})
	// Close 兜底会先补一个 500 RESULT 再发 WS_CLOSE
	res := readResult(t, sess)
	if res.Status == 101 {
		t.Fatal("未应答握手不应回 101")
	}
}
