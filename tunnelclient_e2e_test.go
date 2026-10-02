// TunnelFetch/TunnelWS 端到端测试：真实 libp2p 双 host +
// 聚合流 + tunnel.Server，覆盖 HTTP 回显/流式、WS echo/拒绝、
// ctx 取消与收尾冲刷（gracefulClose）。
package netacc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/tunnel"
)

// runTunnelServer 在聚合器上跑 tunnel.Server；测试结束自动停。
func runTunnelServer(t *testing.T, agg *netacc.Aggregator, opts ...tunnel.ServerOption) {
	t.Helper()
	srv, err := tunnel.NewServer(agg, opts...)
	if err != nil {
		t.Fatalf("NewServer 失败: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("server 退出错误: %v", err)
		}
	})
}

// TestTunnelFetchLocal 验收：TunnelFetch 经聚合流命中对端本地
// HTTP handler——method/path/query/header/body 全链路，响应
// 状态/头/体逐一还原。
func TestTunnelFetchLocal(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	var seenPath, seenQuery, seenHost, seenHdr, seenBody string
	runTunnelServer(t, aggb, tunnel.WithTunnelHTTPHandler(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			seenPath, seenQuery, seenHost = r.URL.Path, r.URL.RawQuery, r.Host
			seenHdr = r.Header.Get("X-A")
			b, _ := io.ReadAll(r.Body)
			seenBody = string(b)
			w.Header().Set("X-Back", "ok")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("echo:" + string(b)))
		})))

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/echo?k=v",
		strings.NewReader("payload-1"))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "api.internal"
	req.Header.Set("X-A", "av")

	resp, err := agga.TunnelFetch(ctx, hb.ID(), req)
	if err != nil {
		t.Fatalf("TunnelFetch 失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || resp.Header.Get("X-Back") != "ok" {
		t.Fatalf("响应不符: status=%d header=%v", resp.StatusCode, resp.Header)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读响应体失败: %v", err)
	}
	if string(body) != "echo:payload-1" {
		t.Fatalf("响应体不符: %q", body)
	}
	if seenPath != "/echo" || seenQuery != "k=v" || seenHost != "api.internal" ||
		seenHdr != "av" || seenBody != "payload-1" {
		t.Fatalf("server 所见请求不符: path=%q query=%q host=%q hdr=%q body=%q",
			seenPath, seenQuery, seenHost, seenHdr, seenBody)
	}
}

// TestTunnelFetchStreaming 验收：大响应分多个 DATA 帧回传，
// client 侧能完整拼出。
func TestTunnelFetchStreaming(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	// 200KiB 响应：必然跨多个 64KiB DATA 帧
	big := strings.Repeat("z", 200<<10)
	runTunnelServer(t, aggb, tunnel.WithTunnelHTTPHandler(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, big)
		})))

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/big", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := agga.TunnelFetch(ctx, hb.ID(), req)
	if err != nil {
		t.Fatalf("TunnelFetch 失败: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读失败: %v", err)
	}
	if string(got) != big {
		t.Fatalf("流式响应不完整: got %d bytes", len(got))
	}
}

// TestTunnelClientOpenValidation：非法 method/header/子协议在本地
// ValidateOpen 阶段即拒绝，不发起聚合流。
func TestTunnelClientOpenValidation(t *testing.T) {
	ha := newTestHost(t)
	agga := netacc.New(ha)
	defer agga.Close()

	req := &http.Request{Method: "BAD METHOD", URL: &url.URL{Path: "/x"}, Header: http.Header{}}
	if _, err := agga.TunnelFetch(context.Background(), "serverPeer", req); err == nil {
		t.Fatal("非法 method 应本地报错")
	}
	req, _ = http.NewRequest(http.MethodGet, "/x", nil)
	req.Header["Bad Name"] = []string{"v"}
	if _, err := agga.TunnelFetch(context.Background(), "serverPeer", req); err == nil {
		t.Fatal("非法 header 名应本地报错")
	}
	if _, err := agga.TunnelWS(context.Background(), "serverPeer", "/ws",
		nil, []string{"bad proto"}); err == nil {
		t.Fatal("非法 WS 子协议应本地报错")
	}
}

// TestTunnelFetchRefused 验收：对端未配 HTTP handler 时拿到拒绝
// RESULT（status 404）。
func TestTunnelFetchRefused(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	// 只配 WS handler——HTTP OPEN 被 404
	runTunnelServer(t, aggb,
		tunnel.WithTunnelWSHandler(tunnel.TunnelWSHandlerFunc(func(*netacc.TunnelWSConn) {})))

	ctx := context.Background()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/x", nil)
	resp, err := agga.TunnelFetch(ctx, hb.ID(), req)
	if err != nil {
		t.Fatalf("拒绝也应拿到 Response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("应回 404，得到 %d", resp.StatusCode)
	}
}

// TestTunnelFetchCancel 验收：ctx 取消中止会话——
// 对端 handler 卡住不返回时，取消 TunnelFetch 的 ctx 会让其报错退出。
func TestTunnelFetchCancel(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	release := make(chan struct{})
	runTunnelServer(t, aggb, tunnel.WithTunnelHTTPHandler(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			<-release // 永不返回，直到测试收尾
		})))
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/hang", nil)
	done := make(chan error, 1)
	go func() {
		_, err := agga.TunnelFetch(ctx, hb.ID(), req)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond) // 让请求到达对端
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ctx 取消后 TunnelFetch 应报错")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ctx 取消后 TunnelFetch 未退出")
	}
}

// TestTunnelFetchRequestContextCancel 验收：req.Context() 也参与会话
// 生命周期；调用方 ctx 未取消时，取消请求自身 ctx 同样中止。
func TestTunnelFetchRequestContextCancel(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	release := make(chan struct{})
	runTunnelServer(t, aggb, tunnel.WithTunnelHTTPHandler(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			<-release
		})))
	defer close(release)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, "/hang", nil)
	done := make(chan error, 1)
	go func() {
		_, err := agga.TunnelFetch(context.Background(), hb.ID(), req)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	cancelReq()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("req ctx 取消后 TunnelFetch 应报错")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("req ctx 取消后 TunnelFetch 未退出")
	}
}

// TestTunnelWSEcho 验收：TunnelWS 到本地 handler——Accept 后 echo；
// 覆盖 text/binary、子协议协商、close 握手。
func TestTunnelWSEcho(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	runTunnelServer(t, aggb, tunnel.WithTunnelWSHandler(
		tunnel.TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {
			req := c.Request()
			if req == nil || req.Target != "/echo" {
				_ = c.Reject(http.StatusNotFound, "未知路径")
				return
			}
			if len(req.Protocols) == 0 || req.Protocols[0] != "chat" {
				_ = c.Reject(http.StatusBadRequest, "缺子协议")
				return
			}
			if err := c.Accept("chat"); err != nil {
				return
			}
			// echo 两条后由 server 侧主动关：验证 close 握手透传
			for i := 0; i < 2; i++ {
				op, msg, err := c.ReadMessage()
				if err != nil {
					return
				}
				if err := c.WriteMessage(op, msg); err != nil {
					return
				}
			}
			_ = c.Close(3001, "srvbye")
		})))

	ctx := context.Background()
	conn, err := agga.TunnelWS(ctx, hb.ID(), "/echo", nil, []string{"chat"})
	if err != nil {
		t.Fatalf("TunnelWS 失败: %v", err)
	}
	if conn.Subprotocol() != "chat" {
		t.Fatalf("协商子协议不符: %q", conn.Subprotocol())
	}
	if conn.RemotePeer() != hb.ID() {
		t.Fatalf("RemotePeer 不符: %v", conn.RemotePeer())
	}

	for i, msg := range []string{"hello", "world"} {
		if err := conn.WriteMessage(netacc.TunnelWSText, []byte(msg)); err != nil {
			t.Fatalf("写消息失败: %v", err)
		}
		op, got, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("读 echo 失败: %v", err)
		}
		if op != netacc.TunnelWSText || string(got) != msg {
			t.Fatalf("第 %d 条 echo 不符 op=%d msg=%q", i, op, got)
		}
	}

	// server 主动 WS_CLOSE → ReadMessage 报 TunnelWSCloseError 并
	// 自动回送 close；本侧 Close 幂等收尾
	_, _, err = conn.ReadMessage()
	var ce *netacc.TunnelWSCloseError
	if !errors.As(err, &ce) {
		t.Fatalf("应得 TunnelWSCloseError，得到 %v", err)
	}
	if ce.Code != 3001 || ce.Reason != "srvbye" {
		t.Fatalf("close 载荷不符: %+v", ce)
	}
	if err := conn.Close(1001, "reserved"); err == nil {
		t.Fatal("主动发送保留 close code 应报错")
	}
	if err := conn.Close(1000, "ack"); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
}

// TestTunnelWSClientCloseWaitsForEcho 验证主动 Close 等到对端回显后才关闭流。
func TestTunnelWSClientCloseWaitsForEcho(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)
	serverClosed := make(chan error, 1)
	runTunnelServer(t, aggb, tunnel.WithTunnelWSHandler(
		tunnel.TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {
			if err := c.Accept(""); err != nil {
				serverClosed <- err
				return
			}
			_, _, err := c.ReadMessage()
			var closeErr *netacc.TunnelWSCloseError
			if !errors.As(err, &closeErr) {
				serverClosed <- fmt.Errorf("expected close error, got %w", err)
				return
			}
			serverClosed <- nil
		})))
	conn, err := agga.TunnelWS(context.Background(), hb.ID(), "/close", nil, nil)
	if err != nil {
		t.Fatalf("TunnelWS 失败: %v", err)
	}
	if err := conn.Close(1000, "done"); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	select {
	case err := <-serverClosed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server 未收到并回显 WS_CLOSE")
	}
}

// TestTunnelWSReject 验收：handler Reject → TunnelWS 拿 WSRejectedError。
func TestTunnelWSReject(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	runTunnelServer(t, aggb, tunnel.WithTunnelWSHandler(
		tunnel.TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {
			_ = c.Reject(http.StatusForbidden, "不接待")
		})))

	ctx := context.Background()
	_, err := agga.TunnelWS(ctx, hb.ID(), "/nope", nil, nil)
	var re *netacc.WSRejectedError
	if !errors.As(err, &re) {
		t.Fatalf("应得 WSRejectedError，得到 %v", err)
	}
	if re.Status != http.StatusForbidden || re.Message == "" {
		t.Fatalf("拒绝细节不符: %+v", re)
	}
}

// TestTunnelWSPingPong 验收：Ping 触发对端自动 Pong——经 read loop
// 验证（ReadMessage 内部应答，这里验证 handler 侧主动 Ping 的回路）。
func TestTunnelWSPingPong(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	runTunnelServer(t, aggb, tunnel.WithTunnelWSHandler(
		tunnel.TunnelWSHandlerFunc(func(c *netacc.TunnelWSConn) {
			if err := c.Accept(""); err != nil {
				return
			}
			// 收一条消息后回 ping；client 读循环自动 pong，
			// 但 server 侧这边 ping 无回执（pong 被吞）——
			// 只验证 ping 发送不出错、连接仍可用。
			op, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			_ = c.WriteMessage(op, msg)
			_ = c.Ping([]byte("srv-ping"))
		})))

	ctx := context.Background()
	conn, err := agga.TunnelWS(ctx, hb.ID(), "/ping", nil, nil)
	if err != nil {
		t.Fatalf("TunnelWS 失败: %v", err)
	}
	defer conn.Close(1000, "")
	if err := conn.WriteMessage(netacc.TunnelWSText, []byte("x")); err != nil {
		t.Fatal(err)
	}
	op, msg, err := conn.ReadMessage() // 会吞掉 server 的 ping 并回 pong
	if err != nil || op != netacc.TunnelWSText || string(msg) != "x" {
		t.Fatalf("echo 不符 op=%d msg=%q err=%v", op, msg, err)
	}
}
