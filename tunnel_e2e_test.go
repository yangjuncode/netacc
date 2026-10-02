package netacc_test

// 隧道应用层端到端补充：真实 libp2p 双 host + tunnel.NewServer。
// 本文件只覆盖 tunnelclient_e2e_test.go 未覆盖的部分——
// 绝对 URL 代理（HTTP/WS，allow/deny）与 OpenOption 透传；
// 共享其 runTunnelServer 辅助。

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/tunnel"
)

// newTunnelPair2 建好 (client agg, server hostID)；server 端聚合器
// 上跑带给定选项的 tunnel.Server。
func newTunnelPair2(t *testing.T, opts ...tunnel.ServerOption) (*netacc.Aggregator, peer.ID) {
	t.Helper()
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	t.Cleanup(func() { agga.Close() })
	t.Cleanup(func() { aggb.Close() })
	connectHosts(t, ha, hb)
	runTunnelServer(t, aggb, opts...)
	return agga, hb.ID()
}

// TestTunnelFetchProxyE2E：绝对 http:// target 经代理到真实上游：
// path/query/响应头透传；非法 scheme 在 client 侧构造期即报错。
func TestTunnelFetchProxyE2E(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "real")
		_, _ = w.Write([]byte("up:" + r.URL.RequestURI()))
	}))
	defer upstream.Close()

	agga, serverID := newTunnelPair2(t, tunnel.WithTunnelHTTPProxy(
		tunnel.HTTPProxyConfig{Allow: func(_ peer.ID, k tunnel.TunnelKind, _ *url.URL) bool {
			return k == tunnel.TunnelKindHTTP
		}}))

	ctx := context.Background()
	req, _ := http.NewRequest(http.MethodGet, upstream.URL+"/p?q=1", nil)
	resp, err := agga.TunnelFetch(ctx, serverID, req)
	if err != nil {
		t.Fatalf("TunnelFetch 失败: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "up:/p?q=1" ||
		resp.Header.Get("X-Upstream") != "real" {
		t.Fatalf("代理响应错误: code=%d body=%q", resp.StatusCode, body)
	}

	// 非 http(s) scheme → client 侧直接报错（不开流）
	req2, _ := http.NewRequest(http.MethodGet, "ftp://x/y", nil)
	if _, err := agga.TunnelFetch(ctx, serverID, req2); err == nil {
		t.Fatal("非法 scheme 应报错")
	}
}

// TestTunnelFetchProxyDenyE2E：Allow=false → RESULT 403
// （仍是合法 HTTP 响应，不是传输错误）；Allow=nil 同理默认拒绝。
func TestTunnelFetchProxyDenyE2E(t *testing.T) {
	deny := tunnel.HTTPProxyConfig{Allow: func(peer.ID, tunnel.TunnelKind, *url.URL) bool {
		return false
	}}
	agga, serverID := newTunnelPair2(t, tunnel.WithTunnelHTTPProxy(deny))
	req, _ := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	resp, err := agga.TunnelFetch(context.Background(), serverID, req)
	if err != nil {
		t.Fatalf("拒绝也应是正常 HTTP 响应: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("应回 403，得到 %d", resp.StatusCode)
	}
}

// TestTunnelFetchOptsPassthrough：opts 透传进 OpenStream——
// WithMinPaths(99) 无法满足时 ErrMinPaths 包装错误冒泡回来。
func TestTunnelFetchOptsPassthrough(t *testing.T) {
	agga, serverID := newTunnelPair2(t,
		tunnel.WithTunnelHTTPHandler(http.NotFoundHandler()))
	req, _ := http.NewRequest(http.MethodGet, "/x", nil)
	_, err := agga.TunnelFetch(context.Background(), serverID, req,
		netacc.WithMinPaths(99))
	if !errors.Is(err, netacc.ErrMinPaths) {
		t.Fatalf("opts 未透传：应冒泡 ErrMinPaths，得到 %v", err)
	}
}

// TestTunnelWSProxyE2E：ws:// 绝对 target 经代理到真实 gorilla
// 上游：echo 往返 + 主动 close 收尾。
func TestTunnelWSProxyE2E(t *testing.T) {
	up := websocket.Upgrader{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer upstream.Close()
	wsURL := "ws" + strings.TrimPrefix(upstream.URL, "http")

	agga, serverID := newTunnelPair2(t, tunnel.WithTunnelWSProxy(
		tunnel.WSProxyConfig{Allow: func(_ peer.ID, k tunnel.TunnelKind, u *url.URL) bool {
			return k == tunnel.TunnelKindWS && u.Scheme == "ws"
		}}))

	conn, err := agga.TunnelWS(context.Background(), serverID, wsURL, nil, nil)
	if err != nil {
		t.Fatalf("TunnelWS 代理握手失败: %v", err)
	}
	if err := conn.WriteMessage(netacc.TunnelWSText, []byte("经代理")); err != nil {
		t.Fatalf("写消息失败: %v", err)
	}
	op, data, err := conn.ReadMessage()
	if err != nil || op != netacc.TunnelWSText || string(data) != "经代理" {
		t.Fatalf("代理 echo 错误: op=%d data=%q err=%v", op, data, err)
	}
	if err := conn.Close(1000, ""); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
}

// TestTunnelWSProxyDenyE2E：WS 代理 Allow=nil → WSRejectedError(403)。
func TestTunnelWSProxyDenyE2E(t *testing.T) {
	agga, serverID := newTunnelPair2(t,
		tunnel.WithTunnelWSProxy(tunnel.WSProxyConfig{}))
	_, err := agga.TunnelWS(context.Background(), serverID, "ws://127.0.0.1:1/x", nil, nil)
	var rej *netacc.WSRejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("Allow=nil 应拒绝，得到 %v", err)
	}
	if rej.Status != 403 {
		t.Fatalf("应为 403，得到 %d", rej.Status)
	}
}
