// e2e 隧道服务端：给 JS 客户端做真实 Go 端 tunnel（HTTP/WS 应用层
// 隧道，internal/tunnelwire 协议）互通验证用的小工具。
//
// 起 libp2p TCP host + netacc.New + tunnel.NewServer：
//   - HTTP handler：任意路径回 200 + X-Echo-* 头；'/inspect' 返回
//     JSON 的 {method,host,headers}，其余路径回显请求体；
//   - WS handler：Accept 首个请求子协议后逐条回显消息。
//
// 用法：go run ./js/e2e/tunnel-server [-listen /ip4/127.0.0.1/tcp/0]
// 输出行 READY <multiaddr> 供调用方抓取。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/libp2p/go-libp2p"
	netacc "github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/tunnel"
)

func main() {
	listen := flag.String("listen", "/ip4/127.0.0.1/tcp/0", "监听 multiaddr")
	flag.Parse()

	h, err := libp2p.New(libp2p.ListenAddrStrings(*listen))
	if err != nil {
		fmt.Fprintln(os.Stderr, "创建 host 失败:", err)
		os.Exit(1)
	}
	defer h.Close()

	agg := netacc.New(h)
	defer agg.Close()

	srv, err := tunnel.NewServer(agg,
		tunnel.WithTunnelHTTPHandler(http.HandlerFunc(httpEcho)),
		tunnel.WithTunnelWSHandler(tunnel.TunnelWSHandlerFunc(wsEcho)),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "创建 tunnel server 失败:", err)
		os.Exit(1)
	}

	for _, a := range h.Addrs() {
		fmt.Printf("READY %s/p2p/%s\n", a.String(), h.ID())
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := srv.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "tunnel server 退出:", err)
	}
}

// httpEcho 是本地 HTTP handler：响应带 X-Echo-Method/X-Echo-Host；
// '/inspect' 返回握手视图 JSON，其余路径回显请求体。
func httpEcho(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Echo-Method", r.Method)
	w.Header().Set("X-Echo-Host", r.Host)
	if r.URL.Path == "/inspect" {
		hdr := map[string][]string{}
		for k := range r.Header {
			hdr[k] = r.Header[k]
		}
		out, _ := json.Marshal(map[string]any{
			"method": r.Method, "host": r.Host, "path": r.URL.RequestURI(),
			"headers": hdr,
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
		return
	}
	_, _ = io.Copy(w, r.Body)
}

// wsEcho 是本地 WS handler：Accept 首个候选子协议后回显每条消息；
// 对端 WS_CLOSE/断流时结束会话。
func wsEcho(c *netacc.TunnelWSConn) {
	proto := ""
	if req := c.Request(); req != nil && len(req.Protocols) > 0 {
		proto = req.Protocols[0]
	}
	if err := c.Accept(proto); err != nil {
		return
	}
	for {
		op, msg, err := c.ReadMessage()
		if err != nil {
			return // close/abort/断流都结束循环
		}
		if err := c.WriteMessage(op, msg); err != nil {
			return
		}
	}
}
