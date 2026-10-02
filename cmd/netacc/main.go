// netacc 命令行：TCP 端口映射与 HTTP/WS 隧道 server 的最小可运行形态。
//
// 用法：
//
//	netacc server -listen /ip4/0.0.0.0/tcp/4001 -upstream 127.0.0.1:80
//	netacc server -listen /ip4/0.0.0.0/tcp/4001 -tunnel-http-echo -tunnel-ws-echo \
//		-tunnel-allow https://api.example.com -tunnel-allow wss://events.example.com
//	netacc client -server /ip4/<host>/tcp/<port>/p2p/<peerID> -listen-port 8080
//
// server 端把未带隧道 magic 的聚合流桥接到 -upstream 指定的固定 TCP 服务；
// 带 "NTUN\x01" 的 HTTP/WS 隧道流按 target 分流到 echo handler 或
// -tunnel-allow 允许的上游代理。client 端把 0.0.0.0:<listen-port> 上的每条
// TCP 连接经一条聚合流桥接到 server peer。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/tunnel"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "server":
		err = runServer(os.Args[2:])
	case "client":
		err = runClient(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "未知子命令 %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `用法:
  netacc server -listen <multiaddr> [-upstream <host:port>]
    [-tunnel-http-echo] [-tunnel-ws-echo] [-tunnel-allow <URL或host:port>...]
  netacc client -server <multiaddr带/p2p/> -listen-port <端口>

示例:
  netacc server -listen /ip4/0.0.0.0/tcp/4001 -upstream 127.0.0.1:80
  netacc server -listen /ip4/0.0.0.0/tcp/4001 -tunnel-http-echo -tunnel-ws-echo \
    -tunnel-allow https://api.example.com -tunnel-allow wss://events.example.com
  netacc client -server /ip4/1.2.3.4/tcp/4001/p2p/12D3KooW... -listen-port 8080
`)
}

// notifyCtx 把 SIGINT/SIGTERM 变成 ctx 取消；Run 视取消为正常退出。
func notifyCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// stringList 收集可重复 flag。
type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error {
	if strings.TrimSpace(v) == "" {
		return errors.New("值不能为空")
	}
	*l = append(*l, v)
	return nil
}

// canonicalHostPort 规整成 host:port；URL 缺省端口按 scheme 补默认端口，
// 使 "https://api.example.com" 与 "https://api.example.com:443" 等价。
func canonicalHostPort(u *url.URL) string {
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "http", "ws":
			port = "80"
		case "https", "wss":
			port = "443"
		default:
			return strings.ToLower(u.Host)
		}
	}
	return strings.ToLower(net.JoinHostPort(u.Hostname(), port))
}

// allowTunnelTargets 生成 HTTP/WS 代理目标过滤器：
//   - "scheme://host[:port][/path-prefix]"：同 scheme+host:port，若规则带
//     path 则 target path 须以它为前缀；
//   - "host:port"：不限 scheme，但同 host:port 放行。
//
// 规则只表达目标白名单；scheme 合法性仍由 server 按 HTTP/WS 会话另行校验。
func allowTunnelTargets(rules []string) (tunnel.TunnelTargetFilter, error) {
	type rule struct {
		scheme string
		host   string
		path   string
	}
	parsed := make([]rule, 0, len(rules))
	for _, raw := range rules {
		raw = strings.TrimSpace(raw)
		if strings.Contains(raw, "://") {
			u, err := url.Parse(raw)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return nil, fmt.Errorf("-tunnel-allow 规则 %q 非法（须为 scheme://host[:port][/path]）", raw)
			}
			parsed = append(parsed, rule{scheme: strings.ToLower(u.Scheme), host: canonicalHostPort(u), path: u.EscapedPath()})
			continue
		}
		h, p, err := net.SplitHostPort(raw)
		if err != nil || h == "" || p == "" {
			return nil, fmt.Errorf("-tunnel-allow 规则 %q 非法（须为 host:port 或带 scheme 的 URL）", raw)
		}
		parsed = append(parsed, rule{host: strings.ToLower(net.JoinHostPort(h, p))})
	}
	return func(_ peer.ID, _ tunnel.TunnelKind, target *url.URL) bool {
		for _, r := range parsed {
			if r.scheme != "" && !strings.EqualFold(r.scheme, target.Scheme) {
				continue
			}
			if r.host != canonicalHostPort(target) {
				continue
			}
			if r.path != "" && r.path != "/" && !strings.HasPrefix(target.EscapedPath(), r.path) {
				continue
			}
			return true
		}
		return false
	}, nil
}

// httpEcho 是 -tunnel-http-echo 使用的最小本地 handler：回显请求体，
// 并通过响应头暴露 method，便于联调 tunnelFetch。
func httpEcho(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Tunnel-Echo", "1")
	w.Header().Set("X-Tunnel-Method", r.Method)
	_, _ = io.Copy(w, r.Body)
}

// wsEcho 是 -tunnel-ws-echo 使用的最小本地 WS handler：接受首个候选
// 子协议并逐条回显消息。
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
			return
		}
		if err := c.WriteMessage(op, msg); err != nil {
			return
		}
	}
}

// runServer 跑隧道出口：Accept 聚合流，按首字节分流到 raw TCP、
// HTTP handler/proxy 或 WS handler/proxy。
func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	listen := fs.String("listen", "/ip4/0.0.0.0/tcp/0", "libp2p 监听 multiaddr")
	upstream := fs.String("upstream", "", "上游 TCP 服务地址 host:port（可选）")
	httpEchoFlag := fs.Bool("tunnel-http-echo", false, "启用相对路径的 HTTP echo handler")
	wsEchoFlag := fs.Bool("tunnel-ws-echo", false, "启用相对路径的 WS echo handler")
	var allow stringList
	fs.Var(&allow, "tunnel-allow", "允许代理的绝对目标：host:port 或 scheme://host[:port][/path]；可重复")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // -h：flag 包已打印用法，不算运行错误
		}
		return err
	}
	if _, err := peer.AddrInfoFromString(*listen); err == nil {
		// -listen 不应带 /p2p/ 后缀——那是给 client 的对端地址形态
		return fmt.Errorf("-listen 不应含 /p2p/<peerID> 部分: %s", *listen)
	}

	var opts []tunnel.ServerOption
	if *upstream != "" {
		opts = append(opts, tunnel.WithUpstream(*upstream))
	}
	if *httpEchoFlag {
		opts = append(opts, tunnel.WithTunnelHTTPHandler(http.HandlerFunc(httpEcho)))
	}
	if *wsEchoFlag {
		opts = append(opts, tunnel.WithTunnelWSHandler(tunnel.TunnelWSHandlerFunc(wsEcho)))
	}
	if len(allow) > 0 {
		filter, err := allowTunnelTargets(allow)
		if err != nil {
			return err
		}
		opts = append(opts,
			tunnel.WithTunnelHTTPProxy(tunnel.HTTPProxyConfig{Allow: filter}),
			tunnel.WithTunnelWSProxy(tunnel.WSProxyConfig{Allow: filter}),
		)
	}
	if len(opts) == 0 {
		return errors.New("缺少服务配置：请指定 -upstream、-tunnel-http-echo、-tunnel-ws-echo 或 -tunnel-allow")
	}

	h, err := libp2p.New(libp2p.ListenAddrStrings(*listen))
	if err != nil {
		return fmt.Errorf("创建 host 失败: %w", err)
	}
	defer h.Close()

	agg := netacc.New(h)
	defer agg.Close()

	srv, err := tunnel.NewServer(agg, opts...)
	if err != nil {
		return err
	}

	fmt.Printf("PeerID: %s\n", h.ID())
	for _, a := range h.Addrs() {
		fmt.Printf("监听: %s/p2p/%s\n", a, h.ID())
	}
	if *upstream != "" {
		fmt.Printf("TCP 上游: %s\n", *upstream)
	}
	if *httpEchoFlag {
		fmt.Println("HTTP 隧道: 相对路径 echo handler 已启用")
	}
	if *wsEchoFlag {
		fmt.Println("WS 隧道: 相对路径 echo handler 已启用")
	}
	if len(allow) > 0 {
		fmt.Printf("HTTP/WS 代理 allowlist: %s\n", strings.Join(allow, ", "))
	}

	ctx, stop := notifyCtx()
	defer stop()
	return srv.Run(ctx)
}

// runClient 跑隧道入口：本地 TCP 监听端口 → 每条连接一条聚合流。
func runClient(args []string) error {
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	server := fs.String("server", "", "server 的 multiaddr，须以 /p2p/<peerID> 结尾（必填）")
	listenPort := fs.Int("listen-port", -1, "TCP 监听端口（必填；固定监听 0.0.0.0）")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // -h：flag 包已打印用法，不算运行错误
		}
		return err
	}
	if *server == "" {
		return errors.New("缺少必填参数 -server（如 -server /ip4/1.2.3.4/tcp/4001/p2p/12D3KooW...）")
	}
	if *listenPort < 0 {
		return errors.New("缺少必填参数 -listen-port（如 -listen-port 8080）")
	}

	// AddrInfoFromString 解析 multiaddr 并拆出 /p2p/<peerID> 部分；
	// 缺 /p2p/ 段时报 ErrInvalidAddr。
	info, err := peer.AddrInfoFromString(*server)
	if err != nil {
		return fmt.Errorf("-server 须是含 /p2p/<peerID> 的 multiaddr: %w", err)
	}

	// client 只主动拨 server，不需要自身监听地址（NAT 友好）；
	// 对端的入向 PATH_ATTACH 拨不进来时仅自动补路径功能受限，
	// 本侧发起的补路径不受影响。
	h, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		return fmt.Errorf("创建 host 失败: %w", err)
	}
	defer h.Close()

	agg := netacc.New(h)
	defer agg.Close()

	ctx, stop := notifyCtx()
	defer stop()

	if err := h.Connect(ctx, *info); err != nil {
		return fmt.Errorf("连接 server %s 失败: %w", info.ID, err)
	}

	cli, err := tunnel.NewClient(agg, info.ID, tunnel.WithListenPort(*listenPort))
	if err != nil {
		return err
	}

	fmt.Printf("已连接 server: %s\n", info.ID)

	// 后台跑 Run，等 listener 就位后打印实际监听地址
	// （-listen-port 0 时拿到内核分配的端口），再阻塞至 Run 返回。
	runErr := make(chan error, 1)
	go func() { runErr <- cli.Run(ctx) }()
	deadline := time.After(3 * time.Second)
	for cli.Addr() == nil {
		select {
		case err := <-runErr:
			return err
		case <-deadline:
			return errors.New("等待 TCP 监听就绪超时")
		case <-time.After(10 * time.Millisecond):
		}
	}
	fmt.Printf("TCP 端口映射已就绪: %s（每条 TCP 连接对应一条聚合流）\n", cli.Addr())
	return <-runErr
}
