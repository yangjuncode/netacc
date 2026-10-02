// netacc 命令行：TCP 端口映射 tunnel 的最小可运行形态。
//
// 用法：
//
//	netacc server -listen /ip4/0.0.0.0/tcp/4001 -upstream 127.0.0.1:80
//	netacc client -server /ip4/<host>/tcp/<port>/p2p/<peerID> -listen-port 8080
//
// server 端把每条聚合流桥接到 -upstream 指定的固定 TCP 服务；
// client 端把 0.0.0.0:<listen-port> 上的每条 TCP 连接经一条聚合流
// 桥接到 server peer——一条 TCP 连接对应一条聚合流。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
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
  netacc server -listen <multiaddr> -upstream <host:port>
  netacc client -server <multiaddr带/p2p/> -listen-port <端口>

示例:
  netacc server -listen /ip4/0.0.0.0/tcp/4001 -upstream 127.0.0.1:80
  netacc client -server /ip4/1.2.3.4/tcp/4001/p2p/12D3KooW... -listen-port 8080
`)
}

// notifyCtx 把 SIGINT/SIGTERM 变成 ctx 取消；Run 视取消为正常退出。
func notifyCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// runServer 跑隧道出口：Accept 聚合流并桥接到固定上游。
func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	listen := fs.String("listen", "/ip4/0.0.0.0/tcp/0", "libp2p 监听 multiaddr")
	upstream := fs.String("upstream", "", "上游 TCP 服务地址 host:port（必填）")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // -h：flag 包已打印用法，不算运行错误
		}
		return err
	}
	if *upstream == "" {
		return errors.New("缺少必填参数 -upstream（如 -upstream 127.0.0.1:80）")
	}
	if _, err := peer.AddrInfoFromString(*listen); err == nil {
		// -listen 不应带 /p2p/ 后缀——那是给 client 的对端地址形态
		return fmt.Errorf("-listen 不应含 /p2p/<peerID> 部分: %s", *listen)
	}

	h, err := libp2p.New(libp2p.ListenAddrStrings(*listen))
	if err != nil {
		return fmt.Errorf("创建 host 失败: %w", err)
	}
	defer h.Close()

	agg := netacc.New(h)
	defer agg.Close()

	srv, err := tunnel.NewServer(agg, tunnel.WithUpstream(*upstream))
	if err != nil {
		return err
	}

	fmt.Printf("PeerID: %s\n", h.ID())
	for _, a := range h.Addrs() {
		fmt.Printf("监听: %s/p2p/%s\n", a, h.ID())
	}
	fmt.Printf("上游: %s\n", *upstream)

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
