// e2e echo 服务端：给 JS 客户端做真实 Go 端互通验证用的小工具。
// 起 libp2p TCP host + netacc.New，对每条被 Accept 的聚合流做
// io.Copy 回声，并向 PATH_ATTACH/事件打印日志。
//
// 用法：go run ./js/e2e/echo-server [-listen /ip4/127.0.0.1/tcp/0]
// 输出行 READY <multiaddr> 供调用方抓取。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/libp2p/go-libp2p"
	netacc "github.com/yangjuncode/netacc"
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

	for _, a := range h.Addrs() {
		fmt.Printf("READY %s/p2p/%s\n", a.String(), h.ID())
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		for {
			st, err := agg.Accept(ctx)
			if err != nil {
				return
			}
			id := st.ID()
			fmt.Printf("ACCEPT agg=%x peer=%s paths=%d\n", id[:4], st.Peer(), len(st.Paths()))
			go func() {
				defer st.Close()
				n, err := io.Copy(st, st) // 回显直至 EOF/出错
				fmt.Printf("ECHO-DONE agg=%x n=%d err=%v\n", id[:4], n, err)
			}()
		}
	}()

	<-ctx.Done()
}
