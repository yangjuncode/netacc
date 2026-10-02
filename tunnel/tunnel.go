// Package tunnel 在 netacc 聚合流之上提供两类隧道能力：
//
//  1. TCP 端口映射：对外是普通 TCP 监听端口，对内每条连接走一条聚合流
//     到达对端，由对端转发到固定的上游 TCP 服务——经典的
//     「端口转发/隧道」形态。一条 TCP 连接对应一条聚合流，互不影响；
//     上游地址只由 Server 的 WithUpstream 固定配置。
//  2. HTTP/WebSocket 应用层隧道：带 "NTUN\x01" magic 的聚合流按
//     docs/spec/tunnel-http-ws.md 的 tunnelwire 帧协议承载一次 HTTP
//     请求或一条 WS 会话；相对路径交给本地 handler，绝对 URL 交给
//     allowlist 控制的 HTTP/WS 代理。
//
// Client.Run 只负责 raw TCP 端口映射；TunnelFetch/TunnelWS 是 root
// netacc.Aggregator 上的客户端 API。Server 可同时配置 WithUpstream
// 与应用层隧道选项，按流前缀 magic 自动分流。
//
// 明确不支持 TCP 半关闭：任一向 io.Copy 返回（EOF/出错）即视为
// 会话结束，立即关闭两端连接并等待另一向退出。聚合流没有
// CloseWrite 语义，单边 EOF 无法透传，统一按「一条会话两连接
// 同生共死」处理。
package tunnel

import (
	"io"
	"net"
	"sync"
)

// bridge 双向转发 a↔b 直到任一向结束：io.Copy 返回（EOF/出错）
// 即关闭两端连接——另一向的拷贝随连接关闭退出，wg 等它收尾后
// 才返回，保证不泄漏 goroutine。
//
// 不支持半关闭是刻意的：netacc.Stream 无 CloseWrite，单边 EOF
// 语义无法透传，任一向结束直接终结整条映射。
func bridge(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(a, b) // b → a
		_ = a.Close()
		_ = b.Close()
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(b, a) // a → b
		_ = a.Close()
		_ = b.Close()
	}()
	wg.Wait()
}

// connSet 跟踪活动连接集合（TCP 连接与聚合流统一看待）：
// closeAll 幂等关闭全部成员；集合关闭后新 add 的连接立即被关，
// 兜住「ctx 取消」与「handler 登记连接」之间的竞态窗口。
type connSet struct {
	mu     sync.Mutex
	set    map[net.Conn]struct{}
	closed bool
}

func newConnSet() *connSet {
	return &connSet{set: make(map[net.Conn]struct{})}
}

// add 登记 conn；集合已关闭时直接关掉 conn（调用方照常走清理路径）。
func (cs *connSet) add(c net.Conn) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.closed {
		_ = c.Close()
		return
	}
	cs.set[c] = struct{}{}
}

// del 摘除 conn（handler 退出前调用；此后 closeAll 不再动它）。
func (cs *connSet) del(c net.Conn) {
	cs.mu.Lock()
	delete(cs.set, c)
	cs.mu.Unlock()
}

// closeAll 关闭集合：先标记 closed 再逐连接 Close，幂等。
// 真正的 Close 放到锁外做，避免个别实现的 Close 短暂阻塞拖住 add/del。
func (cs *connSet) closeAll() {
	cs.mu.Lock()
	if cs.closed {
		cs.mu.Unlock()
		return
	}
	cs.closed = true
	conns := make([]net.Conn, 0, len(cs.set))
	for c := range cs.set {
		conns = append(conns, c)
	}
	cs.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}
