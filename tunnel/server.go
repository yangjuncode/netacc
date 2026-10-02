package tunnel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/internal/tunnelwire"
)

// StreamAccepter 抽象「接收对端发起的聚合流」的能力，
// *netacc.Aggregator 天然满足（签名与 Aggregator.Accept 一致）。
// 与 StreamOpener 同理：返回 *netacc.Stream 是为了让 Aggregator
// 直接实现本接口，tunnel 内部解耦成 net.Conn 使用。
//
// 实现须响应 ctx 取消并返回错误——Server.Run 的退出依赖它。
type StreamAccepter interface {
	Accept(ctx context.Context) (*netacc.Stream, error)
}

// ServerOption 是 NewServer 的构造选项。
type ServerOption interface{ applyServerOption(*serverConfig) }

// serverOption 是 ServerOption 的函数实现。
type serverOption func(*serverConfig)

func (f serverOption) applyServerOption(c *serverConfig) { f(c) }

// serverConfig 是 Server 的生效配置（构造默认 + 选项覆盖）。
type serverConfig struct {
	// upstream：固定上游地址 host:port；为空表示不提供 raw TCP 桥接
	// （此时必须至少配置一个 HTTP/WS 隧道处理器或代理）。
	upstream string

	// ---- 应用层隧道（HTTP/WS over tunnelwire 协议）----
	httpHandler http.Handler     // 相对 target 的 HTTP 本地 handler
	httpProxy   *HTTPProxyConfig // 绝对 http(s):// target 的代理
	wsHandler   TunnelWSHandler  // 相对 target 的 WS 本地 handler
	wsProxy     *WSProxyConfig   // 绝对 ws(s):// target 的代理
	openTimeout time.Duration    // magic 嗅探 + OPEN 帧等待超时（≤0 取默认）
	closeWait   time.Duration    // gracefulClose 的 ACK 等待上限（≤0 取默认）
}

// 默认超时参数。
const (
	// defaultOpenTimeout 是 magic 嗅探与 OPEN 帧等待的默认超时：
	// 防对端建流后迟迟不写字节挂住 handler。
	defaultOpenTimeout = 10 * time.Second
	// defaultCloseWait 是收尾时等末帧被对端确认的默认上限
	// （Stream.Write 只进发送缓冲，立即 Close 会丢未装帧数据）。
	defaultCloseWait = 3 * time.Second
)

// WithUpstream 设置固定上游地址 host:port：每条「未携带隧道 magic」的
// 聚合流都拨向它做 raw TCP 桥接。上游只能由 server 侧配置，client 不可指定。
// 可与 HTTP/WS 隧道选项并存（magic 分流）；不配任何隧道选项时必填。
func WithUpstream(addr string) ServerOption {
	return serverOption(func(c *serverConfig) { c.upstream = addr })
}

// WithTunnelHTTPHandler 设置相对 target（"/path" 形态）的 HTTP 本地
// 处理器：OPEN{kind:"http", target:"/..."} 会被构造成 *http.Request 交给
// h 处理，响应经 RESULT+DATA 帧回传。h 为 nil 等同于未配置。
func WithTunnelHTTPHandler(h http.Handler) ServerOption {
	return serverOption(func(c *serverConfig) { c.httpHandler = h })
}

// WithTunnelHTTPProxy 设置绝对 http(s):// target 的 HTTP 代理：
// OPEN{kind:"http", target:"http(s)://..."} 经 cfg.Allow 过滤后由
// cfg.Transport（nil=http.DefaultTransport）转发到代理目标。
func WithTunnelHTTPProxy(cfg HTTPProxyConfig) ServerOption {
	return serverOption(func(c *serverConfig) { c.httpProxy = &cfg })
}

// WithTunnelWSHandler 设置相对 target（"/path" 形态）的 WebSocket 本地
// 处理器：OPEN{kind:"ws", target:"/..."} 的握手信息经
// netacc.TunnelWSConn.Request() 交给 h，由 h 调 conn.Accept(protocol)
// 接受（RESULT 101）或 conn.Reject(status,msg) 拒绝。
func WithTunnelWSHandler(h TunnelWSHandler) ServerOption {
	return serverOption(func(c *serverConfig) { c.wsHandler = h })
}

// WithTunnelWSProxy 设置绝对 ws(s):// target 的 WebSocket 代理：
// OPEN{kind:"ws", target:"ws(s)://..."} 经 cfg.Allow 过滤后用
// gorilla/websocket 拨代理目标并双向泵转发消息/ping/pong/close。
func WithTunnelWSProxy(cfg WSProxyConfig) ServerOption {
	return serverOption(func(c *serverConfig) { c.wsProxy = &cfg })
}

// WithTunnelOpenTimeout 设置 magic 嗅探 + OPEN 帧等待超时
// （默认 10s）。超时未发 magic 的连接按 raw 桥接处理（保留已 peek 字节）；
// 已发 magic 但迟迟不发 OPEN 的连接直接关闭。
func WithTunnelOpenTimeout(d time.Duration) ServerOption {
	return serverOption(func(c *serverConfig) { c.openTimeout = d })
}

// WithTunnelCloseWait 设置收尾时等待末帧被对端确认的上限
// （默认 3s，合理区间 2-5s）。只影响 *netacc.Stream；
// 普通 net.Conn 无发送缓冲语义，直接关闭。
func WithTunnelCloseWait(d time.Duration) ServerOption {
	return serverOption(func(c *serverConfig) { c.closeWait = d })
}

func (c *serverConfig) openTimeoutOrDefault() time.Duration {
	if c.openTimeout <= 0 {
		return defaultOpenTimeout
	}
	return c.openTimeout
}

func (c *serverConfig) closeWaitOrDefault() time.Duration {
	if c.closeWait <= 0 {
		return defaultCloseWait
	}
	return c.closeWait
}

// hasTunnel 报告是否配置了任一 HTTP/WS 隧道处理器/代理。
func (c *serverConfig) hasTunnel() bool {
	return c.httpHandler != nil || c.httpProxy != nil ||
		c.wsHandler != nil || c.wsProxy != nil
}

// Server 是 tunnel 出口：Accept 聚合流，按首字节分流——
// 带 "NTUN\x01" magic 的流走 HTTP/WS 应用层隧道协议，
// 无 magic 的流桥接到固定上游 TCP 服务。
// 用 NewServer 构造、Run 运行；单次使用——Run 返回后不可复用。
type Server struct {
	cfg    serverConfig
	accept func(ctx context.Context) (net.Conn, error)
	dialer net.Dialer

	actives *connSet       // 活动聚合流 + 上游连接
	wg      sync.WaitGroup // 进行中的 serveStream handler
	ran     atomic.Bool    // Run 只允许进入一次
}

// NewServer 构造 tunnel server：accepter 用于接收聚合流
// （生产环境直接传 *netacc.Aggregator）。
//
// 至少要配置一种服务形态：WithUpstream（raw TCP 桥接）或任一
// WithTunnelHTTPHandler/WithTunnelHTTPProxy/WithTunnelWSHandler/
// WithTunnelWSProxy（应用层隧道）。参数缺失/非法（nil accepter、
// 全部服务形态缺失、upstream 不是 host:port 形态）在构造期返回 error。
func NewServer(accepter StreamAccepter, opts ...ServerOption) (*Server, error) {
	if accepter == nil {
		return nil, errors.New("tunnel: StreamAccepter 不能为 nil")
	}
	cfg := serverConfig{}
	for _, o := range opts {
		if o != nil {
			o.applyServerOption(&cfg)
		}
	}
	if cfg.upstream != "" {
		if err := checkUpstream(cfg.upstream); err != nil {
			return nil, err
		}
	} else if !cfg.hasTunnel() {
		return nil, errors.New("tunnel: 未配置任何服务形态：须 WithUpstream 或至少一个 HTTP/WS 隧道处理器/代理")
	}
	return newServer(func(ctx context.Context) (net.Conn, error) {
		st, err := accepter.Accept(ctx)
		if err != nil {
			return nil, err
		}
		return st, nil
	}, cfg), nil
}

// newServer 是内部构造：accept 已解耦成 net.Conn 粒度，测试可注入
// net.Pipe 等假实现而不必伪造 *netacc.Stream。
func newServer(accept func(ctx context.Context) (net.Conn, error), cfg serverConfig) *Server {
	return &Server{cfg: cfg, accept: accept, actives: newConnSet()}
}

// checkUpstream 构造期校验上游地址：必须是 host:port 完整形态，
// host 非空、端口为 1-65535 数字。域名 host 留到拨号时解析，
// 这里不做 DNS。
func checkUpstream(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("tunnel: 上游地址 %q 非法（须为 host:port 形态）: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("tunnel: 上游地址 %q 缺少 host", addr)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("tunnel: 上游地址 %q 的端口非法（须为 1-65535 数字）", addr)
	}
	return nil
}

// Run 阻塞 Accept 聚合流并逐条分流处理，至 ctx 取消（返回 nil）
// 或 Accept 报非取消错误（包装上下文后返回）。ctx 取消时主动断开
// 全部活动聚合流与上游连接，并等 handler 退出后才返回。
func (s *Server) Run(ctx context.Context) error {
	if !s.ran.CompareAndSwap(false, true) {
		return errors.New("tunnel: Server.Run 只能调用一次")
	}
	stop := context.AfterFunc(ctx, s.actives.closeAll)
	defer func() {
		stop()
		s.actives.closeAll() // 幂等；覆盖非取消路径的收尾
		s.wg.Wait()          // 等全部 handler 收尾，不泄漏 goroutine
	}()

	for {
		st, err := s.accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // ctx 取消视为正常退出
			}
			return fmt.Errorf("tunnel: 接收聚合流失败: %w", err)
		}
		s.actives.add(st)
		s.wg.Add(1)
		go s.serveStream(ctx, st)
	}
}

// bufferedConn 在 conn 读路径前插一个 bufio.Reader：sniff 阶段 Peek
// 出的字节留在缓冲区里，后续 Read 先消耗它们再读底层流——raw 桥接
// 场景下 client 已发出的前导字节不丢。写/关闭/deadline 直通底层 conn。
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// ReadByte 透传 bufio.ReadByte：让 tunnelwire.Session 的帧头 uvarint
// 解码直接命中缓冲，不走逐字节 Read。
func (c *bufferedConn) ReadByte() (byte, error) { return c.r.ReadByte() }

// Stats 适配 netacc 的写侧水位探测：底层是 *netacc.Stream 时返回其
// Stats() 快照（gracefulClose 用），否则返回零值（等价「无发送缓冲，
// 直接关」）。
func (c *bufferedConn) Stats() netacc.StreamStats {
	if st, ok := c.Conn.(*netacc.Stream); ok {
		return st.Stats()
	}
	return netacc.StreamStats{}
}

// serveStream 处理一条聚合流：按 magic 嗅探分流——
// 隧道协议流走 serveTunnelConn，raw 流拨固定上游后双向转发。
// 上游拨号失败只关闭本条聚合流，Accept 循环不受影响。
func (s *Server) serveStream(ctx context.Context, st net.Conn) {
	defer s.wg.Done()
	defer s.actives.del(st)
	defer st.Close()

	bc := &bufferedConn{Conn: st, r: bufio.NewReader(st)}
	if s.cfg.upstream == "" {
		// 纯隧道模式（NewServer 保证此时至少配了一个隧道形态）：
		// 嗅探 client 首字节，magic → 隧道分发，非 magic → 直接关
		if s.sniffMagic(bc) {
			s.serveTunnelConn(ctx, bc)
		}
		return
	}
	s.serveWithUpstream(ctx, bc)
}

// sniffGrace 是「上游先出动静」时额外等 magic 的宽限窗：
// 上游先说话几乎可判 raw，但 client 的 magic 可能恰在路上——
// 给 sniff 一个短宽限，避免把刚发 magic 的隧道流误判为 raw。
// 隧道 client 建流后立即写 magic，延迟超过此窗口属病态。
const sniffGrace = 150 * time.Millisecond

// serveWithUpstream 处理「配置了固定上游」的聚合流：client 侧
// magic 嗅探与「上游拨号 + 首块预读」并发竞速——
//   - client 前缀逐字节匹配上 magic → 隧道协议流；
//   - client 前缀失配 / 嗅探超时 → raw 桥接（peek 字节经 bc 重放）；
//   - client 静默但上游先产出字节/报错 → raw 桥接（上游先说话是
//     raw 的强证据：隧道 client 建流即写 magic，不会先等上游）。
//
// 这种竞速让 banner 类协议（SSH/SMTP 等上游先说话）与
// 「上游只拨、client 先写」的协议都正常工作。
func (s *Server) serveWithUpstream(ctx context.Context, bc *bufferedConn) {
	st := bc.Conn
	timeout := s.cfg.openTimeoutOrDefault()

	sniffCh := make(chan bool, 1)
	go func() { sniffCh <- s.sniffMagic(bc) }()

	// 上游准备 goroutine：拨号结果经 dialCh 先到；拨成后带超时
	// 预读首块经 headCh 再到（banner 类协议立刻产出 raw 证据）。
	// upCtx 取消可中止在途拨号。
	type dialRes struct {
		c   net.Conn
		err error
	}
	type headRes struct {
		head []byte
		err  error
	}
	upCtx, upCancel := context.WithCancel(ctx)
	defer upCancel()
	dialCh := make(chan dialRes, 1)
	headCh := make(chan headRes, 1)
	go func() {
		up, err := s.dialer.DialContext(upCtx, "tcp", s.cfg.upstream)
		dialCh <- dialRes{c: up, err: err}
		if err != nil {
			return
		}
		_ = up.SetReadDeadline(time.Now().Add(timeout))
		head := make([]byte, 32<<10)
		n, rerr := up.Read(head)
		_ = up.SetReadDeadline(time.Time{})
		headCh <- headRes{head: head[:n], err: rerr}
	}()

	// 结束上游准备的残留：回收已拨连接、抽干 headCh。
	// 拨号失败时 headCh 不会有事件，直接退出不等。
	reap := func(dialSeen bool, up net.Conn) {
		upCancel()
		go func() {
			if !dialSeen {
				if d := <-dialCh; d.err != nil {
					return
				} else {
					up = d.c
				}
			}
			if up != nil {
				_ = up.SetReadDeadline(time.Now()) // 踢出在途首块读
				<-headCh
				_ = up.Close()
			}
		}()
	}

	// raw 桥接：回放已预读字节后正常双向转发。
	toRaw := func(up net.Conn, h headRes, sniffPending bool) {
		if sniffPending {
			_ = st.SetReadDeadline(time.Now()) // 踢出阻塞中的 Peek
			<-sniffCh
		}
		_ = st.SetReadDeadline(time.Time{})
		if up == nil {
			return // 上游不可达：只关本流（与旧行为一致）
		}
		s.actives.add(up)
		defer s.actives.del(up)
		defer up.Close()
		if len(h.head) > 0 {
			if _, err := bc.Write(h.head); err != nil {
				return
			}
		}
		if h.err != nil {
			var ne net.Error
			if !errors.As(h.err, &ne) || !ne.Timeout() {
				return // 上游首读即终（EOF/复位）：回放后关流
			}
		}
		bridge(bc, up)
	}

	select {
	case isTun := <-sniffCh:
		if isTun {
			reap(false, nil)
			s.serveTunnelConn(ctx, bc)
			return
		}
		// client 前缀失配/超时 → raw：等拨号结果（拨号本身有 ctx 界）
		d := <-dialCh
		var h headRes
		if d.err == nil {
			_ = d.c.SetReadDeadline(time.Now()) // 收还在等的首块读
			h = <-headCh
			_ = d.c.SetReadDeadline(time.Time{})
		}
		toRaw(d.c, h, false)
	case d := <-dialCh:
		if d.err != nil {
			// 上游拨不通：client 可能是隧道流，给 magic 一个宽限
			select {
			case isTun := <-sniffCh:
				if isTun {
					s.serveTunnelConn(ctx, bc)
				} else {
					toRaw(nil, headRes{}, false)
				}
			case <-time.After(sniffGrace):
				toRaw(nil, headRes{}, true)
			}
			return
		}
		// 上游已连：再等首块或 client 嗅探结果
		select {
		case isTun := <-sniffCh:
			if isTun {
				reap(true, d.c)
				s.serveTunnelConn(ctx, bc)
				return
			}
			_ = d.c.SetReadDeadline(time.Now())
			h := <-headCh
			_ = d.c.SetReadDeadline(time.Time{})
			toRaw(d.c, h, false)
		case h := <-headCh:
			// 上游先产出首块/读终/超时 → raw 倾向；给 magic 短宽限
			select {
			case isTun := <-sniffCh:
				if isTun {
					_ = d.c.Close()
					s.serveTunnelConn(ctx, bc)
					return
				}
				toRaw(d.c, h, false)
			case <-time.After(sniffGrace):
				toRaw(d.c, h, true)
			}
		}
	}
}

// sniffMagic 以读超时为界逐字节前缀匹配 magic：读到 "NTUN\x01"
// 即判定为隧道协议流并消费之；前缀失配立刻返回 false（不凑满
// 5 字节——client 首包不足 5 字节时不等超时），超时/EOF 同样返回
// false。已 peek 的字节留在 bc.r 里，桥接时重放；返回后清除读超时。
func (s *Server) sniffMagic(bc *bufferedConn) bool {
	_ = bc.SetReadDeadline(time.Now().Add(s.cfg.openTimeoutOrDefault()))
	defer bc.SetReadDeadline(time.Time{})
	for i := 0; i < len(tunnelwire.Magic); i++ {
		peek, err := bc.r.Peek(i + 1)
		if err != nil || peek[i] != tunnelwire.Magic[i] {
			return false
		}
	}
	_, _ = bc.r.Discard(len(tunnelwire.Magic))
	return true
}

// streamPeer 取聚合流对端的 peer.ID；非 *netacc.Stream 的测试连接
// 返回空串（TunnelTargetFilter 的调用方须容忍空 peer）。
func streamPeer(c net.Conn) peer.ID {
	if st, ok := c.(*netacc.Stream); ok {
		return st.Peer()
	}
	return ""
}

// gracefulClose 写完最后帧后的收尾：等全部已写字节被对端确认
// （或超时）再关底层流。c 必须能穿透到 *netacc.Stream（直接或经
// Stats() 适配）；普通 net.Conn 直接 Close。
func gracefulClose(c net.Conn, wait time.Duration) {
	var probe tunnelwire.FlushProbe
	if sp, ok := c.(interface{ Stats() netacc.StreamStats }); ok {
		probe = func() (uint64, uint64, int) {
			st := sp.Stats()
			return st.SentBytes, st.AckedBytes, st.PendingBytes
		}
	}
	_ = tunnelwire.GracefulClose(c, probe, wait)
}

// serveTunnelConn 处理已确认 magic 的隧道协议流：在超时内读 OPEN
// 并按 kind/target 形态分发到 HTTP/WS handler 或代理。
func (s *Server) serveTunnelConn(ctx context.Context, bc *bufferedConn) {
	sess := tunnelwire.NewSession(bc)

	// OPEN 帧同样受 openTimeout 约束：magic 之后迟迟不发 OPEN 的连接
	// 不能无限占着 handler。
	_ = bc.SetReadDeadline(time.Now().Add(s.cfg.openTimeoutOrDefault()))
	ft, payload, err := sess.ReadFrame()
	_ = bc.SetReadDeadline(time.Time{})
	if err != nil {
		return
	}
	if ft != tunnelwire.FrameOpen {
		_ = sess.Abort("protocol_error", "隧道首帧必须是 OPEN")
		gracefulClose(bc, s.cfg.closeWaitOrDefault())
		return
	}
	open, err := tunnelwire.DecodeOpen(payload)
	if err != nil {
		_ = sess.Abort("bad_open", err.Error())
		gracefulClose(bc, s.cfg.closeWaitOrDefault())
		return
	}
	if err := tunnelwire.ValidateOpen(open); err != nil {
		_ = sess.Abort("bad_open", err.Error())
		gracefulClose(bc, s.cfg.closeWaitOrDefault())
		return
	}

	p := streamPeer(bc.Conn)
	switch open.Kind {
	case tunnelwire.KindHTTP:
		s.serveHTTP(ctx, bc, sess, open, p)
	case tunnelwire.KindWS:
		s.serveWS(ctx, bc, sess, open, p)
	}
}

// ---------- 代理目标过滤 ----------

// TunnelKind 标识隧道会话类型（与 OPEN.kind 一致）。
type TunnelKind = tunnelwire.Kind

const (
	// TunnelKindHTTP：HTTP 请求会话。
	TunnelKindHTTP TunnelKind = "http"
	// TunnelKindWS：WebSocket 会话。
	TunnelKindWS TunnelKind = "ws"
)

// TunnelTargetFilter 决定一个绝对形态的 target URL 是否允许代理：
// 返回 true 才放行；p 是发起方 peer.ID（测试注入的普通连接为空串）。
// 代理配置里 Allow==nil 表示一律拒绝（默认拒绝，防开放代理）。
type TunnelTargetFilter func(p peer.ID, kind TunnelKind, target *url.URL) bool

// HTTPProxyConfig 是 WithTunnelHTTPProxy 的代理配置。
type HTTPProxyConfig struct {
	// Allow 是目标过滤钩子：nil 表示全部拒绝（必须显式配置才放行）。
	Allow TunnelTargetFilter
	// Transport 是发往上游的 RoundTripper；nil 用 http.DefaultTransport。
	// 刻意用 RoundTripper 而非 http.Client：不自动跟随重定向，
	// 响应原样回传给隧道 client。
	Transport http.RoundTripper
}

// WSProxyConfig 是 WithTunnelWSProxy 的代理配置。
type WSProxyConfig struct {
	// Allow 是目标过滤钩子：nil 表示全部拒绝。
	Allow TunnelTargetFilter
	// Dialer 是拨 WS 上游的 gorilla Dialer；nil 用默认配置。
	// 注意其 Subprotocols 字段会被 OPEN.protocols 覆盖。
	Dialer *websocket.Dialer
}
