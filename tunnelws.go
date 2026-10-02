// 隧道 WS 会话端点（TunnelWSConn）：client 侧由 Aggregator.TunnelWS
// 返回（已建立态），server 侧由 tunnel.Server 在本地 handler 入口
// 经 NewTunnelWSConn(conn, req) 构造（待应答态——handler 调
// Accept/Reject 完成握手应答）。线协议见 internal/tunnelwire 包注释。
package netacc

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc/internal/tunnelwire"
)

// 隧道 WS 消息 opcode（WS_MESSAGE 帧首字节取值，与 RFC6455 对齐）。
const (
	// TunnelWSText 是文本消息 opcode。
	TunnelWSText = tunnelwire.WSOpcodeText
	// TunnelWSBinary 是二进制消息 opcode。
	TunnelWSBinary = tunnelwire.WSOpcodeBinary
)

// TunnelWSCloseError 是对端发来 WS_CLOSE 帧时 ReadMessage 返回的
// 错误（errors.As 可取回 code/reason）。code 遵循 RFC6455 状态码。
type TunnelWSCloseError struct {
	Code   int
	Reason string
}

func (e *TunnelWSCloseError) Error() string {
	return fmt.Sprintf("netacc: 隧道 WS 对端关闭 code=%d reason=%q", e.Code, e.Reason)
}

// TunnelAbortError 是对端发来 ABORT 帧时读侧返回的错误。
type TunnelAbortError struct {
	Code    string
	Message string
}

func (e *TunnelAbortError) Error() string {
	return fmt.Sprintf("netacc: 隧道会话被对端中止 code=%s message=%s", e.Code, e.Message)
}

// WSRejectedError 是 TunnelWS 在对端回非 101 RESULT 时返回的错误：
// 状态码与 error 文本来自对端，Header 保留对端在拒绝响应里带的头。
type WSRejectedError struct {
	Status  int
	Header  http.Header
	Message string
}

func (e *WSRejectedError) Error() string {
	return fmt.Sprintf("netacc: WS 握手被对端拒绝 status=%d: %s", e.Status, e.Message)
}

// defaultGracefulWait 是隧道 API 默认的关流冲刷等待
// （Stream.Write 只进发送缓冲，Close 前须等末帧被对端确认）。
const defaultGracefulWait = 3 * time.Second

// streamFlushProbe 把 Stats() 适配成 tunnelwire.FlushProbe；
// 底层不是 *Stream（或无 Stats 能力）时返回 nil——普通 net.Conn
// 写即送达，gracefulCloseConn 直接 Close。
func streamFlushProbe(c any) tunnelwire.FlushProbe {
	s, ok := c.(interface{ Stats() StreamStats })
	if !ok {
		return nil
	}
	return func() (uint64, uint64, int) {
		st := s.Stats()
		return st.SentBytes, st.AckedBytes, st.PendingBytes
	}
}

// gracefulCloseConn 等已写字节被对端确认（上限 d）后关闭底层连接。
//
// 根因：Stream.Write 只把数据放进发送缓冲（sendQ），发送泵异步装帧
// 下发；写完最后帧立刻 Close 会丢弃未装帧部分。轮询
// pending==0 && AckedBytes>=SentBytes 才真正安全。
func gracefulCloseConn(c net.Conn, d time.Duration) error {
	return tunnelwire.GracefulClose(c, streamFlushProbe(c), d)
}

// TunnelWSRequest 是 WS 握手请求的应用层视图（OPEN 的解构）：
// server 侧 handler 经 TunnelWSConn.Request() 获取；client 侧
// conn 也携带它（回显本端请求参数）。
type TunnelWSRequest struct {
	Peer      peer.ID     // 对端 peer（非聚合流来源时为空）
	Target    string      // OPEN.target 原文（"/path?query" 或 ws(s)://…）
	Host      string      // 显式 Host（OPEN.host 或 headers 里的 Host）
	Header    http.Header // 请求头（保重复名）
	Protocols []string    // 子协议候选
}

// TunnelWSConn 是一条隧道 WS 会话的端点，client/server 两侧共用。
//
// server 侧为「待应答」态（pending）：Request() 查看握手信息，
// Accept(protocol) 回 RESULT 101 接受、Reject(status,msg) 回非 101
// RESULT 拒绝；应答前 ReadMessage/WriteMessage 报「握手未应答」错误。
// client 侧 TunnelWS 返回即为已建立态（Subprotocol() 是协商结果）。
//
// 读侧内联解码、无后台 goroutine：ReadMessage 循环解帧，
// WS_PING 自动回 WS_PONG、WS_PONG 忽略；WS_CLOSE 回送 close 完成
// 关闭握手并转成 *TunnelWSCloseError。不支持并发 ReadMessage；
// 写侧（WriteMessage/Ping）可并发。
type TunnelWSConn struct {
	conn      net.Conn
	sess      *tunnelwire.Session
	peer      peer.ID
	closeWait time.Duration
	readMu    sync.Mutex // 序列化 ReadMessage 与 Close 握手中的读操作

	mu        sync.Mutex // 保护 req/pending/responded/term/protocol
	req       *TunnelWSRequest
	pending   bool   // server 侧待应答握手
	responded bool   // server 侧：Accept/Reject 已发出
	protocol  string // 协商出的子协议（client：RESULT 回填；server：Accept 记录）
	term      error  // 读侧终态错误（sticky）

	closeOnce sync.Once
	closeSent atomic.Bool // 本侧已发 WS_CLOSE
	closed    atomic.Bool
}

// NewTunnelWSConn 在已完成 OPEN 校验的隧道流上构造 WS 端点——
// 供 tunnel.Server 把会话交给本地 handler。conn 须已完成 magic/OPEN
// 消费（通常传带已缓冲读侧的包装连接）；req 为 OPEN 解构出的握手
// 视图，非 nil 即进入待应答态（nil 则为已建立态）。
func NewTunnelWSConn(conn net.Conn, req *TunnelWSRequest) *TunnelWSConn {
	return newTunnelWSConn(tunnelwire.NewSession(conn), conn, req, req != nil)
}

// newTunnelWSConn 是内部构造：sess 复用握手期已建好的帧会话，
// pending 显式区分待应答（server）与已建立（client）态。
func newTunnelWSConn(sess *tunnelwire.Session, conn net.Conn, req *TunnelWSRequest, pending bool) *TunnelWSConn {
	wsc := &TunnelWSConn{
		conn:      conn,
		sess:      sess,
		req:       req,
		pending:   pending,
		closeWait: defaultGracefulWait,
	}
	if s, ok := conn.(interface{ Peer() peer.ID }); ok {
		wsc.peer = s.Peer()
	}
	if wsc.peer == "" && req != nil {
		wsc.peer = req.Peer
	}
	return wsc
}

// RemotePeer 返回聚合流对端的 peer.ID；底层不是 *Stream 时为空串。
func (c *TunnelWSConn) RemotePeer() peer.ID { return c.peer }

// Request 返回握手请求视图；无握手信息（纯 client 场景）时为 nil。
func (c *TunnelWSConn) Request() *TunnelWSRequest { return c.req }

// Subprotocol 返回协商出的子协议（client 侧取自 RESULT.protocol，
// server 侧为 Accept 的参数；未协商为空串）。
func (c *TunnelWSConn) Subprotocol() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.protocol
}

func validWSProtocol(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		ok := ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", rune(ch))
		if !ok {
			return false
		}
	}
	return true
}

// pendingErr 校验「server 侧尚未应答握手」。
func (c *TunnelWSConn) pendingErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending && !c.responded {
		return fmt.Errorf("netacc: WS 握手未应答——须先 Accept 或 Reject")
	}
	return nil
}

// Accept 回 RESULT{status:101,protocol} 完成握手（server 侧）。
// 重复应答或非待应答态调用返回错误。
func (c *TunnelWSConn) Accept(protocol string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.pending {
		return fmt.Errorf("netacc: 非待应答态的 WS 连接无需 Accept")
	}
	if c.responded {
		return fmt.Errorf("netacc: WS 握手已应答")
	}
	if c.closed.Load() {
		return net.ErrClosed
	}
	if protocol != "" {
		if !validWSProtocol(protocol) {
			return fmt.Errorf("netacc: WS 子协议 %q 非法", protocol)
		}
		ok := false
		if c.req != nil {
			for _, offered := range c.req.Protocols {
				if offered == protocol {
					ok = true
					break
				}
			}
		}
		if !ok {
			return fmt.Errorf("netacc: WS 子协议 %q 不在客户端候选列表", protocol)
		}
	}
	if err := c.sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
		Kind: tunnelwire.KindWS, Status: 101, Protocol: protocol}); err != nil {
		return err
	}
	c.responded = true
	c.protocol = protocol
	return nil
}

// Reject 回 RESULT{status,error} 拒绝握手并关闭连接（server 侧）。
// status 必须是 100-599 且非 101；已应答/已关闭时直接返回 nil——
// 方便在 defer 里兜底。
func (c *TunnelWSConn) Reject(status int, message string) error {
	if status < 100 || status > 599 || status == 101 {
		return fmt.Errorf("netacc: WS 拒绝状态码 %d 非法", status)
	}
	c.mu.Lock()
	if !c.pending || c.responded || c.closed.Load() {
		c.mu.Unlock()
		return nil
	}
	c.responded = true
	c.mu.Unlock()
	_ = c.sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
		Kind: tunnelwire.KindWS, Status: status, Error: message})
	return c.Close(1000, "")
}

// ReadMessage 读下一条 WS 消息，返回 opcode（TunnelWSText/
// TunnelWSBinary）与消息字节。WS_PING 自动回 WS_PONG、WS_PONG
// 忽略；WS_CLOSE 转成 *TunnelWSCloseError 并自动回送完成关闭
// 握手；ABORT 转成 *TunnelAbortError；非法帧回 ABORT 后报错。
// 终态后重复调用返回同一错误。
func (c *TunnelWSConn) ReadMessage() (int, []byte, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	return c.readMessageLocked()
}

func (c *TunnelWSConn) readMessageLocked() (int, []byte, error) {
	if err := c.pendingErr(); err != nil {
		return 0, nil, err
	}
	c.mu.Lock()
	if c.term != nil {
		err := c.term
		c.mu.Unlock()
		return 0, nil, err
	}
	c.mu.Unlock()
	for {
		t, p, err := c.sess.ReadFrame()
		if err != nil {
			return 0, nil, c.setTerm(err)
		}
		switch t {
		case tunnelwire.FrameWSMessage:
			if len(p) < 1 || (p[0] != TunnelWSText && p[0] != TunnelWSBinary) {
				c.sendAbort("protocol_error", "WS_MESSAGE opcode 非法")
				return 0, nil, c.setTerm(tunnelwire.ErrProtocol)
			}
			return int(p[0]), p[1:], nil
		case tunnelwire.FrameWSPing:
			if err := c.sess.WriteFrame(tunnelwire.FrameWSPong, p); err != nil {
				return 0, nil, c.setTerm(err)
			}
		case tunnelwire.FrameWSPong:
			// 对端 pong：无需应用感知，跳过
		case tunnelwire.FrameWSClose:
			cm, derr := tunnelwire.DecodeClose(p)
			if derr != nil {
				c.sendAbort("protocol_error", "WS_CLOSE 载荷非法")
				return 0, nil, c.setTerm(derr)
			}
			if derr := tunnelwire.ValidateWSClose(cm); derr != nil {
				c.sendAbort("protocol_error", "WS_CLOSE code/reason 非法")
				return 0, nil, c.setTerm(derr)
			}
			// 关闭握手：本侧尚未发 close 则回送
			if c.closeSent.CompareAndSwap(false, true) {
				_ = c.sess.WriteJSON(tunnelwire.FrameWSClose,
					&tunnelwire.CloseMsg{Code: cm.Code, Reason: cm.Reason})
			}
			return 0, nil, c.setTerm(&TunnelWSCloseError{Code: cm.Code, Reason: cm.Reason})
		case tunnelwire.FrameAbort:
			a, _ := tunnelwire.DecodeAbort(p)
			return 0, nil, c.setTerm(&TunnelAbortError{Code: a.Code, Message: a.Message})
		default:
			c.sendAbort("protocol_error", "WS 会话中出现非法帧")
			return 0, nil, c.setTerm(tunnelwire.ErrProtocol)
		}
	}
}

// setTerm 记录读侧终态错误并返回它（sticky：后续读都返回它）。
func (c *TunnelWSConn) setTerm(err error) error {
	c.mu.Lock()
	if c.term == nil {
		c.term = err
	}
	err = c.term
	c.mu.Unlock()
	return err
}

// WriteMessage 写一条 WS 消息。opcode 须为 TunnelWSText 或
// TunnelWSBinary；消息（含 opcode 字节）超 4MiB 拒绝（首版不分片）。
func (c *TunnelWSConn) WriteMessage(opcode int, data []byte) error {
	if opcode != TunnelWSText && opcode != TunnelWSBinary {
		return fmt.Errorf("netacc: WS opcode %d 非法（text=1/binary=2）", opcode)
	}
	if err := c.pendingErr(); err != nil {
		return err
	}
	if c.closed.Load() {
		return net.ErrClosed
	}
	return c.sess.WriteWSMessage(byte(opcode), data)
}

// Ping 发一条 WS_PING；对端读循环会自动回 WS_PONG。
func (c *TunnelWSConn) Ping(data []byte) error {
	if err := c.pendingErr(); err != nil {
		return err
	}
	if c.closed.Load() {
		return net.ErrClosed
	}
	return c.sess.WriteFrame(tunnelwire.FrameWSPing, data)
}

// Pong 发一条 WS_PONG（主动 pong 一般不需要——ping 会被自动应答）。
func (c *TunnelWSConn) Pong(data []byte) error {
	if err := c.pendingErr(); err != nil {
		return err
	}
	if c.closed.Load() {
		return net.ErrClosed
	}
	return c.sess.WriteFrame(tunnelwire.FrameWSPong, data)
}

// Close 发送 WS_CLOSE{code,reason} 并等待对端关闭回显（最多 closeWait），
// 再等已写字节被对端确认后关闭底层流。code 只允许 1000 或 3000-4999，
// reason ≤123 UTF-8 字节；非法参数返回错误且不关闭连接。幂等。
// server 侧待应答连接被 Close 时先自动补一个 500 RESULT——
// 否则对端还在等握手应答，会直接见到 WS_CLOSE。
func (c *TunnelWSConn) Close(code int, reason string) error {
	if err := tunnelwire.ValidateWSCloseSend(&tunnelwire.CloseMsg{Code: code, Reason: reason}); err != nil {
		return err
	}
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.mu.Lock()
		pendingUnanswered := c.pending && !c.responded
		c.responded = true
		c.mu.Unlock()
		if pendingUnanswered {
			_ = c.sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
				Kind: tunnelwire.KindWS, Status: 500, Error: "握手未应答即关闭"})
			_ = gracefulCloseConn(c.conn, c.closeWait)
			return
		}
		if c.closeSent.CompareAndSwap(false, true) {
			_ = c.sess.WriteJSON(tunnelwire.FrameWSClose,
				&tunnelwire.CloseMsg{Code: code, Reason: reason})
			_ = c.conn.SetReadDeadline(time.Now().Add(c.closeWait))
			c.readMu.Lock()
			for {
				_, _, err := c.readMessageLocked()
				var closeErr *TunnelWSCloseError
				if errors.As(err, &closeErr) {
					break
				}
				if err != nil {
					break
				}
			}
			c.readMu.Unlock()
			_ = c.conn.SetReadDeadline(time.Time{})
		}
		_ = gracefulCloseConn(c.conn, c.closeWait)
	})
	return nil
}

// Abort 发一帧 ABORT{code,message} 后关闭底层流（幂等）。
func (c *TunnelWSConn) Abort(code, message string) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		_ = c.sess.Abort(code, message)
		_ = gracefulCloseConn(c.conn, c.closeWait)
	})
}

// sendAbort 内部用：尽力发 ABORT（不置 closed——仅协议违例告知）。
func (c *TunnelWSConn) sendAbort(code, message string) {
	_ = c.sess.Abort(code, message)
}
