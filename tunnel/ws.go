package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/internal/tunnelwire"
)

// ---------- WS 处理器接口 ----------

// TunnelWSHandler 处理相对 target（"/path" 形态）的隧道 WS 会话。
// 实现须先调 conn.Accept(protocol)（发 RESULT 101）或
// conn.Reject(status, message) 拒绝，随后用 ReadMessage/WriteMessage
// 做消息交换；返回即结束会话（conn 由框架优雅关闭）。
// 不复用 Gorilla Upgrade：握手语义由隧道协议的 RESULT 帧承载。
type TunnelWSHandler interface {
	ServeTunnelWS(c *netacc.TunnelWSConn)
}

// TunnelWSHandlerFunc 是 TunnelWSHandler 的函数适配器。
type TunnelWSHandlerFunc func(c *netacc.TunnelWSConn)

// ServeTunnelWS 实现 TunnelWSHandler。
func (f TunnelWSHandlerFunc) ServeTunnelWS(c *netacc.TunnelWSConn) { f(c) }

// ---------- WS 会话分发 ----------

// serveWS 处理一条 OPEN{kind:"ws"} 会话：
//   - target 以 "/" 开头（且非 "//"）→ 本地 TunnelWSHandler；
//   - 绝对 ws(s):// URL → gorilla 代理转发（Allow 过滤，nil 默认拒绝）；
//   - 其它形态 → RESULT 400。
func (s *Server) serveWS(ctx context.Context, bc *bufferedConn, sess *tunnelwire.Session, open *tunnelwire.OpenMsg, p peer.ID) {
	target := open.Target
	if strings.IndexByte(target, '#') >= 0 {
		s.rejectWS(bc, sess, 400, "tunnel: WS target 不应包含 fragment")
		return
	}
	if strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "//") {
		if s.cfg.wsHandler == nil {
			s.rejectWS(bc, sess, 404, "tunnel: 未配置本地 WS handler")
			return
		}
		s.serveWSLocal(ctx, bc, sess, open, p)
		return
	}
	u, err := url.Parse(target)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") {
		s.rejectWS(bc, sess, 400, "tunnel: WS target 须为 /path 或 ws(s):// 绝对 URL")
		return
	}
	if s.cfg.wsProxy == nil {
		s.rejectWS(bc, sess, 403, "tunnel: 未配置 WS 代理")
		return
	}
	if s.cfg.wsProxy.Allow == nil || !s.cfg.wsProxy.Allow(p, TunnelKindWS, u) {
		s.rejectWS(bc, sess, 403, "tunnel: WS 目标不被允许")
		return
	}
	s.serveWSProxy(ctx, bc, sess, open, u)
}

// rejectWS 回一个非 101 的 RESULT 后收尾关流。
func (s *Server) rejectWS(bc *bufferedConn, sess *tunnelwire.Session, status int, msg string) {
	_ = sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
		Kind: tunnelwire.KindWS, Status: status, Error: msg,
	})
	gracefulClose(bc, s.cfg.closeWaitOrDefault())
}

// serveWSLocal 把会话交给本地 TunnelWSHandler：conn 未握手就绪
// （handler 必须先 Accept/Reject）；handler panic 只终结本会话；
// handler 返回即会话结束，未关闭的 conn 由这里补发 WS_CLOSE(1000)。
func (s *Server) serveWSLocal(ctx context.Context, bc *bufferedConn, sess *tunnelwire.Session, open *tunnelwire.OpenMsg, p peer.ID) {
	_ = ctx // 会话生命周期跟随流；ctx 取消时流被关闭、handler 读写出错退出

	host, pairs := tunnelwire.ExtractHost(open.Headers, open.Host)
	req := &netacc.TunnelWSRequest{
		Peer:      p,
		Target:    open.Target,
		Host:      host,
		Header:    tunnelwire.PairsToHeaders(pairs),
		Protocols: open.Protocols,
	}
	conn := netacc.NewTunnelWSConn(bc, req)
	defer func() {
		_ = recover() // handler panic 只终结本会话，不影响 Accept 循环
		_ = conn.Close(1000, "")
	}()
	s.cfg.wsHandler.ServeTunnelWS(conn)
}

// ---------- WS 代理 ----------

// wsHandshakeHeaders 是转发前必须剥除的 WS 握手头：隧道里的
// headers 只携带应用层附加头，握手字段由 gorilla 自行生成。
var wsHandshakeHeaders = []string{
	"Sec-WebSocket-Key",
	"Sec-WebSocket-Version",
	"Sec-WebSocket-Protocol", // 改由 OPEN.protocols 承载
	"Sec-WebSocket-Extensions",
	"Sec-WebSocket-Accept",
}

// serveWSProxy 把绝对 ws(s):// target 代理到目标服务：
// gorilla 拨号成功后回 RESULT{status:101,...}，然后双向泵
// WS_MESSAGE/WS_PING/WS_PONG/WS_CLOSE。上游拨号失败回
// RESULT{status:目标服务状态码或502, error:...}。
func (s *Server) serveWSProxy(ctx context.Context, bc *bufferedConn, sess *tunnelwire.Session, open *tunnelwire.OpenMsg, u *url.URL) {
	dialer := websocket.Dialer{}
	if s.cfg.wsProxy.Dialer != nil {
		dialer = *s.cfg.wsProxy.Dialer
	}
	dialer.Subprotocols = open.Protocols // 子协议列表来自 OPEN

	host, pairs := tunnelwire.ExtractHost(open.Headers, open.Host)
	hdr := tunnelwire.PairsToHeaders(pairs)
	tunnelwire.StripHopByHop(hdr)
	for _, k := range wsHandshakeHeaders {
		hdr.Del(k)
	}
	if host != "" {
		hdr.Set("Host", host)
	}

	up, resp, err := dialer.DialContext(ctx, u.String(), hdr)
	if err != nil {
		status := 502
		var hdrPairs []tunnelwire.HeaderPair
		if resp != nil {
			status = resp.StatusCode
			hdrPairs = tunnelwire.HeadersToPairs(resp.Header)
		}
		_ = sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
			Kind: tunnelwire.KindWS, Status: status,
			Headers: hdrPairs, Error: fmt.Sprintf("tunnel: WS 代理目标拨号失败: %v", err),
		})
		gracefulClose(bc, s.cfg.closeWaitOrDefault())
		return
	}
	defer up.Close()

	upProto := up.Subprotocol()
	if upProto != "" {
		ok := false
		for _, offered := range open.Protocols {
			if offered == upProto {
				ok = true
				break
			}
		}
		if !ok {
			s.rejectWS(bc, sess, 502, fmt.Sprintf("tunnel: WS 目标服务选择了未请求的子协议 %q", upProto))
			return
		}
	}
	_ = sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
		Kind: tunnelwire.KindWS, Status: 101,
		Protocol: upProto, Headers: tunnelwire.HeadersToPairs(resp.Header),
	})

	// 上游 → 隧道泵：ping/pong 经自定义 handler 转成隧道帧透传；
	// close/读错误统一收敛成 WS_CLOSE 帧发给 client。
	upDone := make(chan struct{})
	go func() {
		defer close(upDone)
		up.SetPingHandler(func(data string) error {
			return sess.WriteFrame(tunnelwire.FrameWSPing, []byte(data))
		})
		up.SetPongHandler(func(data string) error {
			return sess.WriteFrame(tunnelwire.FrameWSPong, []byte(data))
		})
		for {
			mt, data, err := up.ReadMessage()
			if err != nil {
				// 1006 是本地合成码、不能上线上协议；映射为 1011 保留错误语义。
				cm := &tunnelwire.CloseMsg{Code: websocket.CloseInternalServerErr}
				var ce *websocket.CloseError
				if errors.As(err, &ce) {
					cm.Code, cm.Reason = ce.Code, ce.Text
				}
				if tunnelwire.ValidateWSClose(cm) != nil {
					cm = &tunnelwire.CloseMsg{Code: websocket.CloseInternalServerErr}
				}
				_ = sess.WriteJSON(tunnelwire.FrameWSClose, cm)
				return
			}
			if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
				continue
			}
			// 隧道 WS_MESSAGE opcode 与 gorilla text/binary 编号一致（1/2）
			if err := sess.WriteWSMessage(byte(mt), data); err != nil {
				return
			}
		}
	}()

	// 上游先结束时给 client 回 WS_CLOSE 的窗口期：upDone 关闭后
	// 给会话读加一个短 deadline，让主泵不会永远等 client 的 close 回声。
	// 该 goroutine 随 upDone 关闭退出，生命周期有界。
	go func() {
		<-upDone
		_ = bc.SetReadDeadline(time.Now().Add(3 * time.Second))
	}()

	// 隧道 → 上游泵（本 goroutine）：写上游仅此一处，
	// gorilla 内部 close 回声走 WriteControl（并发安全），无需额外锁。
	closeForwarded := false
	protocolErr := ""
loop:
	for {
		ft, pl, err := sess.ReadFrame()
		if err != nil {
			break // client 侧断开/出错
		}
		switch ft {
		case tunnelwire.FrameWSMessage:
			if len(pl) == 0 {
				protocolErr = "WS_MESSAGE 载荷缺少 opcode"
				break loop
			}
			op := int(pl[0])
			if op != websocket.TextMessage && op != websocket.BinaryMessage {
				protocolErr = "WS_MESSAGE opcode 非法"
				break loop
			}
			if err := up.WriteMessage(op, pl[1:]); err != nil {
				break loop
			}
		case tunnelwire.FrameWSPing:
			if err := up.WriteMessage(websocket.PingMessage, pl); err != nil {
				break loop
			}
		case tunnelwire.FrameWSPong:
			if err := up.WriteMessage(websocket.PongMessage, pl); err != nil {
				break loop
			}
		case tunnelwire.FrameWSClose:
			cm, derr := tunnelwire.DecodeClose(pl)
			if derr != nil || tunnelwire.ValidateWSClose(cm) != nil {
				protocolErr = "WS_CLOSE 载荷非法"
				break loop
			}
			_ = up.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(cm.Code, cm.Reason))
			closeForwarded = true
			break loop
		case tunnelwire.FrameAbort:
			break loop // client 中止：直接断开上游
		default:
			protocolErr = fmt.Sprintf("WS 会话中出现非法帧 %s", ft)
			break loop
		}
	}
	if protocolErr != "" {
		_ = sess.Abort("protocol_error", protocolErr)
	}

	if closeForwarded {
		// 正常关闭对舞：给上游一小段时间回 close 帧，
		// 让真实 close code 透传回隧道 client。
		select {
		case <-upDone:
		case <-time.After(3 * time.Second):
		}
	}
	_ = up.Close() // 解开 ReadMessage，让上游泵退出
	select {
	case <-upDone:
	case <-time.After(2 * time.Second):
	}
	gracefulClose(bc, s.cfg.closeWaitOrDefault())
}
