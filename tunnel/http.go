// HTTP 隧道会话的 server 侧实现：相对 target 走本地 http.Handler，
// 绝对 http(s):// target 走 RoundTripper 代理。
//
// 读侧刻意内联（无后台读泵）：请求体由 handler/代理 transport 按需
// 拉取 DATA 帧，ABORT 按线序到达天然按序处理；handler 不读 body 时
// 数据停在聚合流发送缓冲，回压正确传导到对端。
package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc/internal/tunnelwire"
)

// AbortError 是对端 ABORT 帧在本侧的错误形态。
type AbortError struct {
	Code    string
	Message string
}

func (e *AbortError) Error() string {
	return fmt.Sprintf("tunnel: 对端中止会话 code=%s message=%s", e.Code, e.Message)
}

// splitTarget 判定 OPEN.target 的形态："/" 开头且非 "//" 是本地相对
// 路径；否则须为绝对 URL（scheme 合法性由调用方按 kind 校验）。
func splitTarget(target string) (u *url.URL, local bool, err error) {
	if strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "//") {
		return nil, true, nil
	}
	u, err = url.Parse(target)
	if err != nil {
		return nil, false, err
	}
	if !u.IsAbs() || u.Host == "" {
		return nil, false, fmt.Errorf("target %q 须为 '/' 开头的路径或绝对 URL", target)
	}
	return u, false, nil
}

// ---------- 入向帧读泵 ----------

// inFrame 是读泵产出的一帧或终态错误。
type inFrame struct {
	ft  tunnelwire.FrameType
	pl  []byte
	err error
}

// framePump 在后台把 client→server 方向的帧读进有界缓冲：
// handler/代理目标不消费 body 时，client 的 END/ABORT 也能被及时
// 吸收——net.Pipe 这类写即阻塞的同步传输上，没有泵 client 的
// WriteEnd 会因 server 忙于写 RESULT 而死锁。cap 限定未消费
// 帧的内存积压，满了泵阻塞（回压自然传导给对端发送缓冲）。
// 连接断开时泵产出带 err 的终态帧后退出。
type framePump struct {
	ch       chan inFrame
	done     chan struct{}
	aborted  chan struct{} // 读到 ABORT 或读终态时关闭，用于取消请求 ctx
	once     sync.Once
	abortOne sync.Once
}

// startFramePump 启动读泵 goroutine；stop 后即使 channel 已满也会退出。
// 连接断开时泵产出带 err 的终态帧后退出。
func startFramePump(sess *tunnelwire.Session) *framePump {
	p := &framePump{
		ch:      make(chan inFrame, 16),
		done:    make(chan struct{}),
		aborted: make(chan struct{}),
	}
	go func() {
		for {
			ft, pl, err := sess.ReadFrame()
			if err != nil || ft == tunnelwire.FrameAbort {
				p.abortOne.Do(func() { close(p.aborted) })
			}
			select {
			case p.ch <- inFrame{ft: ft, pl: pl, err: err}:
				if err != nil {
					return
				}
			case <-p.done:
				return
			}
		}
	}()
	return p
}

// stop 结束读泵：幂等；不关闭 ch（消费者可能仍在收尾）。
func (p *framePump) stop() {
	p.once.Do(func() { close(p.done) })
}

// ---------- HTTP 分发 ----------

// serveHTTP 按 target 形态分发 HTTP 会话。
func (s *Server) serveHTTP(ctx context.Context, bc *bufferedConn, sess *tunnelwire.Session, open *tunnelwire.OpenMsg, p peer.ID) {
	// 无论最终分发到哪都先起读泵：client 侧写完 OPEN 会继续
	// 写 DATA/END，拒绝路径也要把帧吸走再回 RESULT。
	pump := startFramePump(sess)
	defer pump.stop()
	u, local, err := splitTarget(open.Target)
	if err != nil {
		s.refuseHTTP(bc, sess, http.StatusBadRequest, err.Error())
		return
	}
	if local {
		s.serveHTTPLocal(ctx, bc, sess, pump, open)
		return
	}
	s.serveHTTPProxy(ctx, bc, sess, pump, open, u, p)
}

// refuseHTTP 回一个带 status/error 的 HTTP RESULT + END，再冲刷关流。
func (s *Server) refuseHTTP(bc *bufferedConn, sess *tunnelwire.Session, status int, msg string) {
	_ = sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
		Kind: tunnelwire.KindHTTP, Status: status,
		StatusText: http.StatusText(status), Error: msg})
	_ = sess.WriteEnd()
	gracefulClose(bc, s.cfg.closeWaitOrDefault())
}

// ---------- 请求体（client → server 方向 DATA/END） ----------

// httpBody 实现 io.ReadCloser：从读泵逐帧取 HTTP 请求体——
// DATA 出字节、END 出 EOF、ABORT/流终态出错误。
// 不依赖对端半关闭（聚合流本就没有）。
//
// endCh 在 END 被消费时关闭（恰好一次）：HTTP 代理据此判断
// 「client 方向只剩 ABORT 合法」并起 ABORT 看守——END 之后
// body.Read 不再拉帧，看守独占泵的读侧，无并发冲突。
type httpBody struct {
	pump    *framePump
	pending []byte // 当前 DATA 载荷中未消费的部分
	ended   bool   // END 已收
	endCh   chan struct{}
	endOnce sync.Once
	err     error // 终态错误（sticky）
}

func newHTTPBody(pump *framePump) *httpBody {
	return &httpBody{pump: pump, endCh: make(chan struct{})}
}

func (b *httpBody) Read(p []byte) (int, error) {
	for {
		if len(b.pending) > 0 {
			n := copy(p, b.pending)
			b.pending = b.pending[n:]
			return n, nil
		}
		if b.err != nil {
			return 0, b.err
		}
		if b.ended {
			return 0, io.EOF
		}
		f, ok := <-b.pump.ch
		if !ok || f.err != nil {
			b.err = io.ErrUnexpectedEOF
			if f.err != nil {
				b.err = f.err
			}
			return 0, b.err
		}
		switch f.ft {
		case tunnelwire.FrameData:
			b.pending = f.pl
		case tunnelwire.FrameEnd:
			b.ended = true
			b.endOnce.Do(func() { close(b.endCh) })
		case tunnelwire.FrameAbort:
			a, _ := tunnelwire.DecodeAbort(f.pl)
			b.err = &AbortError{Code: a.Code, Message: a.Message}
			return 0, b.err
		default:
			// HTTP 会话的 client→server 方向只许 DATA/END/ABORT
			b.err = fmt.Errorf("%w: HTTP 请求体收到 %s 帧", tunnelwire.ErrProtocol, f.ft)
			return 0, b.err
		}
	}
}

func (b *httpBody) Close() error { return nil }

// ---------- 本地 HTTP handler ----------

// serveHTTPLocal 用本地 http.Handler 服务相对路径请求。
func (s *Server) serveHTTPLocal(ctx context.Context, bc *bufferedConn, sess *tunnelwire.Session, pump *framePump, open *tunnelwire.OpenMsg) {
	if s.cfg.httpHandler == nil {
		s.refuseHTTP(bc, sess, http.StatusNotFound, "未配置本地 HTTP handler")
		return
	}
	u, err := url.ParseRequestURI(open.Target)
	if err != nil || u.Path == "" || u.IsAbs() {
		s.refuseHTTP(bc, sess, http.StatusBadRequest,
			fmt.Sprintf("target %q 非法（须为 / 开头的路径）", open.Target))
		return
	}

	// Host 优先取 OPEN.host；缺省从 headers 里提取（并从 header 集剔除）
	host, rest := tunnelwire.ExtractHost(open.Headers, open.Host)
	hdr := tunnelwire.PairsToHeaders(rest)
	method := open.Method
	if method == "" {
		method = http.MethodGet
	}
	// ABORT/连接断开即使未被 body 消费，也取消 handler 的 request ctx。
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-pump.aborted:
			cancel()
		case <-hctx.Done():
		}
	}()
	remoteAddr := ""
	if addr := bc.Conn.RemoteAddr(); addr != nil {
		remoteAddr = addr.String()
	}
	req := &http.Request{
		Method:        method,
		URL:           &url.URL{Path: u.Path, RawPath: u.RawPath, RawQuery: u.RawQuery, ForceQuery: u.ForceQuery},
		RequestURI:    open.Target,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Host:          host,
		Header:        hdr,
		Body:          newHTTPBody(pump),
		ContentLength: contentLengthOf(hdr),
		RemoteAddr:    remoteAddr,
	}
	req = req.WithContext(hctx)

	rw := newHTTPResultWriter(sess)
	func() {
		defer func() {
			if r := recover(); r != nil {
				rw.panic500(r)
			}
		}()
		s.cfg.httpHandler.ServeHTTP(rw, req)
	}()

	// handler 返回：确保 RESULT 已发（默认 200），再发 END 收尾。
	rw.finish()
	gracefulClose(bc, s.cfg.closeWaitOrDefault())
}

// contentLengthOf 从 header 里尽力解析 Content-Length（-1 = 未知/流式）。
func contentLengthOf(h http.Header) int64 {
	v := h.Get("Content-Length")
	if v == "" {
		return -1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// ---------- http.ResponseWriter → 隧道帧 ----------

// httpResultWriter 把 handler 的响应映射成 RESULT + DATA 帧：
// 首个 WriteHeader/Write 固化为 RESULT（header 在此刻快照）；
// 之后 Write 走 DATA；handler 返回后由 finish 发 END。
type httpResultWriter struct {
	sess        *tunnelwire.Session
	header      http.Header
	status      int
	wroteHeader bool
	broken      bool // 写帧失败（对端断流等），后续写直接报错
}

func newHTTPResultWriter(sess *tunnelwire.Session) *httpResultWriter {
	return &httpResultWriter{sess: sess, header: make(http.Header)}
}

func (w *httpResultWriter) Header() http.Header { return w.header }

// WriteHeader 发 RESULT。重复调用与 net/http 语义一致：忽略。
func (w *httpResultWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = code
	if err := w.sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
		Kind:       tunnelwire.KindHTTP,
		Status:     code,
		StatusText: http.StatusText(code),
		Headers:    tunnelwire.HeadersToPairs(w.header),
	}); err != nil {
		w.broken = true
	}
}

func (w *httpResultWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.broken {
		return 0, net.ErrClosed
	}
	// WriteData 内部按 ≤64KiB 分帧且单次持锁，不与其它写者交错
	if err := w.sess.WriteData(p); err != nil {
		w.broken = true
		return 0, err
	}
	return len(p), nil
}

// Flush 实现 http.Flusher：确保 RESULT 已发出。
// （不额外发空 DATA——对端按帧边界读，空帧无语义。）
func (w *httpResultWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
}

// panic500 是 handler panic 的兜底：RESULT 未发则回 500；
// 已发则只能 ABORT（状态行已固化，无法改 500）。
func (w *httpResultWriter) panic500(r any) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_ = w.sess.Abort("internal_error", fmt.Sprintf("handler panic: %v", r))
	w.broken = true
}

// finish 在 handler 返回后收尾：补默认 200 RESULT + END 帧。
func (w *httpResultWriter) finish() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	_ = w.sess.WriteEnd()
}

// ---------- HTTP 绝对 URL 代理 ----------

// serveHTTPProxy 把绝对 http(s) URL 请求经 RoundTripper 转发到代理目标，
// 响应头回 RESULT、响应体经 DATA 帧流式回传（不整包缓冲）。
func (s *Server) serveHTTPProxy(ctx context.Context, bc *bufferedConn, sess *tunnelwire.Session, pump *framePump, open *tunnelwire.OpenMsg, u *url.URL, p peer.ID) {
	cfg := s.cfg.httpProxy
	if cfg == nil {
		s.refuseHTTP(bc, sess, http.StatusNotFound, "未配置 HTTP 代理")
		return
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		s.refuseHTTP(bc, sess, http.StatusBadRequest,
			fmt.Sprintf("scheme %q 非法（仅 http/https）", u.Scheme))
		return
	}
	// Allow nil 默认拒绝（开放代理防御）
	if cfg.Allow == nil || !cfg.Allow(p, TunnelKindHTTP, u) {
		s.refuseHTTP(bc, sess, http.StatusForbidden, "目标被隧道策略拒绝")
		return
	}

	method := open.Method
	if method == "" {
		method = http.MethodGet
	}
	// 会话级 ctx：ABORT 看守读到「对端放弃」即取消，让 RoundTrip
	// 与上游 body 读尽快退出。
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	body := newHTTPBody(pump)

	// 感知 ABORT/连接终态：body 未读尽时 pump.aborted 直接取消；
	// body END 之后 client 方向合法帧只剩 ABORT/终态，读泵再拉一帧兜底。
	go func() {
		select {
		case <-pump.aborted:
			cancel()
			return
		case <-body.endCh:
		case <-sctx.Done():
			return
		}
		select {
		case <-pump.aborted:
		case <-pump.ch:
		case <-sctx.Done():
			return
		}
		cancel()
	}()

	outReq, err := http.NewRequestWithContext(sctx, method, u.String(), body)
	if err != nil {
		s.refuseHTTP(bc, sess, http.StatusBadRequest, err.Error())
		return
	}
	host, rest := tunnelwire.ExtractHost(open.Headers, open.Host)
	hdr := tunnelwire.PairsToHeaders(rest)
	tunnelwire.StripHopByHop(hdr)
	outReq.Header = hdr
	if host != "" {
		outReq.Host = host
	}
	if cl := contentLengthOf(hdr); cl >= 0 {
		outReq.ContentLength = cl
	}

	rt := cfg.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	resp, err := rt.RoundTrip(outReq)
	if err != nil {
		if errors.Is(err, tunnelwire.ErrProtocol) {
			_ = sess.Abort("protocol_error", err.Error())
			gracefulClose(bc, s.cfg.closeWaitOrDefault())
			return
		}
		s.refuseHTTP(bc, sess, http.StatusBadGateway, fmt.Sprintf("代理请求失败: %v", err))
		return
	}
	defer resp.Body.Close()

	tunnelwire.StripHopByHop(resp.Header)
	err = sess.WriteJSON(tunnelwire.FrameResult, &tunnelwire.ResultMsg{
		Kind:       tunnelwire.KindHTTP,
		Status:     resp.StatusCode,
		StatusText: http.StatusText(resp.StatusCode),
		Headers:    tunnelwire.HeadersToPairs(resp.Header),
	})
	if err == nil {
		err = streamBody(sess, resp.Body)
	}
	if err == nil {
		err = sess.WriteEnd()
	}
	if err != nil {
		return // 写帧失败说明对端已断/复位，serveStream 的 defer 收尾
	}
	gracefulClose(bc, s.cfg.closeWaitOrDefault())
}

// streamBody 把代理目标响应体逐块（≤32KiB）转发为 DATA 帧。
func streamBody(sess *tunnelwire.Session, r io.Reader) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if werr := sess.WriteFrame(tunnelwire.FrameData, buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			// 代理目标读失败：告知对端后收尾
			_ = sess.Abort("upstream_error", err.Error())
			return err
		}
	}
}
