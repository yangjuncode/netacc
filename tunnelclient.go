// 隧道 client API：Aggregator 直接向对端的 HTTP/WS 应用层隧道
// （tunnelwire 协议，见 internal/tunnelwire）发请求——
// 浏览器 TS 侧的 Go 对应物。
//
//	TunnelFetch：一条聚合流承载一个 HTTP 请求（OPEN → DATA* END →
//	    RESULT DATA* END），返回标准 *http.Response；
//	TunnelWS：一条聚合流承载一条 WebSocket 会话（OPEN → RESULT 101 →
//	    WS_MESSAGE/WS_PING/WS_PONG/WS_CLOSE），返回 *TunnelWSConn。
package netacc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc/internal/tunnelwire"
)

// dataWriter 把 io.Writer 适配成 DATA 帧流（供 io.Copy 用）。
type dataWriter struct {
	sess *tunnelwire.Session
}

func (w dataWriter) Write(p []byte) (int, error) {
	if err := w.sess.WriteData(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// ---------- TunnelFetch ----------

// httpTargetOf 从 req.URL 推出 OPEN.target/host：
// 绝对 http(s):// → 原样 URL（经代理路径）；相对 → path?query
// （须以 "/" 开头且非 "//"）。Host 取 req.Host，为空时绝对 URL 回落
// 到 URL.Host；header 里的 Host 也会提升为 OPEN.host 后剔除。
func httpTargetOf(req *http.Request) (target, host string, err error) {
	u := req.URL
	if u == nil {
		return "", "", errors.New("netacc: req.URL 为空")
	}
	host = req.Host
	if host == "" {
		host = req.Header.Get("Host")
	}
	if u.IsAbs() {
		if u.Scheme != "http" && u.Scheme != "https" {
			return "", "", fmt.Errorf("netacc: URL scheme 须为 http/https: %q", u.Scheme)
		}
		uu := *u // 不修改调用方 URL：fragment 不进入请求
		uu.Fragment = ""
		u = &uu
		target = u.String()
		if host == "" {
			host = u.Host
		}
	} else {
		target = u.RequestURI()
		if target == "" {
			target = "/"
		}
		if !strings.HasPrefix(target, "/") || strings.HasPrefix(target, "//") {
			return "", "", fmt.Errorf("netacc: 相对 target 须以 / 开头（且非 //）: %q", target)
		}
	}
	if len(target) > tunnelwire.MaxTargetLen {
		return "", "", fmt.Errorf("netacc: target 长度 %d 超过上限 %d", len(target), tunnelwire.MaxTargetLen)
	}
	return target, host, nil
}

// headerWithoutHost 复制 header 并剔除 Host 字段（Host 独立承载在
// OPEN.host，避免线上重复）。
func headerWithoutHost(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		if strings.EqualFold(k, "Host") {
			continue
		}
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// TunnelFetch 经一条到 p 的聚合流执行 HTTP 请求：OPEN 携带
// method/target/headers，req.Body（可为 nil）流式切成 DATA 帧、
// 以 END 收尾；对端的 RESULT 还原成 *http.Response 返回，其 Body
// 逐 DATA 帧读取至 END（须 Close；未读完 Close 会发 ABORT 中止）。
//
// opts 原样透传给 OpenStream（如 WithMinPaths/WithHandshakeTimeout）。
// ctx 与 req.Context() 任一取消都会中止整个请求生命周期：建流、收发、
// body 读完前的阻塞读都会随聚合流关闭退出。
// 不做重定向跟随、cookie  jar 等 http.Client 语义——定位是传输通道。
func (a *Aggregator) TunnelFetch(ctx context.Context, p peer.ID, req *http.Request, opts ...OpenOption) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("netacc: req 不能为 nil")
	}
	target, host, err := httpTargetOf(req)
	if err != nil {
		return nil, err
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	open := &tunnelwire.OpenMsg{
		V: 1, Kind: tunnelwire.KindHTTP, Target: target,
		Method: method, Host: host,
		Headers: tunnelwire.HeadersToPairs(headerWithoutHost(req.Header)),
	}
	if err := tunnelwire.ValidateOpen(open); err != nil {
		return nil, err
	}

	// 观察调用方 ctx 与 req.Context() 任一取消；watchCtx 覆盖建流、
	// 请求体泵和响应体读取，直到响应体 Close。
	watchCtx, cancelWatch := context.WithCancel(ctx)
	stopReqWatch := context.AfterFunc(req.Context(), cancelWatch)
	stop := func() {
		stopReqWatch()
		cancelWatch()
	}

	st, err := a.OpenStream(watchCtx, p, opts...)
	if err != nil {
		stop()
		return nil, err
	}
	// ctx/req ctx 取消即断流：让阻塞中的帧读/写立刻退出（生效至 Body.Close）。
	stopWatch := context.AfterFunc(watchCtx, func() { _ = st.Close() })

	fail := func(err error) (*http.Response, error) {
		stopWatch()
		stop()
		_ = st.Close()
		return nil, err
	}

	sess := tunnelwire.NewSession(st)
	if _, err := st.Write(tunnelwire.Magic); err != nil {
		return fail(fmt.Errorf("netacc: 写隧道 magic 失败: %w", err))
	}
	if err := sess.WriteJSON(tunnelwire.FrameOpen, open); err != nil {
		return fail(fmt.Errorf("netacc: 写 OPEN 失败: %w", err))
	}

	// 请求体异步泵：响应可能先于体读完到达（server 可不消费 body
	// 直接应答），泵阻塞只影响自身；出错时关流让读侧退出。
	if req.Body != nil {
		go func() {
			if _, err := io.Copy(dataWriter{sess}, req.Body); err != nil {
				_ = st.Close()
				return
			}
			if err := sess.WriteEnd(); err != nil {
				_ = st.Close()
			}
		}()
	} else if err := sess.WriteEnd(); err != nil {
		return fail(fmt.Errorf("netacc: 写 END 失败: %w", err))
	}

	ft, payload, err := sess.ReadFrame()
	if err != nil {
		return fail(fmt.Errorf("netacc: 等 RESULT 失败: %w", err))
	}
	switch ft {
	case tunnelwire.FrameResult:
		res, err := tunnelwire.DecodeResult(payload)
		if err != nil {
			return fail(err)
		}
		if res.Kind != tunnelwire.KindHTTP {
			return fail(fmt.Errorf("netacc: RESULT.kind=%q 与 http 会话不符", res.Kind))
		}
		if res.Status < 100 || res.Status > 599 {
			return fail(fmt.Errorf("netacc: RESULT.status=%d 非法", res.Status))
		}
		statusText := res.StatusText
		if statusText == "" {
			statusText = http.StatusText(res.Status)
		}
		status := fmt.Sprintf("%d", res.Status)
		if statusText != "" {
			status += " " + statusText
		}
		resp := &http.Response{
			Status:        status,
			StatusCode:    res.Status,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			Header:        tunnelwire.PairsToHeaders(res.Headers),
			ContentLength: -1, // 长度由 END 帧界定
			Body: &tunnelRespBody{sess: sess, st: st, stop: func() {
				stopWatch()
				stop()
			}},
			Request: req,
		}
		return resp, nil
	case tunnelwire.FrameAbort:
		am, derr := tunnelwire.DecodeAbort(payload)
		if derr != nil {
			return fail(errors.New("netacc: 对端中止（ABORT 载荷无法解析）"))
		}
		return fail(&TunnelAbortError{Code: am.Code, Message: am.Message})
	default:
		return fail(fmt.Errorf("netacc: 首帧须为 RESULT/ABORT，得到 %s", ft))
	}
}

// tunnelRespBody 是 TunnelFetch 响应体：逐 DATA 帧读到 END（io.EOF）；
// ABORT 帧转为 *TunnelAbortError。Close 时在未读完的情况下发 ABORT
// 中止并关闭聚合流。
type tunnelRespBody struct {
	sess *tunnelwire.Session
	st   *Stream
	stop func() // 解除 ctx/req ctx 看守

	buf    []byte // 当前 DATA 帧余量
	err    error  // 终态（io.EOF 或错误）
	closed bool
}

func (b *tunnelRespBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	for len(b.buf) == 0 {
		ft, payload, err := b.sess.ReadFrame()
		if err != nil {
			b.err = err
			return 0, err
		}
		switch ft {
		case tunnelwire.FrameData:
			b.buf = payload
		case tunnelwire.FrameEnd:
			b.err = io.EOF
			return 0, io.EOF
		case tunnelwire.FrameAbort:
			am, derr := tunnelwire.DecodeAbort(payload)
			if derr != nil {
				b.err = errors.New("netacc: 对端中止（ABORT 载荷无法解析）")
			} else {
				b.err = &TunnelAbortError{Code: am.Code, Message: am.Message}
			}
			return 0, b.err
		default:
			b.err = fmt.Errorf("netacc: 响应体阶段收到非法帧 %s", ft)
			return 0, b.err
		}
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}

// Close 结束响应体：未读到 END 时尽力发 ABORT 告知对端，
// 然后关闭聚合流并解除 ctx 看守。幂等。
func (b *tunnelRespBody) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.err == nil {
		_ = b.sess.Abort("client_close", "响应体未读完即关闭")
	}
	b.stop()
	return b.st.Close()
}

// ---------- TunnelWS ----------

// wsTargetOf 校验并规整 WS target：相对 "/path"（非 "//"）或绝对
// ws(s):// URL；fragment 不进入请求。返回线上 target 与默认 host。
func wsTargetOf(target string) (wireTarget, host string, err error) {
	if target == "" || len(target) > tunnelwire.MaxTargetLen {
		return "", "", fmt.Errorf("netacc: WS target 为空或超长（上限 %d）", tunnelwire.MaxTargetLen)
	}
	if strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "//") {
		if cut := strings.IndexByte(target, '#'); cut >= 0 {
			target = target[:cut]
		}
		return target, "", nil
	}
	u, err := url.Parse(target)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") {
		return "", "", fmt.Errorf("netacc: WS target 须为 /path 或 ws(s):// 绝对 URL: %q", target)
	}
	u.Fragment = ""
	return u.String(), u.Host, nil
}

// TunnelWS 经一条到 p 的聚合流建 WS 会话：OPEN 携带 target/header/
// protocols；server 回 RESULT{status:101} 后返回就绪的 *TunnelWSConn
// （Subprotocol() 为协商结果）。非 101 的 RESULT 返回 *WSRejectedError，
// ABORT 返回 *TunnelAbortError。
//
// opts 原样透传给 OpenStream。ctx 只约束建流与握手阶段；握手成功后
// 会话生命周期由 TunnelWSConn.Close 管理。
func (a *Aggregator) TunnelWS(ctx context.Context, p peer.ID, target string, header http.Header, protocols []string, opts ...OpenOption) (*TunnelWSConn, error) {
	target, host, err := wsTargetOf(target)
	if err != nil {
		return nil, err
	}
	hdr := headerWithoutHost(header)
	if h := header.Get("Host"); h != "" {
		host = h // 显式 Host 头优先于 URL host
	}
	open := &tunnelwire.OpenMsg{
		V: 1, Kind: tunnelwire.KindWS, Target: target, Host: host,
		Headers:   tunnelwire.HeadersToPairs(hdr),
		Protocols: protocols,
	}
	if err := tunnelwire.ValidateOpen(open); err != nil {
		return nil, err
	}

	st, err := a.OpenStream(ctx, p, opts...)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = st.Close() })
	fail := func(err error) (*TunnelWSConn, error) {
		stop()
		_ = st.Close()
		return nil, err
	}

	sess := tunnelwire.NewSession(st)
	if _, err := st.Write(tunnelwire.Magic); err != nil {
		return fail(fmt.Errorf("netacc: 写隧道 magic 失败: %w", err))
	}
	if err := sess.WriteJSON(tunnelwire.FrameOpen, open); err != nil {
		return fail(fmt.Errorf("netacc: 写 OPEN 失败: %w", err))
	}

	ft, payload, err := sess.ReadFrame()
	if err != nil {
		return fail(fmt.Errorf("netacc: 等 RESULT 失败: %w", err))
	}
	switch ft {
	case tunnelwire.FrameResult:
		res, err := tunnelwire.DecodeResult(payload)
		if err != nil {
			return fail(err)
		}
		if res.Kind != tunnelwire.KindWS {
			return fail(fmt.Errorf("netacc: RESULT.kind=%q 与 ws 会话不符", res.Kind))
		}
		if res.Status != 101 {
			return fail(&WSRejectedError{
				Status:  res.Status,
				Header:  tunnelwire.PairsToHeaders(res.Headers),
				Message: res.Error,
			})
		}
		conn := newTunnelWSConn(sess, st, &TunnelWSRequest{
			Peer: p, Target: target, Host: host,
			Header: hdr, Protocols: protocols,
		}, false)
		conn.protocol = res.Protocol
		stop()
		return conn, nil
	case tunnelwire.FrameAbort:
		am, derr := tunnelwire.DecodeAbort(payload)
		if derr != nil {
			return fail(errors.New("netacc: 对端中止（ABORT 载荷无法解析）"))
		}
		return fail(&TunnelAbortError{Code: am.Code, Message: am.Message})
	default:
		return fail(fmt.Errorf("netacc: 首帧须为 RESULT/ABORT，得到 %s", ft))
	}
}
