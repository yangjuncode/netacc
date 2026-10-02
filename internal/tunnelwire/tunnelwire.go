// Package tunnelwire 定义 netacc 聚合流之上的 HTTP/WS 应用层隧道
// 共享线协议（与 TypeScript 侧严格一致）：
//
//	每条聚合流先写 5 字节 magic：ASCII "NTUN" + 0x01；
//	其后为消息帧序列：uvarint(type) + uvarint(payload_len) + payload。
//
// 帧类型：
//
//	1 OPEN       client→server，JSON（OpenMsg）
//	2 RESULT     server→client，JSON（ResultMsg）
//	3 DATA       双向二进制（HTTP body chunk）
//	4 END        双向空载荷（本方向 HTTP body 结束，方向性半关闭）
//	5 ABORT      双向 JSON（AbortMsg {code,message}）
//	6 WS_MESSAGE 双向（1 字节 WS opcode + message bytes；text=1,binary=2）
//	7 WS_CLOSE   双向 JSON（CloseMsg {code,reason}）
//	8 WS_PING    双向二进制
//	9 WS_PONG    双向二进制
//
// 限制：JSON 控制帧 ≤64KiB；DATA ≤64KiB；单条 WS_MESSAGE ≤4MiB；
// target URL ≤8KiB（headers 总大小由 JSON cap 防御）。
//
// 本包是纯协议编解码层，不 import netacc（避免循环依赖）：
// root netacc（client 侧 API）与 tunnel 子包（server 侧）都可依赖它。
package tunnelwire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Magic 是每条隧道流的前导 5 字节：ASCII "NTUN" + 版本 0x01。
var Magic = []byte{'N', 'T', 'U', 'N', 0x01}

// FrameType 是帧类型（uvarint 编码）。
type FrameType uint64

// 帧类型枚举（线上编号固定，勿改）。
const (
	FrameOpen      FrameType = 1 // client→server，JSON OpenMsg
	FrameResult    FrameType = 2 // server→client，JSON ResultMsg
	FrameData      FrameType = 3 // 双向，HTTP body chunk
	FrameEnd       FrameType = 4 // 双向空载荷，本方向 body 结束
	FrameAbort     FrameType = 5 // 双向，JSON AbortMsg
	FrameWSMessage FrameType = 6 // 双向，1 字节 opcode + message
	FrameWSClose   FrameType = 7 // 双向，JSON CloseMsg
	FrameWSPing    FrameType = 8 // 双向二进制
	FrameWSPong    FrameType = 9 // 双向二进制
)

// String 返回帧类型名（日志/错误信息用）。
func (t FrameType) String() string {
	switch t {
	case FrameOpen:
		return "OPEN"
	case FrameResult:
		return "RESULT"
	case FrameData:
		return "DATA"
	case FrameEnd:
		return "END"
	case FrameAbort:
		return "ABORT"
	case FrameWSMessage:
		return "WS_MESSAGE"
	case FrameWSClose:
		return "WS_CLOSE"
	case FrameWSPing:
		return "WS_PING"
	case FrameWSPong:
		return "WS_PONG"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint64(t))
	}
}

// 协议限制（与 TS 侧一致）。
const (
	// MaxJSONPayload 是 JSON 控制帧（OPEN/RESULT/ABORT/WS_CLOSE）载荷上限。
	MaxJSONPayload = 64 << 10 // 64KiB
	// MaxDataPayload 是单个 DATA 帧载荷上限。
	MaxDataPayload = 64 << 10 // 64KiB
	// MaxWSMessage 是单条 WS_MESSAGE 载荷上限（含 1 字节 opcode）。
	// 首版不分片：超限直接拒绝。
	MaxWSMessage = 4 << 20 // 4MiB
	// MaxTargetLen 是 OPEN.target 的字节上限。
	MaxTargetLen = 8 << 10 // 8KiB
)

// maxPayload 返回该帧类型的载荷上限；未知类型返回 -1。
// END 是空载荷帧：上限 0（携带任何字节都算违例）。
func (t FrameType) maxPayload() int {
	switch t {
	case FrameOpen, FrameResult, FrameAbort, FrameWSClose:
		return MaxJSONPayload
	case FrameData:
		return MaxDataPayload
	case FrameEnd:
		return 0
	case FrameWSMessage, FrameWSPing, FrameWSPong:
		return MaxWSMessage
	default:
		return -1
	}
}

// ---------- JSON 控制消息 ----------

// Kind 是隧道会话类型（OPEN.kind / RESULT.kind）。
type Kind string

// 隧道会话类型值。
const (
	KindHTTP Kind = "http"
	KindWS   Kind = "ws"
)

// ErrProtocol 是线协议违例（帧方向/形态/字段约束被破坏）的错误
// 基类：errors.Is 可判定。读写侧校验发现违例时应回 ABORT 再关流。
var ErrProtocol = errors.New("tunnelwire: 协议违例")

// WS_MESSAGE 载荷首字节的 opcode 取值（与 RFC6455 对齐；无类型常量，
// 赋 byte 或比较 int 均可）。
const (
	// WSOpcodeText：文本消息。
	WSOpcodeText = 1
	// WSOpcodeBinary：二进制消息。
	WSOpcodeBinary = 2
)

// HeaderPair 是一个头字段的线上表示：[name, value]。
// 用二元数组而非 map，保序且保留重复名。
type HeaderPair = [2]string

// OpenMsg 是 OPEN 帧的 JSON 载荷：
//
//	{"v":1,"kind":"http"|"ws","target":"/path"|"http(s)://..."|"ws(s)://...",
//	 "method":"GET","host":"...","headers":[["Name","Value"],...],
//	 "protocols":["..."]}
type OpenMsg struct {
	V         int          `json:"v"`
	Kind      Kind         `json:"kind"`
	Target    string       `json:"target"`
	Method    string       `json:"method,omitempty"`
	Host      string       `json:"host,omitempty"`
	Headers   []HeaderPair `json:"headers,omitempty"`
	Protocols []string     `json:"protocols,omitempty"`
}

// ResultMsg 是 RESULT 帧的 JSON 载荷（HTTP/WS 两形态共用）：
//
//	HTTP：{"kind":"http","status":200,"statusText":"OK","headers":[[...]]}
//	WS：  {"kind":"ws","status":101,"protocol":"subprotocol","headers":[[...]]}
//	拒绝：非 2xx/101 的 status + error 字段。
type ResultMsg struct {
	Kind       Kind         `json:"kind"`
	Status     int          `json:"status"`
	StatusText string       `json:"statusText,omitempty"`
	Protocol   string       `json:"protocol,omitempty"`
	Headers    []HeaderPair `json:"headers,omitempty"`
	Error      string       `json:"error,omitempty"`
}

// AbortMsg 是 ABORT 帧的 JSON 载荷：{"code":"...","message":"..."}。
type AbortMsg struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// CloseMsg 是 WS_CLOSE 帧的 JSON 载荷：{"code":1000,"reason":"..."}。
type CloseMsg struct {
	Code   int    `json:"code"`
	Reason string `json:"reason"`
}

// ---------- OPEN 字段校验 ----------

func isHTTPToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))
		if !ok {
			return false
		}
	}
	return true
}

func hasCtrl(s string) bool {
	return strings.IndexFunc(s, func(r rune) bool {
		return r < 0x20 || r == 0x7f
	}) >= 0
}

func hasBadHeaderValue(s string) bool {
	return strings.ContainsAny(s, "\x00\r\n")
}

// ValidateOpen 在分发前校验 OPEN 控制字段；供 server 防御绕过
// client 本地校验的畸形请求。JSON 体积本身已由帧上限约束。
func ValidateOpen(m *OpenMsg) error {
	if m == nil {
		return fmt.Errorf("%w: OPEN 为空", ErrProtocol)
	}
	if m.V != 1 {
		return fmt.Errorf("%w: OPEN.v=%d（当前协议 v=1）", ErrProtocol, m.V)
	}
	if m.Kind != KindHTTP && m.Kind != KindWS {
		return fmt.Errorf("%w: OPEN.kind=%q 非法", ErrProtocol, m.Kind)
	}
	if m.Target == "" {
		return fmt.Errorf("%w: OPEN.target 为空", ErrProtocol)
	}
	if len(m.Target) > MaxTargetLen {
		return fmt.Errorf("%w: OPEN.target 超过 %d 字节上限", ErrProtocol, MaxTargetLen)
	}
	if hasCtrl(m.Target) || hasCtrl(m.Host) {
		return fmt.Errorf("%w: OPEN target/host 含控制字符", ErrProtocol)
	}
	if m.Method != "" && !isHTTPToken(m.Method) {
		return fmt.Errorf("%w: HTTP method %q 非法", ErrProtocol, m.Method)
	}
	for _, h := range m.Headers {
		if !isHTTPToken(h[0]) {
			return fmt.Errorf("%w: header 名 %q 非法", ErrProtocol, h[0])
		}
		if hasBadHeaderValue(h[1]) {
			return fmt.Errorf("%w: header %s 值含控制字符", ErrProtocol, h[0])
		}
	}
	for _, proto := range m.Protocols {
		if !isHTTPToken(proto) {
			return fmt.Errorf("%w: WebSocket 子协议 %q 非法", ErrProtocol, proto)
		}
	}
	return nil
}

// MaxWSCloseReasonBytes 是 WS close reason 的 UTF-8 字节上限
// （RFC6455 close payload 总长 125；2 字节 code + 123 字节 reason）。
const MaxWSCloseReasonBytes = 123

// validReceivedWSCloseCode 对齐 RFC6455/浏览器可接收的线上 close code：
// 1000-1014 中除保留码（1004/1005/1006/1015 本身就不在区间内）外，
// 以及 3000-4999 的应用/私有码。
func validReceivedWSCloseCode(code int) bool {
	return (code >= 1000 && code <= 1014 &&
		code != 1004 && code != 1005 && code != 1006) ||
		(code >= 3000 && code <= 4999)
}

func validateWSCloseReason(reason string) error {
	if !utf8.ValidString(reason) {
		return fmt.Errorf("%w: WS close reason 不是合法 UTF-8", ErrProtocol)
	}
	if len(reason) > MaxWSCloseReasonBytes {
		return fmt.Errorf("%w: WS close reason 超过 %d 字节", ErrProtocol, MaxWSCloseReasonBytes)
	}
	return nil
}

// ValidateWSClose 校验线上收到的 WS_CLOSE 载荷（接收侧）。
func ValidateWSClose(m *CloseMsg) error {
	if m == nil {
		return fmt.Errorf("%w: WS_CLOSE 为空", ErrProtocol)
	}
	if !validReceivedWSCloseCode(m.Code) {
		return fmt.Errorf("%w: WS close code %d 非法", ErrProtocol, m.Code)
	}
	return validateWSCloseReason(m.Reason)
}

// ValidateWSCloseSend 校验本端主动发送的 close code/reason：
// 与浏览器 WebSocket.close 一致，只允许 1000 或 3000-4999。
func ValidateWSCloseSend(m *CloseMsg) error {
	if m == nil {
		return fmt.Errorf("%w: WS_CLOSE 为空", ErrProtocol)
	}
	if m.Code != 1000 && (m.Code < 3000 || m.Code > 4999) {
		return fmt.Errorf("%w: WS close code %d 非法（仅 1000/3000-4999 可主动发送）", ErrProtocol, m.Code)
	}
	return validateWSCloseReason(m.Reason)
}

// ---------- 帧编解码 ----------

// AppendFrame 把一帧（type uvarint + len uvarint + payload）追加到 dst。
// 不做载荷上限校验——构造路径由调用方自查，读侧校验兜底。
func AppendFrame(dst []byte, ft FrameType, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(ft))
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

// byteReader 给不实现 io.ByteReader 的 io.Reader 补 ReadByte
// （binary.ReadUvarint 的输入要求）。
type byteReader struct {
	r   io.Reader
	one [1]byte
}

func (b *byteReader) ReadByte() (byte, error) {
	for {
		n, err := b.r.Read(b.one[:])
		if n == 1 {
			return b.one[0], nil
		}
		if err != nil {
			return 0, err
		}
		// n==0 && err==nil：io.Reader 契约不建议的返回，重试有限次外只能再来；
		// 对遵循契约的实现不会到这里。
	}
}

// Session 是一条已确认隧道协议身份的流之上的帧读写会话：
// 读帧与写帧各自单线程语义——只允许一个 goroutine 调用 ReadFrame 系；
// 写帧经内部互斥锁串行化，任意多 goroutine 可调 WriteXxx。
// Session 不拥有底层连接：Close 由持有方负责。
type Session struct {
	r   io.Reader     // 原始读流（payload 用 ReadFull 整块读）
	br  io.ByteReader // 帧头 uvarint 用（尽量复用调用方的 ByteReader）
	w   io.Writer
	wmu sync.Mutex
}

// NewSession 在 rw 上建会话（读写同一对象）。
func NewSession(rw io.ReadWriter) *Session {
	return NewSessionRW(rw, rw)
}

// NewSessionRW 在分离的读/写端上建会话（读端若实现 io.ByteReader 直接复用）。
func NewSessionRW(r io.Reader, w io.Writer) *Session {
	br, ok := r.(io.ByteReader)
	if !ok {
		br = &byteReader{r: r}
	}
	return &Session{r: r, br: br, w: w}
}

// ReadFrame 读下一帧：类型、载荷（按类型上限校验）与错误。
// 帧类型未知、载荷超限或流中途断开均返回错误——调用方应视为协议违例处理。
func (s *Session) ReadFrame() (FrameType, []byte, error) {
	tv, err := binary.ReadUvarint(s.br)
	if err != nil {
		return 0, nil, err
	}
	ft := FrameType(tv)
	lim := ft.maxPayload()
	if lim < 0 {
		return 0, nil, fmt.Errorf("%w: 未知帧类型 %d", ErrProtocol, tv)
	}
	n, err := binary.ReadUvarint(s.br)
	if err != nil {
		return 0, nil, err
	}
	if n > uint64(lim) {
		return 0, nil, fmt.Errorf("%w: %s 帧载荷 %d 超过上限 %d", ErrProtocol, ft, n, lim)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(s.r, payload); err != nil {
		return 0, nil, err
	}
	return ft, payload, nil
}

// writeRaw 串行化地把整块 buf 写出（循环写直至写完或出错）。
func (s *Session) writeRaw(buf []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	for len(buf) > 0 {
		n, err := s.w.Write(buf)
		buf = buf[n:]
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteFrame 写一帧；载荷超类型上限时返回错误（不写出任何字节）。
func (s *Session) WriteFrame(ft FrameType, payload []byte) error {
	lim := ft.maxPayload()
	if lim < 0 {
		return fmt.Errorf("tunnelwire: 未知帧类型 %d", uint64(ft))
	}
	if len(payload) > lim {
		return fmt.Errorf("tunnelwire: %s 帧载荷 %d 超过上限 %d", ft, len(payload), lim)
	}
	return s.writeRaw(AppendFrame(nil, ft, payload))
}

// WriteJSON 把 v 编码为 JSON 控制帧写出；编码结果超 64KiB 时拒绝。
func (s *Session) WriteJSON(ft FrameType, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("tunnelwire: 编码 %s 载荷失败: %w", ft, err)
	}
	if len(b) > MaxJSONPayload {
		return fmt.Errorf("tunnelwire: %s JSON 载荷 %d 超过上限 %d", ft, len(b), MaxJSONPayload)
	}
	return s.WriteFrame(ft, b)
}

// WriteData 把 p 切成 ≤MaxDataPayload 的 DATA 帧序列写出；
// 整体在单次持锁中完成，不与其它写者的帧交错。p 为空时不写任何帧。
func (s *Session) WriteData(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	var buf []byte
	for len(p) > 0 {
		n := min(len(p), MaxDataPayload)
		buf = AppendFrame(buf, FrameData, p[:n])
		p = p[n:]
	}
	return s.writeRaw(buf)
}

// WriteEnd 写一帧 END（空载荷）。
func (s *Session) WriteEnd() error {
	return s.WriteFrame(FrameEnd, nil)
}

// Abort 写一帧 ABORT（尽力而为的错误通知）。
func (s *Session) Abort(code, message string) error {
	return s.WriteJSON(FrameAbort, &AbortMsg{Code: code, Message: message})
}

// WriteWSMessage 写一帧 WS_MESSAGE：payload = 1 字节 opcode + message。
// opcode 取值：1=text，2=binary；消息体超 MaxWSMessage-1 时拒绝（首版不分片）。
func (s *Session) WriteWSMessage(opcode byte, msg []byte) error {
	if len(msg)+1 > MaxWSMessage {
		return fmt.Errorf("tunnelwire: WS 消息 %d 超过上限 %d", len(msg)+1, MaxWSMessage)
	}
	return s.WriteFrame(FrameWSMessage, append([]byte{opcode}, msg...))
}

// ---------- 头字段二元数组 <-> http.Header ----------

// HeadersToPairs 把 http.Header 摊平成保重复名的二元数组；
// 按字段名排序使输出确定（线上表示本身无序要求，测试与调试更稳定）。
func HeadersToPairs(h http.Header) []HeaderPair {
	if len(h) == 0 {
		return nil
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []HeaderPair
	for _, k := range keys {
		for _, v := range h[k] {
			out = append(out, HeaderPair{k, v})
		}
	}
	return out
}

// PairsToHeaders 把二元数组还原成 http.Header（同名多值全部保留）。
func PairsToHeaders(pairs []HeaderPair) http.Header {
	if len(pairs) == 0 {
		return make(http.Header)
	}
	h := make(http.Header, len(pairs))
	for _, p := range pairs {
		h.Add(p[0], p[1])
	}
	return h
}

// hopByHopHeaders 是转发前必须剥除的逐跳头字段
// （RFC 7230 §6.1；含 Connection 头动态列出的字段名，见 StripHopByHop）。
var hopByHopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// StripHopByHop 从 h 中就地删除全部逐跳头字段（含 Connection 值里
// 动态点名的字段）。转发到真实 HTTP 目标前调用。
func StripHopByHop(h http.Header) {
	for _, c := range h["Connection"] {
		for _, tok := range strings.Split(c, ",") {
			if name := strings.TrimSpace(tok); name != "" {
				h.Del(name)
			}
		}
	}
	for _, k := range hopByHopHeaders {
		h.Del(k)
	}
}

// ExtractHost 从头字段对中取出 Host 值并从集合中移除
// （HTTP 语义上 Host 独立承载，不应留在 Header 里重复出现）。
// 显式 host 参数优先；为空时才看 headers 里的 Host。
func ExtractHost(pairs []HeaderPair, host string) (string, []HeaderPair) {
	if host != "" {
		return host, pairs
	}
	rest := pairs[:0:0]
	for _, p := range pairs {
		if host == "" && strings.EqualFold(p[0], "Host") {
			host = p[1]
			continue
		}
		rest = append(rest, p)
	}
	return host, rest
}

// ---------- gracefulClose 支持 ----------

// FlushProbe 报告写侧水位：sent=已下发（装帧）字节数、acked=对端
// 已确认字节数、pending=仍在发送缓冲未下发的字节数。
// *netacc.Stream 的 Stats() 可一行适配出本探针。
type FlushProbe func() (sent, acked uint64, pending int)

// GracefulClose 在 Close 之前轮询等待「应用已写的全部字节都被对端
// 确认」，最多等 timeout。
//
// 背景：netacc.Stream.Write 只把数据放入发送缓冲（sendQ），真正装帧
// 下发由发送泵异步完成；写完最后帧立刻 Close 会丢弃尚未装帧的部分。
// 对 *netacc.Stream 传入 probe（见各包的适配函数）；对普通 net.Conn
// （net.Pipe 等）传 nil——Write 即写即达，直接 Close。
func GracefulClose(c io.Closer, probe FlushProbe, timeout time.Duration) error {
	if probe != nil && timeout > 0 {
		deadline := time.Now().Add(timeout)
		for {
			sent, acked, pending := probe()
			if pending == 0 && acked >= sent {
				break // 全部已写字节都被对端确认
			}
			if !time.Now().Before(deadline) {
				break // 超时兜底：不等了，直接关
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	return c.Close()
}

// ---------- JSON 载荷解码 ----------

// DecodeOpen 解码 OPEN 帧载荷。
func DecodeOpen(payload []byte) (*OpenMsg, error) {
	var m OpenMsg
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("tunnelwire: OPEN JSON 非法: %w", err)
	}
	return &m, nil
}

// DecodeResult 解码 RESULT 帧载荷。
func DecodeResult(payload []byte) (*ResultMsg, error) {
	var m ResultMsg
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("tunnelwire: RESULT JSON 非法: %w", err)
	}
	return &m, nil
}

// DecodeAbort 解码 ABORT 帧载荷。
func DecodeAbort(payload []byte) (*AbortMsg, error) {
	var m AbortMsg
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("tunnelwire: ABORT JSON 非法: %w", err)
	}
	return &m, nil
}

// DecodeClose 解码 WS_CLOSE 帧载荷。
func DecodeClose(payload []byte) (*CloseMsg, error) {
	var m CloseMsg
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("tunnelwire: WS_CLOSE JSON 非法: %w", err)
	}
	return &m, nil
}
