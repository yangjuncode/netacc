package tunnelwire

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ---------- 帧编解码 ----------

// TestFrameRoundTrip：各类型帧写入后按序原样读回，
// magic + 帧头（uvarint type/len）线上形态与规格一致。
func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(Magic)
	sess := NewSession(&buf)

	openJSON := []byte(`{"v":1,"kind":"http","target":"/a?b=1"}`)
	if err := sess.WriteFrame(FrameOpen, openJSON); err != nil {
		t.Fatalf("写 OPEN 失败: %v", err)
	}
	if err := sess.WriteData([]byte("hello")); err != nil {
		t.Fatalf("写 DATA 失败: %v", err)
	}
	if err := sess.WriteEnd(); err != nil {
		t.Fatalf("写 END 失败: %v", err)
	}
	if err := sess.WriteWSMessage(WSOpcodeText, []byte("你好")); err != nil {
		t.Fatalf("写 WS_MESSAGE 失败: %v", err)
	}

	got := buf.Bytes()
	if !bytes.Equal(got[:len(Magic)], Magic) {
		t.Fatalf("magic 前缀错误: %x", got[:len(Magic)])
	}

	rs := NewSessionRW(bytes.NewReader(got[len(Magic):]), io.Discard)
	ft, pl, err := rs.ReadFrame()
	if err != nil || ft != FrameOpen || !bytes.Equal(pl, openJSON) {
		t.Fatalf("OPEN 帧读回错误: ft=%s pl=%q err=%v", ft, pl, err)
	}
	ft, pl, err = rs.ReadFrame()
	if err != nil || ft != FrameData || string(pl) != "hello" {
		t.Fatalf("DATA 帧读回错误: ft=%s pl=%q err=%v", ft, pl, err)
	}
	ft, pl, err = rs.ReadFrame()
	if err != nil || ft != FrameEnd || len(pl) != 0 {
		t.Fatalf("END 帧读回错误: ft=%s pl=%q err=%v", ft, pl, err)
	}
	ft, pl, err = rs.ReadFrame()
	if err != nil || ft != FrameWSMessage || pl[0] != WSOpcodeText || string(pl[1:]) != "你好" {
		t.Fatalf("WS_MESSAGE 帧读回错误: ft=%s pl=%q err=%v", ft, pl, err)
	}
	if _, _, err := rs.ReadFrame(); err != io.EOF {
		t.Fatalf("末尾应读得 EOF，得到: %v", err)
	}
}

// TestDataChunking：WriteData 把超 64KiB 数据切成多个 DATA 帧。
func TestDataChunking(t *testing.T) {
	var buf bytes.Buffer
	sess := NewSession(&buf)
	big := bytes.Repeat([]byte("x"), MaxDataPayload*2+100)
	if err := sess.WriteData(big); err != nil {
		t.Fatalf("WriteData 失败: %v", err)
	}
	rs := NewSession(&buf)
	var got []byte
	for i := 0; i < 3; i++ {
		ft, pl, err := rs.ReadFrame()
		if err != nil || ft != FrameData {
			t.Fatalf("第 %d 帧应为大 DATA: ft=%s err=%v", i, ft, err)
		}
		got = append(got, pl...)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("分片重组数据不一致: len(got)=%d len(want)=%d", len(got), len(big))
	}
	// 恰好三帧：64KiB + 64KiB + 100B
	if got == nil || buf.Len() != 0 {
		t.Fatalf("应恰好读完: 剩余 %d", buf.Len())
	}
}

// TestFrameSizeLimits：读侧对超限载荷直接拒绝；
// 写侧 WriteFrame 对超限载荷报错且不写出。
func TestFrameSizeLimits(t *testing.T) {
	// 读侧：手工构造一个声明 100KiB 的 DATA 帧头
	var buf bytes.Buffer
	buf.Write(binary.AppendUvarint(nil, uint64(FrameData)))
	buf.Write(binary.AppendUvarint(nil, uint64(MaxDataPayload+1)))
	buf.Write(make([]byte, 1024))
	rs := NewSession(&buf)
	if _, _, err := rs.ReadFrame(); err == nil {
		t.Fatal("超限 DATA 帧应被拒绝")
	}

	// 写侧：DATA 超 64KiB、JSON 控制帧超 64KiB、END 带载荷都应报错
	var out bytes.Buffer
	ws := NewSession(&out)
	if err := ws.WriteFrame(FrameData, make([]byte, MaxDataPayload+1)); err == nil {
		t.Fatal("写超限 DATA 应报错")
	}
	if err := ws.WriteFrame(FrameOpen, make([]byte, MaxJSONPayload+1)); err == nil {
		t.Fatal("写超限 OPEN 应报错")
	}
	if err := ws.WriteFrame(FrameEnd, []byte("x")); err == nil {
		t.Fatal("END 带载荷应报错")
	}
	if err := ws.WriteWSMessage(WSOpcodeBinary, make([]byte, MaxWSMessage)); err == nil {
		t.Fatal("超 4MiB 的 WS_MESSAGE 应报错")
	}
	if out.Len() != 0 {
		t.Fatalf("被拒绝的写不应产生字节，已写 %d", out.Len())
	}
}

// TestWSMessageLimit4MiB：恰好 4MiB（含 opcode）的 WS_MESSAGE 可读写。
func TestWSMessageLimit4MiB(t *testing.T) {
	var buf bytes.Buffer
	sess := NewSession(&buf)
	msg := make([]byte, MaxWSMessage-1)
	if err := sess.WriteWSMessage(WSOpcodeBinary, msg); err != nil {
		t.Fatalf("恰达上限的 WS_MESSAGE 应可写: %v", err)
	}
	rs := NewSession(&buf)
	ft, pl, err := rs.ReadFrame()
	if err != nil || ft != FrameWSMessage || len(pl) != MaxWSMessage {
		t.Fatalf("4MiB WS_MESSAGE 读回错误: ft=%s len=%d err=%v", ft, len(pl), err)
	}
}

// TestUnknownFrameType：未知帧类型在头部即被拒绝（不读载荷）。
func TestUnknownFrameType(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(binary.AppendUvarint(nil, 99))
	buf.Write(binary.AppendUvarint(nil, 3))
	buf.Write([]byte("abc"))
	rs := NewSession(&buf)
	if _, _, err := rs.ReadFrame(); err == nil {
		t.Fatal("未知帧类型应被拒绝")
	}
}

// TestTruncatedPayload：载荷被截断时返回错误（而非静默成功）。
func TestTruncatedPayload(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(binary.AppendUvarint(nil, uint64(FrameData)))
	buf.Write(binary.AppendUvarint(nil, 10))
	buf.Write([]byte("abc")) // 实际只有 3 字节，声明 10
	rs := NewSession(&buf)
	if _, _, err := rs.ReadFrame(); err == nil {
		t.Fatal("截断载荷应报错")
	}
}

// ---------- JSON 消息 ----------

// TestJSONMessages：OPEN/RESULT/ABORT/WS_CLOSE 的 JSON 字段与
// 规格一致（headers 用二元数组保序、保重复名）。
func TestJSONMessages(t *testing.T) {
	var buf bytes.Buffer
	sess := NewSession(&buf)
	open := &OpenMsg{
		V: 1, Kind: KindHTTP, Target: "/p?q=1", Method: "POST",
		Host: "example.com",
		Headers: []HeaderPair{
			{"X-A", "1"}, {"X-A", "2"}, {"Content-Type", "text/plain"},
		},
		Protocols: []string{"chat"},
	}
	if err := sess.WriteJSON(FrameOpen, open); err != nil {
		t.Fatalf("写 OPEN 失败: %v", err)
	}
	if err := sess.WriteJSON(FrameResult, &ResultMsg{
		Kind: KindHTTP, Status: 200, StatusText: "OK",
		Headers: []HeaderPair{{"Content-Length", "3"}},
	}); err != nil {
		t.Fatalf("写 RESULT 失败: %v", err)
	}
	if err := sess.Abort("bad", "出错了"); err != nil {
		t.Fatalf("写 ABORT 失败: %v", err)
	}
	if err := sess.WriteJSON(FrameWSClose, &CloseMsg{Code: 1000, Reason: "bye"}); err != nil {
		t.Fatalf("写 WS_CLOSE 失败: %v", err)
	}

	rs := NewSession(&buf)
	ft, pl, err := rs.ReadFrame()
	if err != nil || ft != FrameOpen {
		t.Fatalf("OPEN 帧错误: %v", err)
	}
	o, err := DecodeOpen(pl)
	if err != nil || o.V != 1 || o.Kind != KindHTTP || o.Target != "/p?q=1" ||
		o.Method != "POST" || o.Host != "example.com" ||
		len(o.Headers) != 3 || o.Headers[1] != (HeaderPair{"X-A", "2"}) ||
		len(o.Protocols) != 1 || o.Protocols[0] != "chat" {
		t.Fatalf("OPEN 解码不符: %+v err=%v", o, err)
	}

	ft, pl, err = rs.ReadFrame()
	if err != nil || ft != FrameResult {
		t.Fatalf("RESULT 帧错误: %v", err)
	}
	r, err := DecodeResult(pl)
	if err != nil || r.Status != 200 || r.StatusText != "OK" {
		t.Fatalf("RESULT 解码不符: %+v err=%v", r, err)
	}

	ft, pl, err = rs.ReadFrame()
	if err != nil || ft != FrameAbort {
		t.Fatalf("ABORT 帧错误: %v", err)
	}
	a, err := DecodeAbort(pl)
	if err != nil || a.Code != "bad" || a.Message != "出错了" {
		t.Fatalf("ABORT 解码不符: %+v err=%v", a, err)
	}

	ft, pl, err = rs.ReadFrame()
	if err != nil || ft != FrameWSClose {
		t.Fatalf("WS_CLOSE 帧错误: %v", err)
	}
	c, err := DecodeClose(pl)
	if err != nil || c.Code != 1000 || c.Reason != "bye" {
		t.Fatalf("WS_CLOSE 解码不符: %+v err=%v", c, err)
	}
}

// TestValidateOpen：server 分发前的 OPEN 字段防御校验。
func TestValidateOpen(t *testing.T) {
	ok := &OpenMsg{V: 1, Kind: KindHTTP, Target: "/a", Method: "POST",
		Host: "api.internal", Headers: []HeaderPair{{"X-A", "1"}},
		Protocols: []string{"chat"}}
	if err := ValidateOpen(ok); err != nil {
		t.Fatalf("合法 OPEN 被拒: %v", err)
	}
	bad := []OpenMsg{
		{V: 2, Kind: KindHTTP, Target: "/a"},
		{V: 1, Kind: "x", Target: "/a"},
		{V: 1, Kind: KindHTTP, Target: ""},
		{V: 1, Kind: KindHTTP, Target: "/a\r\n"},
		{V: 1, Kind: KindHTTP, Target: "/a", Host: "bad\rhost"},
		{V: 1, Kind: KindHTTP, Target: "/a", Method: "BAD METHOD"},
		{V: 1, Kind: KindHTTP, Target: "/a", Headers: []HeaderPair{{"Bad Name", "v"}}},
		{V: 1, Kind: KindHTTP, Target: "/a", Headers: []HeaderPair{{"X-A", "v\r\nB: x"}}},
		{V: 1, Kind: KindWS, Target: "/ws", Protocols: []string{"bad proto"}},
	}
	for i, m := range bad {
		if err := ValidateOpen(&m); err == nil {
			t.Fatalf("bad[%d] 应被拒绝", i)
		}
	}
}

// TestValidateWSClose：主动发送码受浏览器式约束；接收侧允许
// RFC 线上码（如 1011），但仍拒绝保留/越界码与超长 reason。
func TestValidateWSClose(t *testing.T) {
	if err := ValidateWSCloseSend(&CloseMsg{Code: 1000, Reason: "ok"}); err != nil {
		t.Fatalf("合法 close 被拒: %v", err)
	}
	if err := ValidateWSCloseSend(&CloseMsg{Code: 1011}); err == nil {
		t.Fatal("主动发送 1011 应被拒绝")
	}
	if err := ValidateWSClose(&CloseMsg{Code: 1011}); err != nil {
		t.Fatalf("接收 1011 应合法: %v", err)
	}
	for _, code := range []int{999, 1004, 1005, 1006, 1015, 2000, 5000} {
		if err := ValidateWSClose(&CloseMsg{Code: code}); err == nil {
			t.Fatalf("非法 close code %d 应被拒", code)
		}
	}
	if err := ValidateWSClose(&CloseMsg{Code: 1000,
		Reason: strings.Repeat("x", MaxWSCloseReasonBytes+1)}); err == nil {
		t.Fatal("超长 close reason 应被拒")
	}
}

// ---------- 头字段工具 ----------

// TestHeadersPairs：http.Header ↔ 二元数组互转保重复值；
// 名称排序使输出确定。
func TestHeadersPairs(t *testing.T) {
	h := http.Header{}
	h.Add("X-B", "2")
	h.Add("X-A", "1")
	h.Add("X-B", "3") // 同名重复值
	pairs := HeadersToPairs(h)
	if len(pairs) != 3 || pairs[0][0] != "X-A" {
		t.Fatalf("pairs 排序/数量错误: %v", pairs)
	}
	back := PairsToHeaders(pairs)
	if got := back.Values("X-B"); len(got) != 2 {
		t.Fatalf("重复头字段丢失: %v", got)
	}
}

// TestStripHopByHop：逐跳头（含 Connection 点名的动态字段）被剥除，
// 端到端头保留。
func TestStripHopByHop(t *testing.T) {
	h := http.Header{
		"Connection":          {"keep-alive, X-Hop"},
		"X-Hop":               {"v"},
		"Keep-Alive":          {"timeout=5"},
		"Transfer-Encoding":   {"chunked"},
		"Upgrade":             {"websocket"},
		"Proxy-Authorization": {"Basic x"},
		"Content-Type":        {"application/json"},
	}
	StripHopByHop(h)
	for _, k := range []string{"Connection", "X-Hop", "Keep-Alive", "Transfer-Encoding", "Upgrade", "Proxy-Authorization"} {
		if h.Get(k) != "" {
			t.Fatalf("逐跳头 %s 未被剥除", k)
		}
	}
	if h.Get("Content-Type") != "application/json" {
		t.Fatal("端到端头被误删")
	}
}

// TestExtractHost：显式 host 优先；headers 里的 Host 被提取并移除。
func TestExtractHost(t *testing.T) {
	pairs := []HeaderPair{{"Host", "h1"}, {"X-A", "1"}}
	host, rest := ExtractHost(pairs, "")
	if host != "h1" || len(rest) != 1 {
		t.Fatalf("Host 提取错误: host=%q rest=%v", host, rest)
	}
	host, rest = ExtractHost(pairs, "explicit")
	if host != "explicit" || len(rest) != 2 {
		t.Fatalf("显式 host 应优先且不移除字段: host=%q rest=%v", host, rest)
	}
}

// ---------- GracefulClose ----------

// TestGracefulClose：探针报告全部确认时立即关闭；
// 未确认时轮询等待直至超时。
func TestGracefulClose(t *testing.T) {
	c := &fakeCloser{}

	// 全部确认 → 立即关
	calls := 0
	probe := func() (uint64, uint64, int) {
		calls++
		return 100, 100, 0
	}
	if err := GracefulClose(c, probe, 3e9); err != nil || !c.closed {
		t.Fatalf("GracefulClose 失败: %v", err)
	}
	if calls != 1 {
		t.Fatalf("已确认时应一次探针即关: calls=%d", calls)
	}

	// 一直未确认 → 等满超时后照样关
	c.closed = false
	start := nowMillis()
	probe = func() (uint64, uint64, int) { return 100, 0, 10 }
	if err := GracefulClose(c, probe, 80e6); err != nil { // 80ms
		t.Fatalf("GracefulClose 失败: %v", err)
	}
	if !c.closed {
		t.Fatal("超时后仍应关闭")
	}
	if d := nowMillis() - start; d < 60 {
		t.Fatalf("应等待近超时再关: %dms", d)
	}
}

// TestGracefulCloseNilProbe：nil 探针（普通 net.Conn）直接关。
func TestGracefulCloseNilProbe(t *testing.T) {
	c := &fakeCloser{}
	if err := GracefulClose(c, nil, 3e9); err != nil || !c.closed {
		t.Fatal("nil 探针应立即关闭")
	}
}

type fakeCloser struct{ closed bool }

func (f *fakeCloser) Close() error { f.closed = true; return nil }

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
