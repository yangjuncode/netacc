package netacc

import (
	"bytes"
	"io"
	"testing"
)

// TestDataFrameRoundTrip 验证 DATA 帧头/载荷编码解码往返一致。
func TestDataFrameRoundTrip(t *testing.T) {
	cases := []struct {
		off, ts uint64
		payload []byte
	}{
		{0, 0, []byte("a")},
		{127, 1, bytes.Repeat([]byte("x"), 300)}, // 跨 varint 多字节边界
		{1<<40 + 3, 1<<33 + 7, make([]byte, 1)},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		buf.Write(appendDataFrame(nil, c.off, c.ts, c.payload))
		fr := newFrameReader(&buf)
		ft, f, err := fr.next()
		if err != nil {
			t.Fatalf("解码 DATA 帧失败: %v", err)
		}
		if ft != frameData {
			t.Fatalf("帧类型应为 DATA，得到 %d", ft)
		}
		if f.off != c.off || f.sendTs != c.ts || !bytes.Equal(f.payload, c.payload) {
			t.Fatalf("DATA 帧往返不一致: off=%d/%d ts=%d/%d len=%d/%d",
				f.off, c.off, f.sendTs, c.ts, len(f.payload), len(c.payload))
		}
		// 帧应恰好消费完编码字节（不留尾巴）
		if fr.bufBuffered() != 0 || buf.Len() != 0 {
			t.Fatal("DATA 帧解码后有多余字节")
		}
	}
}

// TestAckFrameRoundTrip 验证 ACK 帧（累积偏移 + SACK ranges + 时间戳回显 + 窗口）往返一致。
func TestAckFrameRoundTrip(t *testing.T) {
	ranges := []byteRange{{100, 200}, {300, 350}, {1 << 40, 1<<40 + 999}}
	buf := appendAckFrame(nil, 4096, 123456789, 42, 65536, 7, ranges)
	fr := newFrameReader(bytes.NewReader(buf))
	ft, f, err := fr.next()
	if err != nil {
		t.Fatalf("解码 ACK 帧失败: %v", err)
	}
	if ft != frameAck {
		t.Fatalf("帧类型应为 ACK，得到 %d", ft)
	}
	if f.cum != 4096 || f.tsEcho != 123456789 || f.tsPath != 42 || f.window != 65536 || f.seq != 7 {
		t.Fatalf("ACK 字段不一致: cum=%d tsEcho=%d tsPath=%d window=%d seq=%d",
			f.cum, f.tsEcho, f.tsPath, f.window, f.seq)
	}
	if len(f.ranges) != len(ranges) {
		t.Fatalf("ranges 数不一致: %d != %d", len(f.ranges), len(ranges))
	}
	for i, r := range ranges {
		if f.ranges[i] != r {
			t.Fatalf("range[%d] 不一致: %+v != %+v", i, f.ranges[i], r)
		}
	}
}

// TestCtrlFrameRoundTrip 验证控制帧（PATH_* 等预留类型）编码解码：type + body_len + body。
func TestCtrlFrameRoundTrip(t *testing.T) {
	body := []byte{0x0a, 0x10, 0xde, 0xad}
	for _, ft := range []frameType{framePathAttach, framePathDrop, framePathRequest,
		framePathReady, framePing, frameTelemetry, frameFin, frameRst} {
		buf := appendCtrlFrame(nil, ft, body)
		fr := newFrameReader(bytes.NewReader(buf))
		got, f, err := fr.next()
		if err != nil {
			t.Fatalf("解码控制帧 %d 失败: %v", ft, err)
		}
		if got != ft || !bytes.Equal(f.body, body) {
			t.Fatalf("控制帧往返不一致: type=%d/%d body=%x/%x", got, ft, f.body, body)
		}
	}
}

// TestFrameDecodeLimits 验证解码端防御上限：超大载荷 / 过多 ranges / 截断输入。
func TestFrameDecodeLimits(t *testing.T) {
	// 载荷超过 maxFramePayload
	var hdr []byte
	hdr = appendUvarintField(hdr, uint64(frameData))
	hdr = appendUvarintField(hdr, 0)                 // offset
	hdr = appendUvarintField(hdr, 0)                 // ts
	hdr = appendUvarintField(hdr, maxFramePayload+1) // len 超限
	fr := newFrameReader(bytes.NewReader(append(hdr, make([]byte, 8)...)))
	if _, _, err := fr.next(); err == nil {
		t.Fatal("超限 DATA 载荷应被拒绝")
	}

	// ranges 数超过 maxAckRanges
	hdr = nil
	hdr = appendUvarintField(hdr, uint64(frameAck))
	hdr = appendUvarintField(hdr, 0) // cum
	hdr = appendUvarintField(hdr, 0) // ts_echo
	hdr = appendUvarintField(hdr, 0) // ts_path
	hdr = appendUvarintField(hdr, 0) // window
	hdr = appendUvarintField(hdr, 0) // seq
	hdr = appendUvarintField(hdr, maxAckRanges+1)
	fr = newFrameReader(bytes.NewReader(hdr))
	if _, _, err := fr.next(); err == nil {
		t.Fatal("超限 ACK ranges 应被拒绝")
	}

	// 截断的帧头
	fr = newFrameReader(bytes.NewReader([]byte{byte(frameData), 0x80}))
	if _, _, err := fr.next(); err == nil {
		t.Fatal("截断帧应报错")
	}

	// 未知帧类型
	fr = newFrameReader(bytes.NewReader([]byte{200}))
	if _, _, err := fr.next(); err == nil {
		t.Fatal("未知帧类型应报错")
	}
}

// TestFrameReaderStream 连续多帧流式解码：DATA + ACK + FIN 背靠背。
func TestFrameReaderStream(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(appendDataFrame(nil, 0, 11, []byte("hello")))
	buf.Write(appendAckFrame(nil, 5, 11, 1, 1024, 0, nil))
	buf.Write(appendCtrlFrame(nil, frameFin, nil))
	fr := newFrameReader(&buf)

	ft, f, err := fr.next()
	if err != nil || ft != frameData || string(f.payload) != "hello" {
		t.Fatalf("第 1 帧错误: ft=%d err=%v", ft, err)
	}
	ft, f, err = fr.next()
	if err != nil || ft != frameAck || f.cum != 5 || f.tsEcho != 11 || f.window != 1024 || f.seq != 0 {
		t.Fatalf("第 2 帧错误: ft=%d f=%+v err=%v", ft, f, err)
	}
	ft, _, err = fr.next()
	if err != nil || ft != frameFin {
		t.Fatalf("第 3 帧错误: ft=%d err=%v", ft, err)
	}
	if _, _, err = fr.next(); err != io.EOF {
		t.Fatalf("流结束应得 io.EOF，得到 %v", err)
	}
}
