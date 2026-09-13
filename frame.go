package netacc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// 数据面帧结构（规格书 §4.4）。
//
// 每条路径是一条保序字节流，帧在流内自描述定界：
// 首字段一律为 varint 帧类型，其后按类型接 varint 字段，DATA 帧末尾
// 紧跟原始载荷、控制帧末尾紧跟 body_len 定界的 protobuf 体。
//
// 编码边界取舍：DATA/ACK 是数据面热路径上的高频帧，逐包 protobuf
// 序列化开销不划算，且字段固定、无演进需求，故保持自定义 varint
// 二进制（规格只强制「数据帧头自定义二进制」）；PATH_*/PING/TELEMETRY
// 等低频控制消息帧体走 protobuf，便于字段演进。
//
// 各类型线格式：
//
//	DATA(1):        type | offset | send_ts_us | payload_len | payload
//	ACK(2):         type | cum_offset | ts_echo_us | window | seq | n_ranges | (start,end)*n
//	控制/其它(3–10): type | body_len | protobuf_body
//
// 序号空间为字节级：offset/cum/ranges 都是载荷在聚合流内的绝对字节
// 偏移，不是帧号——重组、跨路径重发、重分片统一为字节区间操作。
type frameType byte

const (
	frameData        frameType = 1 + iota // DATA：数据载荷
	frameAck                              // ACK：SACK 式确认 + 窗口通告
	framePathAttach                       // PATH_ATTACH：新路径绑定 agg_stream_id（#19）
	framePathDrop                         // PATH_DROP：摘除路径（#19）
	framePathRequest                      // PATH_REQUEST：请求协调中继路径（#21）
	framePathReady                        // PATH_READY：中继 reservation 就绪（#21）
	framePing                             // PING：主动探测（冷启动/疑似降级，#20）
	frameTelemetry                        // TELEMETRY：收端观测速率回报（#20）
	frameFin                              // FIN：发送端正常关闭（本票仅做 EOF 语义）
	frameRst                              // RST：异常重置（本票仅定义常量与解码）
)

// 解码端防御上限：防对端伪造巨大字段撑爆内存。
const (
	// maxFramePayload 是单个 DATA 帧载荷上限。
	// 取 64KiB：与底层拷贝块同量级，更小的帧减小队头阻塞粒度。
	maxFramePayload = 64 << 10
	// maxAckRanges 是单条 ACK 携带的 SACK 区间数上限。
	// 只回告偏移最低的若干段——对重传决策最有价值的是靠近队头的洞。
	maxAckRanges = 32
	// maxCtrlBody 是控制帧 protobuf 体的上限。
	maxCtrlBody = 64 << 10
)

// byteRange 是一个半开字节区间 [start, end)，用于 ACK 的 SACK ranges。
type byteRange struct {
	start, end uint64
}

// frame 是解码后的一帧：按类型取用相应字段。
type frame struct {
	off     uint64 // DATA：载荷流内字节偏移
	sendTs  uint64 // DATA：发送时间戳（微秒，发送端本地基准）
	payload []byte // DATA：载荷

	cum    uint64 // ACK：累积确认偏移（< cum 的字节均已按序收到）
	tsEcho uint64 // ACK：触发本 ACK 的 DATA 帧时间戳回显
	window uint64 // ACK：接收窗口通告（重排缓冲剩余量）
	seq    uint64 // ACK：发送端单调递增序号——多路径下 ACK 会
	// 跨路径超车，接收端只应用 seq 递增的帧（last-writer-wins），
	// 否则晚到的旧 ACK 会把通告窗口覆盖回过期值造成死锁
	ranges []byteRange // ACK：乱序已收区间（SACK）

	body []byte // 控制帧：protobuf 体（本票不解析）
}

func appendUvarintField(dst []byte, v uint64) []byte {
	return binary.AppendUvarint(dst, v)
}

// appendDataFrame 编码一帧 DATA。
func appendDataFrame(dst []byte, off, sendTs uint64, payload []byte) []byte {
	dst = appendUvarintField(dst, uint64(frameData))
	dst = appendUvarintField(dst, off)
	dst = appendUvarintField(dst, sendTs)
	dst = appendUvarintField(dst, uint64(len(payload)))
	return append(dst, payload...)
}

// appendAckFrame 编码一帧 ACK。ranges 会被截断到 maxAckRanges。
// seq 由发送端逐帧递增，供接收端丢弃跨路径超车的过期 ACK。
func appendAckFrame(dst []byte, cum, tsEcho, window, seq uint64, ranges []byteRange) []byte {
	if len(ranges) > maxAckRanges {
		ranges = ranges[:maxAckRanges]
	}
	dst = appendUvarintField(dst, uint64(frameAck))
	dst = appendUvarintField(dst, cum)
	dst = appendUvarintField(dst, tsEcho)
	dst = appendUvarintField(dst, window)
	dst = appendUvarintField(dst, seq)
	dst = appendUvarintField(dst, uint64(len(ranges)))
	for _, r := range ranges {
		dst = appendUvarintField(dst, r.start)
		dst = appendUvarintField(dst, r.end)
	}
	return dst
}

// appendCtrlFrame 编码一帧控制消息（FIN/RST 当前 body 为空）。
func appendCtrlFrame(dst []byte, ft frameType, body []byte) []byte {
	dst = appendUvarintField(dst, uint64(ft))
	dst = appendUvarintField(dst, uint64(len(body)))
	return append(dst, body...)
}

// frameReader 从路径字节流逐帧解码。
type frameReader struct {
	br *bufio.Reader
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{br: bufio.NewReaderSize(r, 64<<10)}
}

// bufBuffered 返回读缓冲中已预读未消费的字节数，供测试断言帧恰好对齐。
func (r *frameReader) bufBuffered() int { return r.br.Buffered() }

func (r *frameReader) uvarint(what string) (uint64, error) {
	v, err := binary.ReadUvarint(r.br)
	if err != nil {
		return 0, fmt.Errorf("读%s失败: %w", what, err)
	}
	return v, nil
}

// next 读出下一帧；流结束返回 io.EOF，格式违例返回非 EOF 错误。
func (r *frameReader) next() (frameType, frame, error) {
	var f frame
	t, err := r.uvarint("帧类型")
	if err != nil {
		if errors.Is(err, io.EOF) { // 帧边界上的 EOF 保持干净的 io.EOF 语义
			return 0, f, io.EOF
		}
		return 0, f, err
	}
	ft := frameType(t)
	switch ft {
	case frameData:
		if f.off, err = r.uvarint("DATA.offset"); err != nil {
			return 0, f, err
		}
		if f.sendTs, err = r.uvarint("DATA.send_ts"); err != nil {
			return 0, f, err
		}
		n, err := r.uvarint("DATA.payload_len")
		if err != nil {
			return 0, f, err
		}
		if n > maxFramePayload {
			return 0, f, fmt.Errorf("DATA 载荷 %d 超过上限 %d", n, maxFramePayload)
		}
		f.payload = make([]byte, n)
		if _, err := io.ReadFull(r.br, f.payload); err != nil {
			return 0, f, fmt.Errorf("读 DATA 载荷失败: %w", err)
		}
	case frameAck:
		if f.cum, err = r.uvarint("ACK.cum"); err != nil {
			return 0, f, err
		}
		if f.tsEcho, err = r.uvarint("ACK.ts_echo"); err != nil {
			return 0, f, err
		}
		if f.window, err = r.uvarint("ACK.window"); err != nil {
			return 0, f, err
		}
		if f.seq, err = r.uvarint("ACK.seq"); err != nil {
			return 0, f, err
		}
		nr, err := r.uvarint("ACK.n_ranges")
		if err != nil {
			return 0, f, err
		}
		if nr > maxAckRanges {
			return 0, f, fmt.Errorf("ACK ranges 数 %d 超过上限 %d", nr, maxAckRanges)
		}
		if nr > 0 {
			f.ranges = make([]byteRange, nr)
			for i := range f.ranges {
				if f.ranges[i].start, err = r.uvarint("ACK.range.start"); err != nil {
					return 0, f, err
				}
				if f.ranges[i].end, err = r.uvarint("ACK.range.end"); err != nil {
					return 0, f, err
				}
				if f.ranges[i].end <= f.ranges[i].start {
					return 0, f, fmt.Errorf("ACK range 非法: [%d,%d)", f.ranges[i].start, f.ranges[i].end)
				}
			}
		}
	case framePathAttach, framePathDrop, framePathRequest, framePathReady,
		framePing, frameTelemetry, frameFin, frameRst:
		// 控制帧统一 body_len 定界；本票不解体内容（PATH_* 等留给 #19/#20，
		// FIN/RST 的语义在 recvLoop 处理）。保留 body_len 字段以便未来
		// 在同一线格式上扩展控制帧体而不破坏定界。
		n, err := r.uvarint("控制帧.body_len")
		if err != nil {
			return 0, f, err
		}
		if n > maxCtrlBody {
			return 0, f, fmt.Errorf("控制帧体 %d 超过上限 %d", n, maxCtrlBody)
		}
		if n > 0 {
			f.body = make([]byte, n)
			if _, err := io.ReadFull(r.br, f.body); err != nil {
				return 0, f, fmt.Errorf("读控制帧体失败: %w", err)
			}
		}
	default:
		return 0, f, fmt.Errorf("未知帧类型 %d", t)
	}
	return ft, f, nil
}
