package netacc

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	msgio "github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc/internal/pb"
)

// aggStreamIDLen 是 agg_stream_id 的字节长度（128bit，规格书 §4.1）。
const aggStreamIDLen = 16

// maxHandshakeMsg 是单条握手消息的最大长度（防御上限，
// 防对端伪造巨大长度前缀撑爆内存；正常 Hello/HelloAck 只有几十字节）。
const maxHandshakeMsg = 4 << 10

// handshakeInitiator 在握手流上执行发起方握手：
// 生成 128bit 随机 agg_stream_id，发 Hello，收 HelloAck 并校验回显一致。
// 握手完成后该流即升格为聚合流的第一条数据路径（规格书 §4.1 分离+复用混合）。
func handshakeInitiator(s msgReadWriter) ([16]byte, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return id, fmt.Errorf("生成 agg_stream_id 失败: %w", err)
	}
	w := msgio.NewVarintWriter(s)
	r := msgio.NewVarintReaderSize(s, maxHandshakeMsg)

	raw, err := proto.Marshal(&pb.Hello{AggStreamId: id[:]})
	if err != nil {
		return id, fmt.Errorf("编码 Hello 失败: %w", err)
	}
	if err := w.WriteMsg(raw); err != nil {
		return id, fmt.Errorf("发送 Hello 失败: %w", err)
	}
	raw, err = r.ReadMsg()
	if err != nil {
		return id, fmt.Errorf("等待 HelloAck 失败: %w", err)
	}
	var ack pb.HelloAck
	if err := proto.Unmarshal(raw, &ack); err != nil {
		return id, fmt.Errorf("解码 HelloAck 失败: %w", err)
	}
	if !bytes.Equal(ack.GetAggStreamId(), id[:]) {
		return id, errors.New("HelloAck 回显的 agg_stream_id 与本地生成不一致")
	}
	return id, nil
}

// msgReadWriter 是握手/绑定阶段对底层流的最小读写抽象。
// network.Stream 与直拨出的 network.MuxedStream 都满足它。
// go-msgio 的 varintReader 逐字节读、无预读缓冲，握手后把底层流
// 交接给 frameReader 不会丢数据面字节。
type msgReadWriter interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}

// 接收方握手拆成读/写两步：Accept 在两者之间把新聚合流注册进
// Aggregator 的 PATH_ATTACH 路由表。顺序要求——HelloAck 一旦到达
// 对端，对端就可能立刻发起 PATH_ATTACH；若注册晚于回执发出，
// 对端的首挂路径会因路由表查无此 agg_stream_id 而被 reset。

// handshakeResponderRead 是接收方握手前半：收 Hello
// （校验 agg_stream_id 恰为 16 字节），返回协商出的聚合流标识。
func handshakeResponderRead(s msgReadWriter) ([16]byte, error) {
	var id [16]byte
	r := msgio.NewVarintReaderSize(s, maxHandshakeMsg)
	raw, err := r.ReadMsg()
	if err != nil {
		return id, fmt.Errorf("等待 Hello 失败: %w", err)
	}
	var hello pb.Hello
	if err := proto.Unmarshal(raw, &hello); err != nil {
		return id, fmt.Errorf("解码 Hello 失败: %w", err)
	}
	if len(hello.GetAggStreamId()) != aggStreamIDLen {
		return id, fmt.Errorf("agg_stream_id 应为 %d 字节，实收 %d",
			aggStreamIDLen, len(hello.GetAggStreamId()))
	}
	copy(id[:], hello.GetAggStreamId())
	return id, nil
}

// handshakeResponderAck 是接收方握手后半：回写 HelloAck 表示接受。
// 应答发出后，握手流升格为该聚合流的第一条数据路径（分离+复用混合，
// 规格书 §4.1）。
func handshakeResponderAck(s msgReadWriter, id [16]byte) error {
	raw, err := proto.Marshal(&pb.HelloAck{AggStreamId: id[:]})
	if err != nil {
		return fmt.Errorf("编码 HelloAck 失败: %w", err)
	}
	w := msgio.NewVarintWriter(s)
	if err := w.WriteMsg(raw); err != nil {
		return fmt.Errorf("发送 HelloAck 失败: %w", err)
	}
	return nil
}

// resettable 是 watchStreamCtx 对底层流的最小抽象：
// network.Stream.Reset 与 network.MuxedStream.Reset 都满足。
type resettable interface {
	Reset() error
}

// watchStreamCtx 让握手受 ctx 约束：ctx 结束时 Reset 底层流，
// 打断阻塞中的握手 I/O（yamux/QUIC 流不感知 ctx，只能由本层兜底）。
// 握手结束（无论成败）必须调用返回的 stop 停掉看守 goroutine。
//
// 注意必须用互斥锁标记 finished，而不是只比 channel：调用方在 stop()
// 之后紧接着 cancel 派生 ctx，若看守 goroutine 在两者都就绪后才被调度，
// select 会随机命中 ctx.Done() 分支，误 Reset 一条已完成握手的流。
func watchStreamCtx(s resettable, ctx context.Context) (stop func()) {
	var mu sync.Mutex
	finished := false
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			mu.Lock()
			if !finished {
				_ = s.Reset()
			}
			mu.Unlock()
		case <-done:
		}
	}()
	return func() {
		mu.Lock()
		finished = true
		mu.Unlock()
		close(done)
	}
}
