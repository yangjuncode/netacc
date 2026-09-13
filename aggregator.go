// Package netacc 是基于 go-libp2p 的流级带宽聚合库：
// 把一个对等点之间的多条网络路径叠加成一条高带宽逻辑流（聚合流），
// 对应用呈现普通可靠有序字节流（net.Conn）语义。
//
// 本文件为 issue #16 的最小骨架：Aggregator 公开面 + /netacc/agg/1.0.0
// 握手协议 + 单路径透传（握手流兼作第一条数据路径）。
// 帧结构 / 多路径 / 调度器在后续 issue 实现。
package netacc

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ProtocolID 是聚合流握手协议（兼首条数据路径）的 multistream 协议号。
const ProtocolID = protocol.ID("/netacc/agg/1.0.0")

// connmgrTag 用于 ConnManager().Protect，防止聚合对端的连接被修剪（规格书 §3.2）。
const connmgrTag = "netacc"

// defaultHandshakeTimeout 是握手阶段的默认超时（调用方 ctx 自带 deadline 时不生效）。
const defaultHandshakeTimeout = time.Minute

// ErrClosed 表示 Aggregator 已关闭。
var ErrClosed = errors.New("netacc: aggregator 已关闭")

// Option 是 New 的可选参数（规格书 §8 预留的扩展点）。
type Option func(*options)

type options struct {
	// acceptBacklog：已完成底层建流、等待 Accept 消费的最大排队数。
	acceptBacklog int
	// handshakeTimeout：握手阶段超时；调用方 ctx 有 deadline 时以 ctx 为准。
	handshakeTimeout time.Duration
}

// WithAcceptBacklog 设置等待 Accept 的入向握手流排队长度（默认 16）。
func WithAcceptBacklog(n int) Option {
	return func(o *options) { o.acceptBacklog = n }
}

// WithHandshakeTimeout 设置握手阶段超时（默认 1 分钟）。
// 仅在调用方传入的 ctx 没有 deadline 时生效；设为 0 表示不加额外超时。
func WithHandshakeTimeout(d time.Duration) Option {
	return func(o *options) { o.handshakeTimeout = d }
}

// OpenOption 是 OpenStream 的逐调用选项（规格书 §8 的 opts...，逐调用覆盖构造默认）。
type OpenOption func(*openOptions)

type openOptions struct {
	// minPaths：OpenStream 的成功门槛——至少 n 条数据路径 attached（规格书 §8）。
	minPaths int
}

// WithMinPaths 要求聚合流至少挂接 n 条数据路径才算建立成功（规格书 §8）。
// 骨架版只有握手流这一条数据路径，n > 1 时 OpenStream 必然返回错误。
func WithMinPaths(n int) OpenOption {
	return func(o *openOptions) { o.minPaths = n }
}

// Aggregator 是聚合库的公开入口：OpenStream 主动发起聚合流，
// Accept 接收对端发起的聚合流（规格书 §8）。
type Aggregator struct {
	host     host.Host
	opts     options
	acceptCh chan network.Stream
	closed   atomic.Bool
}

// New 在 host 上创建 Aggregator 并注册 /netacc/agg/1.0.0 流处理器。
// 同一 host 上重复 New 会互相覆盖流处理器，应避免。
func New(h host.Host, opts ...Option) *Aggregator {
	a := &Aggregator{
		host: h,
		opts: options{
			acceptBacklog:    16,
			handshakeTimeout: defaultHandshakeTimeout,
		},
	}
	for _, o := range opts {
		o(&a.opts)
	}
	a.acceptCh = make(chan network.Stream, a.opts.acceptBacklog)
	h.SetStreamHandler(ProtocolID, a.handleStream)
	return a
}

// handleStream 是入向握手流处理器：只做入队，握手在 Accept 里完成，
// 这样 Accept 的 ctx 能约束整个握手过程。
func (a *Aggregator) handleStream(s network.Stream) {
	if a.closed.Load() {
		_ = s.Reset()
		return
	}
	select {
	case a.acceptCh <- s:
	default:
		// 排队已满，拒绝多余的入向握手
		_ = s.Reset()
	}
}

// OpenStream 向 peer 发起一条聚合流：在已有连接上开 /netacc/agg/1.0.0
// 握手流，协商出 128bit agg_stream_id；握手流同时升格为第一条数据路径。
//
// 目前经 host.NewStream 由 swarm 自选连接（规格书「任意已有连接」）；
// 后续引入多路径时改用 conn.NewStream + 手动 multistream 协商
// （conn.NewStream 不做协议协商，需 msmux.SelectProtoOrFail，见调研 #2）。
//
// 成功即表示至少一条数据路径 attached；握手失败返回 error。
func (a *Aggregator) OpenStream(ctx context.Context, p peer.ID, opts ...OpenOption) (*Stream, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}
	oo := openOptions{minPaths: 1}
	for _, opt := range opts {
		opt(&oo)
	}
	if oo.minPaths > 1 {
		return nil, fmt.Errorf("netacc: 骨架实现仅支持单条数据路径，WithMinPaths(%d) 无法满足", oo.minPaths)
	}
	// 防止 connmgr 修剪聚合对端的连接
	a.host.ConnManager().Protect(p, connmgrTag)

	s, err := a.host.NewStream(ctx, p, ProtocolID)
	if err != nil {
		return nil, fmt.Errorf("netacc: 开握手流失败: %w", err)
	}
	hsCtx, cancel := a.handshakeCtx(ctx)
	defer cancel()
	stop := watchStreamCtx(s, hsCtx)
	defer stop()

	id, err := handshakeInitiator(s)
	if err != nil {
		_ = s.Reset()
		return nil, fmt.Errorf("netacc: 握手失败: %w", err)
	}
	if err := ctx.Err(); err != nil {
		// 握手完成与 ctx 取消竞态：看守 goroutine 可能已 Reset 掉流，
		// 按取消处理，避免向调用方返回一条坏流。
		_ = s.Reset()
		return nil, err
	}
	return newStream(id, s), nil
}

// Accept 接收一条对端发起的聚合流，完成握手应答后返回。
// ctx 同时约束等待入流与握手过程；无入流时阻塞至 ctx 取消。
// 单条入流握手失败不影响后续入流（Reset 后继续等待）。
func (a *Aggregator) Accept(ctx context.Context) (*Stream, error) {
	for {
		select {
		case s := <-a.acceptCh:
			a.host.ConnManager().Protect(s.Conn().RemotePeer(), connmgrTag)
			hsCtx, cancel := a.handshakeCtx(ctx)
			stop := watchStreamCtx(s, hsCtx)
			id, err := handshakeResponder(s)
			stop()
			cancel()
			if err != nil {
				_ = s.Reset()
				continue // 坏握手不影响后续入流
			}
			if err := ctx.Err(); err != nil {
				// 握手完成与 ctx 取消竞态，同上处理
				_ = s.Reset()
				return nil, err
			}
			return newStream(id, s), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// handshakeCtx 为握手阶段派生 ctx：调用方已带 deadline 时原样返回，
// 否则套 Aggregator 的握手超时（防对端建流后挂死不回包）。
func (a *Aggregator) handshakeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || a.opts.handshakeTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, a.opts.handshakeTimeout)
}

// Close 注销流处理器并标记关闭；已建立的聚合流不受影响。
func (a *Aggregator) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		a.host.RemoveStreamHandler(ProtocolID)
	}
	return nil
}
