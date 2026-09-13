// Package netacc 是基于 go-libp2p 的流级带宽聚合库：
// 把一个对等点之间的多条网络路径叠加成一条高带宽逻辑流（聚合流），
// 对应用呈现普通可靠有序字节流（net.Conn）语义。
//
// 数据面帧结构（DATA/ACK + 重排缓冲 + 聚合窗口流控）由 #18 落地，
// 见 stream.go / frame.go / reorder.go；多路径接入（PATH_ATTACH +
// 对称加路径 + 轮询条带）由 #19 落地，见 path.go；调度器在 #20。
package netacc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/transport"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"
)

// ProtocolID 是聚合流握手协议（兼首条数据路径）的 multistream 协议号。
const ProtocolID = protocol.ID("/netacc/agg/1.0.0")

// PathProtocolID 是路径绑定协议的 multistream 协议号（规格书 §4.2/§4.3）：
// 加路径方在新子流上协商本协议，首帧 PATH_ATTACH 携带 agg_stream_id +
// path_id 完成绑定。独立于 ProtocolID，避免与建流握手混淆。
const PathProtocolID = protocol.ID("/netacc/path/1.0.0")

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
	// reorderMin / reorderMax：收端重排缓冲（连接级接收窗口）下界与硬顶，
	// 容量 = clamp(Σest_rate·maxRTT, min, max)（规格书 §6）。
	reorderMin, reorderMax int
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

// WithReorderBuffer 设置收端重排缓冲（连接级接收窗口）的上下界，
// 默认 1MiB / 32MiB（规格书 §6）。容量取
// clamp(Σest_rate·maxRTT, min, max)；min 建议不小于最大单路径 BDP，
// max 是内存保护硬顶。min/max 非法（≤0 或 min>max）时收敛到默认。
func WithReorderBuffer(min, max int) Option {
	return func(o *options) {
		if min <= 0 || max <= 0 || min > max {
			return
		}
		o.reorderMin, o.reorderMax = min, max
	}
}

// OpenOption 是 OpenStream 的逐调用选项（规格书 §8 的 opts...，逐调用覆盖构造默认）。
type OpenOption func(*openOptions)

type openOptions struct {
	// minPaths：OpenStream 的成功门槛——至少 n 条数据路径 attached（规格书 §8）。
	minPaths int
}

// WithMinPaths 要求聚合流至少挂接 n 条数据路径才算建立成功（规格书 §8）。
// n > 1 时 OpenStream 在握手后用 peerstore 中的对端地址逐条直拨补挂，
// 补不齐则关闭聚合流并返回错误。
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

	// streams 是 PATH_ATTACH 路由表：agg_stream_id → 聚合流。
	// 入向路径绑定子流经它找到目标 Stream（handlePathStream）；
	// 流终态时反注册，防止陈旧 id 被误绑。
	streamsMu sync.Mutex
	streams   map[[16]byte]*Stream
}

// New 在 host 上创建 Aggregator 并注册 /netacc/agg/1.0.0 与
// /netacc/path/1.0.0 两个流处理器。同一 host 上重复 New 会互相
// 覆盖流处理器，应避免。
//
// 传输矩阵（规格书 §3.3，#22）：Aggregator 自身不注册传输——
// 能拨/能听哪些地址完全取决于 host 建 host 时注册的传输集
// （AddPath/dialDirect 经 swarm.TransportForDialing 逐地址匹配）。
// go-libp2p 默认传输集已含全部五种：TCP、QUIC、WebSocket（/ws）、
// WebTransport、WebRTC-direct；注意配置 PSK 私网时默认退化为
// 仅 TCP+WebSocket（其余传输不支持 PSK）。自定义传输集时把想要
// 的传输用 libp2p.Transport(...) 注册进 host 即可，本库无需额外
// 配置。
func New(h host.Host, opts ...Option) *Aggregator {
	a := &Aggregator{
		host: h,
		opts: options{
			acceptBacklog:    16,
			handshakeTimeout: defaultHandshakeTimeout,
			reorderMin:       defaultMinReorderBuf,
			reorderMax:       defaultMaxReorderBuf,
		},
		streams: make(map[[16]byte]*Stream),
	}
	for _, o := range opts {
		o(&a.opts)
	}
	a.acceptCh = make(chan network.Stream, a.opts.acceptBacklog)
	h.SetStreamHandler(ProtocolID, a.handleStream)
	h.SetStreamHandler(PathProtocolID, a.handlePathStream)
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

// handlePathStream 是入向路径绑定处理器（/netacc/path/1.0.0）：
// 读首帧 PATH_ATTACH → 按 agg_stream_id 查路由表 → 校验 path_id
// 落在对端命名空间且未用过 → 回显同一 PATH_ATTACH 帧表示接受 →
// 子流升格为该聚合流的新数据路径。任一步失败直接 reset
// （规格书 §4.2：连接级认证 + ID 绑定，无逐路径签名）。
func (a *Aggregator) handlePathStream(s network.Stream) {
	fail := func() { _ = s.Reset() }
	if a.closed.Load() {
		fail()
		return
	}
	if a.opts.handshakeTimeout > 0 {
		// 绑定读防挂死：对端建流后不应迟迟不发 PATH_ATTACH
		_ = s.SetDeadline(time.Now().Add(a.opts.handshakeTimeout))
	}
	fr := newFrameReader(s)
	ft, f, err := fr.next()
	if err != nil || ft != framePathAttach {
		fail()
		return
	}
	aggID, pathID, err := unmarshalPathAttach(f.body)
	if err != nil {
		fail()
		return
	}
	a.streamsMu.Lock()
	st := a.streams[aggID]
	a.streamsMu.Unlock()
	if st == nil {
		fail() // 不认识的 agg_stream_id
		return
	}
	// 预定 path_id：分侧命名空间 + 不重复（死路径重加须换新 id）
	if !st.reserveRemotePathID(pathID) {
		fail()
		return
	}
	// 先回显再挂接：挂接后发送泵可能立刻往该子流条带 DATA，
	// 若回显与之交错会污染帧边界（两侧写同一子流靠各自时序
	// 分隔——回显在 attachPath 之前写出，接收方按序先读回显）。
	// deadline 保留到回显写完：兜住死对端的写阻塞；attachPath
	// 启动 recvLoop 前必须清掉，否则残留 deadline 会误杀路径。
	if _, err := s.Write(appendCtrlFrame(nil, framePathAttach, f.body)); err != nil {
		fail()
		return
	}
	_ = s.SetDeadline(time.Time{})
	if err := st.attachPath(&path{id: pathID, conn: s}, fr); err != nil {
		fail()
		return
	}
}

// register/unregister 维护 PATH_ATTACH 路由表。
func (a *Aggregator) register(st *Stream) {
	a.streamsMu.Lock()
	a.streams[st.id] = st
	a.streamsMu.Unlock()
}

func (a *Aggregator) unregister(id [16]byte, st *Stream) {
	a.streamsMu.Lock()
	if cur := a.streams[id]; cur == st {
		delete(a.streams, id)
	}
	a.streamsMu.Unlock()
}

// dialDirect 用 transport 层直拨建一条到 peer 的新物理连接
// （规格书 §3.1：swarm 公开拨号 API 有去重——已有可接受连接时不再
// 拨新；要确定性地开第二/三条物理连接只能走 TransportForDialing）。
// 直拨连接不进 swarm 连接表（无 identify/notifiee/connmgr），
// 生命周期归本库：随其承载的路径一同关闭。
//
// 对 addr 形态无传输偏好（#22）：/tcp、/ws、/tls/sni/.../ws、
// /quic-v1、/quic-v1/webtransport/certhash/...、
// /webrtc-direct/certhash/... 均可，TransportForDialing 按完整
// 协议栈匹配传输（mafmt 要求整地址被模式消费，复合后缀天然
// 归到最外层传输，QUIC 不会误吞 webtransport 地址）。WT/WebRTC
// 地址缺 certhash 时 Dial 直接报错——certhash 由对端证书决定
// 且会轮换，地址过期属预期，调用方喂新地址重试即可。
func (a *Aggregator) dialDirect(ctx context.Context, p peer.ID, addr ma.Multiaddr) (transport.CapableConn, error) {
	// 容忍 addr 带 /p2p/<peerID> 后缀：剥掉再匹配传输
	if pid, err := peer.IDFromP2PAddr(addr); err == nil && pid != "" {
		addr, _ = ma.SplitLast(addr)
	}
	sw, ok := a.host.Network().(*swarm.Swarm)
	if !ok {
		return nil, errors.New("netacc: 底层 Network 不是 *swarm.Swarm，无法直拨")
	}
	tpt := sw.TransportForDialing(addr)
	if tpt == nil {
		return nil, fmt.Errorf("netacc: 没有能拨 %s 的传输", addr)
	}
	cc, err := tpt.Dial(ctx, addr, p)
	if err != nil {
		return nil, fmt.Errorf("netacc: 直拨 %s 失败: %w", addr, err)
	}
	// 防御拨错：TCP 等无加密对端认证的传输不会校验 peer，
	// 拨到别的对端会让 PATH_ATTACH 被莫名 reset，这里提前拦下。
	if cc.RemotePeer() != p {
		_ = cc.Close()
		return nil, fmt.Errorf("netacc: 直拨 %s 的对端 %s 不是目标 peer %s", addr, cc.RemotePeer(), p)
	}
	return cc, nil
}

// OpenStream 向 peer 发起一条聚合流：在已有连接上开 /netacc/agg/1.0.0
// 握手流，协商出 128bit agg_stream_id；握手流同时升格为第一条数据路径。
//
// 握手流目前经 host.NewStream 由 swarm 自选连接（规格书「任意已有连接」）。
// 成功即表示至少一条数据路径 attached；WithMinPaths(n) 时继续在
// peerstore 地址上直拨补挂，补不齐返回 error（规格书 §8）。
func (a *Aggregator) OpenStream(ctx context.Context, p peer.ID, opts ...OpenOption) (*Stream, error) {
	if a.closed.Load() {
		return nil, ErrClosed
	}
	oo := openOptions{minPaths: 1}
	for _, opt := range opts {
		opt(&oo)
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
	st := newStreamFull(id, s, a.streamCfg(), a, p, true)
	// 先注册再 start：对端收到 HelloAck 后即可反向 PATH_ATTACH 过来，
	// 注册须先于「对端能感知本流存在」的任何信号。
	a.register(st)
	st.start()
	if oo.minPaths > 1 {
		if err := st.attachMinPaths(ctx, oo.minPaths); err != nil {
			_ = st.Close()
			return nil, err
		}
	}
	return st, nil
}

// attachMinPaths 兑现 WithMinPaths(n)：用 peerstore 中的对端地址
// 逐条直拨补挂路径，直到存活路径数 ≥ n。一轮全地址零进展即放弃。
// 同一地址重复直拨会建多条物理连接——规格书 §1：同一传输开多条
// 连接也算多条路径。
func (s *Stream) attachMinPaths(ctx context.Context, n int) error {
	addrs := s.agg.host.Peerstore().Addrs(s.peer)
	if len(addrs) == 0 {
		return fmt.Errorf("netacc: peerstore 无对端地址，WithMinPaths(%d) 无法补挂路径", n)
	}
	// 按 §3.3 传输偏好先排序：TCP/WS 优先于 UDP 系（QUIC/WT/WebRTC
	// 同生共死，未实测带宽前不应抢占补挂名额）。这只是先验排序，
	// 实测降权由调度器负责（#20）。
	SortAddrsByPreference(addrs)
	for len(s.Paths()) < n {
		progress := false
		for _, addr := range addrs {
			if len(s.Paths()) >= n {
				break
			}
			if _, err := s.AddPath(ctx, addr); err == nil {
				progress = true
			}
		}
		if !progress {
			return fmt.Errorf("netacc: 补挂路径后仍只有 %d 条，不满足 WithMinPaths(%d)",
				len(s.Paths()), n)
		}
	}
	return nil
}

// streamCfg 把 Aggregator 选项落到聚合流数据面参数。
func (a *Aggregator) streamCfg() streamConfig {
	return streamConfig{minBuf: a.opts.reorderMin, maxBuf: a.opts.reorderMax}
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
			id, err := handshakeResponderRead(s)
			if err != nil {
				stop()
				cancel()
				_ = s.Reset()
				continue // 坏握手不影响后续入流
			}
			st := newStreamFull(id, s, a.streamCfg(), a, s.Conn().RemotePeer(), false)
			// 注册先于回执：HelloAck 到达对端后，对端的 PATH_ATTACH
			// 随时可能到——路由表必须先就位，否则合法的首挂路径被 reset。
			a.register(st)
			err = handshakeResponderAck(s, id)
			stop()
			cancel()
			if err != nil {
				_ = st.Close() // 回收已注册的流（含握手子流与路由表项）
				continue
			}
			if err := ctx.Err(); err != nil {
				// 握手完成与 ctx 取消竞态，同上处理
				_ = st.Close()
				return nil, err
			}
			st.start()
			return st, nil
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

// Close 注销流处理器并标记关闭；已建立的聚合流不受影响
// （各自持有全部路径连接；路由表保留以便流收尾时反注册，
// 本对象关闭后仅不再接受新的建流与加路径请求）。
func (a *Aggregator) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		a.host.RemoveStreamHandler(ProtocolID)
		a.host.RemoveStreamHandler(PathProtocolID)
	}
	return nil
}
