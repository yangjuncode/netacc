package netacc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/transport"
	ma "github.com/multiformats/go-multiaddr"
	msmux "github.com/multiformats/go-multistream"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc/internal/pb"
)

// path 是聚合流内的一条数据路径（规格书 §3：一条底层连接 + 其上一条
// 子流）。帧归属天然由子流承载，数据帧头不带 path_id（规格书 §4.4）。
//
// 并发约定：p.wmu 串行化该路径上的一切帧写出（发送泵的 DATA/ACK、
// Close 的 FIN、摘除路径时的 PATH_DROP 互不交错）；指标字段一律由
// 所属 Stream.mu 保护（采样/记账/调度都在 mu 下读写，见 scheduler.go）。
type path struct {
	id   uint64    // path_id（创建方命名空间，两端视图一致）
	conn pathConn  // 底层子流：network.Stream 或直拨的 MuxedStream
	own  io.Closer // 直拨出的底层连接（swarm 托管连接/管道为 nil）：
	// 直拨连接不进 swarm 连接表，必须随路径显式关闭，否则 conn 泄漏
	dialed bool       // 本侧是否为该路径的发起方（拨号+attach 一侧）
	wmu    sync.Mutex // 该路径的写串行化
	dead   atomic.Bool

	// ---- 以下为逐路径调度指标（#20，全部由所属 Stream.mu 保护）----
	attachedAt time.Time // 挂接时刻（首个 delivery/rx 采样窗起点）

	// RTT 采样（RFC 6298 EWMA + min_rtt；样本来自 ACK 的
	// ts_echo/ts_path 回显与 PING 应答，见 noteRTT）
	srtt      time.Duration // 平滑 RTT（含底层排队延迟）
	rttvar    time.Duration // RTT 方差（#23 的 RTO 备用）
	minRTT    time.Duration // 传播延迟基线
	hasRTT    bool          // 是否有过有效 RTT 样本
	lastRTTAt time.Time     // 最近 RTT 样本时刻（过期判定）

	// BBR 式 delivery-rate 采样（见 onDelivered）
	inflight   int       // 在途未确认字节数（调度器账本）
	delivered  uint64    // 该路径累计被累积确认字节
	estRate    float64   // 速率估计（字节/秒）
	hasRate    bool      // 是否有过有效速率样本
	rateMark   uint64    // 上次采样窗结束时 delivered 快照
	lastRateAt time.Time // 上次速率样本时刻（est_rate 保鲜期判定）

	// 对端 TELEMETRY 回报的收端观测速率（校准 est_rate，见 effRate）
	peerRate   float64   // 字节/秒
	peerRateAt time.Time // 最近一次回报时刻（TTL 判定）

	// 收端方向记账：本端作为收端观测的该路径到达字节（TELEMETRY
	// 周期换算成速率回报对端，并用作收端窗口容量联动的输入，
	// 见 onTick / updateWindowCapLocked）
	rxBytes uint64    // 累计到达字节
	rxMark  uint64    // 上个回报周期结束时 rxBytes 快照
	rxMarkT time.Time // 上个回报周期结束时刻
	rxRate  float64   // 平滑后的到达速率估计（字节/秒）

	lastPingAt time.Time // 上次主动 PING 时刻（节流）
}

// localAddr 返回本端地址（尽力而为：优先底层连接 multiaddr，退化 net.Addr）。
func (p *path) localAddr() net.Addr {
	if cc, ok := p.own.(transport.CapableConn); ok {
		return maAddr{cc.LocalMultiaddr()}
	}
	if ns, ok := p.conn.(network.Stream); ok {
		return maAddr{ns.Conn().LocalMultiaddr()}
	}
	if nc, ok := p.conn.(net.Conn); ok {
		return nc.LocalAddr()
	}
	return nil
}

// remoteAddr 返回对端地址，同上尽力而为。
func (p *path) remoteAddr() net.Addr {
	if cc, ok := p.own.(transport.CapableConn); ok {
		return maAddr{cc.RemoteMultiaddr()}
	}
	if ns, ok := p.conn.(network.Stream); ok {
		return maAddr{ns.Conn().RemoteMultiaddr()}
	}
	if nc, ok := p.conn.(net.Conn); ok {
		return nc.RemoteAddr()
	}
	return nil
}

// connID 返回底层连接标识，用于区分「同 peer 多连接」的归属
// （规格书 §3.2/验收：stream.Conn() 正确归属路径）。swarm 托管子流取
// Conn().ID()；直拨连接不在 swarm 表内，用其远端 multiaddr 代替。
func (p *path) connID() string {
	if ns, ok := p.conn.(network.Stream); ok {
		return ns.Conn().ID()
	}
	if cc, ok := p.own.(transport.CapableConn); ok {
		return "direct:" + cc.RemoteMultiaddr().String()
	}
	return ""
}

// PathInfo 是一条数据路径的观测快照（规格书 §8 Paths()）。
// #20 起附逐路径调度指标只读快照，为 #25 的 Stats() 铺路。
type PathInfo struct {
	ID     uint64   // path_id（创建方命名空间，两端视图一致）
	Dialed bool     // 本侧是否为该路径的发起方
	ConnID string   // 底层连接标识（同 peer 多连接并存时的归属判据）
	Local  net.Addr // 本端地址（multiaddr 包装，取不到为 nil）
	Remote net.Addr // 对端地址

	// ---- 调度指标快照（无样本时为零值）----
	SRTT     time.Duration // 平滑 RTT（含底层排队延迟）
	MinRTT   time.Duration // 最小 RTT（传播延迟基线）
	EstRate  float64       // 速率估计（字节/秒，含对端 TELEMETRY 校准）
	Inflight int           // 当前在途未确认字节数
}

// ---------- PATH_ATTACH / PATH_DROP 帧体 ----------

// marshalPathAttach 编码 PATH_ATTACH 帧体。
func marshalPathAttach(aggID [16]byte, pathID uint64) ([]byte, error) {
	return proto.Marshal(&pb.PathAttach{AggStreamId: aggID[:], PathId: pathID})
}

// unmarshalPathAttach 解码并校验 PATH_ATTACH 帧体，返回 agg_stream_id 与 path_id。
func unmarshalPathAttach(body []byte) (aggID [16]byte, pathID uint64, err error) {
	var pa pb.PathAttach
	if err := proto.Unmarshal(body, &pa); err != nil {
		return aggID, 0, fmt.Errorf("解码 PathAttach 失败: %w", err)
	}
	if len(pa.GetAggStreamId()) != aggStreamIDLen {
		return aggID, 0, fmt.Errorf("PATH_ATTACH 的 agg_stream_id 应为 %d 字节，实收 %d",
			aggStreamIDLen, len(pa.GetAggStreamId()))
	}
	copy(aggID[:], pa.GetAggStreamId())
	return aggID, pa.GetPathId(), nil
}

// ---------- 加路径 ----------

// attachPath 把一条已完成绑定握手（或信任来源，如首条握手路径/测试管道）
// 的子流挂入路径集并启动其接收循环。fr 为该子流上已消费完绑定帧的
// 帧读器——必须复用同一 bufio 读缓冲继续解帧，否则握手阶段预读到的
// 数据帧会丢失；为 nil（管道/首路径）时新建。
func (s *Stream) attachPath(p *path, fr *frameReader) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	if p.attachedAt.IsZero() {
		p.attachedAt = time.Now()
	}
	s.paths = append(s.paths, p)
	s.pathsByID[p.id] = p
	s.mu.Unlock()
	if fr == nil {
		fr = newFrameReader(p.conn)
	}
	go s.recvLoop(p, fr)
	// 冷启动探测（规格书 §5.2）：新路径尚无 srtt/est_rate 样本，
	// 一条 PING 让逐路径 RTT 先跑起来，不必等首个数据往返。
	// 异步写：底层同步传输（如 net.Pipe）上写会阻塞至对端开读，
	// attachPath 不能因此卡住调用方；PING 与 DATA 共用 wmu
	// 串行化，先后次序不影响正确性。
	go s.sendPing(p, false, s.sendTs())
	s.wakeSend() // 发送泵立刻把新路径纳入调度
	return nil
}

// AddPath 主动为聚合流补挂一条数据路径（规格书 §8 手动路径口）：
// transport 层直拨 addr 建一条新的物理连接（不进 swarm 连接表，
// 生命周期归本流），在其上开 /netacc/path/1.0.0 子流并完成
// PATH_ATTACH 绑定握手。返回分配到的 path_id。
//
// 对称性：发起/接收两侧都可调用（规格书 §4.3 双向对称加路径，
// 覆盖 NAT 后只能出向的一端——出向拨号即可）。
// addr 可带可不带 /p2p/<peerID> 后缀。
func (s *Stream) AddPath(ctx context.Context, addr ma.Multiaddr) (uint64, error) {
	if s.agg == nil {
		return 0, errors.New("netacc: 该聚合流不经 Aggregator 创建，无法拨号加路径")
	}
	// 预定本侧命名空间的 path_id（先取号再拨号：即便拨号失败，
	// 作废一个 id 也无碍——已用 id 永不复用）
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, net.ErrClosed
	}
	pathID := s.nextPathID
	s.nextPathID += 2
	s.mu.Unlock()

	cc, err := s.agg.dialDirect(ctx, s.peer, addr)
	if err != nil {
		return 0, err
	}
	// 从这里起任何失败都必须 cc.Close()：直拨连接不进 swarm 连接表
	ms, err := cc.OpenStream(ctx)
	if err != nil {
		_ = cc.Close()
		return 0, fmt.Errorf("netacc: 直拨连接上开子流失败: %w", err)
	}
	// CapableConn.OpenStream 不做 multistream 协商，必须手动跑
	// SelectProtoOrFail（原型 #13 实测：不协商对端 reset 0x1001）
	if err := msmux.SelectProtoOrFail(PathProtocolID, ms); err != nil {
		_ = cc.Close()
		return 0, fmt.Errorf("netacc: 协商 %s 失败: %w", PathProtocolID, err)
	}

	// 绑定握手受 ctx/握手超时约束：deadline 传导到子流 + 看守兜底 reset
	hsCtx, cancel := s.agg.handshakeCtx(ctx)
	defer cancel()
	stop := watchStreamCtx(ms, hsCtx)
	defer stop()
	if d, ok := hsCtx.Deadline(); ok {
		_ = ms.SetDeadline(d)
		defer ms.SetDeadline(time.Time{}) // 握手结束归还无限期路径
	}
	if err := s.sendPathAttach(ms, pathID); err != nil {
		_ = cc.Close()
		return 0, err
	}
	// 等对端回显同一 PATH_ATTACH 帧表示接受；对端不认识
	// agg_stream_id 或拒绝 path_id 时直接 reset 子流
	fr := newFrameReader(ms)
	if err := s.recvPathAttachAck(fr, pathID); err != nil {
		_ = cc.Close()
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		_ = cc.Close()
		return 0, err
	}
	if err := s.attachPath(&path{id: pathID, conn: ms, own: cc, dialed: true}, fr); err != nil {
		_ = cc.Close()
		return 0, err
	}
	return pathID, nil
}

// sendPathAttach 在新路径子流上写出绑定帧（PATH_ATTACH 为该子流首帧，
// 规格书 §4.2：携带 agg_stream_id 即完成绑定，无逐路径签名）。
func (s *Stream) sendPathAttach(w io.Writer, pathID uint64) error {
	body, err := marshalPathAttach(s.id, pathID)
	if err != nil {
		return fmt.Errorf("编码 PathAttach 失败: %w", err)
	}
	if _, err := w.Write(appendCtrlFrame(nil, framePathAttach, body)); err != nil {
		return fmt.Errorf("发送 PATH_ATTACH 失败: %w", err)
	}
	return nil
}

// recvPathAttachAck 读取对端的接受回显（同构 PATH_ATTACH 帧）。
func (s *Stream) recvPathAttachAck(fr *frameReader, pathID uint64) error {
	ft, f, err := fr.next()
	if err != nil {
		return fmt.Errorf("等待 PATH_ATTACH 回显失败: %w", err)
	}
	if ft != framePathAttach {
		return fmt.Errorf("期望 PATH_ATTACH 回显，实收帧类型 %d", ft)
	}
	aggID, echoID, err := unmarshalPathAttach(f.body)
	if err != nil {
		return err
	}
	if aggID != s.id || echoID != pathID {
		return fmt.Errorf("PATH_ATTACH 回显不一致: agg_stream_id/path_id 与本地不符")
	}
	return nil
}

// RemovePath 摘除一条本流的数据路径（规格书 §8 手动路径口）：
// 关闭底层子流（直拨连接随之关闭），并在存活路径上尽力发 PATH_DROP
// 告知对端摘除其对端视角的对应路径；该路径上的在途未确认数据自动
// 重注入剩余路径。拒绝摘除最后一条存活路径（聚合流存活性要求
// ≥1 条路径，规格书 §4.1）。
func (s *Stream) RemovePath(pathID uint64) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	p := s.pathsByID[pathID]
	if p == nil {
		s.mu.Unlock()
		return fmt.Errorf("netacc: 未知 path_id %d", pathID)
	}
	if len(s.paths) <= 1 {
		s.mu.Unlock()
		return errors.New("netacc: 不能摘除最后一条数据路径")
	}
	s.mu.Unlock()
	s.dropPath(p, nil, true)
	return nil
}

// Paths 返回当前存活数据路径的观测快照（规格书 §8）。
func (s *Stream) Paths() []PathInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := make([]PathInfo, 0, len(s.paths))
	for _, p := range s.paths {
		out = append(out, PathInfo{
			ID:       p.id,
			Dialed:   p.dialed,
			ConnID:   p.connID(),
			Local:    p.localAddr(),
			Remote:   p.remoteAddr(),
			SRTT:     p.srtt,
			MinRTT:   p.minRTT,
			EstRate:  p.effRate(now),
			Inflight: p.inflight,
		})
	}
	return out
}
