package netacc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
	msmux "github.com/multiformats/go-multistream"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc/internal/pb"
)

// 本文件实现中继路径的按需协调（规格书 §4.3，#21）：
//
//	PATH_REQUEST(经中继C) → 对端向 C 做 reservation → PATH_READY →
//	发起端经 <C-addr>/p2p/<C>/p2p-circuit/p2p/<对端> 电路地址 CONNECT
//	拨出一条端到端加密的直拨连接（中继只见密文），其上开
//	/netacc/path/1.0.0 子流走既有 PATH_ATTACH 绑定，此后与直连路径
//	完全同构（调度器无感知，死亡走既有 dropPath 自动摘除）。
//
// reservation 为按需触发：平时不占中继配额；reservation 只决定中继是否
// 接受 CONNECT，电路建立后其到期不影响已存活路径（常驻 reservation
// 清单是规格书明示的扩展项，未做）。

// maxConcurrentReservations 是单条聚合流上同时为对端执行的 reservation
// 数上限——PATH_REQUEST 走带内通道，异常对端理论上能刷屏诱使我方反复
// 向中继做 reservation，加个小上限兜底。
const maxConcurrentReservations = 8

// ---------- PATH_REQUEST / PATH_READY 帧体 ----------

// marshalPathRequest 编码 PATH_REQUEST 帧体（中继路径描述符：
// 中继 peerID + 中继可达地址集 + A→C 段传输偏好 acAddr）。
func marshalPathRequest(reqID uint64, relay peer.AddrInfo, acAddr ma.Multiaddr) ([]byte, error) {
	req := &pb.PathRequest{RequestId: reqID, RelayPeer: []byte(relay.ID)}
	for _, a := range relay.Addrs {
		req.RelayAddrs = append(req.RelayAddrs, a.Bytes())
	}
	if acAddr != nil {
		req.AcAddr = acAddr.Bytes()
	}
	return proto.Marshal(req)
}

// decodeRelayInfo 从 PathRequest 提取中继路径描述符：peerID + 地址集
// （/p2p/<peer> 后缀剥离；坏地址跳过，其余仍可用）。
func decodeRelayInfo(req *pb.PathRequest) (peer.AddrInfo, error) {
	pid, err := peer.IDFromBytes(req.GetRelayPeer())
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("netacc: PathRequest 的 relay_peer 非法: %w", err)
	}
	ai := peer.AddrInfo{ID: pid}
	for _, ab := range req.GetRelayAddrs() {
		a, err := ma.NewMultiaddrBytes(ab)
		if err != nil {
			continue
		}
		ai.Addrs = append(ai.Addrs, stripP2PSuffix(a))
	}
	return ai, nil
}

// marshalPathReady 编码 PATH_READY 帧体；rerr 非空表示 reservation 失败。
func marshalPathReady(reqID uint64, rerr error) ([]byte, error) {
	msg := &pb.PathReady{RequestId: reqID}
	if rerr != nil {
		msg.Error = rerr.Error()
	}
	return proto.Marshal(msg)
}

// ---------- 中继路径发起侧 ----------

// AddRelayPath 经中继 relay 为聚合流补挂一条中继路径（规格书 §4.3
// 按需协调）。relay 即中继路径描述符：
//
//   - relay.ID：中继节点 peerID；
//   - relay.Addrs：中继可达地址集——整集随 PATH_REQUEST 告知对端用于
//     连中继做 reservation；本端另按序取首个可拨地址构造电路地址，
//     A→C 段传输偏好即由该地址的 multiaddr 形态表达（要 WebSocket 就把
//     /ws 形态地址放最前）。C→B 段取对端 reservation 所用传输，
//     由对端自选。
//
// 对称性：发起/接收两侧都可调用（path_id 分侧命名空间沿用）。
//
// 已知限制（上游 circuitv2 client 行为）：A→C 的 hop 流由
// host.NewStream 发出，本端到中继已有存活连接时 swarm 优先复用既有
// 连接，此时 A→C 传输偏好退化为尽力而为；无既有连接时则确定性地拨
// 电路地址里内嵌的那一条。
func (s *Stream) AddRelayPath(ctx context.Context, relay peer.AddrInfo) (uint64, error) {
	if s.agg == nil {
		return 0, errors.New("netacc: 该聚合流不经 Aggregator 创建，无法拨号加路径")
	}
	if relay.ID == "" {
		return 0, errors.New("netacc: 中继路径描述符缺 relay peerID")
	}
	if relay.ID == s.peer || relay.ID == s.agg.host.ID() {
		return 0, fmt.Errorf("netacc: 中继 %s 不能是对端或本端自身", relay.ID)
	}
	// 先定 A→C 段地址（进电路地址）；无可拨地址就不必惊动对端
	hopAddr, err := s.agg.pickRelayAddr(relay)
	if err != nil {
		return 0, err
	}

	// 预订本侧命名空间 path_id 与 request 槽位（先取号：即便后续失败，
	// 作废 id 也无碍——已用 id 永不复用）
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, net.ErrClosed
	}
	pathID := s.nextPathID
	s.nextPathID += 2
	reqID := s.nextRelayReqID
	s.nextRelayReqID++
	readyCh := make(chan error, 1)
	s.pendingRelay[reqID] = readyCh
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pendingRelay, reqID)
		s.mu.Unlock()
	}()

	if err := s.sendPathRequest(reqID, relay, hopAddr); err != nil {
		return 0, fmt.Errorf("netacc: 发送 PATH_REQUEST 失败: %w", err)
	}

	// 等对端完成 reservation（对端可能要现拨中继，走握手超时兜底）
	hsCtx, cancel := s.agg.handshakeCtx(ctx)
	defer cancel()
	select {
	case rerr := <-readyCh:
		if rerr != nil {
			return 0, fmt.Errorf("netacc: 对端向中继 %s 做 reservation 失败: %w", relay.ID, rerr)
		}
	case <-hsCtx.Done():
		return 0, fmt.Errorf("netacc: 等待 PATH_READY 失败: %w", hsCtx.Err())
	case <-s.done:
		return 0, net.ErrClosed
	}

	// 对端 reservation 就绪 → 经电路地址 CONNECT。
	// 电路地址 = <C-addr>/p2p/<C>/p2p-circuit/p2p/<对端>；
	// dialDirect 剥掉尾部 /p2p/<对端> 后命中 swarm 的 circuit
	// transport（isRelayAddr → transports[P_CIRCUIT]），拨出端到端
	// 加密连接（对端 peerID 经安全升级认证，dialDirect 内已校验）。
	circuitAddr := buildCircuitAddr(hopAddr, relay.ID, s.peer)
	cc, err := s.agg.dialDirect(ctx, s.peer, circuitAddr)
	if err != nil {
		return 0, fmt.Errorf("netacc: 经中继 %s 建电路失败: %w", relay.ID, err)
	}
	if err := s.attachDialedConn(ctx, cc, pathID); err != nil {
		return 0, err
	}
	return pathID, nil
}

// sendPathRequest 在任意存活路径上发出 PATH_REQUEST（控制消息带内
// 复用任意存活路径，规格书 §4.1）。acAddr 是发起方选定的 A→C 段
// 中继地址（逐跳传输偏好，信息性告知对端）。
func (s *Stream) sendPathRequest(reqID uint64, relay peer.AddrInfo, acAddr ma.Multiaddr) error {
	body, err := marshalPathRequest(reqID, relay, acAddr)
	if err != nil {
		return fmt.Errorf("编码 PathRequest 失败: %w", err)
	}
	return s.writeCtrl(framePathRequest, body)
}

// handlePathReady 处理对端回送的 PATH_READY：按 request_id 投递给
// 等待中的 AddRelayPath。迟到/重复应答（调用方已超时放弃）直接丢弃。
func (s *Stream) handlePathReady(body []byte) {
	var pr pb.PathReady
	if err := proto.Unmarshal(body, &pr); err != nil {
		return // 帧体损坏：忽略（该路径仍可用）
	}
	s.mu.Lock()
	ch := s.pendingRelay[pr.GetRequestId()]
	s.mu.Unlock()
	if ch == nil {
		return
	}
	var rerr error
	if e := pr.GetError(); e != "" {
		rerr = errors.New(e)
	}
	select {
	case ch <- rerr:
	default:
	}
}

// ---------- 中继路径响应侧 ----------

// handlePathRequest 处理对端发来的 PATH_REQUEST：本侧向指定中继做
// reservation（原生 circuitv2 client），成败都回 PATH_READY。
// 异步执行——reservation 可能要现拨中继，不能阻塞路径接收循环。
func (s *Stream) handlePathRequest(body []byte) {
	var req pb.PathRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return // 帧体损坏：连 request_id 都取不出，无法应答——丢弃
	}
	reqID := req.GetRequestId()
	relay, err := decodeRelayInfo(&req)
	if err != nil {
		s.sendPathReady(reqID, err)
		return
	}
	if s.agg == nil {
		s.sendPathReady(reqID, errors.New("netacc: 该聚合流不经 Aggregator 创建，无法做 reservation"))
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.rsvInFlight >= maxConcurrentReservations {
		s.mu.Unlock()
		s.sendPathReady(reqID, errors.New("netacc: 同时在途的 reservation 协调超限"))
		return
	}
	s.rsvInFlight++
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			s.rsvInFlight--
			s.mu.Unlock()
		}()
		rsvCtx := context.Background()
		var cancel context.CancelFunc = func() {}
		if d := s.agg.opts.handshakeTimeout; d > 0 {
			rsvCtx, cancel = context.WithTimeout(rsvCtx, d)
		}
		defer cancel()
		// 流终态时取消 reservation，不留悬挂 goroutine
		watchDone := make(chan struct{})
		defer close(watchDone)
		go func() {
			select {
			case <-s.done:
				cancel()
			case <-watchDone:
			}
		}()
		// B→C 段传输由本端自选：Reserve 内 host.NewStream 走 swarm
		// 已有连接或按 peerstore 地址拨号（C→B 段即沿用该连接）。
		if _, err := relayclient.Reserve(rsvCtx, s.agg.host, relay); err != nil {
			s.sendPathReady(reqID, fmt.Errorf("netacc: reservation 失败: %w", err))
			return
		}
		s.sendPathReady(reqID, nil)
	}()
}

// sendPathReady 在任意存活路径上回送 PATH_READY；尽力而为。
func (s *Stream) sendPathReady(reqID uint64, rerr error) {
	body, err := marshalPathReady(reqID, rerr)
	if err != nil {
		return
	}
	_ = s.writeCtrl(framePathReady, body)
}

// ---------- 电路地址构造与拨号 ----------

// stripP2PSuffix 剥掉地址尾部的 /p2p/<peerID> 组件（容忍
// AddrInfoToP2pAddrs 形态的地址）；无后缀时原样返回。
func stripP2PSuffix(addr ma.Multiaddr) ma.Multiaddr {
	base, _ := peer.SplitAddr(addr)
	return base
}

// pickRelayAddr 从路径描述符的地址集里挑 A→C 段地址：首个本端有对应
// 传输可拨的地址（顺序即发起方的传输偏好）。本身即 /p2p-circuit 的
// 地址被跳过——中继套中继被上游协议禁止（relay.go isRelayAddr 检查），
// 内嵌进电路地址也没有意义。
func (a *Aggregator) pickRelayAddr(relay peer.AddrInfo) (ma.Multiaddr, error) {
	for _, addr := range relay.Addrs {
		base := stripP2PSuffix(addr)
		if base == nil {
			continue
		}
		if _, err := base.ValueForProtocol(ma.P_CIRCUIT); err == nil {
			continue // 电路地址不能作为 A→C 段载体
		}
		if a.transportForDialing(base) != nil {
			return base, nil
		}
	}
	return nil, fmt.Errorf("netacc: 中继 %s 的地址集 %v 中没有本端可拨的地址", relay.ID, relay.Addrs)
}

// buildCircuitAddr 拼电路地址 <relay-addr>/p2p/<relay>/p2p-circuit/p2p/<dest>。
func buildCircuitAddr(relayAddr ma.Multiaddr, relayID, dest peer.ID) ma.Multiaddr {
	return relayAddr.Encapsulate(ma.StringCast(
		"/p2p/" + relayID.String() + "/p2p-circuit/p2p/" + dest.String()))
}

// ---------- 共用控制帧写出 ----------

// writeCtrl 在存活路径上写一条控制帧；写失败摘除该路径并换下一条重试
// （与 sendAck 同款级联，深度 ≤ 路径数）。无存活路径或流已关时返回错误。
func (s *Stream) writeCtrl(ft frameType, body []byte) error {
	buf := appendCtrlFrame(nil, ft, body)
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return net.ErrClosed
		}
		p := s.pickPathLocked()
		s.mu.Unlock()
		if p == nil {
			return errNoPaths
		}
		p.wmu.Lock()
		_, err := p.conn.Write(buf)
		p.wmu.Unlock()
		if err == nil {
			return nil
		}
		s.dropPath(p, err, true)
	}
}

// ---------- 直拨连接上的路径绑定（直连/中继电路共用） ----------

// attachDialedConn 在一条已拨通的直拨连接（直连路径的传输连接或中继
// 电路连接，都不进 swarm 连接表）上完成路径绑定：开
// /netacc/path/1.0.0 子流 → 手动 multistream 协商 → PATH_ATTACH →
// 等对端回显 → 挂入路径集。任何失败都关闭 cc；成功时路径已开始收帧。
//
// OpenStream 用 WithAllowLimitedConn 兜底：本库中继解除限额不标
// limited，但兼容跑原生限额的外部中继（规格书 §3.2）；对 muxer 级
// OpenStream 是无害 no-op。
func (s *Stream) attachDialedConn(ctx context.Context, cc transport.CapableConn, pathID uint64) error {
	openCtx := network.WithAllowLimitedConn(ctx, "netacc")
	ms, err := cc.OpenStream(openCtx)
	if err != nil {
		_ = cc.Close()
		return fmt.Errorf("netacc: 直拨连接上开子流失败: %w", err)
	}
	// CapableConn.OpenStream 不做 multistream 协商，必须手动跑
	// SelectProtoOrFail（原型 #13 实测：不协商对端 reset 0x1001）
	if err := msmux.SelectProtoOrFail(PathProtocolID, ms); err != nil {
		_ = cc.Close()
		return fmt.Errorf("netacc: 协商 %s 失败: %w", PathProtocolID, err)
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
		return err
	}
	// 等对端回显同一 PATH_ATTACH 帧表示接受；对端不认识
	// agg_stream_id 或拒绝 path_id 时直接 reset 子流
	fr := newFrameReader(ms)
	if err := s.recvPathAttachAck(fr, pathID); err != nil {
		_ = cc.Close()
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = cc.Close()
		return err
	}
	if err := s.attachPath(&path{id: pathID, conn: ms, own: cc, dialed: true}, fr); err != nil {
		_ = cc.Close()
		return err
	}
	return nil
}
