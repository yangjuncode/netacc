package netacc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc/internal/pb"
)

// 数据面默认参数。
const (
	// defaultMinReorderBuf 是重排缓冲下界默认（≈最大单路径 BDP 的保守估计）。
	defaultMinReorderBuf = 1 << 20 // 1MiB
	// defaultMaxReorderBuf 是重排缓冲硬顶默认（规格 §6：32–64MB 量级）。
	defaultMaxReorderBuf = 32 << 20 // 32MiB
)

// errReset 是对端发来 RST 帧（或等价硬重置）时读/写侧返回的错误。
var errReset = errors.New("netacc: 聚合流被对端重置")

// errNoPaths 是最后一条数据路径也消失、且对端未曾正常关闭时
// 聚合流的兜底终态错误。
var errNoPaths = errors.New("netacc: 聚合流已无可用数据路径")

// timeoutError 实现 net.Error，SetReadDeadline/SetWriteDeadline 到期返回。
type timeoutError struct{}

func (timeoutError) Error() string   { return "netacc: i/o 超时" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var errTimeout error = timeoutError{}

// pathConn 是一条数据路径的最小抽象：底层保序可靠字节流。
// 生产环境为 network.Stream（swarm 托管子流）或 network.MuxedStream
// （transport 直拨连接开出的子流）；测试注入 net.Pipe 端点。
type pathConn interface {
	io.Reader
	io.Writer
	io.Closer
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// sendSeg 是一段已发未确认数据：保留至连接级 ACK 释放。
// path 记录它当前在哪条路径在途；路径死亡时改置 nil 表示
// 「待换路重发」（保持原字节偏移，规格书 §5.3）。
type sendSeg struct {
	off  uint64
	data []byte
	path *path // nil = 待重发
}

// streamConfig 是聚合流数据面参数。
type streamConfig struct {
	minBuf int // 重排缓冲下界（规格 §6 min_buf：最大单路径 BDP）
	maxBuf int // 重排缓冲硬顶（规格 §6 max_buf，可配）
	// sendBufCap 是发送侧「未送达对端应用」数据总量（待发队列 +
	// 在途未确认）的上限，防 Write 内存无限增长；与 TCP 发送缓冲
	// 同构。默认取窗口容量的 2 倍：窗口被对端读空重新打开时，待发
	// 队列里始终有足够数据立刻填满窗口。
	sendBufCap int
}

// windowCap 计算重排缓冲容量（= 连接级接收窗口），规格 §6：
//
//	reorder_buf = clamp(Σ est_rate_i · max_srtt_i, minBuf, maxBuf)
//
// 逐路径 est_rate/srtt 采样属 #20；估计量暂恒为 0 → 容量恒取下界
// minBuf。调度器落地后在此公式内填入各路径实测值并按需重算、
// 向对端发窗口更新即可扩容。
func (c streamConfig) windowCap() int {
	estBDP := 0 // TODO(#20): 替换为 Σ est_rate_i · max_srtt_i（逐路径求和）
	if estBDP < c.minBuf {
		return c.minBuf
	}
	if estBDP > c.maxBuf {
		return c.maxBuf
	}
	return estBDP
}

// Stream 是一条聚合流：对应用呈现 net.Conn 语义（可靠有序字节流）。
//
// 数据面（规格 §4.4/§6）：
//   - 写方向：Write → 待发队列 → 发送泵按对端通告窗口切成 DATA 帧、
//     轮询选一条存活路径下发（帧头携带流内字节偏移与发送时间戳）；
//   - 读方向：每条路径一个接收循环解帧 → 共用同一重排缓冲保序 →
//     Read 取走；每收一帧 DATA 回一条 ACK（累积偏移 + SACK ranges +
//     时间戳回显 + 窗口通告），ACK 经任意存活路径回送；
//   - 流控：单一聚合窗口——发送端维护 unacked（已发未累积确认），
//     unacked ≥ 对端通告窗口时发送泵停摆，进而 Write 在发送缓冲
//     占满后阻塞，直到对端 Read 释放窗口；
//   - 路径集合：PATH_ATTACH 绑定新路径（path_id 分侧命名空间），
//     路径死亡即摘除并把其在途未确认段重注入剩余路径（重排缓冲
//     按偏移去重，重复字节安全）；PATH_DROP 带内告知对端。
//
// 并发结构：一个 sendLoop goroutine 独占 DATA/ACK 帧写出（路径间
// 轮询），每条路径一个 recvLoop goroutine 解帧喂入共享重排缓冲；
// 应用侧 Read/Write 经 mu + chan 广播与两者同步，deadline 用
// atomic + timer 实现，不依赖底层流的 SetDeadline（应用 deadline
// 只约束本层阻塞，不传导到路径写，避免误触发摘除）。
//
// 选路点即调度器接缝：当前为轮询条带，最短排空时间优先调度器
// （逐路径 est_rate/srtt/inflight）在 #20 落地——届时只替换
// pickPathLocked 的决策，帧编码与写路径不变。
type Stream struct {
	id   [16]byte
	agg  *Aggregator // 所属聚合器（PATH_ATTACH 路由表注册/反注册）；测试流为 nil
	peer peer.ID     // 对端 peer（pipe 测试流为空）
	cfg  streamConfig
	base time.Time // 发送时间戳基准：ts = 相对 base 的微秒数（含单调钟）

	mu   sync.Mutex
	rbuf reorderBuf

	// 路径集合（mu 保护）：paths 为存活路径列表（轮询选路的遍历序），
	// pathsByID 以 path_id 索引全部存活路径（PATH_DROP 查找用）。
	paths     []*path
	pathsByID map[uint64]*path
	rrNext    uint64 // 轮询游标（#20 换成调度器状态）
	// path_id 分侧命名空间（规格 §4.3）：最低位标识分配侧
	//（0=聚合流发起方，1=接收方），本侧计数器步长 2 保证两端
	// 互不冲突；握手流升格的首条路径恒为发起方 id 0。
	nextPathID   uint64              // 本侧下一个待分配 path_id
	remoteParity uint64              // 对端分配 id 的奇偶位（本侧为 1-remoteParity）
	seenRemote   map[uint64]struct{} // 对端曾用过的 path_id（含已死：重加须换新 id）
	finRecv      bool                // 任一路径上收到过 FIN（对端正常关闭）

	// 发送侧（mu 保护）
	sendQ        [][]byte  // 待发队列（应用已写、尚未装帧）
	qHead        int       // sendQ[0] 内已消费的偏移
	pendingBytes int       // 待发队列总字节
	sentOff      uint64    // 下一待分配流内偏移 = 已装帧字节总数
	cumAcked     uint64    // 对端累积确认偏移
	unackedSegs  []sendSeg // 已发未确认帧，保留至连接级 ACK；按 path 记在途路径
	advWindow    uint64    // 对端最新通告的接收窗口（重排缓冲剩余量）
	gotWindow    bool      // 是否已收到首个窗口通告
	ackSeq       uint64    // 本侧 ACK 序号（逐帧递增）
	peerAckSeq   uint64    // 对端最新 ACK 序号（丢弃跨路径超车的旧 ACK）
	gotPeerAck   bool      // 是否已收到对端首个 ACK

	// 接收侧（mu 保护）
	lastDataTs    uint64 // 最近收到的 DATA 帧发送时间戳（ACK 回显用）
	freedSinceAck int    // 自上次 ACK 以来 Read 释放的缓冲字节数
	rerr          error  // 读侧终态：io.EOF=对端正常关闭；其它=传输错误
	werr          error  // 写侧终态
	closed        bool   // 本地 Close 或 teardown

	rChange chan struct{} // 读侧状态变化广播（chan 替换式）
	wChange chan struct{} // 写侧状态变化广播

	ackCh     chan struct{} // cap1：请求发送泵补一条 ACK
	sendWake  chan struct{} // cap1：通知发送泵「可能有数据/窗口可发」
	done      chan struct{} // 流终态（Close/shutdown 关闭）
	closeOnce sync.Once

	rDeadline atomic.Int64 // unixnano，0 = 无 deadline
	wDeadline atomic.Int64
}

var _ net.Conn = (*Stream)(nil)

// newStreamOnPath 建一条以 p 为首条路径的聚合流（测试用：
// 无 Aggregator、无握手，发起方视角）。
func newStreamOnPath(id [16]byte, p pathConn, cfg streamConfig) *Stream {
	s := newStreamFull(id, p, cfg, nil, "", true)
	s.start()
	return s
}

// newStreamFull 构造聚合流对象但不启动数据面 goroutine——接收方须先
// 把 HelloAck 写回握手流再 start()，否则原始数据帧会抢在 HelloAck
// 之前污染握手字节序（msgio 握手与数据面帧共用同一底层流）。
// initiator 决定本侧 path_id 奇偶命名空间；agg 用于注册 PATH_ATTACH
// 路由；二者在 pipe 测试流下均为零值。
func newStreamFull(id [16]byte, pc pathConn, cfg streamConfig, agg *Aggregator, remote peer.ID, initiator bool) *Stream {
	// 参数收敛：零值/非法配置回落到默认，防 0 容量死流
	if cfg.minBuf <= 0 {
		cfg.minBuf = defaultMinReorderBuf
	}
	if cfg.maxBuf <= 0 {
		cfg.maxBuf = defaultMaxReorderBuf
	}
	if cfg.maxBuf < cfg.minBuf {
		cfg.maxBuf = cfg.minBuf
	}
	if cfg.sendBufCap <= 0 {
		cfg.sendBufCap = 2 * cfg.windowCap()
	}
	s := &Stream{
		id:         id,
		agg:        agg,
		peer:       remote,
		cfg:        cfg,
		base:       time.Now(),
		ackCh:      make(chan struct{}, 1),
		sendWake:   make(chan struct{}, 1),
		done:       make(chan struct{}),
		rChange:    make(chan struct{}),
		wChange:    make(chan struct{}),
		seenRemote: make(map[uint64]struct{}),
	}
	s.rbuf = *newReorderBuf(cfg.windowCap())
	// 首条路径 = 握手流升格，path_id 恒为发起方命名空间的 0，
	// 两端视图一致（PATH_DROP 据此寻址）。
	first := &path{id: 0, conn: pc, dialed: initiator}
	s.paths = []*path{first}
	s.pathsByID = map[uint64]*path{0: first}
	if initiator {
		s.nextPathID = 2   // 偶数命名空间，0 已占
		s.remoteParity = 1 // 对端（接收方）分配奇数 id
	} else {
		s.nextPathID = 1   // 奇数命名空间
		s.remoteParity = 0 // 对端（发起方）分配偶数 id
		s.seenRemote[0] = struct{}{}
	}
	return s
}

// start 启动数据面 goroutine：首条路径的接收循环 + 发送泵。
func (s *Stream) start() {
	go s.recvLoop(s.paths[0], newFrameReader(s.paths[0].conn))
	go s.sendLoop()
}

// ID 返回握手协商出的 128bit agg_stream_id（聚合流标识）。
// 后续 PATH_ATTACH 凭此 ID 把新路径绑定到本流（规格书 §4.2）。
func (s *Stream) ID() [16]byte { return s.id }

// Peer 返回聚合流对端的 peer.ID。
func (s *Stream) Peer() peer.ID { return s.peer }

// sendTs 返回当前发送时间戳（相对 base 的微秒数，含单调钟）。
// ACK 回显后发送端用它算 srtt（#20）；本票只负责携带。
func (s *Stream) sendTs() uint64 {
	d := time.Since(s.base)
	if d < 0 {
		return 0
	}
	return uint64(d.Microseconds())
}

// ---------- 发送侧 ----------

// Write 实现 net.Conn。数据先拷入待发队列（发送缓冲），发送泵再按
// 对端通告窗口装帧下发——Write 返回只表示数据已进入发送缓冲，
// 不表示对端已收（与 TCP send buffer 语义同构）。
// 发送缓冲（待发 + 在途未确认）占满时阻塞，直至窗口推进或出错。
func (s *Stream) Write(b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		s.mu.Lock()
		if s.werr != nil {
			err := s.werr
			s.mu.Unlock()
			return total, err
		}
		if s.closed {
			s.mu.Unlock()
			return total, net.ErrClosed
		}
		outstanding := s.pendingBytes + int(s.sentOff-s.cumAcked)
		if room := s.cfg.sendBufCap - outstanding; room > 0 {
			n := min(room, len(b))
			cp := make([]byte, n)
			copy(cp, b[:n])
			s.sendQ = append(s.sendQ, cp)
			s.pendingBytes += n
			b = b[n:]
			total += n
			s.mu.Unlock()
			s.wakeSend()
			continue
		}
		ch := s.wChange
		s.mu.Unlock()
		if err := s.wait(ch, &s.wDeadline); err != nil {
			return total, err
		}
	}
	return total, nil
}

// sendLoop 发送泵：所有 DATA/ACK 帧写出都经此 goroutine，天然串行
// 无交错。建流即通告初始窗口（对端收到前不发 DATA，防止超发被对端
// 重排缓冲丢弃）。
func (s *Stream) sendLoop() {
	s.sendAck()
	for {
		select {
		case <-s.done:
			return
		case <-s.ackCh:
			s.sendAck()
		case <-s.sendWake:
			for s.trySend() {
			}
		}
	}
}

// pickPathLocked 轮询选一条存活路径。调度器接缝（规格书 §5.1）：
// #20 将替换为「最短排空时间优先」——按 (queued+len)/est_rate 选路，
// 此处帧编码与写路径不变。调用方须持有 mu。
func (s *Stream) pickPathLocked() *path {
	n := len(s.paths)
	if n == 0 {
		return nil
	}
	p := s.paths[int(s.rrNext%uint64(n))]
	s.rrNext++
	return p
}

// firstResendLocked 返回首个「路径已死、待换路重发」的 unacked 段
// 下标，无则 -1。线性扫描：段数受窗口/帧长上界约束（数百量级），
// 代价可忽略；精细化的重传调度（机会重发/软失效/RTO）属 #23。
// 调用方须持有 mu。
func (s *Stream) firstResendLocked() int {
	for i := range s.unackedSegs {
		if s.unackedSegs[i].path == nil {
			return i
		}
	}
	return -1
}

// trySend 发一帧 DATA；返回 false 表示暂无可发。
// 优先级：死路径遗留段重发 > 新数据。重发不受窗口余量约束
// （理由见下）；新数据仍须 unacked < 对端通告窗口。
func (s *Stream) trySend() bool {
	s.mu.Lock()
	if s.closed || s.werr != nil || !s.gotWindow {
		s.mu.Unlock()
		return false
	}
	p := s.pickPathLocked()
	if p == nil {
		s.mu.Unlock()
		return false
	}
	var off uint64
	var payload []byte
	if i := s.firstResendLocked(); i >= 0 {
		// 死路径遗留段优先重发，且豁免窗口检查：这些字节区间本就
		// 计入 unacked（窗口账本之内），对端要么已收（重排缓冲按
		// 偏移去重丢弃）、要么正是它等不到的那块。若按窗口卡死，
		// 「数据已送达但 ACK 随死路径丢失」会让发送端视角的窗口
		// 被僵尸在途量占满——不重发即死锁。
		rs := &s.unackedSegs[i]
		n := int(min(uint64(len(rs.data)), uint64(maxFramePayload)))
		off = rs.off
		payload = rs.data[:n]
		if n == len(rs.data) {
			rs.path = p // 整段挂到新在途路径
		} else {
			// 段超单帧上限：已发部分立独立条目记在 p 名下，余量
			// 留在原位（path 仍 nil）等下一轮——否则 p 再死时已发
			// 部分无人记账，字节区间会永久失联
			sent := sendSeg{off: rs.off, data: payload, path: p}
			rs.off += uint64(n)
			rs.data = rs.data[n:]
			s.unackedSegs = append(s.unackedSegs, sendSeg{})
			copy(s.unackedSegs[i+1:], s.unackedSegs[i:])
			s.unackedSegs[i] = sent
		}
	} else {
		// 新数据受窗口约束：unacked ≥ 对端通告窗口即停摆
		// （uint64 计算防对端通告巨大值溢出）。
		unacked := s.sentOff - s.cumAcked
		if s.pendingBytes == 0 || unacked >= s.advWindow {
			s.mu.Unlock()
			return false
		}
		room := s.advWindow - unacked
		n := int(min(uint64(len(s.sendQ[0])-s.qHead), room, uint64(maxFramePayload)))
		off = s.sentOff
		payload = s.sendQ[0][s.qHead : s.qHead+n : s.qHead+n]
		s.qHead += n
		if s.qHead == len(s.sendQ[0]) {
			s.sendQ[0] = nil
			s.sendQ = s.sendQ[1:]
			s.qHead = 0
		}
		s.sentOff += uint64(n)
		s.pendingBytes -= n
		s.unackedSegs = append(s.unackedSegs, sendSeg{off: off, data: payload, path: p})
	}
	ts := s.sendTs()
	s.mu.Unlock()

	buf := appendDataFrame(nil, off, ts, payload)
	p.wmu.Lock()
	_, err := p.conn.Write(buf)
	p.wmu.Unlock()
	if err != nil {
		// 该路径写失败 → 摘除（含其名下在途段改标待重发）→
		// 返回 true 让泵立刻换下一条路径重试本帧
		s.dropPath(p, err, true)
		return true
	}
	return true
}

// sendAck 编码并经存活路径发出一条 ACK（规格 §4.4：优先走低延迟
// 路径回送属 #20，本票复用轮询选路）：累积偏移 + SACK ranges +
// 时间戳回显 + 当前接收窗口。字段取值时刻为编码时刻，因此重复的
// 触发可安全合并（ackCh cap1 自然去抖）。
//
// 写失败时换下一条存活路径重试：ACK 是累积快照，重发无害；若选中
// 正在死亡的路径写完即丢（不重试则窗口重开通告可能永远到不了
// 对端——实测死锁），而级联重试深度有界（每失败一次少一条路径）。
func (s *Stream) sendAck() {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		cum := s.rbuf.cum()
		tsEcho := s.lastDataTs
		window := uint64(max(s.rbuf.window(), 0))
		ranges := s.rbuf.ranges(maxAckRanges)
		s.freedSinceAck = 0
		seq := s.ackSeq
		s.ackSeq++
		p := s.pickPathLocked()
		s.mu.Unlock()
		if p == nil {
			return // 无存活路径：放弃本次 ACK，后续触发再试
		}

		buf := appendAckFrame(nil, cum, tsEcho, window, seq, ranges)
		p.wmu.Lock()
		_, err := p.conn.Write(buf)
		p.wmu.Unlock()
		if err == nil {
			return
		}
		s.dropPath(p, err, true)
	}
}

// sendPathDrop 在任意存活路径上尽力发 PATH_DROP（规格 §4.1：
// 控制消息带内复用任意存活路径）。
func (s *Stream) sendPathDrop(id uint64) {
	body, err := proto.Marshal(&pb.PathDrop{PathId: id})
	if err != nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	p := s.pickPathLocked()
	s.mu.Unlock()
	if p == nil {
		return
	}
	buf := appendCtrlFrame(nil, framePathDrop, body)
	p.wmu.Lock()
	_, err = p.conn.Write(buf)
	p.wmu.Unlock()
	if err != nil {
		// 连 PATH_DROP 都写不出 → 该路径一并摘除（级联深度 ≤ 路径数）
		s.dropPath(p, err, true)
	}
}

// ---------- 路径集合管理 ----------

// dropPath 摘除一条路径：标记死亡（幂等）、移出路径集、把其名下
// 在途未确认段改标「待重发」、关闭底层子流与自有连接（直拨连接
// 不进 swarm 表，不关即泄漏）。notify 为真时在存活路径上尽力发
// PATH_DROP 告知对端摘除其对端视角的对应路径。若已无存活路径，
// 聚合流转入终态。
func (s *Stream) dropPath(p *path, cause error, notify bool) {
	if !p.dead.CompareAndSwap(false, true) {
		return
	}
	s.mu.Lock()
	for i, q := range s.paths {
		if q == p {
			s.paths = append(s.paths[:i], s.paths[i+1:]...)
			break
		}
	}
	delete(s.pathsByID, p.id)
	for i := range s.unackedSegs {
		if s.unackedSegs[i].path == p {
			s.unackedSegs[i].path = nil
		}
	}
	empty := len(s.paths) == 0
	fin := s.finRecv
	s.mu.Unlock()

	_ = p.conn.Close()
	if p.own != nil {
		_ = p.own.Close()
	}
	if notify {
		s.sendPathDrop(p.id)
	}
	if empty {
		if cause == nil {
			cause = errNoPaths
		}
		if fin {
			// 对端发过 FIN：读侧保持干净 EOF 语义
			s.mu.Lock()
			if s.rerr == nil {
				s.rerr = io.EOF
			}
			s.mu.Unlock()
		}
		s.shutdown(cause)
		return
	}
	// 路径死亡可能带走了在途 ACK（窗口/cum 快照随路径丢失），
	// 在存活路径上补一条最新通告，防对端窗口视图永久过期。
	s.triggerAck()
	s.wakeSend() // 唤醒发送泵把待重发段发到剩余路径
}

// handlePathDrop 处理对端发来的 PATH_DROP：摘除本侧对应路径，
// 其在途未确认段照常重注入剩余路径（对端可能并未收到）。幂等。
func (s *Stream) handlePathDrop(body []byte) {
	var pd pb.PathDrop
	if err := proto.Unmarshal(body, &pd); err != nil {
		return // 帧体损坏：忽略（该路径仍可用，不致于因此 teardown）
	}
	s.mu.Lock()
	q := s.pathsByID[pd.GetPathId()]
	s.mu.Unlock()
	if q != nil {
		s.dropPath(q, nil, false) // 对端已知，无需再通知
	}
}

// reserveRemotePathID 校验并预定一个对端命名空间的 path_id：
// 奇偶位须落在对端命名空间，且该 id 不曾被使用（含已死路径——
// 规格 §4.3：死亡路径重加须换新 id）。
func (s *Stream) reserveRemotePathID(id uint64) bool {
	if id&1 != s.remoteParity {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if _, dup := s.seenRemote[id]; dup {
		return false
	}
	s.seenRemote[id] = struct{}{}
	return true
}

// ---------- 接收侧 ----------

// recvLoop 是单条路径的接收循环：逐帧解码分发，多条路径共用同一
// 重排缓冲（规格 §6：多路径乱序到达、按字节偏移保序）。
// DATA → 重排缓冲 + 回 ACK；ACK → 推进 cum、更新窗口；
// PATH_DROP → 摘除对应路径；FIN → 标记对端关闭并摘除本路径；
// RST → teardown；读出错/EOF → 摘除本路径，仍有存活路径则流不中断。
func (s *Stream) recvLoop(p *path, fr *frameReader) {
	for {
		ft, f, err := fr.next()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return // 流已关：路径随 Close/shutdown 统一收场
			}
			// 传输层 EOF = 对端关断该路径；其它错误 = 传输/帧违例。
			// 两者都只摘除本路径；是否终态由剩余路径数决定。
			s.dropPath(p, err, true)
			return
		}
		switch ft {
		case frameData:
			s.mu.Lock()
			s.rbuf.add(f.off, f.payload)
			s.lastDataTs = f.sendTs
			// 纵深防御：诚实对端受窗口背压约束，size 恒 ≤ cap；
			// 补洞段豁免容量检查意味着恶意对端可无视窗口灌有序
			// 数据——占用超过 4×cap 即视为协议违例，直接 teardown
			// 保内存。阈值取 4 倍留足瞬态余量（永不误伤诚实流）。
			overflow := s.rbuf.sizeBytes() > 4*s.rbuf.cap
			s.broadcastLocked(&s.rChange)
			s.mu.Unlock()
			if overflow {
				s.shutdown(fmt.Errorf("netacc: 对端超发，重排缓冲 %d 超过 4 倍容量上限", s.rbuf.sizeBytes()))
				return
			}
			// ACK 触发策略：每帧即回——实现最简单，且窗口值在编码
			// 时刻取最新，天然携带准确的 cum/窗口通告；ACK 合并
			// （隔帧/定时聚合）是明确的后续优化点。
			s.triggerAck()
		case frameAck:
			s.mu.Lock()
			// 多路径下 ACK 跨路径超车：生成于更早时刻的旧 ACK 可能
			// 晚到，若照单全收会把 advWindow 覆盖回过期值（实测死锁：
			// 窗口已重开 16KiB 的 ACK 先到、旧的 win=0 后到 → 发送端
			// 永久停摆）。按发送端 seq 只应用递增的最新快照。
			if s.gotPeerAck && f.seq <= s.peerAckSeq {
				s.mu.Unlock()
				continue
			}
			s.peerAckSeq = f.seq
			s.gotPeerAck = true
			// 防御：对端不应确认未发送的字节；谎报时收敛到已发边界，
			// 防 sentOff-cumAcked 下溢成天文数字
			if f.cum > s.sentOff {
				f.cum = s.sentOff
			}
			if f.cum > s.cumAcked {
				s.cumAcked = f.cum
				s.freeAckedLocked()
			}
			s.advWindow = f.window
			s.gotWindow = true
			// f.ranges（SACK 乱序区间）留给 #23 的重传/换路决策。
			s.broadcastLocked(&s.wChange)
			s.mu.Unlock()
			s.wakeSend()
		case framePathDrop:
			s.handlePathDrop(f.body)
		case frameFin:
			// FIN 属全流语义：只记「对端已正常关闭」，EOF 待全部
			// 路径收场后统一落地——多路径下其它路径上可能还有
			// 在途数据，提前 EOF 会截断尾包。
			s.mu.Lock()
			s.finRecv = true
			s.broadcastLocked(&s.rChange)
			s.mu.Unlock()
			s.dropPath(p, io.EOF, false)
			return
		case frameRst:
			s.shutdown(errReset)
			return
		default:
			// PATH_ATTACH 只应出现在路径绑定子流首帧（聚合流上
			// 收到即忽略）；PATH_REQUEST/PATH_READY 属 #21，
			// PING/TELEMETRY 属 #20。
		}
	}
}

// freeAckedLocked 释放 cum 之前的发送缓冲（unackedSegs）。
// cum 落在段中间时截段头（防御；正常情况下 cum 总落在帧边界上）。
// 调用方须持有 mu。
func (s *Stream) freeAckedLocked() {
	for len(s.unackedSegs) > 0 {
		f := &s.unackedSegs[0]
		end := f.off + uint64(len(f.data))
		if end <= s.cumAcked {
			s.unackedSegs[0] = sendSeg{}
			s.unackedSegs = s.unackedSegs[1:]
			continue
		}
		if f.off < s.cumAcked {
			f.data = f.data[s.cumAcked-f.off:]
			f.off = s.cumAcked
		}
		break
	}
	if len(s.unackedSegs) == 0 {
		s.unackedSegs = nil
	}
}

// ---------- net.Conn 读/关闭/deadline ----------

// Read 实现 net.Conn：从重排缓冲取已保序字节；无数据时阻塞至
// 数据到达 / 对端关闭（EOF）/ deadline 到期（net.Error timeout）。
func (s *Stream) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if n := s.rbuf.avail(); n > 0 {
			m := s.rbuf.read(b)
			// 窗口释放通告：原窗口为 0（发送端可能已停摆）时立即补
			// ACK；否则累计释放 ≥ 容量 1/4 再发，给慢读应用做去抖。
			prevWindow := s.rbuf.window() - m
			s.freedSinceAck += m
			needAck := prevWindow <= 0 || s.freedSinceAck >= s.rbuf.cap/4
			s.mu.Unlock()
			if needAck {
				s.triggerAck()
			}
			return m, nil
		}
		if s.rerr != nil {
			err := s.rerr
			s.mu.Unlock()
			return 0, err
		}
		if s.closed {
			s.mu.Unlock()
			return 0, net.ErrClosed
		}
		ch := s.rChange
		s.mu.Unlock()
		if err := s.wait(ch, &s.rDeadline); err != nil {
			return 0, err
		}
	}
}

// Close 实现 net.Conn：在全部存活路径上尽力发 FIN 告知对端
// （对端读尽残余数据后得到 EOF），随后关闭全部底层路径并唤醒
// 全部阻塞中的 Read/Write。
//
// 顺序细节：先给每条路径写设一个短 deadline 再拿 wmu 写 FIN——
// 若发送泵正阻塞在 path.Write（对端不再读的同步传输），路径级
// deadline 能把它踢出、释放 wmu，同时给 FIN 写自身兜底；
// 最坏情况只是 FIN 发不出去，底层 Close 同样向对端传达 EOF。
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		paths := append([]*path(nil), s.paths...)
		s.mu.Unlock()

		fin := appendCtrlFrame(nil, frameFin, nil)
		deadline := time.Now().Add(500 * time.Millisecond)
		for _, p := range paths {
			p.dead.Store(true)
			_ = p.conn.SetWriteDeadline(deadline)
			p.wmu.Lock()
			_, _ = p.conn.Write(fin) // 尽力而为；写不出时底层关闭同样传达 EOF
			p.wmu.Unlock()
			_ = p.conn.Close()
			if p.own != nil {
				_ = p.own.Close()
			}
		}

		close(s.done)
		s.mu.Lock()
		s.broadcastLocked(&s.rChange)
		s.broadcastLocked(&s.wChange)
		s.mu.Unlock()
		s.deregister()
	})
	return nil
}

// shutdown 异常终态（全部路径失活 / RST / 帧格式违例）：
// 记录读写错误、关全部路径、唤醒全部阻塞者。幂等。
func (s *Stream) shutdown(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		if s.rerr == nil {
			s.rerr = err
		}
		if s.werr == nil {
			s.werr = err
		}
		paths := append([]*path(nil), s.paths...)
		s.mu.Unlock()

		for _, p := range paths {
			p.dead.Store(true)
			_ = p.conn.Close()
			if p.own != nil {
				_ = p.own.Close()
			}
		}

		close(s.done)
		s.mu.Lock()
		s.broadcastLocked(&s.rChange)
		s.broadcastLocked(&s.wChange)
		s.mu.Unlock()
		s.deregister()
	})
}

// deregister 把本流从 Aggregator 的 PATH_ATTACH 路由表摘除：
// 此后对端再持本流 agg_stream_id 发 PATH_ATTACH 会被 reset。
func (s *Stream) deregister() {
	if s.agg != nil {
		s.agg.unregister(s.id, s)
	}
}

// LocalAddr 返回本端 multiaddr（取首条存活路径的包成 net.Addr，
// 见 maAddr）；无路径时为 nil。
func (s *Stream) LocalAddr() net.Addr {
	if p := s.firstPath(); p != nil {
		return p.localAddr()
	}
	return nil
}

// RemoteAddr 返回对端 multiaddr，同上。
func (s *Stream) RemoteAddr() net.Addr {
	if p := s.firstPath(); p != nil {
		return p.remoteAddr()
	}
	return nil
}

func (s *Stream) firstPath() *path {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.paths) == 0 {
		return nil
	}
	return s.paths[0]
}

func (s *Stream) SetDeadline(t time.Time) error {
	s.rDeadline.Store(deadlineNano(t))
	s.wDeadline.Store(deadlineNano(t))
	s.mu.Lock()
	s.broadcastLocked(&s.rChange)
	s.broadcastLocked(&s.wChange)
	s.mu.Unlock()
	return nil
}

func (s *Stream) SetReadDeadline(t time.Time) error {
	s.rDeadline.Store(deadlineNano(t))
	s.mu.Lock()
	s.broadcastLocked(&s.rChange)
	s.mu.Unlock()
	return nil
}

func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.wDeadline.Store(deadlineNano(t))
	s.mu.Lock()
	s.broadcastLocked(&s.wChange)
	s.mu.Unlock()
	return nil
}

// deadlineNano 把 deadline 转成 unixnano；零值表示无 deadline。
func deadlineNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// ---------- 内部同步原语 ----------

// broadcastLocked 广播一次状态变化：关闭旧 chan 换新的，
// 所有正在等待的 goroutine 被唤醒后重新检查条件。调用方须持有 mu。
func (s *Stream) broadcastLocked(ch *chan struct{}) {
	close(*ch)
	*ch = make(chan struct{})
}

// wakeSend 通知发送泵重新评估发送条件（cap1 去抖，丢失安全——
// 泵内按条件循环而非按信号计数）。
func (s *Stream) wakeSend() {
	select {
	case s.sendWake <- struct{}{}:
	default:
	}
}

// triggerAck 请求发送泵补一条 ACK（cap1 去抖；ACK 在编码时刻取
// 最新 cum/窗口/ranges，合并触发不丢信息）。
func (s *Stream) triggerAck() {
	select {
	case s.ackCh <- struct{}{}:
	default:
	}
}

// wait 在状态 chan / done / deadline 三者上等待。
// 返回 nil 表示被唤醒（调用方重新检查条件），非 nil 为终态错误。
// deadline 可能被 SetXDeadline 并发推后：定时器触发时若新 deadline
// 尚未到期则返回 nil 让外层用新值再等一轮。
func (s *Stream) wait(ch <-chan struct{}, dl *atomic.Int64) error {
	d := dl.Load()
	var tc <-chan time.Time
	var timer *time.Timer
	if d > 0 {
		dd := time.Until(time.Unix(0, d))
		if dd <= 0 {
			return errTimeout
		}
		timer = time.NewTimer(dd)
		defer timer.Stop()
		tc = timer.C
	}
	select {
	case <-ch:
		return nil
	case <-s.done:
		// 不直接报错，回外层循环让调用方取到真正的终态
		// （本地关 → net.ErrClosed；远端异常 → 具体的 werr/rerr）
		return nil
	case <-tc:
		if nd := dl.Load(); nd == 0 || time.Now().UnixNano() < nd {
			// deadline 被推后或清除（nd==0 = 无 deadline），
			// 回外层循环用新值再等
			return nil
		}
		return errTimeout
	}
}

// maAddr 把 multiaddr 适配成 net.Addr：
// network.Stream 的地址是 multiaddr 而非 net.Addr，net.Conn 接口需要后者。
type maAddr struct{ ma ma.Multiaddr }

func (a maAddr) Network() string { return "netacc" }

func (a maAddr) String() string {
	if a.ma == nil {
		return ""
	}
	return a.ma.String()
}
