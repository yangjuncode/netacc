package netacc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
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

// timeoutError 实现 net.Error，SetReadDeadline/SetWriteDeadline 到期返回。
type timeoutError struct{}

func (timeoutError) Error() string   { return "netacc: i/o 超时" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var errTimeout error = timeoutError{}

// pathConn 是一条数据路径的最小抽象：底层保序可靠字节流。
// 生产环境为 network.Stream；测试注入 net.Pipe 端点。
type pathConn interface {
	io.Reader
	io.Writer
	io.Closer
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
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
// 本票骨架只有一条路径，逐路径 est_rate/srtt 采样属 #20；
// 估计量暂恒为 0 → 容量恒取下界 minBuf。多路径接入后在此公式
// 内填入各路径实测值并按需重算、向对端发窗口更新即可扩容。
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
//   - 写方向：Write → 待发队列 → 发送泵按对端通告窗口切成 DATA 帧
//     下发（帧头携带流内字节偏移与发送时间戳）；
//   - 读方向：接收循环解帧 → 重排缓冲保序 → Read 取走；
//     每收一帧 DATA 回一条 ACK（累积偏移 + SACK ranges + 时间戳
//     回显 + 窗口通告），Read 释放缓冲时也按需回窗口更新；
//   - 流控：单一聚合窗口——发送端维护 unacked（已发未累积确认），
//     unacked ≥ 对端通告窗口时发送泵停摆，进而 Write 在发送缓冲
//     占满后阻塞，直到对端 Read 释放窗口。
//
// 并发结构：一个 sendLoop goroutine 独占所有帧写出（DATA 与 ACK
// 在其内串行编码，天然不交错），一个 recvLoop goroutine 解帧；
// 应用侧 Read/Write 经 mu + chan 广播与两者同步，deadline 用
// atomic + timer 实现，不依赖底层流的 SetDeadline（应用 deadline
// 只约束本层阻塞，不传导到路径写，避免误触发 teardown）。
//
// 当前为单路径骨架：握手流升格为唯一数据路径；多路径接入后
// sendLoop 内的选路点换成调度器（最短排空时间优先），recvLoop
// 按路径起多个实例共用同一重排缓冲。
type Stream struct {
	id   [16]byte
	path pathConn
	cfg  streamConfig
	base time.Time // 发送时间戳基准：ts = 相对 base 的微秒数（含单调钟）

	mu   sync.Mutex
	rbuf reorderBuf

	// 发送侧（mu 保护）
	sendQ        [][]byte // 待发队列（应用已写、尚未装帧）
	qHead        int      // sendQ[0] 内已消费的偏移
	pendingBytes int      // 待发队列总字节
	sentOff      uint64   // 下一待分配流内偏移 = 已装帧字节总数
	cumAcked     uint64   // 对端累积确认偏移
	unackedSegs  []seg    // 已发未确认帧，保留至连接级 ACK（#19 换路径重发用）
	advWindow    uint64   // 对端最新通告的接收窗口（重排缓冲剩余量）
	gotWindow    bool     // 是否已收到首个窗口通告

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

	wmu sync.Mutex // 底层路径写串行化（发送泵与 Close 的 FIN 共用）

	rDeadline atomic.Int64 // unixnano，0 = 无 deadline
	wDeadline atomic.Int64
}

var _ net.Conn = (*Stream)(nil)

func newStream(id [16]byte, s network.Stream, cfg streamConfig) *Stream {
	return newStreamOnPath(id, s, cfg)
}

// newStreamOnPath 在给定路径上启动数据面（发送泵 + 接收循环）。
func newStreamOnPath(id [16]byte, p pathConn, cfg streamConfig) *Stream {
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
		id:       id,
		path:     p,
		cfg:      cfg,
		base:     time.Now(),
		ackCh:    make(chan struct{}, 1),
		sendWake: make(chan struct{}, 1),
		done:     make(chan struct{}),
		rChange:  make(chan struct{}),
		wChange:  make(chan struct{}),
	}
	s.rbuf = *newReorderBuf(cfg.windowCap())
	go s.recvLoop()
	go s.sendLoop()
	return s
}

// ID 返回握手协商出的 128bit agg_stream_id（聚合流标识）。
// 后续 PATH_ATTACH 凭此 ID 把新路径绑定到本流（规格书 §4.2）。
func (s *Stream) ID() [16]byte { return s.id }

// Peer 返回聚合流对端的 peer.ID。
func (s *Stream) Peer() peer.ID {
	if ns, ok := s.path.(network.Stream); ok {
		return ns.Conn().RemotePeer()
	}
	return ""
}

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

// sendLoop 发送泵：所有帧写出都经此 goroutine，天然串行无交错。
// 建流即通告初始窗口（对端收到前不发 DATA，防止超发被对端
// 重排缓冲丢弃——本票尚无重传机制可恢复）。
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

// trySend 在窗口允许时发一帧 DATA；返回 false 表示暂无可发。
// 发送条件：已拿到对端窗口通告 && 待发队列非空 && unacked < window。
func (s *Stream) trySend() bool {
	s.mu.Lock()
	if s.closed || s.werr != nil {
		s.mu.Unlock()
		return false
	}
	unacked := s.sentOff - s.cumAcked
	if !s.gotWindow || s.pendingBytes == 0 || unacked >= s.advWindow {
		s.mu.Unlock()
		return false
	}
	// 本帧可发字节数：受剩余窗口与帧载荷上限双重约束。
	// 用 uint64 计算窗口余量再转 int：对端通告值不可信，
	// 直接 int() 巨大窗口会溢出为负数。
	n64 := min(uint64(len(s.sendQ[0])-s.qHead), s.advWindow-unacked, uint64(maxFramePayload))
	n := int(n64)
	off := s.sentOff
	payload := s.sendQ[0][s.qHead : s.qHead+n : s.qHead+n]
	s.qHead += n
	if s.qHead == len(s.sendQ[0]) {
		s.sendQ[0] = nil
		s.sendQ = s.sendQ[1:]
		s.qHead = 0
	}
	s.sentOff += uint64(n)
	s.pendingBytes -= n
	// 保留至连接级 ACK：路径失效时按原偏移换路径重发（#19）
	s.unackedSegs = append(s.unackedSegs, seg{off: off, data: payload})
	ts := s.sendTs()
	s.mu.Unlock()

	buf := appendDataFrame(nil, off, ts, payload)
	s.wmu.Lock()
	_, err := s.path.Write(buf)
	s.wmu.Unlock()
	if err != nil {
		s.shutdown(err)
		return false
	}
	return true
}

// sendAck 编码并发出一条 ACK：累积偏移 + SACK ranges + 时间戳回显
// + 当前接收窗口。字段取值时刻为编码时刻，因此重复的 ACK 触发可
// 安全合并（ackCh cap1 自然去抖）。
func (s *Stream) sendAck() {
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
	s.mu.Unlock()

	buf := appendAckFrame(nil, cum, tsEcho, window, ranges)
	s.wmu.Lock()
	_, err := s.path.Write(buf)
	s.wmu.Unlock()
	if err != nil {
		s.shutdown(err)
	}
}

// ---------- 接收侧 ----------

// recvLoop 接收循环：逐帧解码分发。
// DATA → 重排缓冲 + 回 ACK；ACK → 推进 cum、更新窗口；
// FIN → 读侧 EOF；RST → teardown；其余控制帧类型本票只解不定语
// 义（PATH_* 等留给 #19/#20），跳过帧体即可。
func (s *Stream) recvLoop() {
	fr := newFrameReader(s.path)
	for {
		ft, f, err := fr.next()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			if !closed && errors.Is(err, io.EOF) {
				// 传输层干净关闭 = 对端正常结束
				if s.rerr == nil {
					s.rerr = io.EOF
				}
				s.broadcastLocked(&s.rChange)
			}
			s.mu.Unlock()
			if !closed && !errors.Is(err, io.EOF) {
				// 传输错误 / 帧格式违例 → 全流 teardown，
				// 唤醒写侧并关路径
				s.shutdown(err)
			}
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
			// f.ranges（SACK 乱序区间）留给 #19 的重传/换路决策，
			// 本票单路径无重传，解析后不使用。
			s.broadcastLocked(&s.wChange)
			s.mu.Unlock()
			s.wakeSend()
		case frameFin:
			s.mu.Lock()
			if s.rerr == nil {
				s.rerr = io.EOF
			}
			s.broadcastLocked(&s.rChange)
			s.mu.Unlock()
			return
		case frameRst:
			s.shutdown(errReset)
			return
		default:
			// PATH_ATTACH/PATH_DROP/PATH_REQUEST/PATH_READY/PING/
			// TELEMETRY：帧体已被 frameReader 按 body_len 跳过，语义
			// 留给后续 issue。
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
			s.unackedSegs[0] = seg{}
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

// Close 实现 net.Conn：尽力发 FIN 告知对端（对端 Read 得到 EOF），
// 随后关闭底层路径并唤醒全部阻塞中的 Read/Write。
//
// 顺序细节：先给路径写设一个短 deadline 再拿 wmu 写 FIN——
// 若发送泵正阻塞在 path.Write（对端不再读的同步传输），路径级
// deadline 能把它踢出、释放 wmu，同时给 FIN 写自身兜底；
// 最坏情况只是 FIN 发不出去，底层 Close 同样向对端传达 EOF。
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()

		_ = s.path.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		buf := appendCtrlFrame(nil, frameFin, nil)
		s.wmu.Lock()
		_, _ = s.path.Write(buf) // 尽力而为；写不出时底层关闭同样传达 EOF
		s.wmu.Unlock()
		_ = s.path.Close()

		close(s.done)
		s.mu.Lock()
		s.broadcastLocked(&s.rChange)
		s.broadcastLocked(&s.wChange)
		s.mu.Unlock()
	})
	return nil
}

// shutdown 异常终态（路径写失败 / RST / 帧格式违例）：
// 记录读写错误、关路径、唤醒全部阻塞者。幂等。
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
		s.mu.Unlock()

		_ = s.path.Close()

		close(s.done)
		s.mu.Lock()
		s.broadcastLocked(&s.rChange)
		s.broadcastLocked(&s.wChange)
		s.mu.Unlock()
	})
}

// LocalAddr 返回本端 multiaddr（包成 net.Addr，见 maAddr）。
func (s *Stream) LocalAddr() net.Addr {
	if ns, ok := s.path.(network.Stream); ok {
		return maAddr{ns.Conn().LocalMultiaddr()}
	}
	if nc, ok := s.path.(net.Conn); ok {
		return nc.LocalAddr()
	}
	return nil
}

// RemoteAddr 返回对端 multiaddr（包成 net.Addr，见 maAddr）。
func (s *Stream) RemoteAddr() net.Addr {
	if ns, ok := s.path.(network.Stream); ok {
		return maAddr{ns.Conn().RemoteMultiaddr()}
	}
	if nc, ok := s.path.(net.Conn); ok {
		return nc.RemoteAddr()
	}
	return nil
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
