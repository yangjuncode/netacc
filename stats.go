// 观测面（规格书 §8，issue #25）：Stats() 指标快照 + Subscribe 事件订阅。
//
// 本文件只提供「快照数据 + 事件通知」两类原语，不内置 prometheus 等
// 具体指标库依赖——接入方拿快照自行对接自有观测体系（决策票 #12）。
package netacc

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PathStats 是一条数据路径的指标快照：PathInfo 的字段（标识/传输/
// 地址 + SRTT/MinRTT/EstRate/Inflight）之外，附收发与重传累计计数。
// 全部为快照时刻的只读拷贝，无样本的指标为零值。
type PathStats struct {
	PathInfo // 内嵌：ID/Dialed/ConnID/Transport/Local/Remote + SRTT/MinRTT/EstRate/Inflight

	Delivered   uint64  // 本路径累计被对端累积确认的字节数（delivery-rate 采样的账本）
	RxBytes     uint64  // 本路径累计到达字节数（本端收端视角，TELEMETRY 回报的数据源）
	RxRateBps   float64 // 本路径收端到达速率估计（字节/秒，rxRate 平滑值）
	ResentSegs  uint64  // 在本路径上重发出的段（帧）数——含死路径遗留段的换路重发
	ResentBytes uint64  // 在本路径上重发出的字节数
}

// StreamStats 是聚合流的整体指标快照（规格书 §8 Stats()：逐路径
// RTT/est_rate/inflight/重传计数 + 聚合吞吐）。纯快照语义：调用时
// 在 mu 下一次性拷贝，返回后与原流无共享状态，可安全长期持有。
type StreamStats struct {
	ID   [16]byte // agg_stream_id（握手协商的聚合流标识）
	Peer peer.ID  // 对端 peer（pipe 测试流为空）

	// Paths 是逐路径快照（存活路径全集；已死路径不在其中，
	// 其造成的重传量计入下方 ResentSegs/ResentBytes 聚合值）。
	Paths []PathStats

	// ---- 聚合吞吐（字节/秒）----
	// TxRateBps 发送方向 = Σ 各路径有效 est_rate（TELEMETRY 校准后），
	// 即调度器认为本流当前的可用上行容量；RxRateBps 接收方向 =
	// Σ 各路径收端实测到达速率。冷启动/全过期时为 0。
	TxRateBps float64
	RxRateBps float64

	// ---- 发送账本 ----
	SentBytes    uint64 // 已装帧下发的逻辑字节数（流内偏移总量，重发不重复计）
	AckedBytes   uint64 // 对端已累积确认的字节数
	Inflight     int    // 当前在途未确认字节总量（Σ 各路径 inflight）
	PendingBytes int    // 待发队列字节数（应用已写、尚未装帧）
	SendBufCap   int    // 发送缓冲上限（待发+在途总量的硬顶）

	// ---- 重传统计（全流口径，含已死路径造成的那部分）----
	// LostSegs 是因路径死亡被改判「待换路重发」的段数（重传诱因）；
	// ResentSegs/ResentBytes 是实际重发出的段数/字节数（= Σ 各存活
	// 路径的重发计数 + 已死路径生前承担的）。段数按发出的帧计——
	// 超长段拆分重发时每一帧计一次。
	LostSegs    uint64
	ResentSegs  uint64
	ResentBytes uint64

	// ---- 接收侧缓冲（重排缓冲 = 连接级接收窗口，规格书 §6）----
	RecvCum      uint64 // 已累计保序到达的字节数（累积 ACK 点）
	RecvBufCap   int    // 重排缓冲当前容量（随 Σ到达速率·max_srtt 联动）
	RecvBuffered int    // 重排缓冲占用字节数（乱序暂存 + 已保序未读）
	RecvReady    int    // 其中已保序、可立即被 Read 取走的字节数
	RecvDropped  int    // 因超容量被丢弃的字节数（对端超发的防御性统计）
}

// Stats 返回聚合流的指标快照（规格书 §8）。逐路径字段见 PathStats，
// 聚合吞吐与缓冲占用见 StreamStats；无锁泄漏——快照在锁内一次性
// 拷贝完成即返回，调用方持有的是纯数据。
func (s *Stream) Stats() StreamStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	st := StreamStats{
		ID:           s.id,
		Peer:         s.peer,
		Paths:        make([]PathStats, 0, len(s.paths)),
		SentBytes:    s.sentOff,
		AckedBytes:   s.cumAcked,
		PendingBytes: s.pendingBytes,
		SendBufCap:   s.cfg.sendBufCap,
		LostSegs:     s.lostSegs,
		ResentSegs:   s.resentSegs,
		ResentBytes:  s.resentBytes,
		RecvCum:      s.rbuf.cum(),
		RecvBufCap:   s.rbuf.cap,
		RecvBuffered: s.rbuf.sizeBytes(),
		RecvReady:    s.rbuf.avail(),
		RecvDropped:  s.rbuf.dropped,
	}
	for _, p := range s.paths {
		st.Inflight += p.inflight
		st.TxRateBps += p.effRate(now)
		st.RxRateBps += p.rxRate
		st.Paths = append(st.Paths, PathStats{
			PathInfo:    p.info(now),
			Delivered:   p.delivered,
			RxBytes:     p.rxBytes,
			RxRateBps:   p.rxRate,
			ResentSegs:  p.resentSegs,
			ResentBytes: p.resentBytes,
		})
	}
	return st
}

// ---------- 事件订阅（规格书 §8：路径增删/降级通知） ----------

// EventType 标识聚合流事件类型。
type EventType int

const (
	// EventPathAdded：一条新数据路径挂接进聚合流（对端发起的
	// PATH_ATTACH 与本侧 AddPath/AddRelayPath 都会触发）。
	// 建流即存在的首条握手路径不产生本事件——订阅不可能早于建流。
	EventPathAdded EventType = iota + 1
	// EventPathRemoved：一条数据路径被摘除。Cause 给出摘除原因：
	// 底层传输错误/EOF（路径死亡）、nil（本侧 RemovePath 或对端
	// PATH_DROP 的主动摘除）。事件在在途段改判重发之后发出。
	EventPathRemoved
	// EventPathDegraded：路径「疑似降级」软信号（规格书 §5.2）——
	// 指标曾经有效（有 est_rate 或 RTT 样本）但在有发送需求期间
	// 失联（est_rate 超保鲜期 / RTT 样本陈旧），与主动 PING 探测
	// 共用同一判据。每条路径每次失联只报一次（沿触发，不刷屏）；
	// 指标恢复后可再次触发。路径死亡走 EventPathRemoved，与本事件
	// 互补：降级报「还活着但测不动了」，移除报「没了」。
	EventPathDegraded
)

// String 返回事件类型名（日志/断言用）。
func (t EventType) String() string {
	switch t {
	case EventPathAdded:
		return "path_added"
	case EventPathRemoved:
		return "path_removed"
	case EventPathDegraded:
		return "path_degraded"
	default:
		return "unknown"
	}
}

// Event 是一条聚合流事件。
type Event struct {
	Type   EventType // 事件类型
	PathID uint64    // 涉事路径的 path_id
	Info   PathInfo  // 事发瞬间的路径快照（Removed 为摘除前最后一眼）
	Cause  error     // Removed：摘除原因（主动摘除为 nil）；其余类型恒 nil
	At     time.Time // 事发时刻

	// Dropped 是本订阅者在本事件之前被丢弃的事件数（通道满丢策略下
	// 的漏报计数——订阅者据此知道自己错过了多少，需要完整历史时
	// 应加大 Subscribe 缓冲或改用 Stats() 补快照）。
	Dropped uint64
}

// subscription 是一个活跃订阅：带缓冲事件通道 + 该订阅者的丢弃计数。
type subscription struct {
	ch      chan Event
	dropped uint64
}

// Subscribe 订阅本聚合流的路径事件（规格书 §8 可选事件订阅）：
// 返回只读事件通道与退订函数；退订或流终结（Close/shutdown）时
// 通道被关闭，range 可正常收尾。
//
// bufLen 是事件通道缓冲长度，≤0 时取默认 16。慢订阅者不会阻塞
// 数据面：通道满即丢弃该订阅者的本事件（其它订阅者不受影响），
// 下一条成功投递的事件经 Event.Dropped 携带期间被丢弃的数量。
// 缓冲耗尽丢事件是本订阅语义的既定取舍——订阅只作通知用途，
// 完整指标以 Stats() 快照为准。
func (s *Stream) Subscribe(bufLen int) (<-chan Event, func()) {
	if bufLen <= 0 {
		bufLen = 16
	}
	sub := &subscription{ch: make(chan Event, bufLen)}
	s.subMu.Lock()
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		// 已终结的流：返回立即关闭的通道与空退订，调用方无特判
		s.subMu.Unlock()
		close(sub.ch)
		return sub.ch, func() {}
	}
	if s.subs == nil {
		s.subs = make(map[*subscription]struct{})
	}
	s.subs[sub] = struct{}{}
	s.subMu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			s.subMu.Lock()
			if _, ok := s.subs[sub]; ok {
				delete(s.subs, sub)
				close(sub.ch)
			}
			s.subMu.Unlock()
		})
	}
	return sub.ch, unsubscribe
}

// emitEvent 向全部订阅者非阻塞派发一条事件。须在无 mu 持有的
// 上下文调用（各埋点把信息收集好再调），内部只持 subMu。
// info/cause 由调用方备好（PathInfo 须在 mu 下采集，见 pathInfoOf）。
func (s *Stream) emitEvent(t EventType, info PathInfo, cause error) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	if len(s.subs) == 0 {
		return
	}
	ev := Event{Type: t, PathID: info.ID, Info: info, Cause: cause, At: time.Now()}
	for sub := range s.subs {
		ev.Dropped = sub.dropped
		select {
		case sub.ch <- ev:
			sub.dropped = 0
		default:
			sub.dropped++
		}
	}
}

// closeSubs 在流终结时关闭全部订阅通道（订阅者 range 收尾）。
// 由 Close/shutdown 的 closeOnce 段调用。
func (s *Stream) closeSubs() {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for sub := range s.subs {
		close(sub.ch)
		delete(s.subs, sub)
	}
}

// pathInfoOf 取一条路径的当前快照（内部自拿 mu——指标字段归其
// 保护；emit 的调用方多在无锁上下文，故收敛到这一个入口）。
// 已死路径同样可调（字段仍可读，给出摘除前最后一眼）。
func (s *Stream) pathInfoOf(p *path) PathInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return p.info(time.Now())
}
