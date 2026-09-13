package netacc

import (
	"container/heap"
	"sort"
)

// seg 是一段带流内起始偏移的数据。
type seg struct {
	off  uint64
	data []byte
}

// segHeap 是按起始偏移排序的最小堆（container/heap）。
type segHeap []seg

func (h segHeap) Len() int           { return len(h) }
func (h segHeap) Less(i, j int) bool { return h[i].off < h[j].off }
func (h segHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *segHeap) Push(x any)        { *h = append(*h, x.(seg)) }
func (h *segHeap) Pop() any {
	old := *h
	n := len(old)
	s := old[n-1]
	old[n-1] = seg{} // 释放引用
	*h = old[:n-1]
	return s
}

// reorderBuf 是收端重排缓冲（规格书 §6）：
// 乱序到达的字节段先按偏移进最小堆暂存，队头缺口补齐后移入
// ready 队列供 Read 按序取走；size 计量堆+ready 全部占用，
// window = cap - size 即连接级接收窗口通告值。
//
// 本组件刻意不感知路径/网络，可独立单测（测试直接喂乱序帧）。
type reorderBuf struct {
	cap  int    // 容量上限（= clamp(Σest·maxRTT, min, max) 的结果）
	next uint64 // 已连续收到的下一字节偏移（累积 ACK 点）
	segs segHeap
	// ready 是已保序、待应用读走的段队列；队首段的 data 随消费前移。
	ready      []seg
	size       int // segs + ready 占用字节数
	readyBytes int // ready 中可读字节数
	dropped    int // 因超容量丢弃的字节数（防御性统计）
}

func newReorderBuf(cap int) *reorderBuf {
	return &reorderBuf{cap: cap}
}

// add 把一段到达数据放入缓冲。重复段去重、超容量段丢弃；
// 与 next 衔接的段链会被一次性移入 ready 队列。
func (r *reorderBuf) add(off uint64, data []byte) {
	if len(data) == 0 {
		return
	}
	end := off + uint64(len(data))
	if end <= r.next {
		return // 完全重复（重传），丢弃
	}
	if off < r.next {
		// 前缀已被交付：裁掉重复部分（跨路径重发的重叠段）
		data = data[r.next-off:]
		off = r.next
	}
	if off > r.next && r.size+len(data) > r.cap {
		// 乱序段超容量 → 丢段。窗口背压正常运作时不可达（发送端
		// unacked ≤ window 保证到达量不越界）；这是对端超发/
		// 恶意时的内存保护。被丢段不进 SACK ranges，发送端超时
		// 重传可恢复（#19）。
		// 注意补洞段（off==next）不受此限：它能推进交付、让
		// Read 释放空间；若连它也丢，乱序段占满缓冲后无数据
		// 可读，缓冲将永远无法腾空——死锁。
		r.dropped += len(data)
		return
	}
	heap.Push(&r.segs, seg{off: off, data: data})
	r.size += len(data)
	r.drain()
}

// drain 把堆顶与 next 衔接的段依次移入 ready，推进累积偏移。
func (r *reorderBuf) drain() {
	for len(r.segs) > 0 && r.segs[0].off <= r.next {
		s := heap.Pop(&r.segs).(seg)
		if s.off < r.next {
			// 前缀与已交付区间重叠：裁剪，并把入堆时已计入 size
			// 的重叠字节核销，否则窗口会因重复记账越磨越小
			trim := r.next - s.off
			s.data = s.data[trim:]
			s.off = r.next
			r.size -= int(trim)
		}
		if len(s.data) == 0 {
			continue // 整段落在已交付区间内（被其它乱序段覆盖）
		}
		r.next += uint64(len(s.data))
		r.ready = append(r.ready, s)
		r.readyBytes += len(s.data)
	}
}

// read 从 ready 队列拷贝最多 len(b) 字节，返回实际读到的字节数。
func (r *reorderBuf) read(b []byte) int {
	n := 0
	for n < len(b) && len(r.ready) > 0 {
		m := copy(b[n:], r.ready[0].data)
		n += m
		r.ready[0].data = r.ready[0].data[m:]
		if len(r.ready[0].data) == 0 {
			r.ready[0] = seg{} // 释放底层数组引用
			r.ready = r.ready[1:]
		}
	}
	if len(r.ready) == 0 {
		r.ready = nil // 清空时释放底层数组
	}
	r.size -= n
	r.readyBytes -= n
	return n
}

// avail 返回已保序、可立即读走的字节数。
func (r *reorderBuf) avail() int { return r.readyBytes }

// cum 返回累积确认偏移（下一期望字节）。
func (r *reorderBuf) cum() uint64 { return r.next }

// window 返回接收窗口通告值 = 缓冲剩余量。
func (r *reorderBuf) window() int { return r.cap - r.size }

// setCap 调整容量上限（规格 §6 窗口联动：随 Σest_rate·max_srtt 重算）。
// 缩小容量不丢弃已缓冲数据——window 变负后按 0 通告，等 Read 排空。
func (r *reorderBuf) setCap(c int) { r.cap = c }

// sizeBytes 返回当前占用字节数（含未保序暂存与未读走部分）。
func (r *reorderBuf) sizeBytes() int { return r.size }

// ranges 把乱序暂存段导出为合并后的升序 SACK 区间，最多 max 条。
func (r *reorderBuf) ranges(max int) []byteRange {
	if len(r.segs) == 0 || max <= 0 {
		return nil
	}
	rs := make([]byteRange, 0, len(r.segs))
	for _, s := range r.segs {
		rs = append(rs, byteRange{s.off, s.off + uint64(len(s.data))})
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].start < rs[j].start })
	// 合并重叠/相邻区间
	merged := rs[:0]
	for _, cur := range rs {
		if len(merged) > 0 && cur.start <= merged[len(merged)-1].end {
			if cur.end > merged[len(merged)-1].end {
				merged[len(merged)-1].end = cur.end
			}
			continue
		}
		merged = append(merged, cur)
	}
	if len(merged) > max {
		merged = merged[:max]
	}
	return merged
}
