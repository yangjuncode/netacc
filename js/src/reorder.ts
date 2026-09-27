/**
 * 收端重排缓冲（规格书 §6）——reorder.go 的 TS 移植。
 *
 * 乱序到达的字节段按偏移进最小堆暂存，队头缺口补齐后移入 ready 队列
 * 供 read 按序取走；size 计量堆+ready 全部占用，window = cap - size
 * 即连接级接收窗口通告值。
 *
 * 本组件刻意不感知路径/网络，可独立单测。
 */

import type { ByteRange } from './frame.js'

/** seg 是一段带流内起始偏移的数据。 */
export interface Seg {
	off: number
	data: Uint8Array
}

/** 按起始偏移排序的二叉最小堆。 */
class SegHeap {
	private a: Seg[] = []

	get length(): number {
		return this.a.length
	}

	top(): Seg | undefined {
		return this.a[0]
	}

	push(s: Seg): void {
		const a = this.a
		a.push(s)
		let i = a.length - 1
		// 上浮：与父交换直到堆序恢复
		while (i > 0) {
			const p = (i - 1) >> 1
			if (a[p]!.off <= a[i]!.off) break
			const t = a[p]!
			a[p] = a[i]!
			a[i] = t
			i = p
		}
	}

	pop(): Seg | undefined {
		const a = this.a
		const top = a[0]
		const last = a.pop()
		if (a.length > 0 && last !== undefined) {
			a[0] = last
			let i = 0
			for (;;) {
				const l = i * 2 + 1
				const r = l + 1
				let m = i
				if (l < a.length && a[l]!.off < a[m]!.off) m = l
				if (r < a.length && a[r]!.off < a[m]!.off) m = r
				if (m === i) break
				const t = a[i]!
				a[i] = a[m]!
				a[m] = t
				i = m
			}
		}
		return top
	}

	values(): Seg[] {
		return this.a
	}
}

export class ReorderBuf {
	cap: number // 容量上限（= 连接级接收窗口）
	next = 0 // 已连续收到的下一字节偏移（累积 ACK 点）
	private segs = new SegHeap()
	/** ready 是已保序、待应用读走的段队列；队首段随消费前移。 */
	private ready: Seg[] = []
	private readyHead = 0 // ready[readyHead] 内已消费偏移
	private size = 0 // segs + ready 占用字节数
	private readyBytes = 0 // ready 中可读字节数
	dropped = 0 // 因超容量丢弃的字节数（防御性统计）

	constructor(cap: number) {
		this.cap = cap
	}

	/**
	 * add 把一段到达数据放入缓冲。重复段去重、超容量段丢弃；
	 * 与 next 衔接的段链会被一次性移入 ready 队列。
	 */
	add(off: number, data: Uint8Array): void {
		if (data.length === 0) {
			return
		}
		const end = off + data.length
		if (end <= this.next) {
			return // 完全重复（重传），丢弃
		}
		if (off < this.next) {
			// 前缀已被交付：裁掉重复部分（跨路径重发的重叠段）
			data = data.subarray(this.next - off)
			off = this.next
		}
		if (off > this.next && this.size + data.length > this.cap) {
			// 乱序段超容量 → 丢段。窗口背压正常运作时不可达（发送端
			// unacked ≤ window 保证到达量不越界）；这是对端超发/恶意
			// 时的内存保护。被丢段不进 SACK ranges，发送端超时重传
			// 可恢复。补洞段（off==next）不受此限：它能推进交付、
			// 让 read 释放空间；若连它也丢，缓冲永远无法腾空——死锁。
			this.dropped += data.length
			return
		}
		this.segs.push({ off, data })
		this.size += data.length
		this.drain()
	}

	/** drain 把堆顶与 next 衔接的段依次移入 ready，推进累积偏移。 */
	private drain(): void {
		for (;;) {
			const top = this.segs.top()
			if (top === undefined || top.off > this.next) {
				return
			}
			const s = this.segs.pop()!
			if (s.off < this.next) {
				// 前缀与已交付区间重叠：裁剪，并把入堆时已计入 size
				// 的重叠字节核销，否则窗口会因重复记账越磨越小
				const trim = this.next - s.off
				s.data = s.data.subarray(trim)
				s.off = this.next
				this.size -= trim
			}
			if (s.data.length === 0) {
				continue // 整段落在已交付区间内（被其它乱序段覆盖）
			}
			this.next += s.data.length
			this.ready.push(s)
			this.readyBytes += s.data.length
		}
	}

	/**
	 * read 从 ready 队列取最多 max 字节返回（拷进新数组）。
	 * 返回空数组表示暂无保序数据。
	 */
	read(max?: number): Uint8Array {
		const want = max === undefined ? this.readyBytes : Math.min(max, this.readyBytes)
		if (want <= 0) {
			return new Uint8Array(0)
		}
		const out = new Uint8Array(want)
		let n = 0
		while (n < want && this.ready.length > 0) {
			const head = this.ready[0]!
			const m = Math.min(want - n, head.data.length - this.readyHead)
			out.set(head.data.subarray(this.readyHead, this.readyHead + m), n)
			n += m
			this.readyHead += m
			if (this.readyHead === head.data.length) {
				this.ready.shift()
				this.readyHead = 0
			}
		}
		if (this.ready.length === 0) {
			this.ready = []
			this.readyHead = 0
		}
		this.size -= n
		this.readyBytes -= n
		return out
	}

	/** avail 返回已保序、可立即读走的字节数。 */
	avail(): number {
		return this.readyBytes
	}

	/** cum 返回累积确认偏移（下一期望字节）。 */
	cum(): number {
		return this.next
	}

	/** window 返回接收窗口通告值 = 缓冲剩余量。 */
	window(): number {
		return this.cap - this.size
	}

	/**
	 * setCap 调整容量上限（窗口联动）。缩小容量不丢弃已缓冲数据——
	 * window 变负后按 0 通告，等 read 排空。
	 */
	setCap(c: number): void {
		this.cap = c
	}

	/** sizeBytes 返回当前占用字节数（含未保序暂存与未读走部分）。 */
	sizeBytes(): number {
		return this.size
	}

	/** ranges 把乱序暂存段导出为合并后的升序 SACK 区间，最多 max 条。 */
	ranges(max: number): ByteRange[] {
		if (this.segs.length === 0 || max <= 0) {
			return []
		}
		const rs: ByteRange[] = this.segs.values().map(s => ({ start: s.off, end: s.off + s.data.length }))
		rs.sort((a, b) => a.start - b.start)
		// 合并重叠/相邻区间
		const merged: ByteRange[] = []
		for (const cur of rs) {
			const last = merged[merged.length - 1]
			if (last !== undefined && cur.start <= last.end) {
				if (cur.end > last.end) {
					last.end = cur.end
				}
				continue
			}
			merged.push({ start: cur.start, end: cur.end })
		}
		return merged.slice(0, max)
	}
}
