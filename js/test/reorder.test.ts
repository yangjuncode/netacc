/**
 * 重排缓冲测试：reorder_test.go 的用例移植——乱序、重叠/重复去重、
 * 容量丢弃、SACK ranges 合并、分段读出、窗口占用核算。
 */
import { describe, expect, it } from 'vitest'
import { ReorderBuf } from '../src/reorder.js'

function mkdata(n: number, fill: number): Uint8Array {
	return new Uint8Array(n).fill(fill)
}

function collect(r: ReorderBuf, chunk = 64): Uint8Array {
	const parts: Uint8Array[] = []
	for (;;) {
		const c = r.read(chunk)
		if (c.length === 0) break
		parts.push(c)
	}
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
	let off = 0
	for (const p of parts) {
		out.set(p, off)
		off += p.length
	}
	return out
}

describe('ReorderBuf', () => {
	it('顺序到达：立即可读，窗口占用正确', () => {
		const r = new ReorderBuf(1024)
		r.add(0, mkdata(100, 0x61))
		expect(r.avail()).toBe(100)
		expect(r.cum()).toBe(100)
		expect(r.window()).toBe(1024 - 100)
		const out = r.read(100)
		expect(out).toEqual(mkdata(100, 0x61))
		expect(r.avail()).toBe(0)
		expect(r.window()).toBe(1024)
	})

	it('乱序注入：缺口补齐后按序输出', () => {
		const r = new ReorderBuf(4096)
		r.add(200, mkdata(100, 0x63))
		r.add(100, mkdata(100, 0x62))
		expect(r.avail()).toBe(0)
		expect(r.cum()).toBe(0)
		r.add(0, mkdata(100, 0x61))
		expect(r.avail()).toBe(300)
		expect(r.cum()).toBe(300)
		const want = new Uint8Array(300)
		want.set(mkdata(100, 0x61), 0)
		want.set(mkdata(100, 0x62), 100)
		want.set(mkdata(100, 0x63), 200)
		expect(r.read(300)).toEqual(want)
	})

	it('重复/重叠段：按偏移去重，不重复交付', () => {
		const r = new ReorderBuf(1024)
		r.add(0, mkdata(100, 0x61))
		r.add(0, mkdata(100, 0x78)) // 完全重复
		r.add(50, mkdata(100, 0x79)) // 部分重叠 [50,150)
		const out = r.read(200)
		expect(out.length).toBe(150)
		expect(out.subarray(0, 100)).toEqual(mkdata(100, 0x61))
		expect(out.subarray(100, 150)).toEqual(mkdata(50, 0x79))
		expect(r.sizeBytes()).toBe(0)
		expect(r.window()).toBe(1024)
	})

	it('乱序暂存段互相重叠：drain 裁剪正确核销 size', () => {
		const r = new ReorderBuf(1024)
		r.add(100, mkdata(100, 0x61)) // [100,200)
		r.add(150, mkdata(100, 0x62)) // [150,250) 重叠 50
		expect(r.sizeBytes()).toBe(200)
		r.add(0, mkdata(100, 0x63)) // 补洞 → 全部交付
		expect(r.read(250).length).toBe(250)
		expect(r.sizeBytes()).toBe(0)
		expect(r.window()).toBe(1024)
		expect(r.cum()).toBe(250)
	})

	it('缓冲满时丢弃超限乱序段（补洞段不受限）', () => {
		const r = new ReorderBuf(100)
		r.add(50, mkdata(80, 0x62)) // size=80
		r.add(200, mkdata(50, 0x63)) // 80+50>100 → 丢弃
		expect(r.sizeBytes()).toBe(80)
		r.add(0, mkdata(50, 0x61))
		expect(r.avail()).toBe(130)
	})

	it('SACK ranges：重叠/相邻段合并、按 max 截断', () => {
		const r = new ReorderBuf(4096)
		r.add(200, mkdata(50, 0x62))
		r.add(100, mkdata(50, 0x61)) // [100,150)
		r.add(140, mkdata(20, 0x78)) // [140,160) 重叠合并
		r.add(400, mkdata(50, 0x63))
		expect(r.ranges(10)).toEqual([
			{ start: 100, end: 160 },
			{ start: 200, end: 250 },
			{ start: 400, end: 450 },
		])
		expect(r.ranges(2).length).toBe(2)
	})

	it('分段读出', () => {
		const r = new ReorderBuf(1024)
		r.add(0, mkdata(100, 0x61))
		expect(collect(r, 30)).toEqual(mkdata(100, 0x61))
	})
})
