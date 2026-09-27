/**
 * uvarint 编解码测试：与 Go binary.Uvarint/ReadUvarint 的语义逐字节对齐。
 */
import { describe, expect, it } from 'vitest'
import { appendUvarint, decodeUvarint, toSafeNumber } from '../src/varint.js'

describe('uvarint', () => {
	const cases: Array<[bigint, number[]]> = [
		[0n, [0x00]],
		[1n, [0x01]],
		[127n, [0x7f]],
		[128n, [0x80, 0x01]],
		[300n, [0xac, 0x02]],
		[255n, [0xff, 0x01]],
		[0xffffffffn, [0xff, 0xff, 0xff, 0xff, 0x0f]],
		[0xffff_ffff_ffff_ffffn, [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]],
	]

	it('编码向量与 Go binary.AppendUvarint 一致', () => {
		for (const [v, want] of cases) {
			expect(appendUvarint(new Uint8Array(0), v)).toEqual(new Uint8Array(want))
		}
	})

	it('解码回环', () => {
		for (const [v, bytes] of cases) {
			const r = decodeUvarint(new Uint8Array(bytes))
			expect(r).not.toBeNull()
			expect(r!.value).toBe(v)
			expect(r!.size).toBe(bytes.length)
		}
	})

	it('输入不完整返回 null', () => {
		expect(decodeUvarint(new Uint8Array([0x80]))).toBeNull()
		expect(decodeUvarint(new Uint8Array([0x80, 0x80]))).toBeNull()
		expect(decodeUvarint(new Uint8Array(0))).toBeNull()
	})

	it('溢出（第 10 字节 >1 或仍有续位）报错', () => {
		expect(() => decodeUvarint(new Uint8Array([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]))).toThrow()
		expect(() => decodeUvarint(new Uint8Array([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x81]))).toThrow()
	})

	it('负数拒绝编码', () => {
		expect(() => appendUvarint(new Uint8Array(0), -1)).toThrow()
	})

	it('toSafeNumber：安全值通过，超限拒绝', () => {
		expect(toSafeNumber(12345n)).toBe(12345)
		expect(() => toSafeNumber(9007199254740993n)).toThrow()
	})
})
