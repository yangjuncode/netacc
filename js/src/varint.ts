/**
 * uvarint（LEB128 无符号变长整数）编解码，与 Go `binary.AppendUvarint` /
 * `binary.ReadUvarint` 逐字节一致：每字节低 7 位为数据、最高位为续位标志。
 *
 * 解码返回 bigint 以完整覆盖 uint64 域；数值字段在使用处经
 * `toSafeNumber` 收敛为 JS number（超过 2^53-1 拒绝，防精度静默丢失）。
 */

/** 单字节缓冲区可重复利用的编码器（无状态，纯函数风格）。 */
export function uvarintLen(v: bigint): number {
	let n = 1
	let x = v
	while (x >= 0x80n) {
		x >>= 7n
		n++
	}
	return n
}

/** 把 v 以 uvarint 追加到 dst 末尾，返回新数组。 */
export function appendUvarint(dst: Uint8Array, v: bigint | number): Uint8Array {
	let x = typeof v === 'bigint' ? v : BigInt(v)
	if (x < 0n) {
		throw new RangeError('uvarint 不能编码负数')
	}
	// 大多数值很短，先算长度再一次性分配，避免逐字节扩容。
	const n = uvarintLen(x)
	const out = new Uint8Array(dst.length + n)
	out.set(dst, 0)
	let i = dst.length
	while (x >= 0x80n) {
		out[i++] = Number((x & 0x7fn) | 0x80n)
		x >>= 7n
	}
	out[i] = Number(x)
	return out
}

export interface UvarintResult {
	/** 解码出的值（完整 uint64 精度）。 */
	value: bigint
	/** 消耗的输入字节数。 */
	size: number
}

/**
 * 从 buf[off..] 解码一个 uvarint。
 * 返回 null 表示输入不完整（需要更多字节）；超过 10 字节或溢出报 Error。
 */
export function decodeUvarint(buf: Uint8Array, off = 0): UvarintResult | null {
	let result = 0n
	let shift = 0n
	for (let i = off; i < buf.length; i++) {
		const b = buf[i]!
		if (i - off === 9) {
			// 第 10 字节（下标 9）：只允许值为 0 或 1（uint64 上限），
			// 且不得再有续位——与 Go ReadUvarint 的 overflow 判定一致。
			if (b > 1) {
				throw new Error('uvarint 溢出：超过 uint64 最大值')
			}
			result |= BigInt(b) << shift
			return { value: result, size: i - off + 1 }
		}
		result |= BigInt(b & 0x7f) << shift
		if (b < 0x80) {
			return { value: result, size: i - off + 1 }
		}
		shift += 7n
	}
	return null // 输入不完整
}

/** 把 bigint 收敛为 JS number，超出安全整数域即拒绝。 */
export function toSafeNumber(v: bigint, what = '字段'): number {
	if (v > BigInt(Number.MAX_SAFE_INTEGER)) {
		throw new Error(`${what} 超出安全整数范围: ${v}`)
	}
	return Number(v)
}
