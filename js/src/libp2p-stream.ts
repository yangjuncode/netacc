/**
 * 把 js-libp2p v3 的 Stream（MessageStream：send() + AsyncIterable +
 * 'close'/'drain' 事件）适配成内部 ByteStream。
 *
 * 语义映射：
 *   - read()：消费流的 async iterator；迭代结束（remoteCloseWrite）→
 *     EOF(null)；'close' 事件携带 error → reject；本地 close() 后 → EOF；
 *   - write()：stream.send()；返回 false（写缓冲满）→ 等 onDrain()；
 *     send 抛错/底层失败 → reject；
 *   - close()：stream.close()（写方向正常关闭）；
 *   - abort(err)：stream.abort(err)（发 RESET）。
 *
 * 单一读者、单写者（上层逐路径写链串行化）。
 */

import type { Connection, Stream } from '@libp2p/interface'
import type { Uint8ArrayList } from 'uint8arraylist'
import type { ByteStream } from './bytestream.js'

type Msg = Stream extends AsyncIterable<infer T> ? T : never
type It = AsyncIterator<Msg>

function toU8(v: Msg): Uint8Array {
	// Uint8ArrayList.subarray() 返回 Uint8Array 视图（零拷贝）
	return v instanceof Uint8Array ? v : v.subarray()
}

export class Libp2pByteStream implements ByteStream {
	private readonly it: It
	private pending: Promise<IteratorResult<Msg>> | null = null
	private term: Error | null | undefined // undefined=未终结；null=干净 EOF；Error=传输错误
	private readonly closedP: Promise<void>
	private resolveClosed!: () => void

	readonly connId: string
	readonly remoteAddr: string
	readonly localAddr: string

	constructor(
		private readonly stream: Stream,
		conn?: Connection,
	) {
		this.it = stream[Symbol.asyncIterator]()
		this.connId = conn?.id ?? ''
		this.remoteAddr = conn?.remoteAddr?.toString() ?? ''
		// js Connection 不直接暴露本端地址，观测字段尽力而为
		this.localAddr = ''
		this.closedP = new Promise<void>(resolve => {
			this.resolveClosed = resolve
		})
		stream.addEventListener('close', (ev: { error?: Error }) => {
			if (this.term === undefined) {
				this.term = ev.error ?? null
			}
			this.resolveClosed()
		})
	}

	async read(): Promise<Uint8Array | null> {
		for (;;) {
			if (this.term !== undefined) {
				if (this.term === null) {
					return null
				}
				throw this.term
			}
			const next = (this.pending ??= this.it.next())
			const r = await Promise.race([next.then(v => ({ v }) as const), this.closedP.then(() => 'closed' as const)])
			if (r === 'closed') {
				// 'close' 事件先到：迭代器可能还在挂起，按终态语义收口
				continue
			}
			this.pending = null
			if (r.v.done === true) {
				if (this.term === undefined) {
					this.term = null // 对端关闭写方向：EOF
				}
				continue
			}
			return toU8(r.v.value)
		}
	}

	async write(data: Uint8Array): Promise<void> {
		if (this.term !== undefined && this.term !== null) {
			throw this.term
		}
		if (!this.stream.send(data)) {
			await this.stream.onDrain()
		}
	}

	async close(): Promise<void> {
		try {
			await this.stream.close()
		} catch {
			// 已死路径的善后关闭忽略错误
		}
		if (this.term === undefined) {
			this.term = null
		}
		this.resolveClosed()
	}

	abort(err?: Error): void {
		try {
			this.stream.abort(err ?? new Error('netacc: 路径终止'))
		} catch {
			// 已终结的流 abort 可能抛状态错误，忽略
		}
		if (this.term === undefined) {
			this.term = err ?? new Error('netacc: 路径终止')
		}
		this.resolveClosed()
	}
}
