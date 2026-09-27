/**
 * MemoryByteStream：进程内成对的全双工字节流（等价于 net.Pipe 的
 * 缓冲版），通过 `netacc/testing` 子路径导出，用于单元测试与本地联调——
 * 不经 libp2p 即可驱动聚合流数据面。
 *
 * 语义与 ByteStream 约定一致：
 *   - read() 在对端 close() 后耗尽残余数据再返回 null；
 *   - abort() 立即终结双向并丢弃未读数据；
 *   - write() 即时成功（无背压），对端 abort/close 后写失败。
 */

import type { ByteStream } from './bytestream.js'

export class MemoryByteStream implements ByteStream {
	private q: Uint8Array[] = []
	private waiters: Array<(r: IteratorResult<Uint8Array>) => void> = []
	private peerClosed = false // 对端已 close()：缓冲读尽后 EOF
	private dead: Error | null = null // abort 终态（非空 = 传输错误）
	private localClosed = false

	private peer!: MemoryByteStream
	localAddr?: string
	remoteAddr?: string
	connId?: string

	private constructor() {}

	/** pair 建一对互通的 MemoryByteStream。 */
	static pair(opts?: { localAddr?: string; remoteAddr?: string; connId?: string }): [MemoryByteStream, MemoryByteStream] {
		const a = new MemoryByteStream()
		const b = new MemoryByteStream()
		a.peer = b
		b.peer = a
		if (opts !== undefined) {
			// 观测字段按「本端视角」设置
			a.localAddr = opts.localAddr
			a.remoteAddr = opts.remoteAddr
			b.localAddr = opts.remoteAddr
			b.remoteAddr = opts.localAddr
			a.connId = opts.connId
			b.connId = opts.connId
		}
		return [a, b]
	}

	read(): Promise<Uint8Array | null> {
		const c = this.q.shift()
		if (c !== undefined) {
			return Promise.resolve(c)
		}
		if (this.dead !== null) {
			return Promise.reject(this.dead)
		}
		if (this.peerClosed) {
			return Promise.resolve(null)
		}
		return new Promise(resolve => {
			this.waiters.push(v => {
				resolve(v.done === true ? null : v.value)
			})
		})
	}

	write(data: Uint8Array): Promise<void> {
		if (this.localClosed) {
			return Promise.reject(new Error('netacc: 写向已关闭'))
		}
		if (this.peer.dead !== null || this.peer.localClosed) {
			return Promise.reject(this.peer.dead ?? new Error('netacc: 对端已终止'))
		}
		if (data.length === 0) {
			return Promise.resolve()
		}
		this.peer.deliver(data.slice())
		return Promise.resolve()
	}

	private deliver(data: Uint8Array): void {
		if (this.dead !== null || this.localClosed) {
			return // 读方向已死：丢弃
		}
		const w = this.waiters.shift()
		if (w !== undefined) {
			w({ value: data, done: false })
			return
		}
		this.q.push(data)
	}

	async close(): Promise<void> {
		if (this.localClosed) {
			return
		}
		this.localClosed = true
		// 通知对端：残余缓冲读尽后 EOF
		this.peer.peerClosed = true
		this.peer.flush()
	}

	abort(err?: Error): void {
		const e = err ?? new Error('netacc: 连接终止')
		if (this.localClosed && this.dead !== null) {
			return
		}
		this.localClosed = true
		this.dead = e
		// 远端收到 reset：可读数据清空、读写均报错
		this.peer.dead = e
		this.peer.q.length = 0
		this.flush()
		this.peer.flush()
	}

	private flush(): void {
		for (const w of this.waiters.splice(0)) {
			w({ value: undefined as unknown as Uint8Array, done: true })
		}
	}
}
