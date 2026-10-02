/**
 * ByteStream：聚合路径对底层传输的最小抽象——一条可靠的保序字节流。
 *
 * 生产实现是 Libp2pByteStream（适配 libp2p v3 的 MessageStream/Stream）；
 * 测试可注入 MemoryByteStream（见 testing 导出）。Go 侧对应类型是
 * `pathConn`（network.Stream / MuxedStream / net.Pipe 均满足）。
 *
 * 约定：
 *   - 单一读者：read() 由路径接收循环独占调用，不得并发；
 *   - 写由上层串行化（每路径 write 链），实现自身不必保证并发写安全；
 *   - read() resolve 为 null 表示对端正常结束（EOF），reject 表示传输
 *     错误或对端 reset；
 *   - write() resolve 仅表示数据已被底层发送缓冲接受（不保证对端已读），
 *     reject 表示写失败——上层据此摘除路径并重发未确认数据。
 */

export interface ByteStream {
	/** 读取下一块到达数据；EOF 返回 null，传输错误 reject。 */
	read(): Promise<Uint8Array | null>
	/** 写入一块数据；写失败（对端 reset/底层断连）reject。 */
	write(data: Uint8Array): Promise<void>
	/** 正常关闭（FIN 语义：本端写方向结束；远端随后会看到 EOF）。 */
	close(): Promise<void>
	/** 异常终止（RST 语义：丢弃未读/未发数据，通知对端 reset）。 */
	abort(err?: Error): void
	/** 尽力而为的本端地址（multiaddr 字符串），供观测/诊断。 */
	readonly localAddr?: string
	/** 尽力而为的对端地址。 */
	readonly remoteAddr?: string
	/** 尽力而为的底层连接标识（区分同 peer 多连接的路径归属）。 */
	readonly connId?: string
}

/**
 * StreamReader 是 ByteStream 上的带缓冲读游标：把读侧的字节流切成
 * 精确长度的块/变长整数。帧解码与握手消息共用同一实例——缓冲的预读
 * 字节不会在协议层切换时丢失（对应 Go 侧 msgio 无预读 + frameReader
 * 复用同一 bufio 的约定）。
 *
 * 单读者独占。
 */
export class StreamReader {
	private chunks: Uint8Array[] = []
	private off = 0 // chunks[0] 内已消费偏移
	private eof = false
	private err: Error | undefined

	// 只依赖 read()：隧道层用最小的「可读流」（如聚合流）即可驱动。
	constructor(private readonly bs: Pick<ByteStream, 'read'>) {}

	private get buffered(): number {
		let n = 0
		for (const c of this.chunks) {
			n += c.length
		}
		return n - this.off
	}

	/** 再读一块进缓冲；EOF 返回 false；错误抛出并记住终态。 */
	private async fill(): Promise<boolean> {
		if (this.err != null) {
			throw this.err
		}
		if (this.eof) {
			return false
		}
		try {
			const chunk = await this.bs.read()
			if (chunk == null) {
				this.eof = true
				return false
			}
			if (chunk.length > 0) {
				this.chunks.push(chunk)
			}
			return true
		} catch (e) {
			this.err = e instanceof Error ? e : new Error(String(e))
			throw this.err
		}
	}

	/** 丢弃 chunks[0] 的已消费前缀，保持 buffered 计数正确。 */
	private compact(): void {
		if (this.off > 0) {
			this.chunks[0] = this.chunks[0]!.subarray(this.off)
			this.off = 0
		}
		while (this.chunks.length > 0 && this.chunks[0]!.length === 0) {
			this.chunks.shift()
		}
	}

	/**
	 * 读一个 uvarint：EOF 恰在字段边界（无任何字节缓冲）时返回 null；
	 * 读到一半断流抛「非预期 EOF」。
	 */
	async readUvarint(): Promise<bigint | null> {
		// uvarint 最长 10 字节；先看缓冲里能否解出，不足再 fill。
		for (;;) {
			this.compact()
			const total = this.buffered
			if (total > 0) {
				// 把缓冲拼成一块尝试解码（≤10 字节的短前缀，拷贝代价忽略）。
				const head = this.peekBytes(Math.min(total, 10))
				let result = 0n
				let shift = 0n
				for (let i = 0; i < head.length; i++) {
					const b = head[i]!
					if (i === 9) {
						if (b > 1) {
							throw new Error('uvarint 溢出：超过 uint64 最大值')
						}
						result |= BigInt(b) << shift
						this.consume(i + 1)
						return result
					}
					result |= BigInt(b & 0x7f) << shift
					if (b < 0x80) {
						this.consume(i + 1)
						return result
					}
					shift += 7n
				}
				// head 全消费完仍未解出 → 需要更多字节
			}
			if (!(await this.fill())) {
				if (total === 0) {
					return null // 干净 EOF：恰在字段边界
				}
				throw new Error('读 varint 失败: 非预期 EOF')
			}
		}
	}

	/** 读恰好 n 字节；不足 n 时 EOF/出错抛异常。 */
	async readExact(n: number): Promise<Uint8Array> {
		for (;;) {
			this.compact()
			if (this.buffered >= n) {
				const out = this.peekBytes(n).slice()
				this.consume(n)
				return out
			}
			if (!(await this.fill())) {
				throw new Error('读消息体失败: 非预期 EOF')
			}
		}
	}

	/** 把缓冲前 n 字节拼成连续视图（不消费）；调用前须先 compact()。 */
	private peekBytes(n: number): Uint8Array {
		const out = new Uint8Array(n)
		let copied = 0
		for (const c of this.chunks) {
			if (copied >= n) break
			const m = Math.min(c.length, n - copied)
			out.set(c.subarray(0, m), copied)
			copied += m
		}
		return out
	}

	private consume(n: number): void {
		this.off += n
	}
}
