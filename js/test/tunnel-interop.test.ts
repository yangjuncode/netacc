/**
 * JS ↔ Go 真实互通验证：起一个真实 Go 端 tunnel server
 * （js/e2e/tunnel-server，本仓库模块内的 libp2p + netacc +
 * tunnel.NewServer + HTTP/WS echo handler），JS 侧经 libp2p TCP
 * openStream 走 tunnelwire 应用层协议——直接验证两侧线格式一致。
 *
 * 需要 go 工具链与仓库 Go 依赖可用；缺 go 时整组用例自动跳过。
 */
import { describe, expect, it, beforeAll, afterAll } from 'vitest'
import { createLibp2p, type Libp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { NetaccClient } from '../src/client.js'
import { TunnelWebSocket, type TunnelCloseEvent } from '../src/tunnel-ws.js'

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '../..')
const goAvailable = spawnSync('go', ['version'], { encoding: 'utf8' }).status === 0

describe.skipIf(!goAvailable)('JS↔Go 隧道端到端互通（真实 Go tunnel server）', () => {
	let server: ChildProcess
	let serverAddr = ''
	let node: Libp2p
	let client: NetaccClient

	beforeAll(async () => {
		const out = join(mkdtempSync(join(tmpdir(), 'netacc-e2e-')), 'tunnel-server')
		const build = spawnSync('go', ['build', '-o', out, './js/e2e/tunnel-server'], {
			cwd: repoRoot, encoding: 'utf8', timeout: 180_000,
		})
		if (build.status !== 0) {
			throw new Error(`go build 失败: ${build.stderr}`)
		}
		server = spawn(out, [])
		serverAddr = await new Promise<string>((resolve, reject) => {
			let buf = ''
			server.stdout!.on('data', (d: Uint8Array) => {
				buf += d.toString()
				const m = buf.match(/^READY (.+)$/m)
				if (m) {
					resolve(m[1]!.trim())
				}
			})
			server.on('exit', c => reject(new Error(`tunnel-server 退出 code=${c}`)))
			setTimeout(() => reject(new Error('tunnel-server READY 超时')), 30_000)
		})
		node = await createLibp2p({
			addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] },
			transports: [tcp()],
			connectionEncrypters: [noise()],
			streamMuxers: [yamux()],
		})
		client = new NetaccClient(node, { tunnelTarget: serverAddr })
	}, 200_000)

	afterAll(async () => {
		await client?.close()
		await node?.stop()
		server?.kill('SIGTERM')
	})

	it('tunnelFetch POST /echo：请求体回显 + X-Echo-* 响应头', async () => {
		const res = await client.tunnelFetch('/echo', {
			method: 'POST',
			body: '你好 netacc',
		})
		expect(res.status).toBe(200)
		expect(res.headers.get('x-echo-method')).toBe('POST')
		expect(await res.text()).toBe('你好 netacc')
	}, 30_000)

	it('tunnelFetch /inspect：Host 头提升 + 自定义头透传到 Go handler', async () => {
		const res = await client.tunnelFetch('/inspect?q=1', {
			headers: { Host: 'app.internal', 'X-Custom': 'v1' },
		})
		expect(res.status).toBe(200)
		const got = await res.json() as {
			method: string
			host: string
			path: string
			headers: Record<string, string[]>
		}
		expect(got.method).toBe('GET')
		expect(got.host).toBe('app.internal')
		expect(got.path).toBe('/inspect?q=1')
		// Go 侧 Header map 规范化首字母大写
		expect(got.headers['X-Custom']).toEqual(['v1'])
	}, 30_000)

	it('tunnelFetch 流式请求体（>64KiB 分片 DATA）回显一致', async () => {
		const chunk = new Uint8Array(64 * 1024).map((_, i) => i & 0xff)
		const rs = new ReadableStream<Uint8Array>({
			start(c) {
				c.enqueue(chunk)
				c.enqueue(chunk.subarray(0, 1000)) // 共 66536 字节，跨 DATA 帧
				c.close()
			},
		})
		const res = await client.tunnelFetchTo(serverAddr, '/echo', { method: 'POST', body: rs })
		expect(res.status).toBe(200)
		const got = new Uint8Array(await res.arrayBuffer())
		expect(got.length).toBe(66536)
		expect(got.subarray(0, chunk.length)).toEqual(chunk)
		expect(got.subarray(chunk.length)).toEqual(chunk.subarray(0, 1000))
	}, 30_000)

	it('tunnelWs：101 握手 + 子协议协商 + text/binary 回显 + 干净关闭', async () => {
		const ws = client.tunnelWs('/ws', { protocols: ['chat', 'v2'] })
		expect(ws.readyState).toBe(TunnelWebSocket.CONNECTING)
		await ws.opened
		expect(ws.readyState).toBe(TunnelWebSocket.OPEN)
		expect(ws.protocol).toBe('chat')

		const got = new Promise<{ s?: string; b?: Blob | ArrayBuffer }>(resolve => {
			const out: { s?: string; b?: Blob | ArrayBuffer } = {}
			let n = 0
			ws.onmessage = ev => {
				if (typeof ev.data === 'string') {
					out.s = ev.data
				} else {
					out.b = ev.data as Blob | ArrayBuffer
				}
				if (++n === 2) {
					resolve(out)
				}
			}
		})
		ws.send('你好 ws')
		ws.send(new Uint8Array([1, 2, 3, 255]))
		const r = await got
		expect(r.s).toBe('你好 ws')
		const bin = r.b instanceof Blob ? new Uint8Array(await r.b.arrayBuffer()) : new Uint8Array(r.b as ArrayBuffer)
		expect(bin).toEqual(new Uint8Array([1, 2, 3, 255]))

		const closed = new Promise<TunnelCloseEvent>(r2 => {
			ws.onclose = r2
		})
		ws.close(1000, 'done')
		const ev = await closed
		expect(ev.code).toBe(1000)
		expect(ev.wasClean).toBe(true)
	}, 30_000)

	it('tunnelWs 握手拒绝：未协商的 ws 路径不存在时服务端拒绝', async () => {
		// tunnel-server 的 wsEcho 总是 Accept ——换一个不存在的服务形态：
		// 对 ws target 用 http 语义 OPEN 不能复用；改为直接观察 server 对
		// 未知路径同样 Accept（handler 不校验路径）。此用例验证 ws(s)://
		// 代理未配置时的拒绝路径（wsProxy 为 nil → 403）。
		const ws = client.tunnelWs('ws://127.0.0.1:1/none')
		const closed = new Promise<TunnelCloseEvent>(r => {
			ws.onclose = r
		})
		await expect(ws.opened).rejects.toThrow(/403|拒绝/)
		const ev = await closed
		expect(ev.wasClean).toBe(false)
	}, 30_000)
})
