/**
 * Go ↔ TypeScript 真实互通验证：起一个真实 Go 端 netacc 节点
 * （js/e2e/echo-server，本仓库模块内的 go-libp2p + netacc.New +
 * io.Copy 回声），JS 侧走 libp2p TCP 传输 openStream/addPath。
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

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '../..')
const goAvailable = spawnSync('go', ['version'], { encoding: 'utf8' }).status === 0

describe.skipIf(!goAvailable)('Go↔TS 端到端互通（真实 Go echo 服务端）', () => {
	let server: ChildProcess
	let serverAddr = ''
	let node: Libp2p
	let client: NetaccClient

	beforeAll(async () => {
		// 构建 echo 服务端（仓库内 go.mod 模块覆盖，依赖走本地 module cache）
		const out = join(mkdtempSync(join(tmpdir(), 'netacc-e2e-')), 'echo-server')
		const build = spawnSync('go', ['build', '-o', out, './js/e2e/echo-server'], {
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
				if (m) resolve(m[1]!.trim())
			})
			server.on('exit', c => reject(new Error(`echo-server 退出 code=${c}`)))
			setTimeout(() => reject(new Error('echo-server READY 超时')), 30_000)
		})
		node = await createLibp2p({
			addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] },
			transports: [tcp()],
			connectionEncrypters: [noise()],
			streamMuxers: [yamux()],
		})
		client = new NetaccClient(node)
	}, 200_000)

	afterAll(async () => {
		await client?.close()
		await node?.stop()
		server?.kill('SIGTERM')
	})

	it('openStream → Go 端 echo → 数据一致（含 >64KiB 分片）', async () => {
		const st = await client.openStream(serverAddr)
		expect(st.paths().length).toBe(1)
		expect(st.paths()[0]!.transport).toBe('tcp')

		const want = new Uint8Array(200 * 1024).map((_, i) => i & 0xff)
		void st.write(want)
		const parts: Uint8Array[] = []
		let got = 0
		while (got < want.length) {
			const c = await st.read()
			if (c === null) break
			parts.push(c)
			got += c.length
		}
		const out = new Uint8Array(got)
		let off = 0
		for (const p of parts) { out.set(p, off); off += p.length }
		expect(out).toEqual(want)

		// ACK 应已将 ackedBytes 推进到写出的量
		for (let i = 0; i < 100 && st.stats().ackedBytes < want.length; i++) {
			await sleep(20)
		}
		expect(st.stats().ackedBytes).toBe(want.length)
		await st.close()
	}, 30_000)

	it('addPath：向 Go 端拨第二条 TCP 路径，Go 端回显 PATH_ATTACH 挂接', async () => {
		const st = await client.openStream(serverAddr)
		const pathId = await st.addPath(serverAddr)
		expect(pathId).toBe(2)
		for (let i = 0; i < 100 && st.paths().length < 2; i++) {
			await sleep(20)
		}
		expect(st.paths().length).toBe(2)

		// 双路径下 echo 仍数据一致（调度器自然分流）
		const want = new Uint8Array(96 * 1024).map((_, i) => i & 0xff)
		void st.write(want)
		const parts: Uint8Array[] = []
		let got = 0
		while (got < want.length) {
			const c = await st.read()
			if (c === null) break
			parts.push(c)
			got += c.length
		}
		const out = new Uint8Array(got)
		let off = 0
		for (const p of parts) { out.set(p, off); off += p.length }
		expect(out).toEqual(want)

		st.removePath(pathId)
		expect(st.paths().length).toBe(1)
		expect(() => st.removePath(0)).toThrow(/最后一条/)
		await st.close()
	}, 30_000)
})
