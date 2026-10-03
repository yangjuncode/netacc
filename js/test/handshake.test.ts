/** 拒绝回执边界测试：客户端必须收到拒绝原因，不能误判为接受或消息超长。 */
import { expect, it } from 'vitest'
import { StreamReader } from '../src/bytestream.js'
import { MemoryByteStream } from '../src/memory-stream.js'
import { handshakeInitiator, handshakeResponderRead, handshakeResponderReject } from '../src/handshake.js'

it.each([
	['空原因', '', 'unauthorized'],
	['中文跨截断边界', '中'.repeat(86), '中'.repeat(85)],
	['混合字符边界', 'a'.repeat(255) + '中', 'a'.repeat(255)],
	['四字节字符', '😀'.repeat(65), '😀'.repeat(64)],
	['超长原因', 'x'.repeat(4096), 'x'.repeat(256)],
])('拒绝回执：%s', async (_name, reason, want) => {
	const [client, server] = MemoryByteStream.pair()
	const responder = (async () => {
		const { id } = await handshakeResponderRead(new StreamReader(server))
		await handshakeResponderReject(server, id, reason)
		await server.close()
	})()
	await expect(handshakeInitiator(client, new StreamReader(client)))
		.rejects.toMatchObject({ message: `netacc: 对端拒绝: ${want}` })
	await responder
	await client.close()
})
