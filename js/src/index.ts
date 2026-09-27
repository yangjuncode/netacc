/**
 * netacc 浏览器/TypeScript 客户端公开导出。
 *
 * 用法概要：
 * ```ts
 * import { createLibp2p } from 'libp2p'
 * import { NetaccClient } from 'netacc'
 *
 * const node = await createLibp2p({ transports: [websockets(), webtransport()] /* … *\/ })
 * const client = new NetaccClient(node)
 * const stream = await client.openStream('/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...')
 * await stream.write(data)
 * for await (const chunk of stream) { … }
 * ```
 *
 * 与 Go 端互通的协议地址：
 *   - 聚合流握手：/netacc/agg/1.0.0
 *   - 路径绑定：  /netacc/path/1.0.0
 */

export { NetaccClient } from './client.js'
export type { NetaccClientOptions, OpenStreamOptions } from './client.js'
export { PROTOCOL_AGG, PROTOCOL_PATH } from './client.js'

export { AggregatedStream, AGG_STREAM_ID_LEN } from './stream.js'
export type {
	AggregatedStreamOptions,
	PathDialer,
	DialedPath,
	RelayReserver,
} from './stream.js'

export type { ByteStream } from './bytestream.js'
export { StreamReader } from './bytestream.js'
export { Libp2pByteStream } from './libp2p-stream.js'

export {
	FrameDecoder,
	FrameType,
	MAX_ACK_RANGES,
	MAX_CTRL_BODY,
	MAX_FRAME_PAYLOAD,
	encodeAckFrame,
	encodeCtrlFrame,
	encodeDataFrame,
} from './frame.js'
export type { ByteRange, DecodedFrame } from './frame.js'

export { ReorderBuf } from './reorder.js'

export {
	NetaccError,
	netaccErr,
	StreamEOFError,
} from './types.js'
export type {
	NetaccErrorCode,
	PathInfo,
	PathStats,
	PathTransport,
	StreamEvent,
	StreamEventType,
	StreamStats,
} from './types.js'

export { transportOf, isCircuitAddr } from './multiaddr.js'

export { encodeHello, encodePathAttach, encodePathDrop, encodePing, encodeTelemetry } from './proto.js'
