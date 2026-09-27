/**
 * multiaddr 工具：传输类型反推与地址形态判定。
 * 与 Go 端 transport.go 的 PathTransportOf 语义一致：
 * 复合地址按「最外层传输」归类（/quic-v1/webtransport 归 webtransport；
 * /p2p-circuit 地址无论底层段为何都归 relay）。
 */

import type { Multiaddr } from '@multiformats/multiaddr'
import type { PathTransport } from './types.js'

/**
 * transportOf 从 multiaddr 反推传输类型。
 * 对应 Go 端 PathTransportOf（规格书 §3.3 传输矩阵）。
 */
export function transportOf(addr: Multiaddr | string | undefined): PathTransport {
	if (addr == null) {
		return 'unknown'
	}
	const s = typeof addr === 'string' ? addr : addr.toString()
	// 按优先级扫描组件名：relay > webrtc-direct > webtransport > quic > ws > tcp
	if (s.includes('/p2p-circuit')) {
		return 'relay'
	}
	if (s.includes('/webrtc-direct') || s.includes('/webrtc/')) {
		return 'webrtc-direct'
	}
	if (s.includes('/webtransport')) {
		return 'webtransport'
	}
	if (s.includes('/quic-v1') || s.includes('/quic')) {
		return 'quic'
	}
	if (s.includes('/ws') || s.includes('/wss')) {
		return 'websocket'
	}
	if (s.includes('/tcp')) {
		return 'tcp'
	}
	return 'unknown'
}

/** isCircuitAddr 判断地址是否经 circuit-relay（含 /p2p-circuit 段）。 */
export function isCircuitAddr(addr: Multiaddr | string): boolean {
	const s = typeof addr === 'string' ? addr : addr.toString()
	return s.includes('/p2p-circuit')
}
