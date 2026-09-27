/**
 * 发送泵的分帧决策边界（规格书 §5.1，scheduler.go 的移植）。
 *
 * 最短排空时间优先（与 Linux MPTCP mptcp_subflow_get_send 同构）：
 * 本帧若走路径 i，其队列排空耗时 drain_i = (inflight_i + len)/est_rate_i，
 * 取最小者；叠加每路径在途硬顶 inflight_cap_i = k·est_rate·srtt_i。
 *
 * 所有决策为无副作用的纯函数：路径指标只能由 stream 的采样/记账代码更新。
 */

import { COLD_RATE_BPS, SUSPECT_PENALTY, type Path } from './path.js'

/**
 * pickData 为本帧 DATA 选路；返回 null 表示本轮没有路径可用
 * （全部在途顶满），等 ACK 释放 inflight 后再试。
 *
 * 可疑路径只做兜底：第一趟在非可疑路径里选；全部可疑或全部顶满时
 * 第二趟才纳入，且速率按 1/suspectPenalty 折算——降权是强偏好不是
 * 禁发，单路径/全可疑时仍须发得出。
 */
export function pickData(paths: Path[], frameLen: number, now: number): Path | null {
	return pickMinDrain(paths, frameLen, now, false) ?? pickMinDrain(paths, frameLen, now, true)
}

function pickMinDrain(paths: Path[], frameLen: number, now: number, includeSuspect: boolean): Path | null {
	let best: Path | null = null
	let bestDrain = Infinity
	for (const p of paths) {
		if (p.suspect && !includeSuspect) {
			continue
		}
		if (p.inflight >= p.inflightCap(now)) {
			continue // 在途硬顶：本轮跳过该路径
		}
		let rate = p.effRate(now)
		if (rate <= 0) {
			rate = COLD_RATE_BPS // 冷启动：名义速率参与 → 均等分摊
		}
		if (p.suspect) {
			rate /= SUSPECT_PENALTY
		}
		const drain = (p.inflight + frameLen) / rate
		if (drain < bestDrain || (drain === bestDrain && best !== null &&
			(p.inflight < best.inflight || (p.inflight === best.inflight && p.id < best.id)))) {
			best = p
			bestDrain = drain
		}
	}
	return best
}

/**
 * pickAck 为 ACK/PING/TELEMETRY/PATH_DROP 等控制帧选最低延迟路径
 * （规格书 §4.4：ACK 优先走低延迟路径回送）。
 * 无 RTT 样本的路径排在有样本者之后，全部无样本时退化为取首条；
 * 可疑路径排在非可疑者之后。
 */
export function pickAckPath(paths: Path[], _now: number): Path | null {
	let best: Path | null = null
	for (const p of paths) {
		if (best === null) {
			best = p
		} else if (p.suspect !== best.suspect) {
			if (!p.suspect) {
				best = p
			}
		} else if (p.hasRTT && (!best.hasRTT || p.srtt < best.srtt)) {
			best = p
		}
	}
	return best
}
