package netacc

import (
	"sort"

	ma "github.com/multiformats/go-multiaddr"
)

// PathTransport 标识一条数据路径所用的底层传输（规格书 §3.3 传输矩阵）。
// 由路径两端连接的 multiaddr 反推得出，供调度器（#20）与策略层（#24）
// 消费；不参与协议线格式。
type PathTransport int

const (
	// TransportUnknown 表示无法从 multiaddr 判定传输（如进程内管道）。
	TransportUnknown PathTransport = iota
	// TransportTCP：/tcp 路径。UDP QoS 免疫基线（P0）。
	TransportTCP
	// TransportWebSocket：/ws、/wss、/tls/sni/.../ws 路径。
	// TCP 系第二条腿，wss/443 外观近似 HTTPS，穿透/伪装最强（P0）。
	TransportWebSocket
	// TransportQUIC：/udp/.../quic-v1 路径。未受限时性能最优的
	// 机会型路径（P1）；UDP 整类限速时与 WT/WebRTC 同生共死。
	TransportQUIC
	// TransportWebTransport：/udp/.../quic-v1/webtransport/certhash/...
	// 路径（P2）。与 QUIC 同为 UDP 且多一层 HTTP/3+Noise 握手，
	// go↔go 无增量价值，主要为无 CA 证书的浏览器节点准备。
	TransportWebTransport
	// TransportWebRTCDirect：/udp/.../webrtc-direct/certhash/...
	// 路径（P3）。握手最重（ICE→DTLS→SCTP→Noise）、16KB 消息分帧；
	// go-libp2p 未实现 /webrtc（browser↔browser），go↔go 价值最低。
	TransportWebRTCDirect
	// TransportRelay：/p2p-circuit 中继路径。代价最高，默认排最后；
	// 细分排序由中继路径支持（#21）负责。
	TransportRelay
)

// String 返回传输名（与 multiaddr 协议名一致，便于日志与测试断言）。
func (t PathTransport) String() string {
	switch t {
	case TransportTCP:
		return "tcp"
	case TransportWebSocket:
		return "websocket"
	case TransportQUIC:
		return "quic"
	case TransportWebTransport:
		return "webtransport"
	case TransportWebRTCDirect:
		return "webrtc-direct"
	case TransportRelay:
		return "relay"
	default:
		return "unknown"
	}
}

// Priority 返回规格书 §3.3 的偏好级（P0~P3，数值越小越优先）：
// TCP/WebSocket=0（UDP QoS 免疫主力）、QUIC=1、WebTransport=2、
// WebRTC-direct=3；Relay/Unknown=4（默认最后）。
//
// 这只是「未实测时的先验排序」：QUIC/WT/WebRTC 同为 UDP，整类限速
// 下同生共死，最终权重由调度器按实测带宽决定（#20），本表不提供
// 协议级规避切换的保证。
func (t PathTransport) Priority() int {
	switch t {
	case TransportTCP, TransportWebSocket:
		return 0
	case TransportQUIC:
		return 1
	case TransportWebTransport:
		return 2
	case TransportWebRTCDirect:
		return 3
	default:
		return 4
	}
}

// PathTransportOf 从 multiaddr 反推传输类型。复合地址按「最外层
// 传输」归类：/quic-v1/webtransport 同时含 quic-v1 段，须扫完全部
// 组件再判给 webtransport 而非 quic；/p2p-circuit 地址无论底层段
// 为何都归 Relay。
func PathTransportOf(addr ma.Multiaddr) PathTransport {
	if addr == nil {
		return TransportUnknown
	}
	var hasTCP, hasWS, hasQUIC, hasWT, hasWRTC, hasRelay bool
	for _, p := range addr.Protocols() {
		switch p.Code {
		case ma.P_CIRCUIT:
			hasRelay = true
		case ma.P_WEBRTC_DIRECT:
			hasWRTC = true
		case ma.P_WEBTRANSPORT:
			hasWT = true
		case ma.P_QUIC_V1, ma.P_QUIC:
			hasQUIC = true
		case ma.P_WS, ma.P_WSS:
			hasWS = true
		case ma.P_TCP:
			hasTCP = true
		}
	}
	switch {
	case hasRelay:
		return TransportRelay
	case hasWRTC:
		return TransportWebRTCDirect
	case hasWT:
		return TransportWebTransport
	case hasQUIC:
		return TransportQUIC
	case hasWS:
		return TransportWebSocket
	case hasTCP:
		return TransportTCP
	default:
		return TransportUnknown
	}
}

// SortAddrsByPreference 把一组对端 multiaddr 按 §3.3 传输偏好稳定排序
// （原地排）。供两处使用：
//   - 本库 attachMinPaths 补挂路径时优先拨 TCP/WS（UDP 系同生共死，
//     未实测时不应抢占补挂名额）；
//   - 调用方手工挑地址喂 AddPath 时作排序提示（路径描述符的
//     「传输偏好」字段即按此消费，规格书 §4.3）。
func SortAddrsByPreference(addrs []ma.Multiaddr) {
	sort.SliceStable(addrs, func(i, j int) bool {
		return PathTransportOf(addrs[i]).Priority() < PathTransportOf(addrs[j]).Priority()
	})
}
