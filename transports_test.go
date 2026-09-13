package netacc_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/yangjuncode/netacc"
)

// ---------- 传输分类（规格书 §3.3 优先级表） ----------

// testCertHash 是一个合法的 certhash 组件值（sha2-256 多哈希的
// base58btc multibase 编码），仅用于构造可解析的测试地址。
const testCertHash = "zQmUT21rZhDsapWKXUTm8n9PFrB7b5fw4YUyMgd1sxAt6VS"

func TestPathTransportOf(t *testing.T) {
	cases := []struct {
		addr string
		want netacc.PathTransport
	}{
		{"/ip4/1.2.3.4/tcp/4001", netacc.TransportTCP},
		{"/dns4/example.com/tcp/4001", netacc.TransportTCP},
		{"/ip4/1.2.3.4/tcp/4001/ws", netacc.TransportWebSocket},
		{"/dns4/example.com/tcp/443/wss", netacc.TransportWebSocket},
		{"/dns4/example.com/tcp/443/tls/sni/example.com/ws", netacc.TransportWebSocket},
		{"/ip4/1.2.3.4/udp/4001/quic-v1", netacc.TransportQUIC},
		{"/ip4/1.2.3.4/udp/4001/quic-v1/webtransport/certhash/" + testCertHash, netacc.TransportWebTransport},
		{"/ip4/1.2.3.4/udp/4001/webrtc-direct/certhash/" + testCertHash, netacc.TransportWebRTCDirect},
		{"/ip4/1.2.3.4/udp/4001", netacc.TransportUnknown}, // 裸 UDP（入向 webrtc 对端地址形态）
		{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWEi9FHYjVaKBsJmVNJC2kvnKnBVgEPzJKsE3NdXMNHdqF/p2p-circuit", netacc.TransportRelay},
	}
	for _, c := range cases {
		m := mustMultiaddr(t, c.addr)
		if got := netacc.PathTransportOf(m); got != c.want {
			t.Errorf("PathTransportOf(%s) = %s，期望 %s", c.addr, got, c.want)
		}
	}
	// 优先级表（§3.3）：TCP≈WS > QUIC > WT > WebRTC > relay/unknown
	order := []netacc.PathTransport{
		netacc.TransportTCP, netacc.TransportWebSocket, netacc.TransportQUIC,
		netacc.TransportWebTransport, netacc.TransportWebRTCDirect,
		netacc.TransportRelay, netacc.TransportUnknown,
	}
	for i := 0; i+1 < len(order); i++ {
		if order[i].Priority() > order[i+1].Priority() {
			t.Errorf("优先级非单调: %s(%d) > %s(%d)",
				order[i], order[i].Priority(), order[i+1], order[i+1].Priority())
		}
	}
}

func TestSortAddrsByPreference(t *testing.T) {
	addrs := []ma.Multiaddr{
		mustMultiaddr(t, "/ip4/1.2.3.4/udp/4001/webrtc-direct/certhash/"+testCertHash),
		mustMultiaddr(t, "/ip4/1.2.3.4/udp/4001/quic-v1"),
		mustMultiaddr(t, "/ip4/1.2.3.4/tcp/4001/ws"),
		mustMultiaddr(t, "/ip4/1.2.3.4/udp/4001/quic-v1/webtransport/certhash/"+testCertHash),
		mustMultiaddr(t, "/ip4/1.2.3.4/tcp/4001"),
	}
	netacc.SortAddrsByPreference(addrs)
	var got []netacc.PathTransport
	for _, a := range addrs {
		got = append(got, netacc.PathTransportOf(a))
	}
	// 期望：优先级非降；TCP/WS 同为 P0 且输入序 ws 在 tcp 前，
	// 稳定排序保持该相对序。
	want := []netacc.PathTransport{
		netacc.TransportWebSocket, netacc.TransportTCP, netacc.TransportQUIC,
		netacc.TransportWebTransport, netacc.TransportWebRTCDirect,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("排序结果不符: got %v want %v", got, want)
		}
	}
}

// ---------- 逐传输 loopback 接入 ----------

// tcpOnlyAddr 挑一条纯 TCP 监听地址（剔除 /ws 复合后缀）。
func tcpOnlyAddr(t *testing.T, h host.Host) string {
	t.Helper()
	for _, a := range h.Addrs() {
		s := a.String()
		if strings.Contains(s, "/tcp/") && !strings.Contains(s, "ws") {
			return s
		}
	}
	t.Fatalf("host 没有纯 TCP 监听地址: %v", h.Addrs())
	return ""
}

// transportSet 汇总 Paths() 快照的传输类型。
func transportSet(infos []netacc.PathInfo) map[netacc.PathTransport]bool {
	out := map[netacc.PathTransport]bool{}
	for _, pi := range infos {
		out[pi.Transport] = true
	}
	return out
}

// echoCheck 在聚合流上跑一轮 echo 验证数据面（两侧所有存活路径参与条带）。
func echoCheck(t *testing.T, sa, sb *netacc.Stream, tag string) {
	t.Helper()
	go func() { _, _ = io.Copy(sb, sb) }()
	want := bytes.Repeat([]byte(tag), 16<<10)
	if _, err := sa.Write(want); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo 数据不一致（%s）", tag)
	}
}

// connectTCPOnly 只把 b 的纯 TCP 地址喂给 peerstore，逼首条 swarm
// 连接（握手路径）走 TCP，避免默认拨号排序抢走待测传输的地址。
func connectTCPOnly(t *testing.T, a, b host.Host) {
	t.Helper()
	a.Peerstore().AddAddr(b.ID(), mustMultiaddr(t, tcpOnlyAddr(t, b)), peerstore.PermanentAddrTTL)
	if err := a.Connect(context.Background(), peer.AddrInfo{ID: b.ID()}); err != nil {
		t.Fatalf("连接 host 失败: %v", err)
	}
}

// TestAttachWebSocketPath 验收 #22：WS 路径可建立并参与聚合——
// TCP 建流 + /ws 直拨加路径，两侧 Paths() 都能区分传输类型。
func TestAttachWebSocketPath(t *testing.T) {
	ha := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0")
	hb := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws")
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectTCPOnly(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, addrWith(t, hb, "/ws"))); err != nil {
		t.Fatalf("WS AddPath 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	for name, infos := range map[string][]netacc.PathInfo{"sa": sa.Paths(), "sb": sb.Paths()} {
		ts := transportSet(infos)
		if !ts[netacc.TransportTCP] || !ts[netacc.TransportWebSocket] {
			t.Fatalf("%s 应为 TCP+WebSocket 异构路径: %+v", name, infos)
		}
	}
	echoCheck(t, sa, sb, "ws+tcp")
}

// TestAttachWebTransportPath 验收 #22：带 certhash 的 WT 地址
// 端到端直拨成功。certhash 取自对端监听地址（identify/peerstore
// 学到的即此形态；证书轮换后地址过期，调用方换新地址即可）。
func TestAttachWebTransportPath(t *testing.T) {
	ha := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0")
	hb := newTestHostOn(t,
		"/ip4/127.0.0.1/tcp/0",
		"/ip4/127.0.0.1/udp/0/quic-v1/webtransport",
	)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectTCPOnly(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	// 监听侧自动把 certhash 拼进对外地址
	wtAddr := addrWith(t, hb, "webtransport/certhash")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, wtAddr)); err != nil {
		t.Fatalf("WebTransport AddPath 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	for name, infos := range map[string][]netacc.PathInfo{"sa": sa.Paths(), "sb": sb.Paths()} {
		ts := transportSet(infos)
		if !ts[netacc.TransportTCP] || !ts[netacc.TransportWebTransport] {
			t.Fatalf("%s 应为 TCP+WebTransport 异构路径: %+v", name, infos)
		}
	}
	echoCheck(t, sa, sb, "wt+tcp")
}

// TestAttachWebRTCDirectPath 验收 #22：带 certhash 的 webrtc-direct
// 地址 go↔go loopback 直拨成功（go-libp2p 实现的是 WebRTC-Direct
// 无信令形态；/webrtc browser↔browser 未实现，不在本票范围）。
// 握手链 ICE→DTLS→SCTP→Noise 最重，给足超时。
func TestAttachWebRTCDirectPath(t *testing.T) {
	ha := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0")
	hb := newTestHostOn(t,
		"/ip4/127.0.0.1/tcp/0",
		"/ip4/127.0.0.1/udp/0/webrtc-direct",
	)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectTCPOnly(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	wrtcAddr := addrWith(t, hb, "webrtc-direct/certhash")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, wrtcAddr)); err != nil {
		t.Fatalf("WebRTC-direct AddPath 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	// 入向 webrtc 连接的对端地址是裸 /udp/...（从 ICE 候选反推，
	// 无 webrtc-direct 段）——传输分类依赖本端地址，两侧都应可判。
	for name, infos := range map[string][]netacc.PathInfo{"sa": sa.Paths(), "sb": sb.Paths()} {
		ts := transportSet(infos)
		if !ts[netacc.TransportTCP] || !ts[netacc.TransportWebRTCDirect] {
			t.Fatalf("%s 应为 TCP+WebRTCDirect 异构路径: %+v", name, infos)
		}
	}
	echoCheck(t, sa, sb, "wrtc+tcp")
}

// TestWebSocketTCPStriping 验收 #22：WS 与 TCP 路径并存条带，
// 双向数据都正确。
func TestWebSocketTCPStriping(t *testing.T) {
	ha := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0")
	hb := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws")
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectTCPOnly(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, tcpOnlyAddr(t, hb))); err != nil {
		t.Fatalf("TCP AddPath 失败: %v", err)
	}
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, addrWith(t, hb, "/ws"))); err != nil {
		t.Fatalf("WS AddPath 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 3", func() bool { return len(sb.Paths()) == 3 })

	ts := transportSet(sa.Paths())
	if !ts[netacc.TransportTCP] || !ts[netacc.TransportWebSocket] {
		t.Fatalf("应为 TCP+WS 并存路径: %+v", sa.Paths())
	}
	echoCheck(t, sa, sb, "ws-tcp-stripe")
}

// TestMinPathsPrefersTCPFamily 验收排序提示：peerstore 同时有
// TCP/WS/QUIC 地址时，WithMinPaths 补挂按 §3.3 偏好先吃 TCP 系
// （P0），不应把名额让给 UDP 系。
func TestMinPathsPrefersTCPFamily(t *testing.T) {
	ha := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0")
	hb := newTestHostOn(t,
		"/ip4/127.0.0.1/tcp/0",
		"/ip4/127.0.0.1/tcp/0/ws",
		"/ip4/127.0.0.1/udp/0/quic-v1",
	)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectTCPOnly(t, ha, hb)
	// 显式把 QUIC 与 WS 地址补进 peerstore（quic 在前——若不做
	// 偏好排序，peerstore 序会让 P1 的 QUIC 先被拨），消除对
	// identify 推送时序的依赖。
	ha.Peerstore().AddAddrs(hb.ID(), []ma.Multiaddr{
		mustMultiaddr(t, addrWith(t, hb, "/quic-v1")),
		mustMultiaddr(t, addrWith(t, hb, "/ws")),
	}, peerstore.PermanentAddrTTL)

	type res struct {
		s   *netacc.Stream
		err error
	}
	ach := make(chan res, 1)
	go func() {
		s, err := aggb.Accept(context.Background())
		ach <- res{s, err}
	}()
	sa, err := agga.OpenStream(context.Background(), hb.ID(), netacc.WithMinPaths(3))
	if err != nil {
		t.Fatalf("OpenStream 失败: %v", err)
	}
	defer sa.Close()
	r := <-ach
	if r.err != nil {
		t.Fatalf("Accept 失败: %v", r.err)
	}
	defer r.s.Close()

	// 3 条路径：TCP 握手路径 + 补挂的 TCP、WS——QUIC（P1）排最后
	// 不应被选中。
	ts := transportSet(sa.Paths())
	if len(sa.Paths()) != 3 {
		t.Fatalf("应有 3 条路径: %+v", sa.Paths())
	}
	if !ts[netacc.TransportWebSocket] || ts[netacc.TransportQUIC] {
		t.Fatalf("补挂应优先 TCP 系且不含 QUIC: %+v", sa.Paths())
	}
}
