package netacc_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/yangjuncode/netacc"
	netaccrelay "github.com/yangjuncode/netacc/relay"
)

// newRelayHost 起一台跑本库 netaccrelay 组件的测试中继 host。
// 传输用默认集（不挂公平限速装饰器——本票验证电路转发功能，
// 限速本身由 relay 包自身测试覆盖）。
func newRelayHost(t *testing.T, listen []string, opts ...netaccrelay.Option) (host.Host, *netaccrelay.Relay) {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings(listen...))
	if err != nil {
		t.Fatalf("创建中继 host 失败: %v", err)
	}
	r, err := netaccrelay.New(h, opts...)
	if err != nil {
		h.Close()
		t.Fatalf("启动中继组件失败: %v", err)
	}
	t.Cleanup(func() { r.Close(); h.Close() })
	return h, r
}

// relayedPathInfos 从 Paths() 里挑出中继路径（远端 multiaddr 含
// /p2p-circuit 的那条）。
func relayedPathInfos(infos []netacc.PathInfo) []netacc.PathInfo {
	var out []netacc.PathInfo
	for _, pi := range infos {
		if strings.Contains(pi.Remote.String(), "p2p-circuit") {
			out = append(out, pi)
		}
	}
	return out
}

// TestRelayPathBasic 验收：A 经中继 R 与 B 建立中继路径并加入聚合流，
// 两侧路径集都为 2、中继路径可与直连路径区分，双向往返数据正确。
func TestRelayPathBasic(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	// 白名单须同时含 A 与 B：B 要 RESERVE（AllowReserve），
	// A 要 CONNECT（AllowConnect 校验源+目的两端）
	hr, _ := newRelayHost(t, []string{"/ip4/127.0.0.1/tcp/0"},
		netaccrelay.WithWhitelist(ha.ID(), hb.ID()))
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pathID, err := sa.AddRelayPath(ctx, peer.AddrInfo{ID: hr.ID(), Addrs: hr.Addrs()})
	if err != nil {
		t.Fatalf("AddRelayPath 失败: %v", err)
	}
	if pathID != 2 {
		t.Fatalf("发起方命名空间应为偶数 id，期望 2，得到 %d", pathID)
	}
	if n := len(sa.Paths()); n != 2 {
		t.Fatalf("sa 应有 2 条路径，得到 %d", n)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	// 中继路径在两侧都可区分：远端地址含 /p2p-circuit；
	// 直连路径（含握手路径）不含
	if rp := relayedPathInfos(sa.Paths()); len(rp) != 1 || !rp[0].Dialed {
		t.Fatalf("sa 应恰有 1 条本端发起的中继路径: %+v", sa.Paths())
	}
	if rp := relayedPathInfos(sb.Paths()); len(rp) != 1 || rp[0].Dialed {
		t.Fatalf("sb 应恰有 1 条对端发起的中继路径: %+v", sb.Paths())
	}
	// B 侧 swarm 连接表里应看到入向中继连接（直连 + 电路共 2 条）
	if n := len(hb.Network().ConnsToPeer(ha.ID())); n != 2 {
		t.Fatalf("hb 应有 2 条到 ha 的连接（直连+中继），得到 %d", n)
	}

	// 双向往返数据正确（条带会同时压到中继路径上）
	go func() { _, _ = io.Copy(sb, sb) }()
	want := bytes.Repeat([]byte("relay-path"), 32<<10) // 320KiB，多帧跨两路径
	if _, err := sa.Write(want); err != nil {
		t.Fatalf("a→b Write 失败: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("a Read 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("经中继路径 echo 数据不一致")
	}

	// 再证明中继路径确实承载数据：摘掉直连路径（path 0）后
	// 只剩中继路径，echo 仍应正常
	if err := sa.RemovePath(0); err != nil {
		t.Fatalf("RemovePath(0) 失败: %v", err)
	}
	eventually(t, "sa 仅剩中继路径", func() bool { return len(sa.Paths()) == 1 })
	want2 := bytes.Repeat([]byte("relay-only"), 16<<10)
	if _, err := sa.Write(want2); err != nil {
		t.Fatalf("纯中继路径 Write 失败: %v", err)
	}
	got2 := make([]byte, len(want2))
	if _, err := io.ReadFull(sa, got2); err != nil {
		t.Fatalf("纯中继路径 Read 失败: %v", err)
	}
	if !bytes.Equal(got2, want2) {
		t.Fatal("纯中继路径 echo 数据不一致")
	}

	// 按需协调可重复：再加一条经同一中继的中继路径
	// （对端重新做一次 reservation，path_id 走新号不复用）
	pathID2, err := sa.AddRelayPath(ctx, peer.AddrInfo{ID: hr.ID(), Addrs: hr.Addrs()})
	if err != nil {
		t.Fatalf("第二次 AddRelayPath 失败: %v", err)
	}
	if pathID2 != 4 {
		t.Fatalf("第二条中继路径应分到 id 4，得到 %d", pathID2)
	}
	eventually(t, "sa 路径数收敛到 2", func() bool { return len(sa.Paths()) == 2 })
}

// TestRelayPathHopPreference 验收：A→C 段传输可指定（WebSocket），
// C→B 段取对端 reservation 所用传输——两段互相独立。
// R 同时监听 TCP+WS；B 预先用 TCP 连 R（其 reservation 必走 TCP），
// A 的路径描述符把 /ws 地址放最前（A→C 必走 WS）。
func TestRelayPathHopPreference(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	hr, _ := newRelayHost(t,
		[]string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/tcp/0/ws"},
		netaccrelay.WithWhitelist(ha.ID(), hb.ID()))

	// B 预先经 TCP 连上 R：随后 reservation 的 hop 流复用该连接，
	// 确定性地让 C→B 段 = TCP。注意 ws 地址形态也含 /tcp/，
	// 挑纯 TCP 地址要排除 /ws。
	var tcpAddr, wsAddr ma.Multiaddr
	for _, a := range hr.Addrs() {
		if strings.Contains(a.String(), "/ws") {
			wsAddr = a
		} else if strings.Contains(a.String(), "/tcp/") {
			tcpAddr = a
		}
	}
	if tcpAddr == nil || wsAddr == nil {
		t.Fatalf("中继应有 TCP+WS 双监听地址: %v", hr.Addrs())
	}
	if err := hb.Connect(context.Background(),
		peer.AddrInfo{ID: hr.ID(), Addrs: []ma.Multiaddr{tcpAddr}}); err != nil {
		t.Fatalf("B 连中继失败: %v", err)
	}
	connectHosts(t, ha, hb)
	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// 地址集顺序即 A→C 偏好：ws 在前 → 电路地址内嵌 ws 地址
	if _, err := sa.AddRelayPath(ctx, peer.AddrInfo{ID: hr.ID(), Addrs: []ma.Multiaddr{wsAddr, tcpAddr}}); err != nil {
		t.Fatalf("AddRelayPath(WS 偏好) 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	// A→C 段 = WS：A 侧中继路径的 Remote = <ws-addr>/p2p/<R>/p2p-circuit
	rp := relayedPathInfos(sa.Paths())
	if len(rp) != 1 || !strings.Contains(rp[0].Remote.String(), "/ws/") {
		t.Fatalf("A→C 段应走 WebSocket，实际: %+v", sa.Paths())
	}
	// C→B 段 = B reservation 所用连接（B 预先 TCP 连 R → TCP）：
	// B 侧中继路径的 Remote = <B↔R 连接对端地址>/p2p/<R>/p2p-circuit
	rpb := relayedPathInfos(sb.Paths())
	if len(rpb) != 1 {
		t.Fatalf("sb 应恰有 1 条中继路径: %+v", sb.Paths())
	}
	if strings.Contains(rpb[0].Remote.String(), "/ws/") ||
		!strings.Contains(rpb[0].Remote.String(), "/tcp/") {
		t.Fatalf("C→B 段应沿用 B 的 TCP reservation 连接，实际: %s", rpb[0].Remote)
	}
}

// TestRelayPathKillSurvives 验收：杀掉中继路径底层连接后路径自动摘除，
// 不影响其余路径、流不中断、在途数据不丢。
func TestRelayPathKillSurvives(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	hr, _ := newRelayHost(t, []string{"/ip4/127.0.0.1/tcp/0"},
		netaccrelay.WithWhitelist(ha.ID(), hb.ID()))
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddRelayPath(ctx, peer.AddrInfo{ID: hr.ID(), Addrs: hr.Addrs()}); err != nil {
		t.Fatalf("AddRelayPath 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	go func() { _, _ = io.Copy(sb, sb) }()
	if _, err := sa.Write(bytes.Repeat([]byte("x"), 32<<10)); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}

	// 杀掉中继路径的底层连接：B 侧到 A 的连接里 RemoteMultiaddr
	// 含 /p2p-circuit 的那条（A↔B 直连连接保留）
	killed := false
	for _, c := range hb.Network().ConnsToPeer(ha.ID()) {
		if strings.Contains(c.RemoteMultiaddr().String(), "p2p-circuit") {
			_ = c.Close()
			killed = true
		}
	}
	if !killed {
		t.Fatal("没找到中继路径的底层连接")
	}

	// 流不中断：剩余数据照常经直连路径送达
	want := bytes.Repeat([]byte("after-kill"), 24<<10)
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(want)
		wdone <- err
	}()
	got := make([]byte, 32<<10+len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("杀中继路径后 Read 失败: %v", err)
	}
	if err := <-wdone; err != nil {
		t.Fatalf("杀中继路径后 Write 失败: %v", err)
	}
	wantAll := append(bytes.Repeat([]byte("x"), 32<<10), want...)
	if !bytes.Equal(got, wantAll) {
		t.Fatal("杀中继路径后 echo 数据不一致")
	}
	eventually(t, "sa 路径数收敛到 1", func() bool { return len(sa.Paths()) == 1 })
	eventually(t, "sb 路径数收敛到 1", func() bool { return len(sb.Paths()) == 1 })
}

// TestRelayPathReserveDenied 验收：对端向中继的 reservation 被白名单
// 拒绝时，AddRelayPath 透传错误返回（不 panic、不死等）。
func TestRelayPathReserveDenied(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	// 白名单只含 A：B 的 RESERVE 被 AllowReserve 拒绝
	hr, _ := newRelayHost(t, []string{"/ip4/127.0.0.1/tcp/0"},
		netaccrelay.WithWhitelist(ha.ID()))
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddRelayPath(ctx, peer.AddrInfo{ID: hr.ID(), Addrs: hr.Addrs()}); err == nil {
		t.Fatal("reservation 被拒时 AddRelayPath 应报错")
	}
	if n := len(sa.Paths()); n != 1 {
		t.Fatalf("失败后 sa 应只剩首条路径，得到 %d", n)
	}
}

// TestRelayPathConnectDenied 验收：reservation 成功但中继拒绝 CONNECT
// （源端 A 不在白名单）时，AddRelayPath 透传拨号错误。
func TestRelayPathConnectDenied(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	// 白名单只含 B：B 能 RESERVE；A 的 CONNECT 过不了 AllowConnect
	hr, _ := newRelayHost(t, []string{"/ip4/127.0.0.1/tcp/0"},
		netaccrelay.WithWhitelist(hb.ID()))
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddRelayPath(ctx, peer.AddrInfo{ID: hr.ID(), Addrs: hr.Addrs()}); err == nil {
		t.Fatal("CONNECT 被拒时 AddRelayPath 应报错")
	}
}

// TestRelayPathSymmetric 验收：接收侧也能发起中继路径（对称性，
// 覆盖 NAT 后只能出向的一端）——path_id 落接收方奇数命名空间。
func TestRelayPathSymmetric(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	hr, _ := newRelayHost(t, []string{"/ip4/127.0.0.1/tcp/0"},
		netaccrelay.WithWhitelist(ha.ID(), hb.ID()))
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pathID, err := sb.AddRelayPath(ctx, peer.AddrInfo{ID: hr.ID(), Addrs: hr.Addrs()})
	if err != nil {
		t.Fatalf("接收侧 AddRelayPath 失败: %v", err)
	}
	if pathID != 1 {
		t.Fatalf("接收方命名空间应为奇数 id，期望 1，得到 %d", pathID)
	}
	eventually(t, "sa 路径数收敛到 2", func() bool { return len(sa.Paths()) == 2 })

	go func() { _, _ = io.Copy(sa, sa) }()
	want := bytes.Repeat([]byte("sym-relay"), 32<<10)
	if _, err := sb.Write(want); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sb, got); err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("对称中继路径 echo 数据不一致")
	}
}
