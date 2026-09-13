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
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/internal/pb"
)

// newTestHostOn 建一个监听给定 multiaddr 的 libp2p host。
func newTestHostOn(t *testing.T, listenAddrs ...string) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrs...))
	if err != nil {
		t.Fatalf("创建 host 失败: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// addrWith 从 host 监听地址里挑出包含 kind 子串的一条（如 "tcp"/"quic-v1"）。
func addrWith(t *testing.T, h host.Host, kind string) string {
	t.Helper()
	for _, a := range h.Addrs() {
		if strings.Contains(a.String(), kind) {
			return a.String()
		}
	}
	t.Fatalf("host 没有含 %q 的监听地址: %v", kind, h.Addrs())
	return ""
}

// mustMultiaddr 解析 multiaddr 字符串。
func mustMultiaddr(t *testing.T, s string) ma.Multiaddr {
	t.Helper()
	m, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatalf("解析 multiaddr %q 失败: %v", s, err)
	}
	return m
}

// eventuallyN 轮询等待 cond 成立，防异步路径摘除/挂接造成测试抖动。
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// TestAttachSecondPath 验收：建流后 transport 直拨加第二条 TCP 路径，
// 两侧路径集都为 2、底层连接归属可区分，双向往返数据正确。
func TestAttachSecondPath(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pathID, err := sa.AddPath(ctx, mustMultiaddr(t, addrWith(t, hb, "/tcp/")))
	if err != nil {
		t.Fatalf("AddPath 失败: %v", err)
	}
	if pathID != 2 {
		t.Fatalf("发起方命名空间应为偶数 id，首挂路径期望 2，得到 %d", pathID)
	}
	if n := len(sa.Paths()); n != 2 {
		t.Fatalf("sa 应有 2 条路径，得到 %d", n)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	// 验收 3：同 peer 多连接并存，路径归属不同底层连接
	if conns := hb.Network().ConnsToPeer(ha.ID()); len(conns) != 2 {
		t.Fatalf("hb 侧应有 2 条到 ha 的连接，得到 %d", len(conns))
	}
	pi := sb.Paths()
	if pi[0].ConnID == "" || pi[0].ConnID == pi[1].ConnID {
		t.Fatalf("两条路径应归属不同连接: %+v", pi)
	}
	// 归属视角：两条路径都是 sa 发起建立的——接收侧 Dialed 全为
	// false，发起侧全为 true。
	for _, p := range pi {
		if p.Dialed {
			t.Fatalf("接收侧不应有 Dialed 路径: %+v", pi)
		}
	}
	for _, p := range sa.Paths() {
		if !p.Dialed {
			t.Fatalf("发起侧路径 Dialed 应全为 true: %+v", sa.Paths())
		}
	}

	// 双向往返：a→b 与 b→a 都经条带路径收发
	if _, err := sa.Write(bytes.Repeat([]byte("ab"), 64<<10)); err != nil {
		t.Fatalf("a→b Write 失败: %v", err)
	}
	if _, err := sb.Write(bytes.Repeat([]byte("ba"), 64<<10)); err != nil {
		t.Fatalf("b→a Write 失败: %v", err)
	}
	gotA := make([]byte, 128<<10)
	gotB := make([]byte, 128<<10)
	if _, err := io.ReadFull(sa, gotA); err != nil {
		t.Fatalf("a Read 失败: %v", err)
	}
	if _, err := io.ReadFull(sb, gotB); err != nil {
		t.Fatalf("b Read 失败: %v", err)
	}
	if !bytes.Equal(gotA, bytes.Repeat([]byte("ba"), 64<<10)) ||
		!bytes.Equal(gotB, bytes.Repeat([]byte("ab"), 64<<10)) {
		t.Fatal("双向数据不一致")
	}
	sa.Close()
	sb.Close()
}

// TestAttachQUICPath 验收：TCP 建流 + QUIC 直拨加路径——真正做到
// 「不同传输」的多路径（规格书 §1/§3.3）。
func TestAttachQUICPath(t *testing.T) {
	ha := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0")
	hb := newTestHostOn(t, "/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1")
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	// 只把 TCP 地址喂给 peerstore，逼首条 swarm 连接走 TCP
	// （否则默认拨号排序偏好 QUIC，握手路径会抢占 QUIC，
	// 异构路径就无从谈起）。
	ha.Peerstore().AddAddr(hb.ID(), mustMultiaddr(t, addrWith(t, hb, "/tcp/")), peerstore.PermanentAddrTTL)
	if err := ha.Connect(context.Background(), peer.AddrInfo{ID: hb.ID()}); err != nil {
		t.Fatalf("连接 host 失败: %v", err)
	}

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, addrWith(t, hb, "quic-v1"))); err != nil {
		t.Fatalf("QUIC AddPath 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	// 两条路径应为不同传输：一 tcp 一 quic
	var sawTCP, sawQUIC bool
	for _, pi := range sb.Paths() {
		r := pi.Remote.String()
		sawTCP = sawTCP || strings.Contains(r, "/tcp/")
		sawQUIC = sawQUIC || strings.Contains(r, "quic")
	}
	if !sawTCP || !sawQUIC {
		t.Fatalf("期望 TCP+QUIC 异构路径: %+v", sb.Paths())
	}

	// 异构路径上 echo 正确
	go func() { _, _ = io.Copy(sb, sb) }()
	want := bytes.Repeat([]byte("quic+tcp"), 16<<10)
	if _, err := sa.Write(want); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("TCP+QUIC 双路径 echo 数据不一致")
	}
}

// TestSymmetricAttach 验收：接收侧也能主动加路径（对称性）——
// sb（Accept 方）直拨 ha 的地址完成 PATH_ATTACH。
func TestSymmetricAttach(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pathID, err := sb.AddPath(ctx, mustMultiaddr(t, addrWith(t, ha, "/tcp/")))
	if err != nil {
		t.Fatalf("接收侧 AddPath 失败: %v", err)
	}
	if pathID != 1 {
		t.Fatalf("接收方命名空间应为奇数 id，首挂期望 1，得到 %d", pathID)
	}
	eventually(t, "sa 路径数收敛到 2", func() bool { return len(sa.Paths()) == 2 })
	if n := len(sb.Paths()); n != 2 {
		t.Fatalf("sb 应有 2 条路径，得到 %d", n)
	}

	// 反向挂上的路径同样承载数据
	go func() { _, _ = io.Copy(sa, sa) }()
	want := bytes.Repeat([]byte("sym"), 32<<10)
	if _, err := sb.Write(want); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(sb, got); err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("对称加路径后 echo 数据不一致")
	}
}

// TestPathConnKillSurvives 验收：对端视角直接杀掉路径底层连接，
// 流不中断、已发数据不丢、两侧路径集都自动摘除。
func TestPathConnKillSurvives(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()

	// 记下原有连接，AddPath 后多出的那条即新路径所在连接
	oldConns := map[string]bool{}
	for _, c := range hb.Network().ConnsToPeer(ha.ID()) {
		oldConns[c.ID()] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sa.AddPath(ctx, mustMultiaddr(t, addrWith(t, hb, "/tcp/"))); err != nil {
		t.Fatalf("AddPath 失败: %v", err)
	}
	eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) == 2 })

	go func() { _, _ = io.Copy(sb, sb) }()
	// 写一块数据确认在途，然后杀掉新连接
	if _, err := sa.Write(bytes.Repeat([]byte("x"), 32<<10)); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	var killed bool
	for _, c := range hb.Network().ConnsToPeer(ha.ID()) {
		if !oldConns[c.ID()] {
			_ = c.Close()
			killed = true
		}
	}
	if !killed {
		t.Fatal("没找到 AddPath 新建的底层连接")
	}

	// 流不中断：继续写读全量数据
	want := bytes.Repeat([]byte("after-kill"), 24<<10)
	wdone := make(chan error, 1)
	go func() {
		_, err := sa.Write(want)
		wdone <- err
	}()
	got := make([]byte, 32<<10+len(want))
	if _, err := io.ReadFull(sa, got); err != nil {
		t.Fatalf("杀连接后 Read 失败: %v", err)
	}
	if err := <-wdone; err != nil {
		t.Fatalf("杀连接后 Write 失败: %v", err)
	}
	wantAll := append(bytes.Repeat([]byte("x"), 32<<10), want...)
	if !bytes.Equal(got, wantAll) {
		t.Fatal("杀连接后 echo 数据不一致（在途数据丢失）")
	}
	eventually(t, "sa 路径数收敛到 1", func() bool { return len(sa.Paths()) == 1 })
	eventually(t, "sb 路径数收敛到 1", func() bool { return len(sb.Paths()) == 1 })
}

// TestConnManagerProtect 验收：聚合用 peer 已被 ConnManager().Protect。
func TestConnManagerProtect(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, sb := openPair(t, agga, aggb, hb)
	defer sa.Close()
	defer sb.Close()
	// Protect 在 OpenStream/Accept 内完成（规格 §3.2）
	if !ha.ConnManager().IsProtected(hb.ID(), "netacc") {
		t.Fatal("OpenStream 后 ha 应已 Protect hb")
	}
	if !hb.ConnManager().IsProtected(ha.ID(), "netacc") {
		t.Fatal("Accept 后 hb 应已 Protect ha")
	}
}

// TestAttachUnknownStreamReset 验收：PATH_ATTACH 携带不认识的
// agg_stream_id → 子流被 reset（规格 §4.2 绑定校验）。
func TestAttachUnknownStreamReset(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	_, aggb := netacc.New(ha), netacc.New(hb)
	defer aggb.Close()
	connectHosts(t, ha, hb)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	st, err := ha.NewStream(ctx, hb.ID(), netacc.PathProtocolID)
	if err != nil {
		t.Fatalf("开路径绑定流失败: %v", err)
	}
	defer st.Close()

	// 手搓 PATH_ATTACH 帧：type=3 | body_len | protobuf 体
	var bogus [16]byte
	bogus[0] = 0xff
	body, err := proto.Marshal(&pb.PathAttach{AggStreamId: bogus[:], PathId: 0})
	if err != nil {
		t.Fatal(err)
	}
	frame := append([]byte{3, byte(len(body))}, body...)
	if _, err := st.Write(frame); err != nil {
		t.Fatalf("写 PATH_ATTACH 失败: %v", err)
	}
	st.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := st.Read(make([]byte, 1)); err == nil {
		t.Fatal("未知 agg_stream_id 的 PATH_ATTACH 应被 reset")
	}
}

// TestMinPaths 验收：WithMinPaths(2) 时 OpenStream 自动补挂到 2 条路径。
func TestMinPaths(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)

	ctx := context.Background()
	ach := make(chan *netacc.Stream, 1)
	go func() {
		s, err := aggb.Accept(ctx)
		if err == nil {
			ach <- s
		}
	}()
	sa, err := agga.OpenStream(ctx, hb.ID(), netacc.WithMinPaths(2))
	if err != nil {
		t.Fatalf("WithMinPaths(2) OpenStream 失败: %v", err)
	}
	defer sa.Close()
	if n := len(sa.Paths()); n < 2 {
		t.Fatalf("WithMinPaths(2) 应至少挂 2 条路径，得到 %d", n)
	}
	select {
	case sb := <-ach:
		defer sb.Close()
		eventually(t, "sb 路径数收敛到 2", func() bool { return len(sb.Paths()) >= 2 })
	case <-time.After(15 * time.Second):
		t.Fatal("Accept 超时")
	}
}
