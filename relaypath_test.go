package netacc

import (
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc/internal/pb"
)

// mustPeerID 生成一个真实 peerID（帧体编解码要求合法 multihash）。
func mustPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// TestPathRequestMarshalRoundTrip 单测 PATH_REQUEST 帧体编码：
// relay peerID 与地址集往返一致，/p2p/<peer> 后缀被剥离。
func TestPathRequestMarshalRoundTrip(t *testing.T) {
	pid := mustPeerID(t)
	plain := ma.StringCast("/ip4/127.0.0.1/tcp/4001")
	suffixed := ma.StringCast("/ip4/127.0.0.1/tcp/4002/ws/p2p/" + pid.String())

	body, err := marshalPathRequest(7, peer.AddrInfo{ID: pid, Addrs: []ma.Multiaddr{plain, suffixed}}, plain)
	if err != nil {
		t.Fatal(err)
	}
	var req pb.PathRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		t.Fatal(err)
	}
	if req.GetRequestId() != 7 {
		t.Fatalf("request_id 应为 7，得到 %d", req.GetRequestId())
	}
	if got := req.GetAcAddr(); string(got) != string(plain.Bytes()) {
		t.Fatalf("ac_addr 应为选定的 A→C 地址: %v", got)
	}
	relay, err := decodeRelayInfo(&req)
	if err != nil {
		t.Fatal(err)
	}
	if relay.ID != pid {
		t.Fatalf("relay peerID 不一致: %s != %s", relay.ID, pid)
	}
	if len(relay.Addrs) != 2 {
		t.Fatalf("应有 2 条中继地址，得到 %d", len(relay.Addrs))
	}
	if relay.Addrs[0].String() != "/ip4/127.0.0.1/tcp/4001" {
		t.Fatalf("地址 0 不一致: %s", relay.Addrs[0])
	}
	if relay.Addrs[1].String() != "/ip4/127.0.0.1/tcp/4002/ws" {
		t.Fatalf("/p2p 后缀应被剥离: %s", relay.Addrs[1])
	}
}

// TestDecodeRelayInfoBadPeer 单测：relay_peer 非法时 decodeRelayInfo 报错，
// 调用方据此回 PATH_READY{error}。
func TestDecodeRelayInfoBadPeer(t *testing.T) {
	if _, err := decodeRelayInfo(&pb.PathRequest{RelayPeer: []byte{0x01, 0x02}}); err == nil {
		t.Fatal("非法 relay_peer 应报错")
	}
}

// TestPathReadyMarshalRoundTrip 单测 PATH_READY 帧体：error 字段往返。
func TestPathReadyMarshalRoundTrip(t *testing.T) {
	body, err := marshalPathReady(3, errors.New("reservation 被拒"))
	if err != nil {
		t.Fatal(err)
	}
	var pr pb.PathReady
	if err := proto.Unmarshal(body, &pr); err != nil {
		t.Fatal(err)
	}
	if pr.GetRequestId() != 3 || pr.GetError() != "reservation 被拒" {
		t.Fatalf("PATH_READY 往返不一致: request_id=%d error=%q", pr.GetRequestId(), pr.GetError())
	}
	body, err = marshalPathReady(4, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(body, &pr); err != nil {
		t.Fatal(err)
	}
	if pr.GetRequestId() != 4 || pr.GetError() != "" {
		t.Fatalf("成功 PATH_READY 往返不一致: request_id=%d error=%q", pr.GetRequestId(), pr.GetError())
	}
}

// TestBuildCircuitAddr 单测电路地址拼接形态。
func TestBuildCircuitAddr(t *testing.T) {
	relayID := mustPeerID(t)
	dest := mustPeerID(t)
	addr := buildCircuitAddr(ma.StringCast("/ip4/127.0.0.1/tcp/4001/ws"), relayID, dest)
	want := "/ip4/127.0.0.1/tcp/4001/ws/p2p/" + relayID.String() + "/p2p-circuit/p2p/" + dest.String()
	if addr.String() != want {
		t.Fatalf("电路地址形态不对: %s", addr)
	}
}
