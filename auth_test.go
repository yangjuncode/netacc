package netacc_test

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/yangjuncode/netacc"
	"github.com/yangjuncode/netacc/internal/pb"
)

var testToken = []byte("s3cr3t-token-0123456")

// acceptLoop 模拟常驻 Accept 的服务端：持续消费入向握手（含被拒的），
// 接受成功的聚合流推进 accepted 通道。没有它，被拒的 Hello 会堆在
// acceptBacklog 里不被处理，client 握手只能等超时。
func acceptLoop(agg *netacc.Aggregator, accepted chan<- *netacc.Stream) {
	for {
		s, err := agg.Accept(context.Background())
		if err != nil {
			return
		}
		accepted <- s
	}
}

// TestAuthToken：两端配同一 token 建流成功；不配或配错 token 的
// client 被拒，且能从 HelloAck.error 拿到 "unauthorized" 原因。
func TestAuthToken(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga := netacc.New(ha, netacc.WithAuthToken(testToken))
	defer agga.Close()
	aggb := netacc.New(hb, netacc.WithAuthToken(testToken))
	defer aggb.Close()
	connectHosts(t, ha, hb)

	accepted := make(chan *netacc.Stream, 4)
	go acceptLoop(aggb, accepted)

	ctx := context.Background()
	// 正确 token → 建流成功且 server 侧接受
	sa, err := agga.OpenStream(ctx, hb.ID())
	if err != nil {
		t.Fatalf("正确 token 建流失败: %v", err)
	}
	defer sa.Close()
	select {
	case sb := <-accepted:
		defer sb.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("server 侧未接受到聚合流")
	}

	// 错误 token → ErrHandshake + unauthorized
	hWrong := newTestHost(t)
	aggWrong := netacc.New(hWrong, netacc.WithAuthToken([]byte("wrong-token")))
	defer aggWrong.Close()
	connectHosts(t, hWrong, hb)
	if s, err := aggWrong.OpenStream(ctx, hb.ID()); err == nil {
		s.Close()
		t.Fatal("错误 token 应被拒")
	} else if !errors.Is(err, netacc.ErrHandshake) || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("期望 ErrHandshake 含 unauthorized，得到: %v", err)
	}

	// 不带 token → ErrHandshake + unauthorized（要求鉴权时对方不传也不行）
	hNone := newTestHost(t)
	aggNone := netacc.New(hNone)
	defer aggNone.Close()
	connectHosts(t, hNone, hb)
	if s, err := aggNone.OpenStream(ctx, hb.ID()); err == nil {
		s.Close()
		t.Fatal("不带 token 应被拒")
	} else if !errors.Is(err, netacc.ErrHandshake) || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("期望 ErrHandshake 含 unauthorized，得到: %v", err)
	}
}

// TestAuthTokenIgnoredByOpenServer：client 带了 token 但 server
// 未开鉴权 → 字段被忽略，照常建流。
func TestAuthTokenIgnoredByOpenServer(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga := netacc.New(ha, netacc.WithAuthToken(testToken))
	defer agga.Close()
	aggb := netacc.New(hb) // 不开鉴权
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, _ := openPair(t, agga, aggb, hb)
	defer sa.Close()
}

// TestAuthHandler：自定义校验器——收到对端 peerID 与凭证，
// 返回值决定去留；自定义拒绝原因经 HelloAck.error 透传给 client。
func TestAuthHandler(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga := netacc.New(ha, netacc.WithAuthToken([]byte("let-me-in")))
	defer agga.Close()

	var gotPeer peer.ID
	var gotAuth []byte
	aggb := netacc.New(hb, netacc.WithAuthHandler(func(p peer.ID, auth []byte) error {
		gotPeer, gotAuth = p, auth
		if string(auth) != "let-me-in" {
			return errors.New("banned")
		}
		return nil
	}))
	defer aggb.Close()
	connectHosts(t, ha, hb)

	accepted := make(chan *netacc.Stream, 4)
	go acceptLoop(aggb, accepted)

	ctx := context.Background()
	sa, err := agga.OpenStream(ctx, hb.ID())
	if err != nil {
		t.Fatalf("合法凭证建流失败: %v", err)
	}
	defer sa.Close()
	select {
	case sb := <-accepted:
		defer sb.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("server 侧未接受到聚合流")
	}
	if gotPeer != ha.ID() {
		t.Fatalf("handler 收到的 peerID 不符: %v", gotPeer)
	}
	if string(gotAuth) != "let-me-in" {
		t.Fatalf("handler 收到的凭证不符: %q", gotAuth)
	}

	// 凭证不符 → handler 的自定义原因透传给 client
	hBad := newTestHost(t)
	aggBad := netacc.New(hBad, netacc.WithAuthToken([]byte("knock-knock")))
	defer aggBad.Close()
	connectHosts(t, hBad, hb)
	if s, err := aggBad.OpenStream(ctx, hb.ID()); err == nil {
		s.Close()
		t.Fatal("handler 应拒绝不匹配凭证")
	} else if !errors.Is(err, netacc.ErrHandshake) || !strings.Contains(err.Error(), "banned") {
		t.Fatalf("期望 ErrHandshake 含 banned，得到: %v", err)
	}
}

// TestAuthHandlerOverridesToken：handler 与 token 同配时
// 接收方判定全权交给 handler（token 不参与）。
func TestAuthHandlerOverridesToken(t *testing.T) {
	ha, hb := newTestHost(t), newTestHost(t)
	agga := netacc.New(ha, netacc.WithAuthToken([]byte("client-token")))
	defer agga.Close()
	aggb := netacc.New(hb,
		netacc.WithAuthToken(testToken), // 与 client 不一致，但被 handler 覆盖
		netacc.WithAuthHandler(func(peer.ID, []byte) error { return nil }),
	)
	defer aggb.Close()
	connectHosts(t, ha, hb)

	sa, _ := openPair(t, agga, aggb, hb)
	defer sa.Close()
}

// TestAuthHandlerRejectReasons 验证拒绝总能回传非空、有效且有界的原因。
func TestAuthHandlerRejectReasons(t *testing.T) {
	cases := []struct {
		name, reason, want string
	}{
		{"空原因", "", "unauthorized"},
		{"中文跨截断边界", strings.Repeat("中", 86), strings.Repeat("中", 85)},
		{"混合字符边界", strings.Repeat("a", 255) + "中", strings.Repeat("a", 255)},
		{"四字节字符", strings.Repeat("😀", 65), strings.Repeat("😀", 64)},
		{"超长原因", strings.Repeat("x", 4096), strings.Repeat("x", 256)},
		{"非法UTF8", "拒绝\xff", "拒绝\uFFFD"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ha, hb := newTestHost(t), newTestHost(t)
			client := netacc.New(ha)
			defer client.Close()
			server := netacc.New(hb, netacc.WithAuthHandler(func(peer.ID, []byte) error {
				return errors.New(tc.reason)
			}))
			defer server.Close()
			connectHosts(t, ha, hb)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := server.Accept(ctx)
				done <- err
			}()
			s, err := client.OpenStream(ctx, hb.ID())
			if s != nil {
				s.Close()
			}
			cancel()
			<-done
			want := "netacc: " + netacc.ErrHandshake.Error() + ": 对端拒绝: " + tc.want
			if !errors.Is(err, netacc.ErrHandshake) || err.Error() != want {
				t.Fatalf("拒绝原因不符: got %v, want %s", err, want)
			}
		})
	}
}

// TestPathAttachRejectsForeignPeer：第三方 peer 凭获知的
// agg_stream_id 伪造 PATH_ATTACH 必须被 reset——绑定子流的
// 对端必须是聚合流对端本人。
func TestPathAttachRejectsForeignPeer(t *testing.T) {
	ha, hb, hc := newTestHost(t), newTestHost(t), newTestHost(t)
	agga, aggb := netacc.New(ha), netacc.New(hb)
	defer agga.Close()
	defer aggb.Close()
	connectHosts(t, ha, hb)
	connectHosts(t, hc, hb)

	sa, _ := openPair(t, agga, aggb, hb)
	defer sa.Close()
	aggID := sa.ID()

	// C 在到 B 的连接上开绑定子流，伪造 PATH_ATTACH 挂到 A↔B 的流上。
	// pathID=2 落在对端（发起方 A）命名空间——唯一会拦下它的校验
	// 就是「子流对端 != 聚合流对端」。
	ctx := context.Background()
	s, err := hc.NewStream(ctx, hb.ID(), netacc.PathProtocolID)
	if err != nil {
		t.Fatalf("开绑定子流失败: %v", err)
	}
	defer s.Close()

	body, err := proto.Marshal(&pb.PathAttach{AggStreamId: aggID[:], PathId: 2})
	if err != nil {
		t.Fatalf("编码 PATH_ATTACH 失败: %v", err)
	}
	var frame []byte
	frame = binary.AppendUvarint(frame, 3) // framePathAttach
	frame = binary.AppendUvarint(frame, uint64(len(body)))
	frame = append(frame, body...)
	if _, err := s.Write(frame); err != nil {
		t.Fatalf("写 PATH_ATTACH 失败: %v", err)
	}

	// 被拒 → 子流 reset，读不到回显
	_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, err := s.Read(make([]byte, 1)); err == nil {
		t.Fatalf("第三方 PATH_ATTACH 未被拒绝，读到 %d 字节", n)
	}
}
