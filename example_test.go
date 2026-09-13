package netacc_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
)

// Example 演示最小用法：构造 Aggregator（构造默认选项）→
// OpenStream（逐调用选项）→ 按 net.Conn 收发 → Stats 快照与
// 事件订阅。函数无 Output 断言，go test 只编译不运行。
func Example() {
	// 两侧各起一个 libp2p host（真实应用复用现有 host 即可）。
	ha, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		log.Fatal(err)
	}
	defer ha.Close()
	hb, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		log.Fatal(err)
	}
	defer hb.Close()

	// 构造默认：WithMinPaths 等选项在这里是 Aggregator 级默认值。
	agga := netacc.New(ha, netacc.WithMinPaths(1))
	defer agga.Close()
	aggb := netacc.New(hb)
	defer aggb.Close()

	// 拨号前先让 a 知道 b 的地址（真实部署经 identify/自己的渠道）。
	ha.Peerstore().AddAddrs(hb.ID(), hb.Addrs(), 0)
	if err := ha.Connect(context.Background(), peer.AddrInfo{ID: hb.ID()}); err != nil {
		log.Fatal(err)
	}

	// 接收侧：Accept 模型同 net.Listener。
	go func() {
		in, err := aggb.Accept(context.Background())
		if err != nil {
			return
		}
		defer in.Close()
		_, _ = io.Copy(in, in) // echo
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 逐调用选项覆盖构造默认：本次建流要求 ≥2 条数据路径。
	s, err := agga.OpenStream(ctx, hb.ID(), netacc.WithMinPaths(2))
	if err != nil {
		log.Fatal(err) // errors.Is(err, netacc.ErrMinPaths/ErrHandshake/...) 可判定
	}
	defer s.Close()

	// 事件订阅：路径增删/降级通知；缓冲满即丢，不阻塞数据面。
	events, unsub := s.Subscribe(16)
	defer unsub()
	go func() {
		for ev := range events {
			log.Printf("事件 %s path=%d dropped=%d cause=%v",
				ev.Type, ev.PathID, ev.Dropped, ev.Cause)
		}
	}()

	// 聚合流即 net.Conn：直接读写。
	if _, err := s.Write([]byte("hello")); err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(s, buf); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("echo: %s\n", buf)

	// Stats 快照：逐路径指标 + 聚合吞吐；只给数据，不接指标库。
	st := s.Stats()
	for _, p := range st.Paths {
		fmt.Printf("path %d: transport=%s srtt=%v est=%.0fB/s inflight=%d\n",
			p.ID, p.Transport, p.SRTT, p.EstRate, p.Inflight)
	}
}
