package netaccrelay

import (
	"context"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/tcpreuse"
	ma "github.com/multiformats/go-multiaddr"
)

// ---------- transport 装饰器（公平带宽分配的数据面） ----------

// limStream 是套了读侧限速的 MuxedStream。
// 限速全靠回压传导：读出的 n 字节按实消耗令牌、不足则 sleep——
// 中继读循环被减速 → 底层连接接收窗口收缩 → 对端发送自动降速，
// 不丢包、不碰 ACK。
//
// 注意必须按底层实际返回的 n 取令牌（而非读前的 len(p)）：yamux 常返回
// 不足一帧的部分读，按 len(p) 预取会让令牌消耗快于实际字节、有效速率被
// 打到配额的一半以下（实测恰好 ~R/2）。
type limStream struct {
	network.MuxedStream
	a *Allocator
	b *bucket
}

func (s *limStream) Read(p []byte) (int, error) {
	// 单次读不超过一个拷贝块，保证 n ≤ burst 且各流交织充分
	if len(p) > maxChunk {
		p = p[:maxChunk]
	}
	n, err := s.MuxedStream.Read(p)
	if n > 0 {
		s.a.accountRead(s.b, n)
		s.a.throttle(s.b, n)
	}
	return n, err
}

// shapedConn 装饰 transport.CapableConn：AcceptStream/OpenStream 产出的
// MuxedStream 全部按对端 peerID 套共享限速桶。
// 对 relay/identify/协议协商完全透明——接口不变，仅 Read 变慢。
type shapedConn struct {
	transport.CapableConn
	a *Allocator
}

func (c *shapedConn) wrap(s network.MuxedStream, err error) (network.MuxedStream, error) {
	if err != nil {
		return nil, err
	}
	return &limStream{MuxedStream: s, a: c.a, b: c.a.bucketFor(c.RemotePeer())}, nil
}

func (c *shapedConn) AcceptStream() (network.MuxedStream, error) {
	return c.wrap(c.CapableConn.AcceptStream())
}

func (c *shapedConn) OpenStream(ctx context.Context) (network.MuxedStream, error) {
	return c.wrap(c.CapableConn.OpenStream(ctx))
}

// shapingListener 把 Accept 出的连接逐个包装为 shapedConn。
type shapingListener struct {
	transport.Listener
	a *Allocator
}

func (l *shapingListener) Accept() (transport.CapableConn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &shapedConn{CapableConn: c, a: l.a}, nil
}

// shapingTpt 是 transport.Transport 装饰器：Listen/Dial 返回的连接
// 全部替换为 shapedConn，其余方法（CanDial/Proxy/Protocols 等）透传。
type shapingTpt struct {
	transport.Transport
	a *Allocator
}

func (t *shapingTpt) Listen(addr ma.Multiaddr) (transport.Listener, error) {
	l, err := t.Transport.Listen(addr)
	if err != nil {
		return nil, err
	}
	return &shapingListener{Listener: l, a: t.a}, nil
}

func (t *shapingTpt) Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (transport.CapableConn, error) {
	c, err := t.Transport.Dial(ctx, raddr, p)
	if err != nil {
		return nil, err
	}
	return &shapedConn{CapableConn: c, a: t.a}, nil
}

// WrapTransport 装饰一个已构造好的传输：其 Listen/Dial 产出的连接上
// 所有流都按对端 peerID 套共享限速桶，可包任意传输（TCP/QUIC/WebSocket 等）。
// 包装传输构造函数传给 libp2p.Transport 的写法：
//
//	libp2p.Transport(func(u transport.Upgrader, rcmgr network.ResourceManager) (transport.Transport, error) {
//	    t, err := someTpt.New(u, rcmgr)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return alloc.WrapTransport(t), nil
//	})
func (a *Allocator) WrapTransport(t transport.Transport) transport.Transport {
	return &shapingTpt{Transport: t, a: a}
}

// TCPTransport 返回把「带公平限速的 TCP 传输」注册进 host 的 libp2p.Option，
// 与 libp2p.Transport(tcp.NewTCPTransport, opts...) 等价、只是多套一层装饰器。
// 需配合 libp2p.NoTransports 使用以避免默认传输重复注册：
//
//	h, _ := libp2p.New(libp2p.NoTransports, netaccrelay.TCPTransport(alloc), ...)
//
// 挂进来的传输必须与 New 的 WithAllocator 用同一个 alloc，限速才生效。
func TCPTransport(a *Allocator, opts ...tcp.Option) libp2p.Option {
	anyOpts := make([]any, len(opts))
	for i, o := range opts {
		anyOpts[i] = o
	}
	newTpt := func(u transport.Upgrader, rcmgr network.ResourceManager, shared *tcpreuse.ConnMgr, tOpts ...tcp.Option) (transport.Transport, error) {
		inner, err := tcp.NewTCPTransport(u, rcmgr, shared, tOpts...)
		if err != nil {
			return nil, err
		}
		return a.WrapTransport(inner), nil
	}
	return libp2p.Transport(newTpt, anyOpts...)
}

// 编译期接口断言。
var (
	_ transport.Transport   = (*shapingTpt)(nil)
	_ transport.Listener    = (*shapingListener)(nil)
	_ transport.CapableConn = (*shapedConn)(nil)
	_ network.MuxedStream   = (*limStream)(nil)
)
