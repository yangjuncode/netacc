package netacc

import (
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// Stream 是一条聚合流：对应用呈现 net.Conn 语义（可靠有序字节流）。
//
// 骨架版内部只有一条路径——握手流在握手完成后升格为数据路径，
// 读写直接透传到底层 network.Stream（yamux 流自带 deadline 支持）。
// 多路径接入后，这里换成「调度器 + 重排缓冲」的数据面实现。
type Stream struct {
	id [16]byte
	s  network.Stream
}

var _ net.Conn = (*Stream)(nil)

func newStream(id [16]byte, s network.Stream) *Stream {
	return &Stream{id: id, s: s}
}

// ID 返回握手协商出的 128bit agg_stream_id（聚合流标识）。
// 后续 PATH_ATTACH 凭此 ID 把新路径绑定到本流（规格书 §4.2）。
func (s *Stream) ID() [16]byte { return s.id }

// Peer 返回聚合流对端的 peer.ID。
func (s *Stream) Peer() peer.ID { return s.s.Conn().RemotePeer() }

func (s *Stream) Read(b []byte) (int, error)  { return s.s.Read(b) }
func (s *Stream) Write(b []byte) (int, error) { return s.s.Write(b) }

// Close 关闭聚合流：骨架版即关闭握手流（唯一路径）。
func (s *Stream) Close() error { return s.s.Close() }

// LocalAddr 返回本端 multiaddr（包成 net.Addr，见 maAddr）。
func (s *Stream) LocalAddr() net.Addr { return maAddr{s.s.Conn().LocalMultiaddr()} }

// RemoteAddr 返回对端 multiaddr（包成 net.Addr，见 maAddr）。
func (s *Stream) RemoteAddr() net.Addr { return maAddr{s.s.Conn().RemoteMultiaddr()} }

func (s *Stream) SetDeadline(t time.Time) error      { return s.s.SetDeadline(t) }
func (s *Stream) SetReadDeadline(t time.Time) error  { return s.s.SetReadDeadline(t) }
func (s *Stream) SetWriteDeadline(t time.Time) error { return s.s.SetWriteDeadline(t) }

// maAddr 把 multiaddr 适配成 net.Addr：
// network.Stream 的地址是 multiaddr 而非 net.Addr，net.Conn 接口需要后者。
type maAddr struct{ ma ma.Multiaddr }

func (a maAddr) Network() string { return "netacc" }

func (a maAddr) String() string {
	if a.ma == nil {
		return ""
	}
	return a.ma.String()
}
