package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/yangjuncode/netacc"
)

// StreamAccepter 抽象「接收对端发起的聚合流」的能力，
// *netacc.Aggregator 天然满足（签名与 Aggregator.Accept 一致）。
// 与 StreamOpener 同理：返回 *netacc.Stream 是为了让 Aggregator
// 直接实现本接口，tunnel 内部解耦成 net.Conn 使用。
//
// 实现须响应 ctx 取消并返回错误——Server.Run 的退出依赖它。
type StreamAccepter interface {
	Accept(ctx context.Context) (*netacc.Stream, error)
}

// ServerOption 是 NewServer 的构造选项。
type ServerOption interface{ applyServerOption(*serverConfig) }

// serverOption 是 ServerOption 的函数实现。
type serverOption func(*serverConfig)

func (f serverOption) applyServerOption(c *serverConfig) { f(c) }

// serverConfig 是 Server 的生效配置（构造默认 + 选项覆盖）。
type serverConfig struct {
	// upstream：固定上游地址 host:port（NewServer 校验必填）。
	upstream string
}

// WithUpstream 设置固定上游地址 host:port（必填）：每条被 Accept 的
// 聚合流都拨向它。上游只能由 server 侧配置，client 不可指定。
func WithUpstream(addr string) ServerOption {
	return serverOption(func(c *serverConfig) { c.upstream = addr })
}

// Server 是 tunnel 出口：Accept 聚合流，逐条桥接到固定上游 TCP
// 服务。用 NewServer 构造、Run 运行；单次使用——Run 返回后不可复用。
type Server struct {
	upstream string
	accept   func(ctx context.Context) (net.Conn, error)
	dialer   net.Dialer

	actives *connSet       // 活动聚合流 + 上游连接
	wg      sync.WaitGroup // 进行中的 serveStream handler
	ran     atomic.Bool    // Run 只允许进入一次
}

// NewServer 构造 tunnel server：accepter 用于接收聚合流
// （生产环境直接传 *netacc.Aggregator）。
//
// 参数缺失/非法（nil accepter、缺 WithUpstream 或地址不是
// host:port 形态）在构造期返回 error。
func NewServer(accepter StreamAccepter, opts ...ServerOption) (*Server, error) {
	if accepter == nil {
		return nil, errors.New("tunnel: StreamAccepter 不能为 nil")
	}
	cfg := serverConfig{}
	for _, o := range opts {
		if o != nil {
			o.applyServerOption(&cfg)
		}
	}
	if err := checkUpstream(cfg.upstream); err != nil {
		return nil, err
	}
	return newServer(func(ctx context.Context) (net.Conn, error) {
		st, err := accepter.Accept(ctx)
		if err != nil {
			return nil, err
		}
		return st, nil
	}, cfg.upstream), nil
}

// newServer 是内部构造：accept 已解耦成 net.Conn 粒度，测试可注入
// net.Pipe 等假实现而不必伪造 *netacc.Stream。
func newServer(accept func(ctx context.Context) (net.Conn, error), upstream string) *Server {
	return &Server{upstream: upstream, accept: accept, actives: newConnSet()}
}

// checkUpstream 构造期校验上游地址：必须是 host:port 完整形态，
// host 非空、端口为 1-65535 数字。域名 host 留到拨号时解析，
// 这里不做 DNS。
func checkUpstream(addr string) error {
	if addr == "" {
		return errors.New("tunnel: 缺少上游地址，须用 WithUpstream 指定 host:port")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("tunnel: 上游地址 %q 非法（须为 host:port 形态）: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("tunnel: 上游地址 %q 缺少 host", addr)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("tunnel: 上游地址 %q 的端口非法（须为 1-65535 数字）", addr)
	}
	return nil
}

// Run 阻塞 Accept 聚合流并逐条桥接至上游，至 ctx 取消（返回 nil）
// 或 Accept 报非取消错误（包装上下文后返回）。ctx 取消时主动断开
// 全部活动聚合流与上游连接，并等 handler 退出后才返回。
func (s *Server) Run(ctx context.Context) error {
	if !s.ran.CompareAndSwap(false, true) {
		return errors.New("tunnel: Server.Run 只能调用一次")
	}
	stop := context.AfterFunc(ctx, s.actives.closeAll)
	defer func() {
		stop()
		s.actives.closeAll() // 幂等；覆盖非取消路径的收尾
		s.wg.Wait()          // 等全部 bridge 收尾，不泄漏 goroutine
	}()

	for {
		st, err := s.accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // ctx 取消视为正常退出
			}
			return fmt.Errorf("tunnel: 接收聚合流失败: %w", err)
		}
		s.actives.add(st)
		s.wg.Add(1)
		go s.serveStream(ctx, st)
	}
}

// serveStream 处理一条聚合流：net.Dialer 拨固定上游后双向转发。
// 上游拨号失败只关闭本条聚合流，Accept 循环不受影响。
func (s *Server) serveStream(ctx context.Context, st net.Conn) {
	defer s.wg.Done()
	defer s.actives.del(st)
	defer st.Close()

	up, err := s.dialer.DialContext(ctx, "tcp", s.upstream)
	if err != nil {
		return
	}
	s.actives.add(up)
	defer s.actives.del(up)
	defer up.Close()

	bridge(st, up)
}
