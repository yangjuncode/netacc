package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/yangjuncode/netacc"
)

// StreamOpener 抽象「向指定 peer 发起一条聚合流」的能力，
// *netacc.Aggregator 天然满足（签名与 Aggregator.OpenStream 一致）。
//
// 方法返回具体类型 *netacc.Stream 而非 net.Conn，是为了让
// Aggregator 无需包装直接实现本接口；tunnel 内部经函数适配器把它
// 解耦成 net.Conn（测试因此能用 net.Pipe 注入假流）。
type StreamOpener interface {
	OpenStream(ctx context.Context, p peer.ID, opts ...netacc.OpenOption) (*netacc.Stream, error)
}

// ClientOption 是 NewClient 的构造选项（沿用 netacc 的选项接口风格：
// 非导出方法使选项只能由本包的 WithXxx 构造函数产出）。
type ClientOption interface{ applyClientOption(*clientConfig) }

// clientOption 是 ClientOption 的函数实现。
type clientOption func(*clientConfig)

func (f clientOption) applyClientOption(c *clientConfig) { f(c) }

// clientConfig 是 Client 的生效配置（构造默认 + 选项覆盖）。
type clientConfig struct {
	// listenPort：TCP 监听端口；-1 表示未设置（NewClient 校验必填）。
	listenPort int
	// openOpts：逐连接透传给 StreamOpener.OpenStream 的选项。
	openOpts []netacc.OpenOption
}

// WithListenPort 设置 TCP 监听端口（必填，取值 0-65535）。
// 监听地址固定为 0.0.0.0:<port>——对公网/局域网可达，只想本机
// 访问请自行限制。port=0 由内核分配端口，经 Client.Addr() 取实际值。
func WithListenPort(port int) ClientOption {
	return clientOption(func(c *clientConfig) { c.listenPort = port })
}

// WithOpenOptions 追加透传给每次 OpenStream 的逐调用选项
// （如 netacc.WithMinPaths(2) 要求每条映射流至少挂两条路径）。
func WithOpenOptions(opts ...netacc.OpenOption) ClientOption {
	return clientOption(func(c *clientConfig) {
		c.openOpts = append(c.openOpts, opts...)
	})
}

// Client 是 tunnel 入口：监听 TCP 端口，把每条接入连接经一条
// 新建聚合流桥接到固定 server peer。用 NewClient 构造、Run 运行；
// 单次使用——Run 返回后不可复用。
type Client struct {
	port int
	open func(ctx context.Context) (net.Conn, error)

	actives *connSet       // 活动 TCP 连接 + 聚合流
	wg      sync.WaitGroup // 进行中的 serveTCP handler
	ran     atomic.Bool    // Run 只允许进入一次

	mu           sync.Mutex   // 保护 ln 与 shuttingDown
	ln           net.Listener // Run 期间非 nil；shutdown 时置回 nil
	shuttingDown bool         // shutdown 已执行（防 ctx 取消与监听注册的竞态）
}

// NewClient 构造 tunnel client：opener 用于向 target 开聚合流
// （生产环境直接传 *netacc.Aggregator）。
//
// 参数缺失/非法（nil opener、空 target、缺 WithListenPort 或端口
// 越界）在构造期返回 error，不拖到运行期。
func NewClient(opener StreamOpener, target peer.ID, opts ...ClientOption) (*Client, error) {
	if opener == nil {
		return nil, errors.New("tunnel: StreamOpener 不能为 nil")
	}
	if err := target.Validate(); err != nil {
		return nil, fmt.Errorf("tunnel: 目标 peer.ID 非法: %w", err)
	}
	cfg := clientConfig{listenPort: -1}
	for _, o := range opts {
		if o != nil {
			o.applyClientOption(&cfg)
		}
	}
	if cfg.listenPort < 0 || cfg.listenPort > 65535 {
		return nil, fmt.Errorf("tunnel: 监听端口缺失或非法: %d（取值 0-65535，0 = 内核分配）", cfg.listenPort)
	}
	return newClient(func(ctx context.Context) (net.Conn, error) {
		st, err := opener.OpenStream(ctx, target, cfg.openOpts...)
		if err != nil {
			return nil, err
		}
		return st, nil
	}, cfg.listenPort), nil
}

// newClient 是内部构造：open 已解耦成 net.Conn 粒度，测试可注入
// net.Pipe 等假实现而不必伪造 *netacc.Stream。
func newClient(open func(ctx context.Context) (net.Conn, error), port int) *Client {
	return &Client{port: port, open: open, actives: newConnSet()}
}

// Addr 返回实际监听地址；Run 未在进行（未启动或已收尾）时为 nil。
// 配合 WithListenPort(0) 取内核分配的端口。
func (c *Client) Addr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ln == nil {
		return nil
	}
	return c.ln.Addr()
}

// Run 启动监听并阻塞服务至 ctx 取消或监听失败：ctx 取消时关闭
// listener、主动断掉全部活动 TCP 连接与聚合流、等 handler 退出后
// 返回 nil（取消视为正常退出）；Accept 的非取消错误包装后返回。
func (c *Client) Run(ctx context.Context) error {
	if !c.ran.CompareAndSwap(false, true) {
		return errors.New("tunnel: Client.Run 只能调用一次")
	}
	// tcp4 钉死 IPv4 通配："tcp"+"0.0.0.0" 在支持双栈的系统上会退化
	// 成 [::] 监听，与「监听地址必须是 0.0.0.0:<port>」的字面要求不符。
	addr := fmt.Sprintf("0.0.0.0:%d", c.port)
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return fmt.Errorf("tunnel: 监听 %s 失败: %w", addr, err)
	}

	// ctx 取消 → 关 listener 与全部活动连接，让 Accept/handler 退出。
	// AfterFunc 先注册再登记 ln：shutdown 可能与登记并发——
	// 若它已跑完，新 listener 无人来关，这里自行关闭退出。
	stop := context.AfterFunc(ctx, c.shutdown)
	defer func() {
		stop()
		c.shutdown() // 幂等；覆盖非取消路径的收尾
		c.wg.Wait()  // 等全部 bridge 收尾，不泄漏 goroutine
	}()

	c.mu.Lock()
	if c.shuttingDown {
		c.mu.Unlock()
		_ = ln.Close()
		return nil
	}
	c.ln = ln
	c.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil // ctx 取消视为正常退出
			}
			return fmt.Errorf("tunnel: 接受 TCP 连接失败: %w", err)
		}
		c.actives.add(conn)
		c.wg.Add(1)
		go c.serveTCP(ctx, conn)
	}
}

// shutdown 幂等收尾：关 listener（Addr 随之变 nil）并断掉全部活动连接。
func (c *Client) shutdown() {
	c.mu.Lock()
	if c.shuttingDown {
		c.mu.Unlock()
		return
	}
	c.shuttingDown = true
	ln := c.ln
	c.ln = nil
	c.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	c.actives.closeAll()
}

// serveTCP 处理一条入向 TCP 连接：开一条聚合流后双向转发。
// OpenStream 失败只关闭本条连接，监听循环不受影响。
func (c *Client) serveTCP(ctx context.Context, conn net.Conn) {
	defer c.wg.Done()
	defer c.actives.del(conn)
	defer conn.Close()

	st, err := c.open(ctx)
	if err != nil {
		return
	}
	c.actives.add(st)
	defer c.actives.del(st)
	defer st.Close()

	bridge(conn, st)
}
