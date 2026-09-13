// Package netaccrelay 提供 netacc 中继组件：可嵌入任意 libp2p host 的
// circuit-relay v2 服务 + peerID 白名单 + 按 peer 的公平带宽限速
// （transport 装饰器 + 集中式配额重算器，max-min fairness 语义）。
//
// 典型用法：
//
//	alloc := netaccrelay.NewAllocator(200 << 20) // 中继总容量 200 MiB/s
//	defer alloc.Close()
//
//	h, _ := libp2p.New(
//		libp2p.NoTransports,
//		netaccrelay.TCPTransport(alloc), // 传输装饰器挂同一分配器
//		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/4001"),
//	)
//
//	r, _ := netaccrelay.New(h,
//		netaccrelay.WithAllocator(alloc),
//		netaccrelay.WithACLFilter(netaccrelay.NewWhitelist(peers...)),
//	)
//	defer r.Close()
//
// wire protocol（pbv2）与 client 包零改动：客户端用原生
// p2p/protocol/circuitv2/client 即可。
package netaccrelay

import (
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

// 组件默认参数。
const (
	// DefaultBandwidth 未显式指定时的中继总容量，128 MiB/s。
	DefaultBandwidth = 128 << 20
	// MinBufferSize relay 转发缓冲下限，规格 §7.1 要求 ≥16KiB。
	MinBufferSize = 16 << 10
	// DefaultBufferSize 默认转发缓冲，大流量场景取 64KiB。
	DefaultBufferSize = 64 << 10
)

// Relay 是装配好的中继组件：原生 relay 服务 + 配额重算器 + ACL。
type Relay struct {
	alloc  *Allocator
	svc    *relay.Relay
	ownAll bool // alloc 是否由本组件创建（决定 Close 是否停它）
}

// Option 配置中继组件。
type Option func(*config)

type config struct {
	alloc     *Allocator
	bandwidth float64
	allocOpts []AllocatorOption
	acl       relay.ACLFilter
	resources *relay.Resources
	relayOpts []relay.Option
}

// WithAllocator 复用已建分配器——挂传输装饰器的必须是同一个实例，
// 否则限速不生效。不传则组件按 WithBandwidth 自建。
func WithAllocator(a *Allocator) Option {
	return func(c *config) { c.alloc = a }
}

// WithBandwidth 设中继总容量 R（字节/秒），仅在未传 WithAllocator 时生效。
func WithBandwidth(bytesPerSec float64) Option {
	return func(c *config) { c.bandwidth = bytesPerSec }
}

// WithAllocatorOptions 传分配器参数（重算周期、突发等），
// 仅在未传 WithAllocator 时生效。
func WithAllocatorOptions(opts ...AllocatorOption) Option {
	return func(c *config) { c.allocOpts = append(c.allocOpts, opts...) }
}

// WithACLFilter 设访问控制过滤器（如 NewWhitelist）。
// 不设则不挂 ACL（等价于放行一切，慎用）。
func WithACLFilter(acl relay.ACLFilter) Option {
	return func(c *config) { c.acl = acl }
}

// WithWhitelist 便捷选项：直接给 peerID 白名单。
func WithWhitelist(peers ...peer.ID) Option {
	return func(c *config) { c.acl = NewWhitelist(peers...) }
}

// WithResources 设 relay 资源参数（MaxCircuits/ReservationTTL/BufferSize 等）。
// BufferSize 会被抬到 ≥16KiB；Limit 字段会被忽略——本组件一律解除逐电路限额
// （WithInfiniteLimits），带宽约束由公平限速承担。
func WithResources(rc relay.Resources) Option {
	return func(c *config) { c.resources = &rc }
}

// WithRelayOptions 透传额外原生 relay.Option（如 WithMetricsTracer）。
func WithRelayOptions(opts ...relay.Option) Option {
	return func(c *config) { c.relayOpts = append(c.relayOpts, opts...) }
}

// New 在已有 host 上装配中继组件：
// 直连 relay.New（跳过 EnableRelayService 的公网可达性门控），
// 解除逐电路限额，挂 ACL 与资源参数。
//
// 注意：公平限速靠传输装饰器生效——host 的传输必须在建 host 时
// 就用 TCPTransport(alloc)/alloc.WrapTransport 包装，且与这里
// WithAllocator 传入的是同一个分配器。
func New(h host.Host, opts ...Option) (*Relay, error) {
	c := &config{bandwidth: DefaultBandwidth}
	for _, o := range opts {
		o(c)
	}

	alloc := c.alloc
	ownAll := false
	if alloc == nil {
		alloc = NewAllocator(c.bandwidth, c.allocOpts...)
		ownAll = true
	}

	rc := relay.DefaultResources()
	rc.Limit = nil // 解除逐电路限额
	rc.BufferSize = DefaultBufferSize
	if c.resources != nil {
		rc = *c.resources
		rc.Limit = nil
		if rc.BufferSize < MinBufferSize {
			rc.BufferSize = MinBufferSize
		}
	}

	relayOpts := []relay.Option{relay.WithInfiniteLimits(), relay.WithResources(rc)}
	if c.acl != nil {
		relayOpts = append(relayOpts, relay.WithACL(c.acl))
	}
	relayOpts = append(relayOpts, c.relayOpts...)

	svc, err := relay.New(h, relayOpts...)
	if err != nil {
		if ownAll {
			alloc.Close()
		}
		return nil, err
	}
	return &Relay{alloc: alloc, svc: svc, ownAll: ownAll}, nil
}

// Allocator 返回组件使用的配额重算器。
func (r *Relay) Allocator() *Allocator { return r.alloc }

// Close 停 relay 服务；分配器由本组件自建时一并停止。
func (r *Relay) Close() error {
	if r.ownAll {
		r.alloc.Close()
	}
	return r.svc.Close()
}
