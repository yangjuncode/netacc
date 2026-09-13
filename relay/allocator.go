package netaccrelay

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// 默认参数。
const (
	// DefaultRecomputeInterval 配额重算周期，规格 §7.2 要求 100–500ms。
	DefaultRecomputeInterval = 200 * time.Millisecond
	// DefaultBurst 每桶突发字节数，约 1–2 个拷贝块，抑制突发叠加。
	DefaultBurst = 128 << 10
	// DefaultMinShare 保底份额（字节/秒）：零需求 peer 突然来流量时
	// 能立刻拿到细流速率，等待信号下一周期再把它抬到应得配额。
	DefaultMinShare = 64 << 10
	// maxChunk 单次取令牌的最大块，须 ≤ burst。
	maxChunk = 64 << 10
)

// bucket 是一个对端 peer 的共享限速桶：同一 peerID 的全部连接/流共用。
type bucket struct {
	lim    *rate.Limiter // 每 peer 令牌桶，速率由重算器热更
	cur    atomic.Int64  // 本周期已读字节
	waited atomic.Bool   // 本周期是否发生过令牌等待（= 被限速，需求不封顶）
	demand float64       // 需求估计，字节/秒
	share  float64       // 当前配额，字节/秒
}

// Allocator 集中式配额重算器：
// 按对端 peerID 维护共享令牌桶，每 interval 用需求感知的
// progressive filling 算 max-min 配额并 SetLimit 热更；
// 叠加一个 cap=total 的全局桶做总量保险。
//
// 需求信号（原型 #15 实测结论）：本周期发生过令牌等待 → 需求视为不封顶；
// 未等待才用观测速率 EWMA。直接用观测速率当需求会配额缩水死亡螺旋。
type Allocator struct {
	total     float64       // 中继总容量 R，字节/秒
	interval  time.Duration // 重算周期
	burst     int           // 每桶突发
	minShare  float64       // 保底份额
	ewmaAlpha float64       // 观测速率 EWMA 系数

	global *rate.Limiter // cap=R 全局桶

	mu      sync.Mutex
	buckets map[peer.ID]*bucket

	stopCh chan struct{}
	doneCh chan struct{}
}

// AllocatorOption 调整 Allocator 参数。
type AllocatorOption func(*Allocator)

// WithRecomputeInterval 设配额重算周期，规格建议 100–500ms。
// 超出范围会被收敛到 [100ms, 500ms]。
func WithRecomputeInterval(d time.Duration) AllocatorOption {
	return func(a *Allocator) {
		if d < 100*time.Millisecond {
			d = 100 * time.Millisecond
		}
		if d > 500*time.Millisecond {
			d = 500 * time.Millisecond
		}
		a.interval = d
	}
}

// WithBurst 设每 peer 桶与全局桶的突发字节数（≥64KiB）。
func WithBurst(n int) AllocatorOption {
	return func(a *Allocator) {
		if n < maxChunk {
			n = maxChunk
		}
		a.burst = n
	}
}

// WithMinShare 设每 peer 保底份额（字节/秒）。
func WithMinShare(v float64) AllocatorOption {
	return func(a *Allocator) {
		if v > 0 {
			a.minShare = v
		}
	}
}

// NewAllocator 建配额重算器并立即启动后台重算 goroutine；
// totalBytesPerSec 为中继总容量 R。用 Close 停止。
func NewAllocator(totalBytesPerSec float64, opts ...AllocatorOption) *Allocator {
	a := &Allocator{
		total:     totalBytesPerSec,
		interval:  DefaultRecomputeInterval,
		burst:     DefaultBurst,
		minShare:  DefaultMinShare,
		ewmaAlpha: 0.5,
		buckets:   map[peer.ID]*bucket{},
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	for _, o := range opts {
		o(a)
	}
	if a.total <= 0 {
		a.total = 1 << 30
	}
	a.global = rate.NewLimiter(rate.Limit(a.total), a.burst)
	go a.loop()
	return a
}

// Close 停止后台重算 goroutine，幂等。
func (a *Allocator) Close() {
	select {
	case <-a.doneCh:
		return
	default:
	}
	close(a.stopCh)
	<-a.doneCh
}

func (a *Allocator) loop() {
	defer close(a.doneCh)
	t := time.NewTicker(a.interval)
	defer t.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-t.C:
			a.recompute()
		}
	}
}

// bucketFor 取（或建）peer 的共享桶；新桶初始配额为总量（先信后分）。
func (a *Allocator) bucketFor(p peer.ID) *bucket {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.buckets[p]
	if b == nil {
		b = &bucket{lim: rate.NewLimiter(rate.Limit(a.total), a.burst)}
		b.share = a.total
		b.demand = a.total
		a.buckets[p] = b
	}
	return b
}

// recompute 采样需求 → progressive filling → SetLimit 热更。
func (a *Allocator) recompute() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.buckets) == 0 {
		return
	}
	peers := make([]peer.ID, 0, len(a.buckets))
	demands := make([]float64, 0, len(a.buckets))
	for p, b := range a.buckets {
		observed := float64(b.cur.Swap(0)) / a.interval.Seconds()
		if b.waited.Swap(false) {
			// 被限速过 → 需求不封顶，交给 progressive filling 均分
			b.demand = a.total
		} else {
			b.demand = a.ewmaAlpha*b.demand + (1-a.ewmaAlpha)*observed
		}
		peers = append(peers, p)
		demands = append(demands, b.demand)
	}
	shares := progressiveFill(demands, a.total, a.minShare)
	for i, p := range peers {
		b := a.buckets[p]
		b.share = shares[i]
		b.lim.SetLimit(rate.Limit(shares[i]))
	}
}

// progressiveFill 经典注水算法：配额从 0 同步抬升，需求小者先冻结在需求，
// 剩余带宽继续均分给未冻结者。返回与 demands 对应的份额（均 ≥ minShare）。
func progressiveFill(demands []float64, total, minShare float64) []float64 {
	n := len(demands)
	shares := make([]float64, n)
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(i, j int) bool { return demands[idx[i]] < demands[idx[j]] })
	remain := total
	left := n
	for _, i := range idx {
		d := demands[i]
		if d < minShare {
			d = minShare
		}
		share := remain / float64(left)
		if d < share {
			share = d // 需求小的冻结在需求
		}
		shares[i] = share
		remain -= share
		left--
	}
	return shares
}

// throttle 读侧限速入口：n 字节同时向 per-peer 桶与全局桶约令牌，
// 只等两者中较大的延迟（各份额之和本就不超 R，全局桶只是配额滞后/
// burst 叠加时的总量保险，不应与 peer 桶串行叠加限速）。
// 单次等待以 interval 为片上界：超长延迟先取消重约，以便及时感知
// SetLimit 热更。发生过等待即置位 b.waited——这是配额重算器的需求信号。
func (a *Allocator) throttle(b *bucket, n int) {
	for {
		now := time.Now()
		rsv := b.lim.ReserveN(now, n)
		grsv := a.global.ReserveN(now, n)
		if !rsv.OK() || !grsv.OK() {
			// n > burst，理论上不可达（n 已被 maxChunk 封顶 ≤ burst）；
			// 保底直接放行，不阻塞数据面。
			return
		}
		d := rsv.Delay()
		if gd := grsv.Delay(); gd > d {
			d = gd
		}
		if d <= 0 {
			return
		}
		b.waited.Store(true)
		if d <= a.interval {
			time.Sleep(d)
			return
		}
		rsv.Cancel()
		grsv.Cancel()
		time.Sleep(a.interval)
	}
}

func (a *Allocator) accountRead(b *bucket, n int) {
	b.cur.Add(int64(n))
}

// PeerShare 返回 peer 当前配额（字节/秒），供观测与测试。
func (a *Allocator) PeerShare(p peer.ID) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if b := a.buckets[p]; b != nil {
		return b.share
	}
	return 0
}

// PeerDemand 返回 peer 当前需求估计（字节/秒），供观测与测试。
func (a *Allocator) PeerDemand(p peer.ID) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if b := a.buckets[p]; b != nil {
		return b.demand
	}
	return 0
}
