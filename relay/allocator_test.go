package netaccrelay

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestProgressiveFill 验证 max-min 公平分配的核心算法（progressive filling）。
func TestProgressiveFill(t *testing.T) {
	const mib = 1 << 20
	cases := []struct {
		name     string
		demands  []float64 // 字节/秒
		total    float64
		minShare float64
		want     []float64
	}{
		{
			name:     "单需求独占全部",
			demands:  []float64{100 * mib},
			total:    50 * mib,
			minShare: 1,
			want:     []float64{50 * mib},
		},
		{
			name:     "两个大需求均分",
			demands:  []float64{100 * mib, 100 * mib},
			total:    50 * mib,
			minShare: 1,
			want:     []float64{25 * mib, 25 * mib},
		},
		{
			name:     "小需求冻结后剩余给大需求",
			demands:  []float64{10 * mib, 100 * mib},
			total:    80 * mib,
			minShare: 1,
			want:     []float64{10 * mib, 70 * mib},
		},
		{
			name:     "全部需求小于均分时各自拿需求",
			demands:  []float64{5 * mib, 8 * mib},
			total:    100 * mib,
			minShare: 1,
			want:     []float64{5 * mib, 8 * mib},
		},
		{
			name:     "三级需求逐步冻结",
			demands:  []float64{4 * mib, 20 * mib, 90 * mib},
			total:    60 * mib,
			minShare: 1,
			// 均分 20 → 4 冻结；剩余 56 对半 → 20 冻结；34 全给最大需求
			want: []float64{4 * mib, 20 * mib, 36 * mib},
		},
		{
			name:     "零需求拿保底份额",
			demands:  []float64{0, 100 * mib},
			total:    10 * mib,
			minShare: 64 * 1024,
			want:     []float64{64 * 1024, 10*mib - 64*1024},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := progressiveFill(tc.demands, tc.total, tc.minShare)
			if len(got) != len(tc.want) {
				t.Fatalf("份额数 %d != 需求数 %d", len(got), len(tc.want))
			}
			sum := 0.0
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("demands=%v total=%v：第 %d 个份额 = %v，期望 %v（全部 %v）",
						tc.demands, tc.total, i, got[i], tc.want[i], got)
				}
				sum += got[i]
			}
			if sum > tc.total+1 {
				t.Fatalf("份额总和 %v 超过总量 %v", sum, tc.total)
			}
		})
	}
}

// TestDemandSignalWaited 验证需求信号语义：
// 本周期发生过令牌等待 → 需求不封顶（= 总量）；未等待 → 观测速率 EWMA。
func TestDemandSignalWaited(t *testing.T) {
	a := NewAllocator(100<<20, WithRecomputeInterval(50*time.Millisecond))
	defer a.Close()

	p := peer.ID("peer-waited")
	b := a.bucketFor(p)

	// 发生过等待 → 需求顶到总量
	b.waited.Store(true)
	a.recompute()
	if b.demand != a.total {
		t.Fatalf("等待过的桶需求应为总量 %v，实际 %v", a.total, b.demand)
	}

	// 未等待 + 本周期读了 1MiB（50ms 周期 → 观测 20MiB/s）→ EWMA 混合
	b.waited.Store(false)
	b.cur.Store(1 << 20)
	a.recompute()
	observed := float64(1<<20) / a.interval.Seconds()
	want := 0.5*a.total + 0.5*observed // EWMA α=0.5：旧需求 100MiB/s 与新观测 20MiB/s 混合
	if b.demand != want {
		t.Fatalf("未等待桶需求应为 EWMA %v，实际 %v", want, b.demand)
	}
}

// TestRecomputeSetsLimits 验证 recompute 把 progressive filling 结果热更到各桶。
func TestRecomputeSetsLimits(t *testing.T) {
	const total = 100 << 20
	a := NewAllocator(total, WithRecomputeInterval(50*time.Millisecond))
	defer a.Close()

	b1 := a.bucketFor(peer.ID("p1"))
	b2 := a.bucketFor(peer.ID("p2"))
	b1.waited.Store(true)
	b2.waited.Store(true)
	a.recompute()
	if float64(b1.lim.Limit()) != total/2 || float64(b2.lim.Limit()) != total/2 {
		t.Fatalf("两个不封顶需求应各得 %v，实际 %v / %v",
			total/2, float64(b1.lim.Limit()), float64(b2.lim.Limit()))
	}
	if got := a.PeerShare(peer.ID("p1")); got != total/2 {
		t.Fatalf("PeerShare 应为 %v，实际 %v", total/2, got)
	}
}
