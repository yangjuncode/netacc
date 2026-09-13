package netacc

import (
	"bytes"
	"testing"
)

func mkdata(off, n int, fill byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = fill
	}
	return b
}

// TestReorderInOrder 顺序到达：立即可读，窗口占用正确。
func TestReorderInOrder(t *testing.T) {
	r := newReorderBuf(1024)
	r.add(0, mkdata(0, 100, 'a'))
	if r.avail() != 100 {
		t.Fatalf("顺序数据应立即可读，avail=%d", r.avail())
	}
	if r.cum() != 100 {
		t.Fatalf("cum 应推进到 100，得到 %d", r.cum())
	}
	if r.window() != 1024-100 {
		t.Fatalf("window 应为 cap-占用=%d，得到 %d", 1024-100, r.window())
	}
	out := make([]byte, 100)
	if n := r.read(out); n != 100 || !bytes.Equal(out, mkdata(0, 100, 'a')) {
		t.Fatalf("读出数据不正确 n=%d", n)
	}
	if r.avail() != 0 || r.window() != 1024 {
		t.Fatal("读完后缓冲应清空、窗口恢复")
	}
}

// TestReorderOutOfOrder 乱序注入：数据先暂存，缺口补齐后按序输出。
// 这是验收标准 2 的核心：收端必须在人造乱序下输出保序字节流。
func TestReorderOutOfOrder(t *testing.T) {
	r := newReorderBuf(4096)
	// 逆序喂三段：[200,300) [100,200) [0,100)
	r.add(200, mkdata(200, 100, 'c'))
	r.add(100, mkdata(100, 100, 'b'))
	if r.avail() != 0 {
		t.Fatalf("队头缺口未补齐前不应可读，avail=%d", r.avail())
	}
	if r.cum() != 0 {
		t.Fatalf("cum 不应越过缺口，得到 %d", r.cum())
	}
	r.add(0, mkdata(0, 100, 'a'))
	if r.avail() != 300 || r.cum() != 300 {
		t.Fatalf("缺口补齐后应一次交付 300 字节，avail=%d cum=%d", r.avail(), r.cum())
	}
	out := make([]byte, 300)
	n := r.read(out)
	want := append(append(mkdata(0, 100, 'a'), mkdata(100, 100, 'b')...), mkdata(200, 100, 'c')...)
	if n != 300 || !bytes.Equal(out, want) {
		t.Fatal("乱序注入后输出不保序")
	}
}

// TestReorderDuplicate 重复/重叠段（重传）：按序号去重，不重复交付。
func TestReorderDuplicate(t *testing.T) {
	r := newReorderBuf(1024)
	r.add(0, mkdata(0, 100, 'a'))
	// 完全重复
	r.add(0, mkdata(0, 100, 'x'))
	// 部分重叠：[50,150)
	r.add(50, mkdata(50, 100, 'y'))
	out := make([]byte, 200)
	n := r.read(out)
	if n != 150 {
		t.Fatalf("应交付 150 字节（去重后），得到 %d", n)
	}
	if !bytes.Equal(out[:100], mkdata(0, 100, 'a')) {
		t.Fatal("前 100 字节应为首次到达的数据")
	}
	if !bytes.Equal(out[100:150], mkdata(0, 50, 'y')) {
		t.Fatal("重叠段的非重复部分应被交付")
	}
	// 重叠部分在入堆时重复计账、裁剪时必须核销：
	// 读完后窗口应完全恢复，不允许留虚高占用
	if r.sizeBytes() != 0 || r.window() != 1024 {
		t.Fatalf("重叠裁剪后 size 应归零，size=%d window=%d", r.sizeBytes(), r.window())
	}
}

// TestReorderOverlapAccounting 两段乱序暂存段互相重叠时，
// drain 裁剪的重叠前缀必须从 size 核销（否则窗口越磨越小直至假死）。
func TestReorderOverlapAccounting(t *testing.T) {
	r := newReorderBuf(1024)
	r.add(100, mkdata(100, 100, 'a')) // [100,200)
	r.add(150, mkdata(150, 100, 'b')) // [150,250) 与上段重叠 50 字节
	if r.sizeBytes() != 200 {
		t.Fatalf("两段暂存应计 200 字节，size=%d", r.sizeBytes())
	}
	r.add(0, mkdata(0, 100, 'c')) // 补洞 → 全部交付
	out := make([]byte, 250)
	if n := r.read(out); n != 250 {
		t.Fatalf("应交付 250 字节，n=%d", n)
	}
	if r.sizeBytes() != 0 || r.window() != 1024 {
		t.Fatalf("重叠核销后 size 应归零，size=%d window=%d", r.sizeBytes(), r.window())
	}
	if r.cum() != 250 {
		t.Fatalf("cum 应为 250，得到 %d", r.cum())
	}
}

// TestReorderCapDrop 缓冲满时丢弃超限段：窗口背压正常运作时不可达，
// 但作为对端超发时的内存保护必须存在。
func TestReorderCapDrop(t *testing.T) {
	r := newReorderBuf(100)
	// 队头留个洞，乱序段占用容量
	r.add(50, mkdata(50, 80, 'b'))   // size=80
	r.add(200, mkdata(200, 50, 'c')) // 80+50>100 → 丢弃
	if r.sizeBytes() != 80 {
		t.Fatalf("超容量段应被丢弃，size=%d", r.sizeBytes())
	}
	// 填满洞后交付，容量释放
	r.add(0, mkdata(0, 50, 'a'))
	if r.avail() != 130 {
		t.Fatalf("应交付 130 字节，avail=%d", r.avail())
	}
}

// TestReorderRanges 乱序区间的 SACK ranges 输出：合并相邻/重叠段。
func TestReorderRanges(t *testing.T) {
	r := newReorderBuf(4096)
	r.add(200, mkdata(200, 50, 'b'))
	r.add(100, mkdata(100, 50, 'a')) // [100,150)
	r.add(140, mkdata(140, 20, 'x')) // [140,160) 与 [100,150) 重叠 → 合并
	r.add(400, mkdata(400, 50, 'c'))
	ranges := r.ranges(10)
	// 期望：[100,160) [200,250) [400,450)
	want := []byteRange{{100, 160}, {200, 250}, {400, 450}}
	if len(ranges) != len(want) {
		t.Fatalf("ranges 数不符: %+v", ranges)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("ranges[%d]=%+v 应为 %+v", i, ranges[i], want[i])
		}
	}
	// 截断到 max
	if rs := r.ranges(2); len(rs) != 2 {
		t.Fatalf("ranges 应按 max 截断，得到 %d", len(rs))
	}
}

// TestReorderReadPartial 读缓冲比可交付数据小：分段读出。
func TestReorderReadPartial(t *testing.T) {
	r := newReorderBuf(1024)
	r.add(0, mkdata(0, 100, 'a'))
	out := make([]byte, 30)
	var got []byte
	for r.avail() > 0 {
		n := r.read(out)
		got = append(got, out[:n]...)
	}
	if !bytes.Equal(got, mkdata(0, 100, 'a')) {
		t.Fatal("分段读出数据不一致")
	}
}
