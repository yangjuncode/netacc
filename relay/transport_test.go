package netaccrelay

import (
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// fakeStream 是测试用 MuxedStream：Read 立即返回满缓冲。
type fakeStream struct {
	network.MuxedStream
}

func (f *fakeStream) Read(p []byte) (int, error)  { return len(p), nil }
func (f *fakeStream) Write(p []byte) (int, error) { return len(p), nil }
func (f *fakeStream) Close() error                { return nil }
func (f *fakeStream) Reset() error                { return nil }

type errStream struct{ *fakeStream }

func (e *errStream) Read(p []byte) (int, error) { return 0, io.ErrUnexpectedEOF }

// TestLimStreamReadThrottles 验证读侧限速语义：
// burst 内立即放行；burst 耗尽后的读发生令牌等待并置 waited 标志
// （配额重算器的需求信号），放行字节计入 cur。
func TestLimStreamReadThrottles(t *testing.T) {
	const total = 1 << 20 // 1 MiB/s
	a := NewAllocator(total, WithRecomputeInterval(500*time.Millisecond))
	defer a.Close()

	b := a.bucketFor(peer.ID("p"))
	s := &limStream{MuxedStream: &fakeStream{}, a: a, b: b}

	buf := make([]byte, maxChunk)
	// burst=128KiB=2×maxChunk：前两次读立即返回
	for i := 0; i < 2; i++ {
		if _, err := s.Read(buf); err != nil {
			t.Fatalf("burst 内读失败：%v", err)
		}
	}
	if b.waited.Load() {
		t.Fatal("burst 内不应发生令牌等待")
	}
	if b.cur.Load() != 2*maxChunk {
		t.Fatalf("cur 应为 %d，实际 %d", 2*maxChunk, b.cur.Load())
	}

	// 第三次读：令牌耗尽 → 按 1MiB/s 速率等待 ~64ms
	start := time.Now()
	if _, err := s.Read(buf); err != nil {
		t.Fatalf("限速读失败：%v", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("令牌耗尽时应发生回压等待，实际仅 %v", elapsed)
	}
	if !b.waited.Load() {
		t.Fatal("令牌等待后 waited 应置位（需求信号）")
	}
	if b.cur.Load() != 3*maxChunk {
		t.Fatalf("cur 应为 %d，实际 %d", 3*maxChunk, b.cur.Load())
	}
}

// TestLimStreamReadError 验证底层读错误原样透传且不误记流量。
func TestLimStreamReadError(t *testing.T) {
	a := NewAllocator(1<<20, WithRecomputeInterval(500*time.Millisecond))
	defer a.Close()

	b := a.bucketFor(peer.ID("p"))
	s := &limStream{MuxedStream: &errStream{fakeStream: &fakeStream{}}, a: a, b: b}
	if _, err := s.Read(make([]byte, 1024)); err != io.ErrUnexpectedEOF {
		t.Fatalf("底层错误应透传，实际 %v", err)
	}
	if b.cur.Load() != 0 {
		t.Fatalf("读错误不应计入 cur，实际 %d", b.cur.Load())
	}
}
