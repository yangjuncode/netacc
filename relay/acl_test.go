package netaccrelay

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestWhitelistACL 单元校验白名单的 AllowReserve/AllowConnect 语义
// 及运行期增删。
func TestWhitelistACL(t *testing.T) {
	a, b, c := peer.ID("a"), peer.ID("b"), peer.ID("c")
	w := NewWhitelist(a, b)

	if !w.AllowReserve(a, nil) {
		t.Fatal("白名单内 peer 的 AllowReserve 应放行")
	}
	if w.AllowReserve(c, nil) {
		t.Fatal("白名单外 peer 的 AllowReserve 应拒绝")
	}

	// AllowConnect 源/目的双端校验
	if !w.AllowConnect(a, nil, b) {
		t.Fatal("双端都在白名单的 AllowConnect 应放行")
	}
	if w.AllowConnect(c, nil, b) {
		t.Fatal("源端不在白名单的 AllowConnect 应拒绝")
	}
	if w.AllowConnect(a, nil, c) {
		t.Fatal("目的端不在白名单的 AllowConnect 应拒绝")
	}

	// 运行期增删
	w.Remove(a)
	if w.Contains(a) || w.AllowReserve(a, nil) {
		t.Fatal("Remove 后 a 应出白名单")
	}
	w.Add(c)
	if !w.Contains(c) || !w.AllowReserve(c, nil) {
		t.Fatal("Add 后 c 应入白名单")
	}
}
