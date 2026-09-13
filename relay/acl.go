package netaccrelay

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

// Whitelist 是 peerID 白名单，实现 relay.ACLFilter：
// 只允许白名单内 peer 做 reservation，且要求电路两端都在白名单内
// （目的端若不在白名单，本来就过不了 AllowReserve）。
// 集合可运行期增删。
type Whitelist struct {
	mu sync.RWMutex
	ps map[peer.ID]struct{}
}

// NewWhitelist 建白名单，可传入初始成员。
func NewWhitelist(peers ...peer.ID) *Whitelist {
	w := &Whitelist{ps: make(map[peer.ID]struct{}, len(peers))}
	for _, p := range peers {
		w.ps[p] = struct{}{}
	}
	return w
}

// Add 加 peer 进白名单。
func (w *Whitelist) Add(p peer.ID) {
	w.mu.Lock()
	w.ps[p] = struct{}{}
	w.mu.Unlock()
}

// Remove 把 peer 移出白名单。
func (w *Whitelist) Remove(p peer.ID) {
	w.mu.Lock()
	delete(w.ps, p)
	w.mu.Unlock()
}

// Contains 查询 peer 是否在白名单。
func (w *Whitelist) Contains(p peer.ID) bool {
	w.mu.RLock()
	_, ok := w.ps[p]
	w.mu.RUnlock()
	return ok
}

// AllowReserve 实现 relay.ACLFilter：仅白名单内 peer 可预留。
func (w *Whitelist) AllowReserve(p peer.ID, _ ma.Multiaddr) bool {
	return w.Contains(p)
}

// AllowConnect 实现 relay.ACLFilter：源与目的都须在白名单内。
func (w *Whitelist) AllowConnect(src peer.ID, _ ma.Multiaddr, dest peer.ID) bool {
	return w.Contains(src) && w.Contains(dest)
}

// 编译期接口断言。
var _ relay.ACLFilter = (*Whitelist)(nil)
