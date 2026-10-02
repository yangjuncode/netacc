package main

import (
	"net/url"
	"testing"

	"github.com/yangjuncode/netacc/tunnel"
)

// TestAllowTunnelTargets 验证 CLI 的 host:port 与 scheme+path 白名单规则。
func TestAllowTunnelTargets(t *testing.T) {
	allow, err := allowTunnelTargets([]string{
		"127.0.0.1:8089",
		"https://api.example.com/v1/",
		"wss://events.example.com",
	})
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	cases := []struct {
		raw    string
		wantOK bool
	}{
		{"http://127.0.0.1:8089/a", true},
		{"ws://127.0.0.1:8089/socket", true},
		{"https://api.example.com/v1/users", true},
		{"https://api.example.com:443/v1/users", true},
		{"https://api.example.com/v2/users", false},
		{"wss://events.example.com/live", true},
		{"https://events.example.com/live", false},
	}
	for _, c := range cases {
		u, err := url.Parse(c.raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := allow("", tunnel.TunnelKindHTTP, u); got != c.wantOK {
			t.Fatalf("target %s: got %v, want %v", c.raw, got, c.wantOK)
		}
	}
}

// TestAllowTunnelTargetsBad 验证非法 allowlist 在启动期报错。
func TestAllowTunnelTargetsBad(t *testing.T) {
	if _, err := allowTunnelTargets([]string{"not-a-hostport"}); err == nil {
		t.Fatal("非法 host:port 规则应报错")
	}
	if _, err := allowTunnelTargets([]string{"https:///missing-host"}); err == nil {
		t.Fatal("缺 host 的 URL 规则应报错")
	}
}
