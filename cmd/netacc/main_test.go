package main

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"

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

// TestIdentityConcurrentCreate 验证并发首启、后续重启及磁盘文件使用同一身份。
func TestIdentityConcurrentCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.key")
	start := make(chan struct{})
	keys := make(chan crypto.PrivKey, 64)
	var wg sync.WaitGroup
	for i := 0; i < cap(keys); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			key, err := loadOrCreateIdentity(path)
			if err != nil {
				t.Errorf("并发加载身份失败: %v", err)
				return
			}
			keys <- key
		}()
	}
	close(start)
	wg.Wait()
	close(keys)
	reloaded, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	for key := range keys {
		if !key.Equals(reloaded) {
			t.Fatal("运行密钥与重启加载的密钥不同")
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("私钥权限不符: %o", info.Mode().Perm())
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("临时密钥文件未清理: %v", files)
	}
}

// TestIdentityInvalidFile 验证已有损坏文件不会被静默替换为新身份。
func TestIdentityInvalidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.key")
	data := []byte("无效私钥")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateIdentity(path); err == nil {
		t.Fatal("损坏的身份文件应报错")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("已有身份文件被改动: %v", err)
	}
}
