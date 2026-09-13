package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

var errTestNoUpstream = errors.New("no upstream for test")

type countingCloser struct {
	closes atomic.Int32
}

func (c *countingCloser) RoundTrip(_ *http.Request) (*http.Response, error) {
	return nil, errTestNoUpstream
}

func (c *countingCloser) CloseIdleConnections() {
	c.closes.Add(1)
}

type plainTripper struct{}

func (plainTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return nil, errTestNoUpstream
}

func testPoolWithCloser(t *testing.T, closers ...*countingCloser) *transportPool {
	t.Helper()
	p := &transportPool{name: "shared"}
	for i, c := range closers {
		_ = i
		pt := &proxyTransport{index: len(p.items), name: "direct", pool: "shared", client: &http.Client{Transport: c}}
		pt.healthy.Store(true)
		p.items = append(p.items, pt)
	}
	return p
}

func TestTransportPoolCloseIdleConnections(t *testing.T) {
	a, b := &countingCloser{}, &countingCloser{}
	p := testPoolWithCloser(t, a, b)
	p.CloseIdleConnections()
	if a.closes.Load() != 1 || b.closes.Load() != 1 {
		t.Fatalf("closes=%d/%d want 1/1", a.closes.Load(), b.closes.Load())
	}
	// Nil-safe and non-closer transports skipped without panic.
	var nilPool *transportPool
	nilPool.CloseIdleConnections()
	plain := &transportPool{name: "x", items: []*proxyTransport{{client: &http.Client{Transport: plainTripper{}}}}}
	plain.CloseIdleConnections()
}

func TestGatewayCloseIdleSharedDedup(t *testing.T) {
	a := &countingCloser{}
	pool := testPoolWithCloser(t, a)
	gw := &Gateway{cfg: testGatewayConfigForClose(), pools: map[string]*transportPool{"shared": pool}}
	// All three channels reference the same pool pointer: must close once.
	gw.CloseIdleConnections()
	if a.closes.Load() != 1 {
		t.Fatalf("shared closes=%d want 1", a.closes.Load())
	}
	var nilGw *Gateway
	nilGw.CloseIdleConnections()
}

func testGatewayConfigForClose() Config {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	return cfg
}

func TestGatewayApplyClosesOldIdle(t *testing.T) {
	// Frequent Apply must not panic and must close old idle pools exactly
	// once per Apply via uniquePools (race detector covers concurrency).
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	gw, err := NewGateway(cfg, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	// Replace transports with counting closers to observe closes.
	closer := &countingCloser{}
	for _, pool := range gw.uniquePools() {
		for _, px := range pool.items {
			px.client = &http.Client{Transport: closer}
		}
	}
	old := gw
	old.CloseIdleConnections()
	if closer.closes.Load() < 1 {
		t.Fatal("expected at least one close")
	}
}

func TestRuntimeFrequentApplyNoPanic(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	base := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	normalized, err := NormalizeConfig(cfgPath, base)
	if err != nil {
		t.Fatal(err)
	}
	level := new(slog.LevelVar)
	mgr := &RuntimeManager{configPath: cfgPath, root: context.Background(), logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor(), level: level}
	rt, err := mgr.build(normalized)
	if err != nil {
		t.Fatal(err)
	}
	mgr.current.Store(rt)
	mgr.start(rt)
	// Frequent Apply with fresh candidates; each swap must close the
	// previous idle pools without duplicating shared pointers or panicking.
	// cloneConfig gives each Apply its own slice backing so background
	// refresh on the old Gateway never races the next Normalize.
	for i := 0; i < 5; i++ {
		cand := cloneConfig(normalized)
		if _, err := mgr.Apply(cand, false); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	mgr.ShutdownWithTimeout(0)
}
