package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHistoryCloseCompletes(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	if !store.Active() {
		t.Fatal("expected active store")
	}
	if !store.CloseWithTimeout(5 * time.Second) {
		t.Fatal("expected close to complete")
	}
	// Idempotent: second close must not panic and reports completed.
	if !store.CloseWithTimeout(5 * time.Second) {
		t.Fatal("expected idempotent close to report completed")
	}
	store.Close()
}

func TestHistoryCloseTimeoutContinuesBackground(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	oldHook := historyCloseHook
	historyCloseHook = func() { time.Sleep(300 * time.Millisecond) }
	defer func() { historyCloseHook = oldHook }()
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	if !store.Active() {
		t.Fatal("expected active store")
	}
	start := time.Now()
	if store.CloseWithTimeout(20 * time.Millisecond) {
		t.Fatal("expected timeout to report false")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout close blocked too long: %v", elapsed)
	}
	// Writer continues in background and eventually finishes; second close
	// with generous budget must complete without panic (idempotent).
	if !store.CloseWithTimeout(5 * time.Second) {
		t.Fatal("expected background writer to finish")
	}
	store.Close()
}

func TestHistoryCloseDisabledIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: false, RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	if !store.CloseWithTimeout(time.Second) {
		t.Fatal("disabled close must report completed")
	}
	store.Close()
	store.Close()
	var nilStore *HistoryStore
	if !nilStore.CloseWithTimeout(time.Second) {
		t.Fatal("nil close must report completed")
	}
	nilStore.Close()
}
