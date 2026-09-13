package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "config.json", "path to config.json")
	listen := flag.String("listen", "", "override the configured API listen address")
	webListen := flag.String("web-listen", "", "override the configured WebUI listen address")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *webListen != "" {
		cfg.WebUI.Listen = *webListen
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	level := new(slog.LevelVar)
	setLogLevel(level, cfg.Logging.Level)
	hub := NewLogHub(cfg.Logging.RingSize)
	redactor := NewSecretRedactor()
	redactor.Replace(cfg)
	logger := NewStructuredLogger(level, hub, redactor)
	monitor := NewMonitor()
	manager, err := NewRuntimeManager(ctx, *configPath, cfg, logger, monitor, hub, redactor, level)
	if err != nil {
		logger.Error("failed to initialize runtime", "component", "runtime", "event", "runtime_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer manager.Shutdown()

	apiServer := &http.Server{
		Addr: cfg.Listen, Handler: manager.Handler(), ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 120 * time.Second,
	}
	servers := []*http.Server{apiServer}
	go serveHTTP(cancel, logger, apiServer, "api")

	if cfg.WebUI.Enabled {
		admin := NewAdminServer(manager, monitor, hub, logger)
		webServer := &http.Server{
			Addr: cfg.WebUI.Listen, Handler: admin.Handler(), ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 120 * time.Second,
		}
		servers = append(servers, webServer)
		go serveHTTP(cancel, logger, webServer, "webui")
	}

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", "component", "server", "event", "shutdown_failed", "address", server.Addr, "error", err)
		}
	}
	// Drain history inside the same 15s budget; ShutdownWithContext caps the
	// wait at the remaining deadline (default 10s). The deferred Shutdown
	// remains as a best-effort fallback.
	manager.ShutdownWithContext(shutdownCtx)
}

func serveHTTP(cancel context.CancelFunc, logger *slog.Logger, server *http.Server, component string) {
	logger.Info("server listening", "component", component, "event", "server_started", "address", server.Addr, "version", version)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server stopped unexpectedly", "component", component, "event", "server_failed", "address", server.Addr, "error", err)
		cancel()
	}
}
