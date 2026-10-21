package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"mediaflow/backend/internal/health"
	"mediaflow/backend/internal/platform/config"
	"mediaflow/backend/internal/platform/database"
	"mediaflow/backend/internal/platform/lifecycle"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("API 退出", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.NewPool(ctx, database.Options{
		URL:               cfg.DatabaseURL,
		ConnectTimeout:    cfg.DBConnectTimeout,
		MaxConns:          cfg.DBMaxConns,
		MinConns:          cfg.DBMinConns,
		MaxConnIdleTime:   cfg.DBMaxConnIdleTime,
		HealthCheckPeriod: cfg.DBHealthCheckPeriod,
	})
	if err != nil {
		return err
	}
	pingCtx, cancelPing := context.WithTimeout(ctx, cfg.DBPingTimeout)
	err = pool.Ping(pingCtx)
	cancelPing()
	if err != nil {
		pool.Close()
		return fmt.Errorf("PostgreSQL 启动连接检查失败: %w", err)
	}
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           health.NewRouter(pool, cfg.DBPingTimeout, logger),
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		pool.Close()
		return fmt.Errorf("监听 HTTP 地址失败: %w", err)
	}
	logger.Info("API 启动", "addr", listener.Addr().String())
	err = lifecycle.Run(ctx, server, listener, pool, cfg.ShutdownTimeout)
	if ctx.Err() != nil && err == nil {
		logger.Info("HTTP 服务与数据库连接池已关闭")
	}
	return err
}
