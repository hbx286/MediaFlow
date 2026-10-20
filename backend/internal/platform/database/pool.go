package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	URL               string
	ConnectTimeout    time.Duration
	MaxConns          int32
	MinConns          int32
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

func NewPool(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("解析 PostgreSQL 连接配置失败")
	}
	cfg.ConnConfig.ConnectTimeout = opts.ConnectTimeout
	cfg.MaxConns = opts.MaxConns
	cfg.MinConns = opts.MinConns
	cfg.MaxConnIdleTime = opts.MaxConnIdleTime
	cfg.HealthCheckPeriod = opts.HealthCheckPeriod
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建 PostgreSQL 连接池失败: %w", err)
	}
	return pool, nil
}
