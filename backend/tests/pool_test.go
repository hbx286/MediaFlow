package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"mediaflow/backend/internal/platform/database"
)

func TestNewPoolConfigAndClose(t *testing.T) {
	pool, err := database.NewPool(context.Background(), database.Options{
		URL:               "postgres://user:secret@127.0.0.1:1/mediaflow?sslmode=disable",
		ConnectTimeout:    3 * time.Second,
		MaxConns:          4,
		MinConns:          0,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	if cfg.MaxConns != 4 || cfg.MinConns != 0 || cfg.ConnConfig.ConnectTimeout != 3*time.Second || cfg.MaxConnIdleTime != 5*time.Minute || cfg.HealthCheckPeriod != time.Minute {
		pool.Close()
		t.Fatalf("连接池配置不符: max=%d min=%d connect=%s idle=%s health=%s", cfg.MaxConns, cfg.MinConns, cfg.ConnConfig.ConnectTimeout, cfg.MaxConnIdleTime, cfg.HealthCheckPeriod)
	}
	pool.Close()
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("关闭后的连接池不应继续获取连接")
	}
}

func TestNewPoolRejectsMalformedDSNWithoutLeakingPassword(t *testing.T) {
	_, err := database.NewPool(context.Background(), database.Options{URL: "postgres://user:secret@%invalid/mediaflow"})
	if err == nil {
		t.Fatal("无效 PostgreSQL DSN 应被拒绝")
	}
	if err.Error() != "解析 PostgreSQL 连接配置失败" || strings.Contains(err.Error(), "secret") {
		t.Fatal("连接配置错误信息不符合预期或包含数据库密码")
	}
}
