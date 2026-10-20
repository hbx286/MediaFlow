package tests

import (
	"strings"
	"testing"
	"time"

	"mediaflow/backend/internal/platform/config"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:secret@localhost:5432/mediaflow")
	for _, key := range []string{"HTTP_ADDR", "HTTP_READ_HEADER_TIMEOUT", "HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT", "HTTP_IDLE_TIMEOUT", "SHUTDOWN_TIMEOUT", "DB_PING_TIMEOUT", "DB_MAX_CONNS", "DB_MIN_CONNS", "DB_MAX_CONN_IDLE_TIME", "DB_HEALTH_CHECK_PERIOD"} {
		t.Setenv(key, "")
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.DBMaxConns != 10 || cfg.DBPingTimeout != 2*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name, key, value, want string
	}{
		{"missing database", "DATABASE_URL", "", "DATABASE_URL"},
		{"bad port", "HTTP_ADDR", ":invalid", "HTTP_ADDR"},
		{"bad duration", "DB_PING_TIMEOUT", "0s", "DB_PING_TIMEOUT"},
		{"bad pool size", "DB_MAX_CONNS", "0", "DB_MAX_CONNS"},
		{"pool minimum exceeds maximum", "DB_MIN_CONNS", "11", "DB_MIN_CONNS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://user:secret@localhost:5432/mediaflow")
			t.Setenv("HTTP_ADDR", "")
			t.Setenv("DB_PING_TIMEOUT", "")
			t.Setenv("DB_MAX_CONNS", "")
			t.Setenv("DB_MIN_CONNS", "")
			t.Setenv(tt.key, tt.value)
			_, err := config.Load()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load() error = %v, want %s", err, tt.want)
			}
		})
	}
}
