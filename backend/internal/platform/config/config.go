package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr              string
	HTTPReadHeaderTimeout time.Duration
	HTTPReadTimeout       time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration
	ShutdownTimeout       time.Duration
	DatabaseURL           string
	DBConnectTimeout      time.Duration
	DBPingTimeout         time.Duration
	DBMaxConns            int32
	DBMinConns            int32
	DBMaxConnIdleTime     time.Duration
	DBHealthCheckPeriod   time.Duration
}

func Load() (Config, error) {
	cfg := Config{DatabaseURL: strings.TrimSpace(os.Getenv("DATABASE_URL"))}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	u, err := url.Parse(cfg.DatabaseURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be a PostgreSQL URL")
	}
	cfg.HTTPAddr = envOr("HTTP_ADDR", ":8080")
	_, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return Config{}, fmt.Errorf("HTTP_ADDR must be host:port")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return Config{}, fmt.Errorf("HTTP_ADDR has invalid port")
	}
	for _, item := range []struct {
		name string
		def  time.Duration
		dst  *time.Duration
	}{
		{"HTTP_READ_HEADER_TIMEOUT", 5 * time.Second, &cfg.HTTPReadHeaderTimeout},
		{"HTTP_READ_TIMEOUT", 10 * time.Second, &cfg.HTTPReadTimeout},
		{"HTTP_WRITE_TIMEOUT", 15 * time.Second, &cfg.HTTPWriteTimeout},
		{"HTTP_IDLE_TIMEOUT", 60 * time.Second, &cfg.HTTPIdleTimeout},
		{"SHUTDOWN_TIMEOUT", 10 * time.Second, &cfg.ShutdownTimeout},
		{"DB_CONNECT_TIMEOUT", 5 * time.Second, &cfg.DBConnectTimeout},
		{"DB_PING_TIMEOUT", 2 * time.Second, &cfg.DBPingTimeout},
		{"DB_MAX_CONN_IDLE_TIME", 30 * time.Minute, &cfg.DBMaxConnIdleTime},
		{"DB_HEALTH_CHECK_PERIOD", time.Minute, &cfg.DBHealthCheckPeriod},
	} {
		value, err := time.ParseDuration(envOr(item.name, item.def.String()))
		if err != nil || value <= 0 {
			return Config{}, fmt.Errorf("%s must be a positive duration", item.name)
		}
		*item.dst = value
	}
	for _, item := range []struct {
		name string
		def  int32
		dst  *int32
	}{
		{"DB_MAX_CONNS", 10, &cfg.DBMaxConns},
		{"DB_MIN_CONNS", 1, &cfg.DBMinConns},
	} {
		value, err := strconv.ParseInt(envOr(item.name, strconv.FormatInt(int64(item.def), 10)), 10, 32)
		if err != nil || value < 0 || (item.name == "DB_MAX_CONNS" && value == 0) {
			return Config{}, fmt.Errorf("%s must be a valid connection count", item.name)
		}
		*item.dst = int32(value)
	}
	if cfg.DBMinConns > cfg.DBMaxConns {
		return Config{}, fmt.Errorf("DB_MIN_CONNS cannot exceed DB_MAX_CONNS")
	}
	return cfg, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
