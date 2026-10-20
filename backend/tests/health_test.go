package tests

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mediaflow/backend/internal/health"
)

type pingFunc func(context.Context) error

func (f pingFunc) Ping(ctx context.Context) error { return f(ctx) }

func TestHealthRoutes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ping        pingFunc
		live, ready int
	}{
		{"database available", func(context.Context) error { return nil }, http.StatusOK, http.StatusOK},
		{"database unavailable", func(context.Context) error { return errors.New("secret database detail") }, http.StatusOK, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := health.NewRouter(tc.ping, time.Second, nil)
			for path, want := range map[string]int{"/health/live": tc.live, "/health/ready": tc.ready} {
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
				if recorder.Code != want {
					t.Fatalf("%s status = %d, want %d", path, recorder.Code, want)
				}
				if recorder.Header().Get("Content-Type") != "application/json; charset=utf-8" {
					t.Fatalf("%s content type = %q", path, recorder.Header().Get("Content-Type"))
				}
				if strings.Contains(recorder.Body.String(), "secret database detail") {
					t.Fatal("database error leaked")
				}
			}
		})
	}
}

func TestReadyRespectsPingDeadline(t *testing.T) {
	router := health.NewRouter(pingFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}), time.Millisecond, nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestReadyPanicUsesSafeRecovery(t *testing.T) {
	var logs bytes.Buffer
	router := health.NewRouter(pingFunc(func(context.Context) error {
		panic("private-panic-value")
	}), time.Second, slog.New(slog.NewJSONHandler(&logs, nil)))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if recorder.Code != http.StatusInternalServerError || recorder.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("response = %d %q", recorder.Code, recorder.Header().Get("Content-Type"))
	}
	if strings.Contains(logs.String(), "private-panic-value") || strings.Contains(recorder.Body.String(), "private-panic-value") {
		t.Fatal("panic value leaked")
	}
}
