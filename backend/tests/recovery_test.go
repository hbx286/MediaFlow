package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/gin-gonic/gin"
	"mediaflow/backend/internal/health"
)

func TestRecoveryReturnsProblemDetailsWithoutLeakingRequest(t *testing.T) {
	var logs bytes.Buffer
	var ginLogs bytes.Buffer
	previousGinWriter := gin.DefaultErrorWriter
	gin.DefaultErrorWriter = &ginLogs
	defer func() { gin.DefaultErrorWriter = previousGinWriter }()
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	router := gin.New()
	router.Use(health.Recovery(logger))
	router.GET("/panic", func(c *gin.Context) {
		panic("unreviewed panic secret")
	})
	request := httptest.NewRequest(http.MethodGet, "/panic?token=query-secret", strings.NewReader("body-secret"))
	request.Header.Set("Authorization", "Bearer auth-secret")
	request.Header.Set("Cookie", "session=cookie-secret")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("content type = %q", got)
	}
	var problem struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	if problem.Type != "about:blank" || problem.Title != "Internal Server Error" || problem.Status != 500 {
		t.Fatalf("problem = %+v", problem)
	}
	for _, secret := range []string{"query-secret", "auth-secret", "cookie-secret", "body-secret", "unreviewed panic secret"} {
		if strings.Contains(logs.String(), secret) || strings.Contains(ginLogs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
			t.Fatalf("sensitive value leaked: %s", secret)
		}
	}
	if ginLogs.Len() != 0 {
		t.Fatalf("Gin default error writer received %d bytes", ginLogs.Len())
	}
	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatalf("log is not JSON: %v", err)
	}
	if record["level"] != "ERROR" || record["route"] != "/panic" {
		t.Fatalf("unexpected log: %v", record)
	}
}

func TestRecoveryDoesNotRewriteCommittedOrBrokenResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler gin.HandlerFunc
		status  int
		body    string
	}{
		{"committed", func(c *gin.Context) { c.String(http.StatusAccepted, "started"); panic("secret") }, http.StatusAccepted, "started"},
		{"broken connection", func(c *gin.Context) { panic(errors.Join(syscall.EPIPE, errors.New("secret"))) }, http.StatusOK, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			router := gin.New()
			router.Use(health.Recovery(slog.New(slog.NewJSONHandler(&logs, nil))))
			router.GET("/panic", tc.handler)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/panic", nil))
			if recorder.Code != tc.status || recorder.Body.String() != tc.body {
				t.Fatalf("response = %d %q", recorder.Code, recorder.Body.String())
			}
			if strings.Contains(logs.String(), "secret") {
				t.Fatal("panic value leaked")
			}
		})
	}
}
