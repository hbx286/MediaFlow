package health

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

type Pinger interface {
	Ping(context.Context) error
}

func NewRouter(db Pinger, pingTimeout time.Duration, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(Recovery(logger))
	router.Use(func(c *gin.Context) {
		started := time.Now()
		c.Next()
		logger.Info("HTTP 请求", "method", c.Request.Method, "path", c.FullPath(), "status", c.Writer.Status(), "duration_ms", time.Since(started).Milliseconds())
	})
	router.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/health/ready", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), pingTimeout)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			logger.Warn("数据库就绪检查失败")
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	return router
}

func Recovery(logger *slog.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				broken := false
				if err, ok := recovered.(error); ok {
					broken = errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)
				}
				written := c.Writer.Written()
				logger.Error("HTTP 请求处理异常", "method", c.Request.Method, "route", c.FullPath(), "connection_broken", broken, "response_written", written)
				c.Abort()
				if broken || written {
					return
				}
				c.Data(http.StatusInternalServerError, "application/problem+json", []byte(`{"type":"about:blank","title":"Internal Server Error","status":500}`))
			}
		}()
		c.Next()
	}
}
