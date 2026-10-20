package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

type HTTPServer interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
	Close() error
}

type Pool interface {
	Close()
}

func Run(ctx context.Context, server HTTPServer, listener net.Listener, pool Pool, shutdownTimeout time.Duration) error {
	defer pool.Close()
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.Serve(listener)
	}()

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		closeErr := server.Close()
		return errors.Join(fmt.Errorf("HTTP 服务异常退出: %w", err), wrapCloseError(closeErr))
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		shutdownErr := server.Shutdown(shutdownCtx)
		cancel()
		var closeErr error
		if shutdownErr != nil {
			closeErr = server.Close()
		}
		serveErr := <-serverErrors
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		if shutdownErr == nil && serveErr == nil {
			return nil
		}
		var wrappedShutdown, wrappedServe error
		if shutdownErr != nil {
			wrappedShutdown = fmt.Errorf("HTTP 优雅关闭失败: %w", shutdownErr)
		}
		if serveErr != nil {
			wrappedServe = fmt.Errorf("HTTP Serve 退出失败: %w", serveErr)
		}
		return errors.Join(wrappedShutdown, wrapCloseError(closeErr), wrappedServe)
	}
}

func wrapCloseError(err error) error {
	if err != nil {
		return fmt.Errorf("HTTP 强制关闭失败: %w", err)
	}
	return nil
}
