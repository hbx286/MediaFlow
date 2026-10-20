package tests

import (
	"context"
	"errors"
	"net"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"mediaflow/backend/internal/platform/lifecycle"
)

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type fakeHTTPServer struct {
	started  chan struct{}
	release  chan struct{}
	once     sync.Once
	log      *eventLog
	serveErr error
	shutdown func(context.Context) error
	closeErr error
}

func (s *fakeHTTPServer) Serve(net.Listener) error {
	close(s.started)
	<-s.release
	s.log.add("serve stopped")
	return s.serveErr
}

func (s *fakeHTTPServer) Shutdown(ctx context.Context) error {
	s.log.add("shutdown")
	return s.shutdown(ctx)
}

func (s *fakeHTTPServer) Close() error {
	s.log.add("force close")
	s.once.Do(func() { close(s.release) })
	return s.closeErr
}

type fakePool struct{ log *eventLog }

func (p fakePool) Close() { p.log.add("pool close") }

func TestLifecycleShutdownOrder(t *testing.T) {
	log := &eventLog{}
	server := &fakeHTTPServer{started: make(chan struct{}), release: make(chan struct{}), log: log, serveErr: http.ErrServerClosed}
	server.shutdown = func(context.Context) error {
		server.once.Do(func() { close(server.release) })
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- lifecycle.Run(ctx, server, nil, fakePool{log}, time.Second) }()
	<-server.started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := []string{"shutdown", "serve stopped", "pool close"}
	if got := log.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestLifecycleShutdownTimeoutReportsForceCloseError(t *testing.T) {
	log := &eventLog{}
	closeErr := errors.New("force close failed")
	server := &fakeHTTPServer{started: make(chan struct{}), release: make(chan struct{}), log: log, serveErr: http.ErrServerClosed, closeErr: closeErr}
	server.shutdown = func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- lifecycle.Run(ctx, server, nil, fakePool{log}, time.Millisecond) }()
	<-server.started
	cancel()
	err := <-done
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, closeErr) {
		t.Fatalf("error = %v", err)
	}
	want := []string{"shutdown", "force close", "serve stopped", "pool close"}
	if got := log.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestLifecycleServeFailureClosesResources(t *testing.T) {
	log := &eventLog{}
	serveErr := errors.New("accept failed")
	server := &fakeHTTPServer{started: make(chan struct{}), release: make(chan struct{}), log: log, serveErr: serveErr}
	server.once.Do(func() { close(server.release) })
	err := lifecycle.Run(context.Background(), server, nil, fakePool{log}, time.Second)
	if !errors.Is(err, serveErr) {
		t.Fatalf("error = %v", err)
	}
	want := []string{"serve stopped", "force close", "pool close"}
	if got := log.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}
