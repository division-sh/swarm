package serveapp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type serveInstalledHandler struct{ handler http.Handler }

type serveProbeHandler struct {
	standby http.Handler
	live    atomic.Pointer[serveInstalledHandler]
}

func (h *serveProbeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if live := h.live.Load(); live != nil {
		live.handler.ServeHTTP(w, r)
		return
	}
	h.standby.ServeHTTP(w, r)
}

type serveProcessProbe struct {
	server   *http.Server
	listener net.Listener
	handler  *serveProbeHandler
	joined   chan struct{}
}

func startServeProcessProbe(ctx context.Context, work *worklifetime.Process, addr string, onFailure func(string, error)) (*serveProcessProbe, error) {
	listener, err := cliapp.ListenServeHTTPListener("api", addr)
	if err != nil {
		return nil, err
	}
	lease, err := work.Begin(ctx)
	if err != nil {
		return nil, errors.Join(err, listener.Close())
	}
	var ready atomic.Bool
	standby := newAPIServer(&ready, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "waiting for selected-store process lease", http.StatusServiceUnavailable)
	}), nil, ctx)
	handler := &serveProbeHandler{standby: standby.Handler}
	standby.Handler = handler
	probe := &serveProcessProbe{server: standby, listener: listener, handler: handler, joined: make(chan struct{})}
	go func() {
		defer close(probe.joined)
		defer func() { _ = lease.Done() }()
		serveHTTPServer("api", standby, listener, onFailure)
	}()
	return probe, nil
}

func (p *serveProcessProbe) Install(ctx context.Context, handler http.Handler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if handler == nil || !p.handler.live.CompareAndSwap(nil, &serveInstalledHandler{handler: handler}) {
		return errors.New("serving handler must be installed exactly once after process admission")
	}
	return nil
}

func (p *serveProcessProbe) Close(ctx context.Context) error {
	err := shutdownHTTPServer(ctx, "api", p.server)
	if err != nil {
		err = errors.Join(err, p.server.Close())
	}
	<-p.joined
	return err
}
