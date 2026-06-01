package apiv1

import (
	"context"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestOperatorRuntimeControlHandlersUseIngressOwnerAndIdempotency(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ingress := runtimeingress.NewController(pg, bus, runtimeingress.Options{
		Now: func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
	})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(ingress)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:            func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) },
			Ready:          func() bool { return true },
			Database:       fakePinger{},
			Idempotency:    pg,
			RuntimeIngress: ingress,
		}),
	})

	pauseBody := `{"jsonrpc":"2.0","id":"pause","method":"runtime.pause","params":{"idempotency_key":"idem-pause"}}`
	pause := rpcCall(t, handler, pauseBody)
	if pause.Error != nil {
		t.Fatalf("runtime.pause error = %#v", pause.Error)
	}
	if result := asMap(t, pause.Result); result["ok"] != true {
		t.Fatalf("runtime.pause result = %#v", result)
	}
	if state, err := pg.LoadRuntimeIngressState(context.Background()); err != nil {
		t.Fatalf("LoadRuntimeIngressState: %v", err)
	} else if state.Status != runtimeingress.StatusPaused {
		t.Fatalf("runtime ingress status = %q, want paused", state.Status)
	}
	if count := countEventsByName(t, db, "platform.paused"); count != 1 {
		t.Fatalf("platform.paused events = %d, want 1", count)
	}

	replay := rpcCall(t, handler, pauseBody)
	if replay.Error != nil {
		t.Fatalf("runtime.pause replay error = %#v", replay.Error)
	}
	if count := countEventsByName(t, db, "platform.paused"); count != 1 {
		t.Fatalf("platform.paused events after replay = %d, want 1", count)
	}

	duplicatePause := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"pause-again","method":"runtime.pause","params":{}}`)
	if duplicatePause.Error == nil {
		t.Fatal("fresh duplicate runtime.pause error = nil")
	}
	if data := asMap(t, duplicatePause.Error.Data); data["code"] != RuntimeAlreadyPausedCode {
		t.Fatalf("fresh duplicate runtime.pause data = %#v, want %s", data, RuntimeAlreadyPausedCode)
	}

	resumeBody := `{"jsonrpc":"2.0","id":"resume","method":"runtime.resume","params":{"idempotency_key":"idem-resume"}}`
	resume := rpcCall(t, handler, resumeBody)
	if resume.Error != nil {
		t.Fatalf("runtime.resume error = %#v", resume.Error)
	}
	if result := asMap(t, resume.Result); result["ok"] != true {
		t.Fatalf("runtime.resume result = %#v", result)
	}
	if state, err := pg.LoadRuntimeIngressState(context.Background()); err != nil {
		t.Fatalf("LoadRuntimeIngressState: %v", err)
	} else if state.Status != runtimeingress.StatusRunning {
		t.Fatalf("runtime ingress status = %q, want running", state.Status)
	}
	if count := countEventsByName(t, db, "platform.resumed"); count != 1 {
		t.Fatalf("platform.resumed events = %d, want 1", count)
	}

	resumeReplay := rpcCall(t, handler, resumeBody)
	if resumeReplay.Error != nil {
		t.Fatalf("runtime.resume replay error = %#v", resumeReplay.Error)
	}
	if count := countEventsByName(t, db, "platform.resumed"); count != 1 {
		t.Fatalf("platform.resumed events after replay = %d, want 1", count)
	}

	duplicateResume := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"resume-again","method":"runtime.resume","params":{}}`)
	if duplicateResume.Error == nil {
		t.Fatal("fresh duplicate runtime.resume error = nil")
	}
	if data := asMap(t, duplicateResume.Error.Data); data["code"] != RuntimeNotPausedCode {
		t.Fatalf("fresh duplicate runtime.resume data = %#v, want %s", data, RuntimeNotPausedCode)
	}
}
