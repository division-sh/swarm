package apiv1

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	runtimebus "swarm/internal/runtime/bus"
	runtimeruncontrol "swarm/internal/runtime/runcontrol"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, bus, runtimeruncontrol.Options{
		Now: func() time.Time { return time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC) },
	})
	bus.SetRunDispatchGate(controller)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Now:         func() time.Time { return time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC) },
			Ready:       func() bool { return true },
			Database:    fakePinger{},
			Idempotency: pg,
			RunControl:  controller,
		}),
	})
	ctx := context.Background()
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	pauseBody := runControlBody("run.pause", runID, "idem-pause")
	pause := rpcCall(t, handler, pauseBody)
	if pause.Error != nil {
		t.Fatalf("run.pause error = %#v", pause.Error)
	}
	if result := asMap(t, pause.Result); result["ok"] != true {
		t.Fatalf("run.pause result = %#v", result)
	}
	assertRunControlState(t, db, runID, "paused", "paused")

	pauseReplay := rpcCall(t, handler, pauseBody)
	if pauseReplay.Error != nil {
		t.Fatalf("run.pause replay error = %#v", pauseReplay.Error)
	}
	if count := countAPIIdempotencyRows(t, db); count != 1 {
		t.Fatalf("api_idempotency rows after pause replay = %d, want 1", count)
	}

	duplicatePause := rpcCall(t, handler, runControlBody("run.pause", runID, ""))
	if duplicatePause.Error == nil {
		t.Fatal("fresh duplicate run.pause error = nil")
	}
	if data := asMap(t, duplicatePause.Error.Data); data["code"] != RunAlreadyPausedCode {
		t.Fatalf("fresh duplicate run.pause data = %#v, want %s", data, RunAlreadyPausedCode)
	}

	continueBody := runControlBody("run.continue", runID, "idem-continue")
	continued := rpcCall(t, handler, continueBody)
	if continued.Error != nil {
		t.Fatalf("run.continue error = %#v", continued.Error)
	}
	assertRunControlState(t, db, runID, "running", "running")

	continueReplay := rpcCall(t, handler, continueBody)
	if continueReplay.Error != nil {
		t.Fatalf("run.continue replay error = %#v", continueReplay.Error)
	}
	duplicateContinue := rpcCall(t, handler, runControlBody("run.continue", runID, ""))
	if duplicateContinue.Error == nil {
		t.Fatal("fresh duplicate run.continue error = nil")
	}
	if data := asMap(t, duplicateContinue.Error.Data); data["code"] != RunNotPausedCode {
		t.Fatalf("fresh duplicate run.continue data = %#v, want %s", data, RunNotPausedCode)
	}

	stopBody := runControlBody("run.stop", runID, "idem-stop")
	stopped := rpcCall(t, handler, stopBody)
	if stopped.Error != nil {
		t.Fatalf("run.stop error = %#v", stopped.Error)
	}
	assertRunControlState(t, db, runID, "cancelled", "stopped")

	stopReplay := rpcCall(t, handler, stopBody)
	if stopReplay.Error != nil {
		t.Fatalf("run.stop replay error = %#v", stopReplay.Error)
	}
	terminalPause := rpcCall(t, handler, runControlBody("run.pause", runID, ""))
	if terminalPause.Error == nil {
		t.Fatal("terminal run.pause error = nil")
	}
	if data := asMap(t, terminalPause.Error.Data); data["code"] != RunAlreadyTerminalCode {
		t.Fatalf("terminal run.pause data = %#v, want %s", data, RunAlreadyTerminalCode)
	}
}

func TestOperatorRunControlHandlersTypedResourceErrors(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := &store.PostgresStore{DB: db}
	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, bus, runtimeruncontrol.Options{})
	bus.SetRunDispatchGate(controller)
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Idempotency: pg,
			RunControl:  controller,
		}),
	})

	missingRunID := uuid.NewString()
	for _, method := range []string{"run.stop", "run.pause", "run.continue"} {
		resp := rpcCall(t, handler, runControlBody(method, missingRunID, ""))
		if resp.Error == nil {
			t.Fatalf("%s missing run error = nil", method)
		}
		if data := asMap(t, resp.Error.Data); data["code"] != RunNotFoundCode {
			t.Fatalf("%s missing data = %#v, want %s", method, data, RunNotFoundCode)
		}
	}

	materializedLikePausedRunID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'paused')`, materializedLikePausedRunID); err != nil {
		t.Fatalf("seed paused run without control owner: %v", err)
	}
	resp := rpcCall(t, handler, runControlBody("run.continue", materializedLikePausedRunID, ""))
	if resp.Error == nil {
		t.Fatal("run.continue without operator pause owner error = nil")
	}
	if data := asMap(t, resp.Error.Data); data["code"] != RunNotPausedCode {
		t.Fatalf("run.continue without owner data = %#v, want %s", data, RunNotPausedCode)
	}
}

func runControlBody(method, runID, idempotencyKey string) string {
	if idempotencyKey == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":"control","method":%q,"params":{"run_id":%q}}`, method, runID)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"control","method":%q,"params":{"run_id":%q,"idempotency_key":%q}}`, method, runID, idempotencyKey)
}

func assertRunControlState(t *testing.T, db *sql.DB, runID, wantRunStatus, wantControlStatus string) {
	t.Helper()
	var runStatus, controlStatus string
	if err := db.QueryRowContext(context.Background(), `
		SELECT r.status, COALESCE(rc.control_status, '')
		FROM runs r
		LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&runStatus, &controlStatus); err != nil {
		t.Fatalf("load run control state: %v", err)
	}
	if runStatus != wantRunStatus || controlStatus != wantControlStatus {
		t.Fatalf("run/control status = %s/%s, want %s/%s", runStatus, controlStatus, wantRunStatus, wantControlStatus)
	}
}
