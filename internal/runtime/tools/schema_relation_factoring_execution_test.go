package tools_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func relationExecutionEntry(t *testing.T, url string, minimum int) (runtimecontracts.ToolSchemaEntry, error) {
	t.Helper()
	text := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaMinLength(minimum))
	object := func(name string, field runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
		return runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{name: field}), runtimecontracts.ToolSchemaRequired(name), runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(false))
	}
	return runtimecontracts.NewToolSchemaEntry(
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), object("receipt", text)),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: url}),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}),
		runtimecontracts.WithToolResponseMapping(map[string]any{"receipt": "{{response.body.receipt}}"}),
		runtimecontracts.WithToolCompiledResult(runtimecontracts.CompiledResultProjection{
			Fields:       map[string]runtimecontracts.CompiledResultField{"projected": {From: "result.receipt"}},
			OutputSchema: object("projected", runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaMinLength(2))),
		}),
	)
}

func TestSchemaRelationFactoringBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveRelationSelectedExecution(t, backend, false) })
	}
}

func TestSchemaRelationFactoringRetainedReconstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveRelationSelectedExecution(t, backend, true) })
	}
}

func proveRelationSelectedExecution(t *testing.T, backend string, reconstruct bool) {
	t.Helper()
	var selected httpSettlementSelectedStore
	var location string
	if backend == "sqlite" {
		location = filepath.Join(t.TempDir(), "runtime.db")
		selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), location)
	} else {
		location = testutil.StartPostgresDSN(t)
		selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, location)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"receipt":"provider-42"}`))
	}))
	t.Cleanup(server.Close)
	refused, err := relationExecutionEntry(t, server.URL, 0)
	if err == nil || !strings.Contains(err.Error(), "compiled result result.receipt -> projected string length") || !refused.IsZero() {
		t.Fatalf("invalid direction acquired execution authority: zero=%t err=%v", refused.IsZero(), err)
	}
	attempts, err := storetest.ReadExternalAttemptStorage(context.Background(), selected)
	if err != nil || len(attempts) != 0 || calls.Load() != 0 {
		t.Fatalf("pre-authority refusal produced effects: %v %v calls=%d", attempts, err, calls.Load())
	}
	entry, err := relationExecutionEntry(t, server.URL, 3)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := entry.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := selectedHTTPSettlementContext(t, selected, "schema-relation-selected")
	makeExecutor := func(entry runtimecontracts.ToolSchemaEntry) *runtimetools.Executor {
		return runtimetools.NewExecutorWithOptions(&humanTaskRuntimeLogBus{}, runtimetools.ExecutorOptions{WorkflowSource: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"settlement-proof": entry}})})
	}
	executor := makeExecutor(entry)
	out, err := executor.Execute(ctx, "settlement-proof", map[string]any{})
	if err != nil || !reflect.DeepEqual(out, map[string]any{"receipt": "provider-42"}) || calls.Load() != 1 {
		t.Fatalf("selected connector execution: %v %v calls=%d", out, err, calls.Load())
	}
	// The HTTP executor returns connector output. The activity owner consumes
	// this admitted projection; its integrated path has separate channel proof.
	projection, _ := entry.CompiledResultExecution()
	mapped, err := projection.Project(out)
	if err != nil || !reflect.DeepEqual(mapped, map[string]any{"projected": "provider-42"}) {
		t.Fatalf("admitted mapping: %v %v", mapped, err)
	}
	attempts, err = storetest.ReadExternalAttemptStorage(context.Background(), selected)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("selected attempts: %v %v", attempts, err)
	}
	requireHTTPSettlementOutcome(t, selected, attempts[0].OperationID, runtimeeffects.StateSettled, 1)
	if !reconstruct {
		return
	}
	// Execution is synchronous and settled. Retire the exact old native owner;
	// recreate the executor and its admitted entry, never recover its old pool.
	if err := selected.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if backend == "sqlite" {
		selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), location)
	} else {
		selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, location)
	}
	fresh, err := relationExecutionEntry(t, server.URL, 3)
	if err != nil {
		t.Fatal(err)
	}
	freshIdentity, err := fresh.CanonicalHash()
	if err != nil || freshIdentity != identity || !entry.OutputSchema().Equal(fresh.OutputSchema()) {
		t.Fatal("fresh admission changed source or projection identity")
	}
	freshProjection, _ := fresh.CompiledResultExecution()
	projected, err := freshProjection.Project(map[string]any{"receipt": "provider-42"})
	if err != nil || !reflect.DeepEqual(projected, mapped) {
		t.Fatalf("fresh mapping changed: %v %v", projected, err)
	}
	requireHTTPSettlementOutcome(t, selected, attempts[0].OperationID, runtimeeffects.StateSettled, 1)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
	executor = makeExecutor(fresh)
	if replay, err := executor.Execute(ctx, "settlement-proof", map[string]any{}); replay != nil || err == nil || calls.Load() != 1 {
		t.Fatalf("retained settled operation dispatched again: %v %v calls=%d", replay, err, calls.Load())
	}
	requireHTTPSettlementOutcome(t, selected, attempts[0].OperationID, runtimeeffects.StateSettled, 1)
}
