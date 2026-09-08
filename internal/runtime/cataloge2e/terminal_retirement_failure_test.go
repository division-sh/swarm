package cataloge2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	flowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestTerminalMiddleMemberFailureRetainsSuffixBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, selectedForkReadinessCatalogFixture(t, 3, "agent"), backend, true)
			path := "worker-flow/worker-001"
			entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
			unrelatedPath := "worker-flow/worker-002"
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			unrelatedEntity := uuid.NewString()
			activationCtx := runtimeeffects.WithExecutionMode(worklifetime.WithOccurrence(ctx, h.rt.WorkOccurrence()), executionmode.Live)
			at := time.Now().UTC()
			trigger := eventtest.ExistingRunRootIngress(uuid.NewString(), "catalog.selected_fork_source_admitted", "cataloge2e", "", nil, 0, catalogRuntimeRunID, events.EnvelopeForEntityID(events.EventEnvelope{}, unrelatedEntity), at)
			if err := h.rt.Manager.ActivateFlowInstance(activationCtx, runtimepipeline.FlowInstanceActivationRequest{
				ContractBundle: semanticview.Wrap(h.bundle),
				Instance:       flowidentity.Stored(semanticview.Wrap(h.bundle), "worker-flow", unrelatedPath, "worker-002", unrelatedEntity, ""),
				Config:         map[string]any{"worker_id": "worker-002"}, Fields: map[string]any{"worker_id": "worker-002"}, TriggerEvent: trigger, OccurredAt: at,
			}); err != nil {
				t.Fatal(err)
			}
			identities := []runtimeagentidentity.Identity{}
			for _, cfg := range h.rt.Manager.ListAgentConfigs() {
				identity, err := cfg.ConcreteIdentity()
				if err != nil {
					t.Fatal(err)
				}
				if identity.RunID == catalogRuntimeRunID && identity.FlowInstance() == path {
					identities = append(identities, identity)
				}
			}
			sort.Slice(identities, func(i, j int) bool { return runtimeagentidentity.Less(identities[i], identities[j]) })
			if len(identities) != 3 {
				t.Fatalf("terminal set has %d agents", len(identities))
			}
			const injected = "terminal middle member rollback proof"
			condition := fmt.Sprintf("NEW.agent_id = '%s' AND NEW.flow_instance = '%s' AND NEW.lifecycle_phase = 'terminated'", strings.ReplaceAll(identities[1].AgentID(), "'", "''"), path)
			ddl := "CREATE TRIGGER terminal_middle_failure BEFORE UPDATE ON agents WHEN " + condition + " BEGIN SELECT RAISE(ABORT, '" + injected + "'); END"
			if backend == catalogBackendPostgres {
				if _, err := h.db.ExecContext(ctx, "CREATE FUNCTION terminal_middle_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION '"+injected+"'; END $$"); err != nil {
					t.Fatal(err)
				}
				ddl = "CREATE TRIGGER terminal_middle_failure BEFORE UPDATE ON agents FOR EACH ROW WHEN (" + condition + ") EXECUTE FUNCTION terminal_middle_failure()"
			}
			if _, err := h.db.ExecContext(ctx, ddl); err != nil {
				t.Fatal(err)
			}
			expectTerminalShutdownFailure(t, h, injected)
			request := runtimepipeline.FlowInstanceDeactivationRequest{Instance: flowidentity.Stored(nil, "worker-flow", path, "worker-001", entity, ""), FinalState: "complete"}
			if err := h.rt.Manager.DeactivateFlowInstanceModel(ctx, request); err == nil || !strings.Contains(err.Error(), injected) {
				t.Fatalf("terminal caller lost middle-member failure: %v", err)
			}
			if err := h.rt.Manager.WaitForQuiescence(ctx); err == nil || !strings.Contains(err.Error(), injected) {
				t.Fatalf("terminal join lost middle-member failure: %v", err)
			}
			var reader runtimemanager.AgentLifecycleStateReader
			if h.pg != nil {
				reader = h.pg
			} else {
				reader = h.sqlite
			}
			for i, identity := range identities {
				state, found, err := reader.LoadAgentLifecycleState(ctx, identity)
				if err != nil || !found {
					t.Fatalf("read exact terminal member %d: found=%t err=%v", i, found, err)
				}
				if i != 1 && state.Phase != runtimemanager.AgentLifecycleTerminated {
					t.Fatalf("terminal failure abandoned member %d: %+v", i, state)
				}
				if i == 1 && state.Phase == runtimemanager.AgentLifecycleTerminated {
					t.Fatal("failed member mutation did not roll back")
				}
			}
			for _, cfg := range h.rt.Manager.ListAgentConfigs() {
				if cfg.Identity.FlowInstance() == path {
					t.Fatalf("fenced failed flow still exposes executable agent %s", cfg.ID)
				}
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(unrelatedPath+"/worker.inspect"), "cataloge2e", "", nil, 0, catalogRuntimeRunID,
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, unrelatedEntity), unrelatedPath),
				eventtest.ConcreteTemplateRoutingSource("worker-flow", unrelatedPath, unrelatedEntity), time.Now().UTC())
			if err := h.rt.Bus.PublishAndWait(ctx, event); err != nil {
				t.Fatalf("unrelated flow execution: %v", err)
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(unrelatedPath))
			if err != nil {
				t.Fatal(err)
			}
			state, found, err := h.workflow.Load(ctx, owner)
			if err != nil || !found || state.CurrentState != "complete" || state.Status != "terminated" {
				t.Fatalf("unrelated flow terminal readback: state=%+v found=%t err=%v", state, found, err)
			}
		})
	}
}

func expectTerminalShutdownFailure(t *testing.T, h *runtimeHarness, injected string) {
	t.Helper()
	// Assert the expected error; the same process join and capability release
	// still precede store cleanup. Failure is not detached or called success.
	t.Cleanup(func() {
		h.shutdownOnce.Do(func() {
			err := h.rt.Shutdown()
			if err == nil || !strings.Contains(err.Error(), injected) {
				t.Errorf("shutdown lost terminal completion evidence: %v", err)
			} else {
				t.Logf("expected retained terminal shutdown failure: %v", err)
			}
			h.cancel()
			joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			h.processOwner.Retire()
			if _, err := h.processOwner.Join(joinCtx); err != nil {
				t.Errorf("join failed terminal process before store release: %v", err)
				return
			}
			if err := h.processTopology.Release(joinCtx); err != nil {
				t.Errorf("release joined process capability: %v", err)
			}
		})
	})
}

func TestDirectTerminalCommitPanicRetainsWholeSetBothStores(t *testing.T) {
	proveDirectTerminalCommitUnwind(t, "panic")
}

func TestDirectTerminalCommitCancellationRetainsWholeSetBothStores(t *testing.T) {
	proveDirectTerminalCommitUnwind(t, "cancellation")
}

func proveDirectTerminalCommitUnwind(t *testing.T, mode string) {
	t.Helper()
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, selectedForkReadinessCatalogFixture(t, 2, "agent"), backend, true)
			path := "worker-flow/worker-001"
			entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			configs := h.rt.Manager.ListAgentConfigs()
			callerCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			probe := &terminalUnwindProbe{mode: mode, status: "terminated", cancel: cancel}
			h.rt.Pipeline.SetTestLifecycleProbe(probe)
			const injected = "test panic after durable terminal commit"
			if mode == "panic" {
				expectTerminalShutdownFailure(t, h, injected)
			}
			evt := catalogRunScopedWorkerReadyEvent(t, catalogRuntimeRunID, path, entity, uuid.NewString())
			request := runtimepipeline.FlowInstanceDeactivationRequest{Instance: flowidentity.Stored(nil, "worker-flow", path, "worker-001", entity, "")}
			err := h.rt.Manager.DeactivateFlowInstanceModel(runtimecorrelation.WithInboundEvent(callerCtx, evt), request)
			if probe.calls.Load() != 1 {
				t.Fatalf("direct committed unwind: calls=%d err=%v", probe.calls.Load(), err)
			}
			joinErr := h.rt.Manager.WaitForQuiescence(ctx)
			if mode == "panic" {
				if err == nil || !strings.Contains(err.Error(), injected) || joinErr == nil || !strings.Contains(joinErr.Error(), injected) {
					t.Fatalf("committed failure escaped owning join: caller=%v join=%v", err, joinErr)
				}
			} else if err != nil || joinErr != nil || callerCtx.Err() == nil {
				t.Fatalf("cancelled caller lost owned terminal completion: caller=%v join=%v context=%v", err, joinErr, callerCtx.Err())
			}
			var reader runtimemanager.AgentLifecycleStateReader
			if h.pg != nil {
				reader = h.pg
			} else {
				reader = h.sqlite
			}
			members := 0
			for _, cfg := range configs {
				if cfg.Identity.FlowInstance() != path {
					continue
				}
				members++
				state, found, err := reader.LoadAgentLifecycleState(ctx, cfg.Identity)
				if err != nil || !found || state.Phase != runtimemanager.AgentLifecycleTerminated {
					t.Fatalf("committed panic abandoned member: %+v found=%t err=%v", state, found, err)
				}
			}
			if members != 2 {
				t.Fatalf("tested terminal members=%d, want 2", members)
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(path))
			if err != nil {
				t.Fatal(err)
			}
			state, found, err := h.workflow.Load(ctx, owner)
			if err != nil || !found || state.Status != "terminated" {
				t.Fatalf("direct committed terminal readback: %+v found=%t err=%v", state, found, err)
			}
			if h.rt.Bus.HasFlowInstanceRoute(owner) {
				t.Fatal("committed unwind retained a process-visible terminal flow route")
			}
		})
	}
}
