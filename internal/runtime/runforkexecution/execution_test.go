package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providerconnectors"
	swaruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestExecuteSelectedContractRunForkRejectsDeferredWorkBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name            string
		fixture         string
		eventName       string
		seedSourceTimer bool
		wantCode        string
		wantCapability  string
	}{
		{
			name:            "revisioned active source timer",
			fixture:         "tests/tier5-flow-lifecycle/test-timer-fire",
			eventName:       "timer.scheduled",
			seedSourceTimer: true,
			wantCode:        runfork.RunForkBlockerTimerHistoryUnproven,
			wantCapability:  selectedContractDeferredWorkRevisionTimerHistory,
		},
		{
			name:           "selected handler can create workflow timer",
			fixture:        "tests/tier5-flow-lifecycle/test-timer-fire",
			eventName:      "timer.scheduled",
			wantCode:       selectedContractDeferredWorkOwnerUnavailable,
			wantCapability: selectedContractDeferredWorkWorkflowTimer,
		},
		{
			name:           "selected handler can arm workflow join timeout",
			fixture:        "examples/routing/fan-in/barrier",
			eventName:      "portfolio.setup",
			wantCode:       selectedContractDeferredWorkOwnerUnavailable,
			wantCapability: selectedContractDeferredWorkWorkflowJoinTimeout,
		},
		{
			name:           "selected connect can create dynamic flow",
			fixture:        "examples/routing/template-create-minted-key",
			eventName:      "validation.triggered",
			wantCode:       selectedContractDeferredWorkOwnerUnavailable,
			wantCapability: selectedContractDeferredWorkDynamicFlowCreation,
		},
		{
			name:           "selected connect can select or create missing dynamic flow",
			fixture:        "examples/routing/template-select-or-create",
			eventName:      "producer/account.requested",
			wantCode:       selectedContractDeferredWorkOwnerUnavailable,
			wantCapability: selectedContractDeferredWorkDynamicFlowCreation,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			ctx := runForkTestContext(t)
			repoRoot := runForkExecutionRepoRoot(t)
			contractsRoot := filepath.Join(repoRoot, test.fixture)
			loader := ContractBundleSourceLoader{
				RepoRoot:         repoRoot,
				PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot),
			}
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			sourceEventID := uuid.NewString()
			at := time.Unix(1700002210, 0).UTC()
			seedSelectedExecutionSourceRunWithPrimaryRoute(
				t,
				db,
				sourceRunID,
				entityID,
				sourceEventID,
				test.eventName,
				at,
				selectedExecutionTestAgentRoute(t, "source-agent-that-must-not-route", "flow-a/1"),
				nil,
			)

			sourceTimerID := ""
			if test.seedSourceTimer {
				sourceTimerID = uuid.NewString()
				ref := timeridentity.WorkflowTimerActivationRef{
					ActivationID:        sourceTimerID,
					DeclarationKey:      "test-node.check_timer",
					DeclarationRevision: "sha256:test-node-check-timer",
					Cause:               timeridentity.WorkflowTimerActivationCauseInitial,
				}
				if _, err := db.ExecContext(ctx, `
					INSERT INTO timers (
						timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id, flow_instance, fire_event,
						fire_payload, routing_source, execution_mode, fire_at, owner_agent, owner_kind, task_type, status, created_at
					)
					VALUES (
						$1::uuid, $2::uuid, $3, $4::uuid, 'flow-a', '1', 'flow-a/1', 'timer.check',
						'{}'::jsonb, jsonb_build_object('kind', 'flow_owned_control', 'route', jsonb_build_object('flow_id', 'flow-a', 'flow_instance', 'flow-a/1', 'entity_id', $4::text)),
						'live', $5, 'test-node', 'system', 'workflow_timer', 'active', $6
					)
				`, sourceTimerID, sourceRunID, ref.TaskID(), entityID, at.Add(time.Hour), at); err != nil {
					t.Fatalf("seed source workflow timer: %v", err)
				}
			}
			captureSelectedExecutionSourceRevision(t, db, sourceRunID)
			var sourceStatusBefore string
			if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatusBefore); err != nil {
				t.Fatalf("load source status before rejection: %v", err)
			}
			runtimeTopologyBefore := selectedContractDynamicRuntimeGlobalSnapshot(t, ctx, db)

			result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID:         sourceRunID,
				At:                  sourceEventID,
				ConfirmSourceFreeze: true,
				Owner:               selectedContractExecutionOwnerForTest(t, pg),
				SourceLoader:        loader,
				ContractSelection: runfork.RunForkContractSelection{
					Mode:          runfork.RunForkContractSelectionModeSelectedContracts,
					ContractsRoot: contractsRoot,
				},
			})
			failure, ok := runtimefailures.EnvelopeFromError(err)
			if err == nil || !ok || failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != test.wantCode {
				t.Fatalf("ExecuteSelectedContractRunFork result=%#v error=%v, want %s rejection", result, err, test.wantCode)
			}
			capabilities, ok := failure.Detail.Attributes["capabilities"].([]string)
			if !ok || !slices.Contains(capabilities, test.wantCapability) {
				t.Fatalf("failure capabilities = %#v, want %q", failure.Detail.Attributes["capabilities"], test.wantCapability)
			}
			if result.Owner != runfork.RunForkSelectedContractExecutionOwner || result.Materialization.ForkRunID != "" {
				t.Fatalf("rejected result = %#v, want owner and no materialization", result)
			}

			assertSelectedContractDeferredWorkRejectionHasNoForkMutation(t, ctx, db, sourceRunID)
			runtimeTopologyAfter := selectedContractDynamicRuntimeGlobalSnapshot(t, ctx, db)
			for fact, want := range runtimeTopologyBefore {
				if got := runtimeTopologyAfter[fact]; got != want {
					t.Fatalf("deferred-work rejection changed global %s from %d to %d", fact, want, got)
				}
			}
			var sourceStatusAfter string
			if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatusAfter); err != nil {
				t.Fatalf("load source status after rejection: %v", err)
			}
			if sourceStatusAfter != sourceStatusBefore {
				t.Fatalf("source status changed from %q to %q during rejected deferred-work fork", sourceStatusBefore, sourceStatusAfter)
			}
			if sourceTimerID != "" {
				var status string
				if err := db.QueryRowContext(ctx, `SELECT status FROM timers WHERE timer_id = $1::uuid AND run_id = $2::uuid`, sourceTimerID, sourceRunID).Scan(&status); err != nil {
					t.Fatalf("load rejected source timer: %v", err)
				}
				if status != "active" {
					t.Fatalf("source timer status = %q, want active and untouched", status)
				}
			}
		})
	}
}

func TestActivateSelectedContractRunForkRejectsDeferredWorkBeforeExecutableMutation(t *testing.T) {
	for _, test := range []struct {
		name           string
		fixture        string
		eventName      string
		stateOnly      bool
		wantCapability string
	}{
		{
			name:           "delivery replay workflow timer",
			fixture:        "tests/tier5-flow-lifecycle/test-timer-fire",
			eventName:      "timer.scheduled",
			wantCapability: selectedContractDeferredWorkWorkflowTimer,
		},
		{
			name:           "state only workflow timer",
			fixture:        "tests/tier5-flow-lifecycle/test-timer-fire",
			eventName:      "timer.scheduled",
			stateOnly:      true,
			wantCapability: selectedContractDeferredWorkWorkflowTimer,
		},
		{
			name:           "delivery replay workflow join timeout",
			fixture:        "examples/routing/fan-in/barrier",
			eventName:      "portfolio.setup",
			wantCapability: selectedContractDeferredWorkWorkflowJoinTimeout,
		},
		{
			name:           "state only workflow join timeout",
			fixture:        "examples/routing/fan-in/barrier",
			eventName:      "portfolio.setup",
			stateOnly:      true,
			wantCapability: selectedContractDeferredWorkWorkflowJoinTimeout,
		},
		{
			name:           "delivery replay dynamic flow creation",
			fixture:        "examples/routing/template-create-minted-key",
			eventName:      "validation.triggered",
			wantCapability: selectedContractDeferredWorkDynamicFlowCreation,
		},
		{
			name:           "delivery replay select or create missing dynamic flow",
			fixture:        "examples/routing/template-select-or-create",
			eventName:      "producer/account.requested",
			wantCapability: selectedContractDeferredWorkDynamicFlowCreation,
		},
		{
			name:           "state only select or create missing dynamic flow",
			fixture:        "examples/routing/template-select-or-create",
			eventName:      "producer/account.requested",
			stateOnly:      true,
			wantCapability: selectedContractDeferredWorkDynamicFlowCreation,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			ctx := runForkTestContext(t)
			repoRoot := runForkExecutionRepoRoot(t)
			contractsRoot := filepath.Join(repoRoot, test.fixture)
			loader := ContractBundleSourceLoader{
				RepoRoot:         repoRoot,
				PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot),
			}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
				Mode:          runfork.RunForkContractSelectionModeSelectedContracts,
				ContractsRoot: contractsRoot,
			})
			if err != nil {
				t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
			}
			selection := runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot)

			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			sourceEventID := uuid.NewString()
			at := time.Unix(1700002215, 0).UTC()
			if test.stateOnly {
				seedSelectedExecutionStateOnlySourceRun(t, db, sourceRunID, sourceEventID, test.eventName, at, loaded.BundleSourceFact)
			} else {
				seedSelectedExecutionSourceRunWithPrimaryRoute(
					t,
					db,
					sourceRunID,
					entityID,
					sourceEventID,
					test.eventName,
					at,
					selectedExecutionTestAgentRoute(t, "source-agent-that-must-not-route", "flow-a/1"),
					nil,
					loaded.BundleSourceFact,
				)
			}
			captureSelectedExecutionSourceRevision(t, db, sourceRunID)
			plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: sourceEventID})
			if err != nil {
				t.Fatalf("PlanRunFork: %v", err)
			}
			replayAdmission := runfork.RunForkReplayResumeAdmissionWithSelectedRouteResolution(
				runfork.RunForkSelectedContractReplayResumeAdmission(plan),
			)
			if test.stateOnly {
				if !replayAdmission.StateOnlyExecutionReady || replayAdmission.DeliveryEventReplayReady {
					t.Fatalf("state-only replay admission = %#v", replayAdmission)
				}
			} else if !replayAdmission.DeliveryEventReplayReady {
				t.Fatalf("delivery replay admission = %#v", replayAdmission)
			}

			materialized := materializeSelectedExecutionForkForTest(t, ctx, pg, loaded, selection, sourceRunID, sourceEventID)
			before := selectedForkExecutableMutationSnapshotForTest(t, ctx, db, materialized.ForkRunID)
			var sourceStatusBefore string
			if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatusBefore); err != nil {
				t.Fatalf("load source status before activation: %v", err)
			}

			result, err := activateLiveSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
				ForkRunID:           materialized.ForkRunID,
				ConfirmSourceFreeze: true,
				Store:               pg,
				ExecutionOwner:      selectedContractExecutionOwnerForTest(t, pg),
				SourceLoader:        loader,
			})
			failure, ok := runtimefailures.EnvelopeFromError(err)
			if err == nil || !ok || failure.Class != runtimefailures.ClassDependencyUnavailable ||
				failure.Detail.Code != selectedContractDeferredWorkOwnerUnavailable {
				t.Fatalf("ActivateSelectedContractRunFork result=%#v error=%v, failure=%#v", result, err, failure)
			}
			capabilities, ok := failure.Detail.Attributes["capabilities"].([]string)
			if !ok || !slices.Contains(capabilities, test.wantCapability) {
				t.Fatalf("failure capabilities = %#v, want %q", failure.Detail.Attributes["capabilities"], test.wantCapability)
			}
			after := selectedForkExecutableMutationSnapshotForTest(t, ctx, db, materialized.ForkRunID)
			for fact, want := range before {
				if got := after[fact]; got != want {
					t.Fatalf("activation changed %s from %q to %q", fact, want, got)
				}
			}
			var sourceStatusAfter string
			if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatusAfter); err != nil {
				t.Fatalf("load source status after activation: %v", err)
			}
			if sourceStatusAfter != sourceStatusBefore {
				t.Fatalf("source status changed from %q to %q", sourceStatusBefore, sourceStatusAfter)
			}
		})
	}
}

func selectedForkExecutableMutationSnapshotForTest(t testing.TB, ctx context.Context, db *sql.DB, forkRunID string) map[string]string {
	t.Helper()
	snapshot := map[string]string{}
	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, forkRunID).Scan(&runStatus); err != nil {
		t.Fatalf("load selected fork status: %v", err)
	}
	snapshot["run_status"] = runStatus
	for _, probe := range []struct {
		name  string
		query string
	}{
		{name: "events", query: `SELECT COUNT(*)::text FROM events WHERE run_id = $1::uuid`},
		{name: "deliveries", query: `SELECT COUNT(*)::text FROM event_deliveries WHERE run_id = $1::uuid`},
		{name: "entity_state", query: `SELECT COUNT(*)::text FROM entity_state WHERE run_id = $1::uuid`},
		{name: "timers", query: `SELECT COUNT(*)::text FROM timers WHERE run_id = $1::uuid`},
		{name: "readiness", query: `SELECT COUNT(*)::text FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid`},
		{name: "agents", query: `SELECT COUNT(*)::text FROM agents WHERE $1::uuid IS NOT NULL`},
		{name: "routes", query: `SELECT COUNT(*)::text FROM routing_rules WHERE $1::uuid IS NOT NULL`},
		{name: "flow_instances", query: `SELECT COUNT(*)::text FROM flow_instances WHERE $1::uuid IS NOT NULL`},
		{name: "execution_lineage", query: `SELECT COUNT(*)::text FROM run_fork_selected_contract_executions WHERE fork_run_id = $1::uuid`},
		{name: "runtime_executions", query: `SELECT COUNT(*)::text FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id = $1::uuid`},
	} {
		var value string
		if err := db.QueryRowContext(ctx, probe.query, forkRunID).Scan(&value); err != nil {
			t.Fatalf("load selected fork %s snapshot: %v", probe.name, err)
		}
		snapshot[probe.name] = value
	}
	return snapshot
}

func selectedContractDynamicRuntimeGlobalSnapshot(t testing.TB, ctx context.Context, db *sql.DB) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, table := range []string{
		"agents",
		"routing_rules",
		"flow_instances",
		"flow_instance_runtime_readiness",
		"timers",
	} {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("count selected-contract dynamic runtime %s: %v", table, err)
		}
		out[table] = count
	}
	return out
}

func assertSelectedContractDeferredWorkRejectionHasNoForkMutation(t testing.TB, ctx context.Context, db *sql.DB, sourceRunID string) {
	t.Helper()
	for _, probe := range []struct {
		name  string
		query string
	}{
		{name: "runs", query: `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`},
		{name: "bindings", query: `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE source_run_id = $1::uuid`},
		{name: "route recoveries", query: `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE source_run_id = $1::uuid`},
		{name: "branch divergences", query: `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE source_run_id = $1::uuid`},
		{name: "execution lineage", query: `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE source_run_id = $1::uuid`},
		{name: "runtime executions", query: `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE source_run_id = $1::uuid`},
		{name: "events", query: `SELECT COUNT(*) FROM events WHERE run_id IN (SELECT run_id FROM runs WHERE forked_from_run_id = $1::uuid)`},
		{name: "deliveries", query: `SELECT COUNT(*) FROM event_deliveries WHERE run_id IN (SELECT run_id FROM runs WHERE forked_from_run_id = $1::uuid)`},
		{name: "timers", query: `SELECT COUNT(*) FROM timers WHERE forked_from_run_id = $1::uuid`},
	} {
		var count int
		if err := db.QueryRowContext(ctx, probe.query, sourceRunID).Scan(&count); err != nil {
			t.Fatalf("count rejected fork %s: %v", probe.name, err)
		}
		if count != 0 {
			t.Fatalf("deferred-work rejection created %d fork %s row(s)", count, probe.name)
		}
	}
}

func seedRunForkAgentTurnCapabilitySurface(t testing.TB, ctx context.Context, pg *store.PostgresStore, runID, turnID, sessionID string, identity agentidentity.Identity, runtimeMode string) string {
	t.Helper()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: identity, RuntimeMode: runtimeMode, Provider: "test", Transport: "api", ProviderContract: "run-fork-test",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: "run-fork-test-runtime",
			RunID: runID, SessionID: sessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build run-fork agent-turn capability surface: %v", err)
	}
	if err := pg.SaveManagedCapabilitySurface(ctx, surface); err != nil {
		t.Fatalf("persist run-fork agent-turn capability surface: %v", err)
	}
	return surface.ID
}

func selectedForkExecutionTestContext(t testing.TB, ctx context.Context, authority runtimeeffects.Authority) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork,
		authority.SelectedFork.ExecutionID,
		authority.SelectedFork.Generation,
		authority.SelectedFork.ForkRunID,
		authority.SelectedFork.ActorCensusFingerprint,
		runForkTestBundleHash,
		nil,
	)
	if err != nil {
		t.Fatalf("build selected-fork test admission: %v", err)
	}
	ctx = runtimeeffects.WithAuthority(ctx, authority)
	ctx = runtimeeffects.WithExecutionMode(ctx, authority.ExecutionMode)
	return managedexecution.WithAdmission(ctx, admission)
}

func TestExecuteSelectedContractRunForkWritesForkLocalExecutionAndLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, loaded.BundleSourceFact)
	sourceScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, loaded.BundleSourceFact.BundleHash())
	if err != nil {
		t.Fatalf("resolve source scope: %v", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, sourceScope)
	descriptors, err := swaruntime.AuthorActivityEventDescriptors(loaded.Source)
	if err != nil {
		t.Fatalf("project source descriptors: %v", err)
	}
	lease, err := pg.RegisterAuthorActivityEventCatalog(sourceScope, descriptors)
	if err != nil {
		t.Fatalf("register source descriptors: %v", err)
	}
	t.Cleanup(lease.Release)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002200, 0).UTC()
	seedSelectedExecutionSourceRunWithPrimaryRouteAndMode(t, db, sourceRunID, entityID, sourceEventID, "item.received", at,
		executionmode.Mock,
		selectedExecutionEntitylessNodeRoute("source-only-node"), nil, loaded.BundleSourceFact)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly},
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Owner != runfork.RunForkSelectedContractExecutionOwner || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
		t.Fatalf("result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		runfork.RunForkSelectedContractExecutionOwner,
		sourceRunID,
		result.Materialization.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	if result.SelectedContractExecutionAdmission.RecipientPlanning == nil ||
		result.SelectedContractExecutionAdmission.RecipientPlanning.Owner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		!result.SelectedContractExecutionAdmission.RecipientPlanning.RecipientPlanningSupported ||
		len(result.SelectedContractExecutionAdmission.RecipientPlanning.RecipientPlanEvents) != 1 {
		t.Fatalf("recipient planning admission = %#v", result.SelectedContractExecutionAdmission.RecipientPlanning)
	}
	forkEventID := result.ForkEvents[0].ForkEventID
	if forkEventID == "" || forkEventID == sourceEventID {
		t.Fatalf("fork event id = %q, source = %q", forkEventID, sourceEventID)
	}

	var forkEventRun, forkEventName, forkSourceEvent, forkExecutionMode string
	if err := db.QueryRowContext(ctx, `
		SELECT run_id::text, event_name, COALESCE(source_event_id::text, ''), execution_mode
		FROM events
		WHERE event_id = $1::uuid
	`, forkEventID).Scan(&forkEventRun, &forkEventName, &forkSourceEvent, &forkExecutionMode); err != nil {
		t.Fatalf("load fork event: %v", err)
	}
	if forkEventRun != result.Materialization.ForkRunID || forkEventName != "item.received" {
		t.Fatalf("fork event = run:%s name:%s", forkEventRun, forkEventName)
	}
	if forkSourceEvent == sourceEventID {
		t.Fatalf("fork event source_event_id copied source event %s; lineage must be explicit table evidence", sourceEventID)
	}
	if forkExecutionMode != string(executionmode.Mock) {
		t.Fatalf("fork event execution mode = %q, want mock causal mode", forkExecutionMode)
	}

	var lineageCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND source_event_id = $3::uuid
		  AND fork_event_id = $4::uuid
	`, result.Materialization.ForkRunID, sourceRunID, sourceEventID, forkEventID).Scan(&lineageCount); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if lineageCount != 1 {
		t.Fatalf("selected execution lineage rows = %d, want 1", lineageCount)
	}
	routeRecovery, ok, err := pg.LoadRunForkSelectedContractRouteRecovery(ctx, result.Materialization.ForkRunID)
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractRouteRecovery: %v", err)
	}
	if !ok {
		t.Fatal("selected-contract route recovery row missing")
	}
	if routeRecovery.Owner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		routeRecovery.RuntimeRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner ||
		routeRecovery.RouteTopologyOwner != runfork.RunForkSelectedContractRouteTopologyOwner ||
		routeRecovery.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		routeRecovery.ForkRunID != result.Materialization.ForkRunID ||
		routeRecovery.RecipientPlanEventCount != 1 ||
		routeRecovery.FrontierEvidenceFingerprint == "" ||
		routeRecovery.RouteTopologyFingerprint == "" ||
		routeRecovery.RecipientPlanningFingerprint == "" {
		t.Fatalf("route recovery = %#v", routeRecovery)
	}
	recoveredRoutes, err := RecoverSelectedContractRouteTruth(ctx, pg)
	if err != nil {
		t.Fatalf("RecoverSelectedContractRouteTruth: %v", err)
	}
	if len(recoveredRoutes) != 1 || recoveredRoutes[0].ForkRunID != result.Materialization.ForkRunID {
		t.Fatalf("recovered route truth = %#v", recoveredRoutes)
	}

	var copiedCurrentRoutes int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM routing_rules
		WHERE flow_instance = 'flow-a/1'
		  AND is_materialized = true
	`).Scan(&copiedCurrentRoutes); err != nil {
		t.Fatalf("count current route rows: %v", err)
	}
	if copiedCurrentRoutes != 0 {
		t.Fatalf("selected route recovery copied current routing_rules rows = %d, want 0", copiedCurrentRoutes)
	}

	var sourceCopiedEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&sourceCopiedEvents); err != nil {
		t.Fatalf("count copied source event ids: %v", err)
	}
	if sourceCopiedEvents != 0 {
		t.Fatalf("copied source event ids into fork run = %d, want 0", sourceCopiedEvents)
	}

	var forkReceipts, targetNodeDeliveries, sourceNodeDeliveries int
	targetNodeID := runForkSourceNode(t, loaded.Source, "test-node").Key()
	sourceNodeID := mustRunForkRootNode("source-only-node").Key()
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&forkReceipts); err != nil {
		t.Fatalf("count fork receipts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $3
	`, result.Materialization.ForkRunID, forkEventID, targetNodeID).Scan(&targetNodeDeliveries); err != nil {
		t.Fatalf("count target node fork deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $3
	`, result.Materialization.ForkRunID, forkEventID, sourceNodeID).Scan(&sourceNodeDeliveries); err != nil {
		t.Fatalf("count source node fork deliveries: %v", err)
	}
	if forkReceipts == 0 || targetNodeDeliveries != 1 || sourceNodeDeliveries != 0 {
		t.Fatalf("fork outcomes = receipts:%d targetNodeDeliveries:%d sourceNodeDeliveries:%d, want target node only", forkReceipts, targetNodeDeliveries, sourceNodeDeliveries)
	}

	var emittedFollowUps, mockFollowUps int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE execution_mode = 'mock')
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'item.processed'
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, forkEventID).Scan(&emittedFollowUps, &mockFollowUps); err != nil {
		t.Fatalf("count emitted follow-ups: %v", err)
	}
	if emittedFollowUps != 1 || mockFollowUps != 1 {
		t.Fatalf("fork follow-up events = total:%d mock:%d, want one mock-causal event", emittedFollowUps, mockFollowUps)
	}

	var sourceStatus, forkStatus, forkEntityState string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, result.Materialization.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, result.Materialization.ForkRunID, entityID).Scan(&forkEntityState); err != nil {
		t.Fatalf("load fork entity state: %v", err)
	}
	if sourceStatus != runfork.RunForkSourceFrozenStatus || forkStatus != runfork.RunForkActivatedStatus || forkEntityState == "" {
		t.Fatalf("post execution = source:%s fork:%s entity:%s", sourceStatus, forkStatus, forkEntityState)
	}
}

func TestExecuteSelectedContractRunForkAdmitsExactSourceModeBeforeMaterialization(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	loader := ContractBundleSourceLoader{
		RepoRoot:         repoRoot,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          runfork.RunForkContractSelectionModeSelectedContracts,
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, loaded.BundleSourceFact)
	sourceScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, loaded.BundleSourceFact.BundleHash())
	if err != nil {
		t.Fatalf("resolve source scope: %v", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, sourceScope)
	descriptors, err := swaruntime.AuthorActivityEventDescriptors(loaded.Source)
	if err != nil {
		t.Fatalf("project source descriptors: %v", err)
	}
	lease, err := pg.RegisterAuthorActivityEventCatalog(sourceScope, descriptors)
	if err != nil {
		t.Fatalf("register source descriptors: %v", err)
	}
	t.Cleanup(lease.Release)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002201, 0).UTC()
	seedSelectedExecutionSourceRunWithPrimaryRouteAndMode(
		t,
		db,
		sourceRunID,
		entityID,
		sourceEventID,
		"item.received",
		at,
		executionmode.Live,
		selectedExecutionEntitylessNodeRoute("source-only-node"),
		nil,
		loaded.BundleSourceFact,
	)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
		AgentRuntime: SelectedContractAgentRuntimeOptions{
			ExecutionPosture: executionposture.MockOnly,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "mock_only") {
		t.Fatalf("ExecuteSelectedContractRunFork result=%#v error=%v, want exact live source-mode rejection", result, err)
	}
	if result.Materialization.ForkRunID != "" {
		t.Fatalf("rejected result materialization = %#v, want none", result.Materialization)
	}
	assertSelectedContractDeferredWorkRejectionHasNoForkMutation(t, ctx, db, sourceRunID)
}

func TestSelectedContractPipelineConsumesExactMockConnectorResponseOwner(t *testing.T) {
	plan, err := providerconnectors.NewMockResponsePlan(map[string]map[string]any{
		"provider.write": {"ok": true},
	})
	if err != nil {
		t.Fatalf("NewMockResponsePlan: %v", err)
	}
	opts := selectedContractPipelineCoordinatorOptions(
		nil,
		&selectedContractExecutionPorts{},
		LoadedSelectedContractSource{
			BundleSourceFact:       testEphemeralBundleSourceFact("bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			MockConnectorResponses: plan,
		},
		SelectedContractAgentRuntimeOptions{},
		nil,
	)
	if opts.MockConnectorResponses != plan {
		t.Fatal("selected-contract pipeline did not retain exact mock connector response owner")
	}
	if got := opts.BundleSourceFact.BundleHash(); got != "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("selected-contract pipeline bundle hash = %q", got)
	}
}

func TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.TemplateCreateMintedKey)
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := canonicalrouting.ExampleRoot(t, canonicalrouting.TemplateCreateMintedKey)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          runfork.RunForkContractSelectionModeSelectedContracts,
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	sourceScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, loaded.BundleSourceFact.BundleHash())
	if err != nil {
		t.Fatalf("resolve source scope: %v", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, sourceScope)
	descriptors, err := swaruntime.AuthorActivityEventDescriptors(loaded.Source)
	if err != nil {
		t.Fatalf("project source descriptors: %v", err)
	}
	lease, err := pg.RegisterAuthorActivityEventCatalog(sourceScope, descriptors)
	if err != nil {
		t.Fatalf("register source descriptors: %v", err)
	}
	t.Cleanup(lease.Release)

	sourceRunID := uuid.NewString()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: sourceRunID, BundleHash: loaded.BundleSourceFact.BundleHash(), BundleSource: storerunlifecycle.BundleSourceEphemeral})
	workOwner := testGatewayWorkOwner(t)
	var manager *runtimemanager.AgentManager
	sourceBus, err := bus.NewEventBusWithOptions(pg, bus.EventBusOptions{
		ExecutionPosture:    executionposture.Live,
		WorkOwner:           workOwner,
		PipelineObligations: pg.PipelineObligations(),
		ContractBundle:      loaded.Source,
		BundleSourceFact:    loaded.BundleSourceFact,
		Durable: bus.DurableDependencies{
			ReplyContext: pg, RunLifecycle: pg, DeliveryLifecycle: pg,
			FlowRoutes: pg, FlowRouteRecords: pg, FlowRouteSets: pg, FlowRouteTopology: pg, FlowRouteRollback: pg,
			ActiveAgents: pg, ActiveFlows: pg, TargetOwners: pg, WorkflowInstances: pg, PreparedEvents: pg,
			TargetFailureRecorder: pg, RunOrigins: pg,
		},
		InterceptorProvider: func() []bus.EventInterceptor {
			return nil
		},
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return errors.New("source lifecycle manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		}, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	workflow, err := runtimepipeline.LoadWorkflowDefinition(loaded.Source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(loaded.Source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	workflowOwner := selectedContractTestWorkflowModule{
		source: loaded.Source, workflow: workflow, nodes: nodes,
		guards:  runtimepipeline.NewContractGuardRegistry(loaded.Source),
		actions: runtimepipeline.NewContractActionRegistry(loaded.Source),
	}
	workflowStore := runtimepipeline.NewPipelineCoordinatorWithOptions(sourceBus, runtimepipeline.PipelineCoordinatorOptions{
		Module:                  workflowOwner,
		Persistence:             runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:            pg,
		PipelineObligations:     pg.PipelineObligations(),
		DeliveryStore:           pg,
		DeadLetters:             pg,
		DecisionCards:           pg,
		ProposedEffects:         pg,
		HumanTasks:              pg,
		DecisionCardDraftExpiry: pg,
		HumanTaskExpiry:         pg,
		DeliveryRuntime:         sourceBus,
		WorkOwner:               workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	})

	manager = runtimemanager.NewAgentManagerWithOptions(sourceBus, nil, runtimemanager.AgentManagerOptions{
		SemanticSource:    loaded.Source,
		WorkflowInstances: workflowStore,
		WorkOwner:         workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	}, pg)
	t.Cleanup(func() { _ = manager.Shutdown() })

	sourceEventID := uuid.NewString()
	payload := json.RawMessage(`{"candidate":"candidate-1"}`)
	sourceEvent := eventtest.ExistingRunRootIngress(
		sourceEventID,
		events.EventType(loaded.Source.ResolveFlowEventReference("producer", "validation.triggered")),
		sourceRunID,
		"",
		payload,
		0,
		sourceRunID,
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	sourceCtx := runtimecorrelation.WithRunID(ctx, sourceRunID)
	proof := semanticview.ResolveFlowEventProof(loaded.Source, "producer", string(sourceEvent.Type()))
	if !proof.HasSchema {
		t.Fatalf("source event %s has no semantic descriptor proof", sourceEvent.Type())
	}
	disposition := runtimeauthoractivity.StoryDifferent
	if _, ok := loaded.Source.AuthoredResolvedEventCatalog()[strings.TrimSpace(proof.CatalogKey)]; ok {
		disposition = runtimeauthoractivity.StoryAuthored
	}
	sourceCtx, err = runtimeauthoractivity.WithResolvedEventDescriptor(sourceCtx, sourceScope, runtimeauthoractivity.EventDescriptor{
		EventType:          string(sourceEvent.Type()),
		Disposition:        disposition,
		AuthorSummaryField: strings.TrimSpace(proof.Entry.AuthorSummaryField),
	})
	if err != nil {
		t.Fatalf("bind source event descriptor: %v", err)
	}
	preflight, err := sourceBus.CheckPublishRecipientPlan(sourceCtx, sourceEvent)
	if err != nil {
		t.Fatalf("plan source create event: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) == 0 {
		t.Fatalf("source root preflight = failure:%s routes:%#v", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	commitRunForkTestEvent(t, sourceCtx, pg, sourceEvent, preflight.DeliveryRoutes)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	failure, ok := runtimefailures.EnvelopeFromError(err)
	if err == nil || !ok || failure.Class != runtimefailures.ClassDependencyUnavailable ||
		failure.Detail.Code != selectedContractDeferredWorkOwnerUnavailable {
		t.Fatalf("ExecuteSelectedContractRunFork result=%#v error=%v, want dynamic creation owner rejection", result, err)
	}
	capabilities, ok := failure.Detail.Attributes["capabilities"].([]string)
	if !ok || !slices.Contains(capabilities, selectedContractDeferredWorkDynamicFlowCreation) {
		t.Fatalf("failure capabilities = %#v, want %q", failure.Detail.Attributes["capabilities"], selectedContractDeferredWorkDynamicFlowCreation)
	}
	assertSelectedContractDeferredWorkRejectionHasNoForkMutation(t, ctx, db, sourceRunID)
}

type selectedContractTestWorkflowModule struct {
	source   semanticview.Source
	workflow *runtimepipeline.WorkflowDefinition
	nodes    []runtimepipeline.WorkflowNode
	guards   runtimepipeline.GuardRegistry
	actions  runtimepipeline.ActionRegistry
}

func (m selectedContractTestWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m selectedContractTestWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m selectedContractTestWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m selectedContractTestWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}

func (m selectedContractTestWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
}

func TestExecuteSelectedContractRunForkLoadsDBBackedSourceAndStampsPersistedIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, platformSpecPath)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, bundlecatalog.Upsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002202, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	persistedSource, err := runtimecorrelation.NewPersistedBundleSourceFact(projection.BundleHash)
	if err != nil {
		t.Fatalf("construct persisted source run bundle identity: %v", err)
	}
	if err := runlifecyclefixture.RevisePostgresSource(ctx, db, sourceRunID, persistedSource); err != nil {
		t.Fatalf("stamp source run bundle identity: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		BundleSourceFact:    testPersistedBundleSourceFact(projection.BundleHash),
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader: BundleCatalogSelectedContractSourceLoader{
			RepoRoot: repoRoot,
			Store:    pg,
		},
		ContractSelection: runforkadmission.SelectedContractSelection(
			semanticview.Wrap(bundle),
			"/stale/db-loaded/source-root",
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Owner != runfork.RunForkSelectedContractExecutionOwner || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
		t.Fatalf("result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		runfork.RunForkSelectedContractExecutionOwner,
		sourceRunID,
		result.Materialization.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	var forkBundleHash, forkBundleSource string
	if err := db.QueryRowContext(ctx, `
		SELECT bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, result.Materialization.ForkRunID).Scan(&forkBundleHash, &forkBundleSource); err != nil {
		t.Fatalf("load fork run bundle identity: %v", err)
	}
	if forkBundleHash != projection.BundleHash || forkBundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("fork run bundle identity = hash:%q source:%q", forkBundleHash, forkBundleSource)
	}
}

func TestExecuteSelectedContractRunForkDispatchesSourceEventsInPersistedChronology(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode: "selected_contracts", ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	sourceScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, loaded.BundleSourceFact.BundleHash())
	if err != nil {
		t.Fatalf("resolve source scope: %v", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, sourceScope)
	descriptors, err := swaruntime.AuthorActivityEventDescriptors(loaded.Source)
	if err != nil {
		t.Fatalf("project source descriptors: %v", err)
	}
	lease, err := pg.RegisterAuthorActivityEventCatalog(sourceScope, descriptors)
	if err != nil {
		t.Fatalf("register source descriptors: %v", err)
	}
	t.Cleanup(lease.Release)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	earlierEventID := "ffffffff-ffff-4fff-8fff-ffffffffffff"
	laterEventID := "00000000-0000-4000-8000-000000000001"
	earlierAt := time.Unix(1700002201, 0).UTC()
	laterAt := earlierAt.Add(time.Second)
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, earlierEventID, "item.received", earlierAt, loaded.BundleSourceFact)
	payload, _ := json.Marshal(map[string]any{"entity_id": entityID})
	laterEvent := eventtest.PersistedChildForProducer(laterEventID, events.EventType("item.received"),
		eventtest.Producer(events.EventProducerNode, "source-node"), "", payload, 0, sourceRunID, earlierEventID,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1"), laterAt)
	commitRunForkTestEvent(t, ctx, pg, laterEvent, []events.DeliveryRoute{selectedExecutionEntitylessNodeRoute("test-node")})
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID: sourceRunID, At: laterEventID, ConfirmSourceFreeze: true,
		Owner: selectedContractExecutionOwnerForTest(t, pg), SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.ExecutedEventCount != 2 || len(result.ForkEvents) != 2 {
		t.Fatalf("execution result = %#v, want two sequential fork events", result)
	}
	if result.ForkEvents[0].SourceEventID != earlierEventID || result.ForkEvents[1].SourceEventID != laterEventID {
		t.Fatalf("sequential fork execution order = %#v, want [%s %s]", result.ForkEvents, earlierEventID, laterEventID)
	}
}

func TestExecuteSelectedContractRunForkFailsClosedBeforeMaterializationForAgentRecipientWithoutHandlerMaterializer(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier7-composition/test-agent-emits-to-node")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	processCapability := selectedContractTestProcessCapability(t, ctx, pg, loaded)
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002201, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at, loaded.BundleSourceFact)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
		AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: processCapability},
	})
	if err == nil ||
		!strings.Contains(err.Error(), runfork.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported) ||
		!strings.Contains(err.Error(), runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner) ||
		!strings.Contains(err.Error(), "test-agent") {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want selected agent materialization blocker for test-agent", err)
	}
	if result.Owner != runfork.RunForkSelectedContractExecutionOwner ||
		result.Materialization.ForkRunID != "" ||
		result.Activation.ForkRunID != "" ||
		result.ExecutedEventCount != 0 ||
		len(result.ForkEvents) != 0 {
		t.Fatalf("result mutated before selected agent materialization rejection: %#v", result)
	}
	assertNoSelectedContractExecutionMutationForSource(t, db, sourceRunID, sourceEventID)
}

func TestExecuteSelectedContractRunForkMaterializesAndExecutesForkLocalAgentRuntime(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier7-composition/test-agent-emits-to-node")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	if _, ok := loaded.Source.ExecutableNodeEventHandler(runForkSourceNode(t, loaded.Source, "complete-node"), "task.completed"); !ok {
		t.Fatal("selected source omitted complete-node task.completed handler")
	}
	processCapability := selectedContractTestProcessCapability(t, ctx, pg, loaded)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002202, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at, loaded.BundleSourceFact)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	agent := &selectedContractForkTestAgent{}
	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
		AgentRuntime: SelectedContractAgentRuntimeOptions{
			ProcessCapability: processCapability,
			AgentFactory: func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
				agent.Configure(cfg)
				return agent, nil
			},
			QuiescenceTimeout: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.AgentRuntimeMaterialization == nil ||
		result.AgentRuntimeMaterialization.Owner != runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner ||
		result.AgentRuntimeMaterialization.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		result.AgentRuntimeMaterialization.ExecutionOwner != runfork.RunForkSelectedContractExecutionOwner ||
		!result.AgentRuntimeMaterialization.MaterializationRequired ||
		!result.AgentRuntimeMaterialization.MaterializationSupported ||
		!result.AgentRuntimeMaterialization.EphemeralForkLocal ||
		!containsSelectedContractAgentID(result.AgentRuntimeMaterialization.AgentRecipients, "test-agent") ||
		!containsSelectedContractAgentID(result.AgentRuntimeMaterialization.ConfiguredAgentIdentities, "test-agent") {
		t.Fatalf("agent runtime materialization = %#v", result.AgentRuntimeMaterialization)
	}
	if result.Owner != runfork.RunForkSelectedContractExecutionOwner ||
		result.Materialization.ForkRunID == "" ||
		!result.Activation.Activated ||
		result.ExecutedEventCount != 1 ||
		len(result.ForkEvents) != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		runfork.RunForkSelectedContractExecutionOwner,
		sourceRunID,
		result.Materialization.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	if got := agent.SeenRunIDs(); len(got) != 1 || got[0] != result.Materialization.ForkRunID {
		t.Fatalf("agent saw run ids = %#v, want fork run %s", got, result.Materialization.ForkRunID)
	}
	if got := agent.SeenEventIDs(); len(got) != 1 || got[0] != result.ForkEvents[0].ForkEventID {
		t.Fatalf("agent saw event ids = %#v, want fork event %s", got, result.ForkEvents[0].ForkEventID)
	}

	var persistedAgents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agents
		WHERE agent_id = 'test-agent'
	`).Scan(&persistedAgents); err != nil {
		t.Fatalf("count persisted selected agent rows: %v", err)
	}
	if persistedAgents != 0 {
		t.Fatalf("selected-fork runtime persisted current-runtime agent rows = %d, want 0", persistedAgents)
	}

	forkEventID := result.ForkEvents[0].ForkEventID
	var sourceCopiedEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&sourceCopiedEvents); err != nil {
		t.Fatalf("count copied source event ids: %v", err)
	}
	if sourceCopiedEvents != 0 {
		t.Fatalf("copied source event ids into fork run = %d, want 0", sourceCopiedEvents)
	}

	var agentDeliveries, agentOutcomes int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'test-agent'
	`, result.Materialization.ForkRunID, forkEventID).Scan(&agentDeliveries); err != nil {
		t.Fatalf("count fork agent deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_delivery_outcomes o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = 'test-agent'
		  AND o.outcome = 'delivered'
	`, forkEventID).Scan(&agentOutcomes); err != nil {
		t.Fatalf("count fork agent outcomes: %v", err)
	}
	if agentDeliveries != 1 || agentOutcomes != 1 {
		t.Fatalf("fork-local agent outcomes deliveries=%d outcomes=%d, want 1/1", agentDeliveries, agentOutcomes)
	}

	var agentFollowUps, finalizedEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'task.completed'
		  AND source_event_id = $2::uuid
		  AND produced_by = 'test-agent'
	`, result.Materialization.ForkRunID, forkEventID).Scan(&agentFollowUps); err != nil {
		t.Fatalf("count fork-local agent follow-ups: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'task.finalized'
	`, result.Materialization.ForkRunID).Scan(&finalizedEvents); err != nil {
		t.Fatalf("count finalized events: %v", err)
	}
	if agentFollowUps != 1 || finalizedEvents != 1 {
		var timeline string
		_ = db.QueryRowContext(ctx, `
			SELECT COALESCE(string_agg(
				e.event_name || ':' || COALESCE(d.subscriber_type, '-') || '/' || COALESCE(d.subscriber_id, '-') || ':' || COALESCE(d.status, '-'),
				';' ORDER BY e.created_at, e.event_id, d.delivery_id
			), '')
			FROM events e
			LEFT JOIN event_deliveries d ON d.event_id = e.event_id
			WHERE e.run_id = $1::uuid
		`, result.Materialization.ForkRunID).Scan(&timeline)
		var diagnostics string
		_ = db.QueryRowContext(ctx, `
			SELECT COALESCE(string_agg(
				e.event_name || ':' || COALESCE(e.payload::text, '-'),
				';' ORDER BY e.created_at, e.event_id
			), '')
			FROM events e
			WHERE e.run_id = $1::uuid
		`, result.Materialization.ForkRunID).Scan(&diagnostics)
		var routeDiagnostic string
		_ = db.QueryRowContext(ctx, `
			SELECT COALESCE(string_agg(
				e.event_name || ':' || COALESCE(e.flow_instance, '-') || ':' || COALESCE(d.delivery_target_route::text, '-') || ':' || COALESCE(d.delivery_payload_projection::text, '-'),
				';' ORDER BY e.created_at, e.event_id, d.delivery_id
			), '')
			FROM events e
			JOIN event_deliveries d ON d.event_id = e.event_id
			WHERE e.run_id = $1::uuid
		`, result.Materialization.ForkRunID).Scan(&routeDiagnostic)
		t.Fatalf("fork-local follow-ups task.completed=%d task.finalized=%d, want 1/1; timeline=%s; routes=%s; diagnostics=%s", agentFollowUps, finalizedEvents, timeline, routeDiagnostic, diagnostics)
	}

	var typedRuntimeDiagnostics int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'platform.runtime_log'
		  AND source_event_id = $2::uuid
		  AND payload->'details'->>'runtime_lineage_owner' = $3
		  AND payload->'details'->>'runtime_lineage_row_category' = 'diagnostic'
		  AND payload->'details'->>'runtime_lineage_classification' = 'fork_local'
	`, result.Materialization.ForkRunID, forkEventID, runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner).Scan(&typedRuntimeDiagnostics); err != nil {
		t.Fatalf("count typed runtime diagnostics: %v", err)
	}
	if typedRuntimeDiagnostics == 0 {
		t.Fatalf("typed runtime diagnostics parented to fork event = %d, want > 0", typedRuntimeDiagnostics)
	}
}

const selectedForkCapabilityProofQuiescenceTimeout = 15 * time.Second

func TestSelectedContractForkProviderTurnsUseCanonicalExecutionFrames(t *testing.T) {
	tests := []struct {
		name          string
		backend       string
		credentialEnv string
		adapter       string
		model         string
		initial       string
		continuation  string
	}{
		{
			name: "anthropic", backend: llmselection.BackendAnthropic, credentialEnv: "ANTHROPIC_API_KEY", adapter: "anthropic_api", model: "claude-selected-fork",
			initial:      `{"model":"claude-selected-fork","usage":{"input_tokens":11,"output_tokens":3},"content":[{"type":"tool_use","id":"lookup-1","name":"lookup_data","input":{"query":"status"}}]}`,
			continuation: `{"model":"claude-selected-fork","usage":{"input_tokens":13,"output_tokens":4},"content":[{"type":"tool_use","id":"emit-1","name":"emit_task_completed","input":{}}]}`,
		},
		{
			name: "openai_compatible", backend: llmselection.BackendOpenAICompatible, credentialEnv: llmselection.OpenAICompatibleCredentialEnv, adapter: "openai_compatible", model: "gpt-selected-fork",
			initial:      `{"model":"gpt-selected-fork","choices":[{"message":{"role":"assistant","tool_calls":[{"id":"lookup-1","type":"function","function":{"name":"lookup_data","arguments":"{\"query\":\"status\"}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}`,
			continuation: `{"model":"gpt-selected-fork","choices":[{"message":{"role":"assistant","content":"completed","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":13,"completion_tokens":4,"total_tokens":17}}`,
		},
		{
			name: "openai_responses", backend: llmselection.BackendOpenAIResponses, credentialEnv: llmselection.OpenAIResponsesCredentialEnv, adapter: "openai_responses", model: "gpt-selected-fork",
			initial:      `{"id":"resp-selected-fork-1","model":"gpt-selected-fork","output":[{"id":"lookup-1","type":"function_call","call_id":"lookup-1","name":"lookup_data","arguments":"{\"query\":\"status\"}"}],"usage":{"input_tokens":11,"output_tokens":3,"total_tokens":14}}`,
			continuation: `{"id":"resp-selected-fork-2","model":"gpt-selected-fork","output":[{"id":"emit-1","type":"function_call","call_id":"emit-1","name":"emit_task_completed","arguments":"{}"}],"usage":{"input_tokens":13,"output_tokens":4,"total_tokens":17}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			ctx := runForkTestContext(t)
			repoRoot := runForkExecutionRepoRoot(t)
			lookupCalls := atomic.Int32{}
			lookup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				lookupCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"status":"ready"}`))
			}))
			defer lookup.Close()
			contractsRoot := selectedForkFrameContracts(t, repoRoot, lookup.URL)
			loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}
			loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: contractsRoot})
			if err != nil {
				t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
			}
			processCapability := selectedContractTestProcessCapability(t, ctx, pg, loaded)

			var cfg *config.Config
			var providerCalls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ordinal := providerCalls.Add(1)
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Errorf("decode %s provider request: %v", tc.name, err)
				}
				requestJSON, _ := json.Marshal(request)
				if request["model"] != tc.model {
					t.Errorf("%s provider request model = %#v, want sealed frame model %q", tc.name, request["model"], tc.model)
				}
				if !jsonValueContains(request, "emit_task_completed") || !jsonValueContains(request, "lookup_data") {
					t.Errorf("%s provider request omits exact delivered tools: %s", tc.name, requestJSON)
				}
				wantKind := `"kind":"initial"`
				response := tc.initial
				if ordinal == 2 {
					wantKind = `"kind":"tool_continuation"`
					response = tc.continuation
				}
				if ordinal > 2 || !jsonValueContains(request, wantKind) {
					t.Errorf("%s provider request %d omits %s frame: %s", tc.name, ordinal, wantKind, requestJSON)
				}
				if ordinal == 1 {
					cfg.LLM.Models = llmselection.ModelAliases{
						llmselection.ModelAliasRegular: {tc.backend: "hostile-config-model-after-frame"},
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(response))
			}))
			defer provider.Close()

			if tc.backend == llmselection.BackendAnthropic {
				target, err := url.Parse(provider.URL)
				if err != nil {
					t.Fatalf("parse provider URL: %v", err)
				}
				previous := http.DefaultTransport
				http.DefaultTransport = selectedForkProviderRedirectTransport{target: target, base: previous}
				defer func() { http.DefaultTransport = previous }()
			}

			providerCredentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			if err := providerCredentials.Set(ctx, tc.credentialEnv, "test-key"); err != nil {
				t.Fatalf("store provider credential: %v", err)
			}
			cfg = selectedForkAPIProviderConfig(tc.backend, tc.model, provider.URL)

			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			sourceEventID := uuid.NewString()
			at := time.Unix(1700002203, 0).UTC()
			seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at, loaded.BundleSourceFact)
			seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
			captureSelectedExecutionSourceRevision(t, db, sourceRunID)
			result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
				SourceRunID: sourceRunID, At: sourceEventID, ConfirmSourceFreeze: true,
				Owner: selectedContractExecutionOwnerForTest(t, pg), SourceLoader: loader,
				ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot),
				AgentRuntime: SelectedContractAgentRuntimeOptions{
					Config: cfg, ProviderCredentials: providerCredentials, ProcessCapability: processCapability,
					QuiescenceTimeout: selectedForkCapabilityProofQuiescenceTimeout,
				},
			})
			if err != nil {
				var receiptFailure, deadLetterFailure, deliveryFailure string
				_ = db.QueryRowContext(ctx, `SELECT COALESCE(failure::text,'') FROM event_receipts WHERE failure IS NOT NULL ORDER BY updated_at DESC LIMIT 1`).Scan(&receiptFailure)
				_ = db.QueryRowContext(ctx, `SELECT COALESCE(failure::text,'') FROM dead_letters WHERE failure IS NOT NULL ORDER BY created_at DESC LIMIT 1`).Scan(&deadLetterFailure)
				_ = db.QueryRowContext(ctx, `
					SELECT COALESCE(jsonb_build_object(
						'status', d.status,
						'reason', d.reason_code,
						'failure', d.failure,
						'attempt_outcome', a.outcome,
						'attempt_failure', a.failure
					)::text, '')
					FROM event_deliveries d
					LEFT JOIN event_delivery_attempts a ON a.delivery_id = d.delivery_id
					WHERE d.run_id = $1::uuid AND d.subscriber_type = 'agent'
					ORDER BY d.created_at DESC, a.attempt_number DESC
					LIMIT 1
				`, result.Materialization.ForkRunID).Scan(&deliveryFailure)
				t.Fatalf("ExecuteSelectedContractRunFork: %v\nlatest agent delivery: %s\nlatest receipt failure: %s\nlatest dead letter failure: %s", err, deliveryFailure, receiptFailure, deadLetterFailure)
			}
			if providerCalls.Load() != 2 || lookupCalls.Load() != 1 {
				var deliveryDiagnostic string
				var runtimeDiagnostic string
				_ = db.QueryRowContext(ctx, `
					SELECT COALESCE(jsonb_agg(jsonb_build_object(
						'subscriber', d.subscriber_id,
						'status', d.status,
						'reason', d.reason_code,
						'failure', d.failure,
						'attempt_outcome', a.outcome,
						'attempt_failure', a.failure
					) ORDER BY d.created_at)::text, '[]')
					FROM event_deliveries d
					LEFT JOIN event_delivery_attempts a
					  ON a.delivery_id = d.delivery_id
					WHERE d.run_id = $1::uuid
				`, result.Materialization.ForkRunID).Scan(&deliveryDiagnostic)
				_ = db.QueryRowContext(ctx, `
					SELECT COALESCE(jsonb_agg(payload ORDER BY created_at)::text, '[]')
					FROM events
					WHERE run_id = $1::uuid
					  AND event_name = 'platform.runtime_log'
				`, result.Materialization.ForkRunID).Scan(&runtimeDiagnostic)
				t.Fatalf("provider calls=%d lookup calls=%d, want 2/1; deliveries=%s; runtime=%s", providerCalls.Load(), lookupCalls.Load(), deliveryDiagnostic, runtimeDiagnostic)
			}
			assertSelectedForkProviderCapabilityEvidence(t, ctx, db, result, tc.adapter, 2)
		})
	}
}

func selectedForkFrameContracts(t testing.TB, repoRoot, lookupURL string) string {
	t.Helper()
	source := filepath.Join(repoRoot, "tests/tier7-composition/test-agent-emits-to-node")
	root := filepath.Join(t.TempDir(), "contracts")
	if err := os.CopyFS(root, os.DirFS(source)); err != nil {
		t.Fatalf("copy selected-fork frame contracts: %v", err)
	}
	agents := `test-agent:
  id: test-agent
  model: regular
  intent: prompts/test-agent.md
  subscriptions:
    - task.assigned
  emit_events:
    - task.completed
  tools:
    - lookup_data
`
	tools := `lookup_data:
  description: Look up selected-fork status.
  handler_type: http
  http:
    method: POST
    url: __LOOKUP_URL__
  input_schema:
    type: object
    properties:
      query:
        type: string
    required:
      - query
    additionalProperties: false
  output_schema:
    type: object
    properties:
      status:
        type: string
    required:
      - status
    additionalProperties: false
  response_success:
    kind: http_status_2xx
`
	if err := os.WriteFile(filepath.Join(root, "agents.yaml"), []byte(agents), 0o644); err != nil {
		t.Fatalf("write selected-fork frame agents: %v", err)
	}
	tools = strings.ReplaceAll(tools, "__LOOKUP_URL__", lookupURL)
	if err := os.WriteFile(filepath.Join(root, "tools.yaml"), []byte(tools), 0o644); err != nil {
		t.Fatalf("write selected-fork frame tools: %v", err)
	}
	return root
}

func jsonValueContains(value any, needle string) bool {
	switch typed := value.(type) {
	case string:
		return strings.Contains(typed, needle)
	case []any:
		for _, item := range typed {
			if jsonValueContains(item, needle) {
				return true
			}
		}
	case map[string]any:
		for key, item := range typed {
			if strings.Contains(key, needle) || jsonValueContains(item, needle) {
				return true
			}
		}
	}
	return false
}

type selectedForkProviderRedirectTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t selectedForkProviderRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.EqualFold(req.URL.Hostname(), "api.anthropic.com") {
		return t.base.RoundTrip(req)
	}
	redirected := req.Clone(req.Context())
	redirected.URL = new(url.URL)
	*redirected.URL = *req.URL
	redirected.URL.Scheme = t.target.Scheme
	redirected.URL.Host = t.target.Host
	redirected.Host = t.target.Host
	return t.base.RoundTrip(redirected)
}

func selectedForkAPIProviderConfig(backend, model, baseURL string) *config.Config {
	cfg := &config.Config{LLM: config.LLMConfig{
		Backend: backend,
		Models: llmselection.ModelAliases{
			llmselection.ModelAliasRegular: {backend: model},
		},
		Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3},
	}}
	switch backend {
	case llmselection.BackendOpenAICompatible:
		cfg.LLM.OpenAICompatible.BaseURL = baseURL
	case llmselection.BackendOpenAIResponses:
		cfg.LLM.OpenAIResponses.BaseURL = baseURL
	}
	return cfg
}

func assertSelectedForkProviderCapabilityEvidence(t testing.TB, ctx context.Context, db *sql.DB, result SelectedContractExecutionResult, adapter string, wantTurns int) {
	t.Helper()
	proof := result.ForkLocalRuntimeContainer
	if proof == nil || proof.RuntimeExecutionID == "" || proof.RuntimeGeneration == 0 || proof.AuthorityExecutionOwner == "" {
		t.Fatalf("selected completion runtime authority proof = %#v", proof)
	}

	var operationCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_operations
		WHERE effect_kind = 'provider_turn'
		  AND authority_kind = 'selected_contract_fork'
		  AND authority_id = $1
		  AND selected_execution_id = $1::uuid
		  AND generation = $2
		  AND authority_evidence->>'execution_id' = $1
		  AND (authority_evidence->>'generation')::bigint = $2
	`, proof.RuntimeExecutionID, proof.RuntimeGeneration).Scan(&operationCount); err != nil {
		t.Fatalf("count selected completion operations: %v", err)
	}
	if operationCount != wantTurns {
		t.Fatalf("selected completion operations = %d, want %d", operationCount, wantTurns)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT a.capability_surface_id::text, t.capability_surface_id::text, s.surface::text,
		       o.capability_plan_fingerprint
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
		JOIN managed_agent_capability_surfaces s ON s.surface_id = a.capability_surface_id
		WHERE o.selected_execution_id = $1::uuid
		  AND a.adapter = $2
		  AND a.generation = $3
		  AND a.execution_owner = $4
		  AND a.state = 'settled'
		  AND t.run_id = $5::uuid
		  AND t.agent_id = 'test-agent'
		  AND t.usage_exactness = 'exact'
		ORDER BY t.created_at, t.turn_id
	`, proof.RuntimeExecutionID, adapter, proof.RuntimeGeneration, proof.AuthorityExecutionOwner, result.Materialization.ForkRunID)
	if err != nil {
		t.Fatalf("load selected completion capability evidence: %v", err)
	}
	defer rows.Close()
	turnCount := 0
	for rows.Next() {
		turnCount++
		var attemptSurfaceID, turnSurfaceID, rawSurface, planFingerprint string
		if err := rows.Scan(&attemptSurfaceID, &turnSurfaceID, &rawSurface, &planFingerprint); err != nil {
			t.Fatalf("scan selected completion capability evidence: %v", err)
		}
		if attemptSurfaceID == "" || attemptSurfaceID != turnSurfaceID {
			t.Fatalf("capability surface attempt=%q turn=%q, want one exact identity", attemptSurfaceID, turnSurfaceID)
		}
		var surface managedcapabilities.Surface
		if err := json.Unmarshal([]byte(rawSurface), &surface); err != nil {
			t.Fatalf("decode selected completion capability surface: %v", err)
		}
		if surface.ID != attemptSurfaceID || surface.ActorID != "test-agent" || surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork || surface.Authority.ExecutionAuthorityID != proof.RuntimeExecutionID || surface.Authority.RunID != result.Materialization.ForkRunID {
			t.Fatalf("selected completion capability surface = %#v", surface)
		}
		if names := surface.EffectiveNames(); !slices.Equal(names, []string{"emit_task_completed", "lookup_data"}) {
			t.Fatalf("selected completion effective tools = %#v, want [emit_task_completed lookup_data]", names)
		}
		wantFingerprint, err := surface.PlanFingerprint()
		if err != nil {
			t.Fatalf("selected completion plan fingerprint: %v", err)
		}
		if planFingerprint != wantFingerprint {
			t.Fatalf("operation plan fingerprint = %q, want %q", planFingerprint, wantFingerprint)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate selected completion capability evidence: %v", err)
	}
	if turnCount != wantTurns {
		t.Fatalf("selected completion evidence rows = %d, want %d", turnCount, wantTurns)
	}
	assertSelectedForkCompletionModelAlias(t, ctx, db, proof.RuntimeExecutionID, adapter, wantTurns)
}

func TestExecuteSelectedContractRunForkClaudeOAuthPersistsStartupAndTurnCapabilityAuthority(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-host-token")
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	sourceContractsRoot := filepath.Join(repoRoot, "internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp")
	contractsRoot := filepath.Join(t.TempDir(), "contracts")
	if err := os.CopyFS(contractsRoot, os.DirFS(sourceContractsRoot)); err != nil {
		t.Fatalf("copy selected contract fixture: %v", err)
	}
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: contractsRoot})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	processCapability := selectedContractTestProcessCapability(t, ctx, pg, loaded)

	captureDir := t.TempDir()
	dockerPath := filepath.Join(captureDir, "fake-docker.sh")
	script := `#!/bin/sh
set -eu
capture_dir="${SELECTED_FORK_CLAUDE_CAPTURE_DIR}"
counter="$capture_dir/count"
count=0
if [ -f "$counter" ]; then count="$(cat "$counter")"; fi
count=$((count + 1))
printf '%s' "$count" > "$counter"
printf '%s\n' "$@" > "$capture_dir/$count.args"
cat > "$capture_dir/$count.stdin"
session_id=""
previous=""
mcp_config=""
for arg in "$@"; do
  if [ "$previous" = "--session-id" ]; then session_id="$arg"; fi
  if [ "$previous" = "--mcp-config" ]; then mcp_config="$arg"; fi
  previous="$arg"
done
if [ -z "$session_id" ]; then echo "missing session id" >&2; exit 2; fi
if [ "$count" -gt 1 ]; then
  python3 - "$mcp_config" 2> "$capture_dir/$count.mcp-error" <<'PY'
import json, sys, urllib.request
config = json.loads(sys.argv[1])
server = config["mcpServers"]["runtime-tools"]
url = server["url"].replace("host.docker.internal", "127.0.0.1")
headers = {"Content-Type": "application/json", **server.get("headers", {})}
responses = []
for payload in [
    {"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fake-claude","version":"1"}}},
    {"jsonrpc":"2.0","method":"notifications/initialized","params":{}},
    {"jsonrpc":"2.0","id":"list","method":"tools/list","params":{}},
]:
    request = urllib.request.Request(url, json.dumps(payload).encode(), headers=headers)
    with urllib.request.urlopen(request) as response:
        raw = response.read()
        if payload.get("id") is not None:
            responses.append(json.loads(raw))
listed = responses[-1]["result"]["tools"]
listed_names = [tool.get("name") for tool in listed]
if listed_names != ["emit_task_completed"]:
    raise SystemExit(f"selected-fork MCP tools/list names = {listed_names!r}, want exact executor projection")
emit = next((tool for tool in listed if tool["name"] == "emit_task_completed"), None)
expected_schema = {
    "type": "object",
    "properties": {"fork_result": {"type": "string"}},
    "required": ["fork_result"],
    "additionalProperties": False,
}
expected_description = "Emit worker/task.completed event\n\nUsage:\nCall this emit_* tool only to publish the named workflow event. Provide concrete JSON payload values matching the input schema. Do not include envelope-owned fields unless the schema declares them. Arguments are concrete payload values, not workflow expressions."
if emit is None or emit.get("description") != expected_description or emit.get("inputSchema") != expected_schema:
    raise SystemExit(f"selected-fork flow emit mismatch: {emit!r}")
call = {
    "jsonrpc": "2.0",
    "id": "call",
    "method": "tools/call",
    "params": {
        "name": "emit_task_completed",
        "arguments": {"fork_result": "selected-fork-flow-complete"},
        "_meta": {"claudecode/toolUseId": "toolu-selected-fork-flow"},
    },
}
request = urllib.request.Request(url, json.dumps(call).encode(), headers=headers)
with urllib.request.urlopen(request) as response:
    result = json.loads(response.read())
if result.get("error") is not None or result.get("result", {}).get("isError") is True:
    raise SystemExit(f"selected-fork MCP call failed: {result!r}")
PY
fi
printf '%s\n' "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$session_id\",\"mcp_servers\":[{\"name\":\"runtime-tools\",\"status\":\"connected\"}],\"tools\":[\"WebFetch\",\"WebSearch\",\"mcp__runtime-tools__emit_task_completed\"]}"
if [ "$count" -gt 1 ]; then
  printf '%s\n' "{\"type\":\"result\",\"subtype\":\"success\",\"session_id\":\"$session_id\",\"model\":\"claude-selected-fork\",\"result\":\"selected-fork-flow-complete\",\"total_cost_usd\":0.001,\"usage\":{\"input_tokens\":7,\"output_tokens\":2}}"
fi
`
	if err := os.WriteFile(dockerPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake Docker executable: %v", err)
	}
	t.Setenv("SELECTED_FORK_CLAUDE_CAPTURE_DIR", captureDir)

	providerCredentials, err := runtimecredentials.NewFileStore(filepath.Join(captureDir, "provider-credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := providerCredentials.Set(ctx, "CLAUDE_CODE_OAUTH_TOKEN", "selected-fork-oauth-token"); err != nil {
		t.Fatalf("store Claude OAuth credential: %v", err)
	}
	cfg := &config.Config{LLM: config.LLMConfig{
		Backend: llmselection.BackendClaudeCLI,
		Models: llmselection.ModelAliases{
			llmselection.ModelAliasRegular: {llmselection.BackendClaudeCLI: "claude-selected-fork"},
		},
		Session:   config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3},
		ClaudeCLI: config.ClaudeCLIConfig{Command: "claude", OutputFormat: "stream-json"},
	}}
	cfg.Workspace.DockerBin = dockerPath

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002303, 0).UTC()
	seedSelectedExecutionRootSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at, loaded.BundleSourceFact)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID: sourceRunID, At: sourceEventID, ConfirmSourceFreeze: true,
		Owner: selectedContractExecutionOwnerForTest(t, pg), SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot),
		AgentRuntime: SelectedContractAgentRuntimeOptions{
			Config: cfg, ProviderCredentials: providerCredentials, ProcessCapability: processCapability,
			Workspace: selectedForkWorkspaceLifecycle{target: &workspace.Target{
				Backend: workspace.BackendDocker, Container: "swarm-agent-selected-fork", Workdir: "/workspace",
			}},
			QuiescenceTimeout: selectedForkCapabilityProofQuiescenceTimeout,
		},
	})
	if err != nil {
		var receiptFailure, deadLetterFailure string
		_ = db.QueryRowContext(ctx, `SELECT COALESCE(failure::text,'') FROM event_receipts WHERE failure IS NOT NULL ORDER BY updated_at DESC LIMIT 1`).Scan(&receiptFailure)
		_ = db.QueryRowContext(ctx, `SELECT COALESCE(failure::text,'') FROM dead_letters WHERE failure IS NOT NULL ORDER BY created_at DESC LIMIT 1`).Scan(&deadLetterFailure)
		captures := map[string]string{}
		for _, name := range []string{"count", "1.args", "1.stdin", "2.args", "2.stdin", "2.mcp-error", "3.mcp-error"} {
			if raw, readErr := os.ReadFile(filepath.Join(captureDir, name)); readErr == nil {
				captures[name] = string(raw)
			}
		}
		t.Fatalf("ExecuteSelectedContractRunFork: %v\nlatest receipt failure: %s\nlatest dead letter failure: %s\ncaptures: %#v", err, receiptFailure, deadLetterFailure, captures)
	}
	countRaw, err := os.ReadFile(filepath.Join(captureDir, "count"))
	if err != nil {
		t.Fatalf("read Claude invocation count: %v", err)
	}
	if strings.TrimSpace(string(countRaw)) != "2" {
		captures := map[string]string{}
		for _, name := range []string{"1.args", "1.stdin", "2.args", "2.stdin", "2.mcp-error", "3.args", "3.stdin", "3.mcp-error"} {
			if raw, readErr := os.ReadFile(filepath.Join(captureDir, name)); readErr == nil {
				captures[name] = string(raw)
			}
		}
		t.Fatalf("Claude invocations = %q, want startup probe plus live turn; captures: %#v", countRaw, captures)
	}
	for _, invocation := range []string{"1", "2"} {
		args, err := os.ReadFile(filepath.Join(captureDir, invocation+".args"))
		if err != nil {
			t.Fatalf("read Claude invocation %s args: %v", invocation, err)
		}
		if !strings.Contains(string(args), "CLAUDE_CODE_OAUTH_TOKEN=selected-fork-oauth-token") || strings.Contains(string(args), "stale-host-token") {
			t.Fatalf("Claude invocation %s credential projection = %q", invocation, args)
		}
		if invocation == "2" && capturedSelectedForkArgValue(t, args, "--model") != "claude-selected-fork" {
			t.Fatalf("Claude live invocation model = %q, want sealed selected-fork frame model", capturedSelectedForkArgValue(t, args, "--model"))
		}
		allowed := strings.Split(capturedSelectedForkArgValue(t, args, "--allowedTools"), ",")
		slices.Sort(allowed)
		if want := []string{"ExitPlanMode", "WebFetch", "WebSearch", "mcp__runtime-tools__emit_task_completed"}; !slices.Equal(allowed, want) {
			t.Fatalf("Claude invocation %s allowed tools = %v, want %v", invocation, allowed, want)
		}
		selected := strings.Split(capturedSelectedForkArgValue(t, args, "--tools"), ",")
		slices.Sort(selected)
		if want := []string{"ExitPlanMode", "WebFetch", "WebSearch"}; !slices.Equal(selected, want) {
			t.Fatalf("Claude invocation %s selected builtins = %v, want %v", invocation, selected, want)
		}
		if strings.Contains(string(args), "--disallowedTools") {
			t.Fatalf("Claude invocation %s retained negative builtin catalog: %q", invocation, args)
		}
	}
	startupInput, err := os.ReadFile(filepath.Join(captureDir, "1.stdin"))
	if err != nil {
		t.Fatalf("read selected-fork startup input: %v", err)
	}
	liveInput, err := os.ReadFile(filepath.Join(captureDir, "2.stdin"))
	if err != nil {
		t.Fatalf("read selected-fork live input: %v", err)
	}
	if !strings.Contains(string(startupInput), "Startup validation probe") || strings.Contains(string(liveInput), "Startup validation probe") {
		t.Fatalf("selected-fork invocation order is not startup then live: startup=%q live=%q", startupInput, liveInput)
	}
	var emitted int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND payload->>'fork_result' = 'selected-fork-flow-complete'
	`, result.Materialization.ForkRunID).Scan(&emitted); err != nil {
		t.Fatalf("count selected-fork flow-scoped MCP emission: %v", err)
	}
	if emitted != 1 {
		t.Fatalf("selected-fork flow-scoped MCP emissions = %d, want 1", emitted)
	}
	assertSelectedForkClaudeCapabilityEvidence(t, ctx, db, result)
}

func capturedSelectedForkArgValue(t testing.TB, raw []byte, name string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	for index := 0; index+1 < len(lines); index++ {
		if lines[index] == name {
			return lines[index+1]
		}
	}
	t.Fatalf("captured Claude arguments omitted %s: %q", name, raw)
	return ""
}

type selectedForkWorkspaceLifecycle struct {
	target *workspace.Target
}

func (s selectedForkWorkspaceLifecycle) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return s.target, nil
}

func (selectedForkWorkspaceLifecycle) ValidateSource(context.Context, semanticview.Source) error {
	return nil
}
func (selectedForkWorkspaceLifecycle) EnsurePrereqs(context.Context) error          { return nil }
func (selectedForkWorkspaceLifecycle) EnsureSystemWorkspaces(context.Context) error { return nil }
func (selectedForkWorkspaceLifecycle) EnsureEntityWorkspace(context.Context, string) error {
	return nil
}
func (selectedForkWorkspaceLifecycle) StopEntityWorkspace(context.Context, string) error { return nil }

func assertSelectedForkClaudeCapabilityEvidence(t testing.TB, ctx context.Context, db *sql.DB, result SelectedContractExecutionResult) {
	t.Helper()
	proof := result.ForkLocalRuntimeContainer
	if proof == nil {
		t.Fatal("selected Claude runtime authority proof is missing")
	}
	var startupSurfaces, startupAttempts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT s.surface_id), COUNT(DISTINCT a.attempt_id)
		FROM managed_agent_capability_surfaces s
		JOIN runtime_external_effect_attempts a ON a.capability_surface_id = s.surface_id
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND o.authority_kind = 'selected_contract_fork'
		  AND o.authority_id = $1::text
		  AND a.adapter = 'claude_cli_startup_probe'
		  AND a.state = 'settled'
		  AND s.execution_kind = 'selected_contract_fork'
		  AND s.execution_authority_id = $1::text
		  AND s.run_id = $2::uuid
		  AND s.actor_id = 'test-agent'
		  AND s.surface->'authority'->>'kind' = 'startup_probe'
		  AND s.surface->'tools'->0->'evidence' @> '[{"kind":"mcp_listed","status":"confirmed"}]'::jsonb
	`, proof.RuntimeExecutionID, result.Materialization.ForkRunID).Scan(&startupSurfaces, &startupAttempts); err != nil {
		t.Fatalf("load selected Claude startup capability evidence: %v", err)
	}
	if startupSurfaces != 1 || startupAttempts != 1 {
		t.Fatalf("selected Claude startup surfaces=%d attempts=%d, want 1/1", startupSurfaces, startupAttempts)
	}
	var rawStartupSurface string
	if err := db.QueryRowContext(ctx, `
		SELECT s.surface::text
		FROM managed_agent_capability_surfaces s
		JOIN runtime_external_effect_attempts a ON a.capability_surface_id = s.surface_id
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND a.adapter = 'claude_cli_startup_probe'
		  AND a.state = 'settled'
		  AND s.run_id = $2::uuid
		  AND s.actor_id = 'test-agent'
	`, proof.RuntimeExecutionID, result.Materialization.ForkRunID).Scan(&rawStartupSurface); err != nil {
		t.Fatalf("load selected Claude startup surface: %v", err)
	}
	var startupSurface managedcapabilities.Surface
	if err := json.Unmarshal([]byte(rawStartupSurface), &startupSurface); err != nil {
		t.Fatalf("decode selected Claude startup surface: %v", err)
	}
	assertSelectedForkClaudeManagedSurface(t, startupSurface, proof.RuntimeExecutionID, result.Materialization.ForkRunID, managedcapabilities.AuthorityStartupProbe)

	var attemptSurfaceID, turnSurfaceID, rawSurface string
	if err := db.QueryRowContext(ctx, `
		SELECT a.capability_surface_id::text, t.capability_surface_id::text, s.surface::text
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
		JOIN managed_agent_capability_surfaces s ON s.surface_id = a.capability_surface_id
		WHERE o.selected_execution_id = $1::uuid
		  AND a.adapter = 'claude_cli'
		  AND a.state = 'settled'
		  AND t.run_id = $2::uuid
		  AND t.agent_id = 'test-agent'
	`, proof.RuntimeExecutionID, result.Materialization.ForkRunID).Scan(&attemptSurfaceID, &turnSurfaceID, &rawSurface); err != nil {
		rows, queryErr := db.QueryContext(ctx, `
			SELECT a.adapter, a.state, COALESCE(a.capability_surface_id::text,''),
			       COALESCE(t.capability_surface_id::text,''), COALESCE(t.failure::text,'')
			FROM runtime_external_effect_attempts a
			JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
			LEFT JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
			WHERE o.selected_execution_id = $1::uuid
			ORDER BY a.authorized_at
		`, proof.RuntimeExecutionID)
		if queryErr == nil {
			defer rows.Close()
			for rows.Next() {
				var adapter, state, attemptSurface, turnSurface, failure string
				_ = rows.Scan(&adapter, &state, &attemptSurface, &turnSurface, &failure)
				t.Logf("selected Claude attempt adapter=%s state=%s attempt_surface=%s turn_surface=%s failure=%s", adapter, state, attemptSurface, turnSurface, failure)
			}
		}
		t.Fatalf("load selected Claude turn capability evidence: %v", err)
	}
	if attemptSurfaceID == "" || attemptSurfaceID != turnSurfaceID {
		t.Fatalf("Claude capability surface attempt=%q turn=%q, want one exact identity", attemptSurfaceID, turnSurfaceID)
	}
	var surface managedcapabilities.Surface
	if err := json.Unmarshal([]byte(rawSurface), &surface); err != nil {
		t.Fatalf("decode selected Claude turn surface: %v", err)
	}
	assertSelectedForkClaudeManagedSurface(t, surface, proof.RuntimeExecutionID, result.Materialization.ForkRunID, managedcapabilities.AuthorityProviderTurn)
	assertSelectedForkCompletionModelAlias(t, ctx, db, proof.RuntimeExecutionID, "claude_cli", 1)
}

func assertSelectedForkCompletionModelAlias(t testing.TB, ctx context.Context, db *sql.DB, executionID, adapter string, wantTurns int) {
	t.Helper()
	var total, canonical int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE l.model_alias = $3)
		FROM spend_ledger l
		JOIN runtime_external_effect_attempts a ON a.attempt_id = l.external_effect_attempt_id
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND a.adapter = $2
	`, executionID, adapter, llmselection.ModelAliasRegular).Scan(&total, &canonical); err != nil {
		t.Fatalf("load selected completion model aliases: %v", err)
	}
	if total != wantTurns || canonical != wantTurns {
		t.Fatalf("selected completion model aliases total=%d canonical=%d, want %d exact regular aliases", total, canonical, wantTurns)
	}
}

func assertSelectedForkClaudeManagedSurface(t testing.TB, surface managedcapabilities.Surface, executionID, runID string, authorityKind managedcapabilities.AuthorityKind) {
	t.Helper()
	if surface.Authority.Kind != authorityKind ||
		surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork ||
		surface.Authority.ExecutionAuthorityID != executionID ||
		surface.Authority.RunID != runID {
		t.Fatalf("selected Claude %s authority = %#v", authorityKind, surface.Authority)
	}
	if got := surface.EffectiveNames(); !slices.Equal(got, []string{"emit_task_completed", "web_search"}) {
		t.Fatalf("selected Claude %s effective capabilities = %v", authorityKind, got)
	}
	if got := surface.PlannedBindingNames(managedcapabilities.BindingProviderBuiltin); !slices.Equal(got, []string{"WebFetch", "WebSearch"}) {
		t.Fatalf("selected Claude %s provider bindings = %v", authorityKind, got)
	}
	for _, kind := range []managedcapabilities.BindingKind{managedcapabilities.BindingMCPTool, managedcapabilities.BindingMCPProvider} {
		if got := surface.PlannedBindingNames(kind); !slices.Equal(got, []string{"mcp__runtime-tools__emit_task_completed"}) {
			t.Fatalf("selected Claude %s %s bindings = %v", authorityKind, kind, got)
		}
	}
	for _, kind := range []managedcapabilities.BindingKind{managedcapabilities.BindingAPIDefinition, managedcapabilities.BindingLocalRuntime} {
		if got := surface.BindingNames(kind); len(got) != 0 {
			t.Fatalf("selected Claude %s acquired fallback %s bindings = %v", authorityKind, kind, got)
		}
	}
	for _, tool := range surface.Tools {
		if tool.Name != "web_search" {
			continue
		}
		if len(tool.Bindings) != 2 {
			t.Fatalf("selected Claude %s web_search bindings = %#v", authorityKind, tool.Bindings)
		}
		for _, binding := range tool.Bindings {
			if binding.Kind != managedcapabilities.BindingProviderBuiltin {
				t.Fatalf("selected Claude %s web_search acquired fallback binding %#v", authorityKind, binding)
			}
		}
		return
	}
	t.Fatalf("selected Claude %s surface omitted web_search: %#v", authorityKind, surface)
}

func TestSelectedContractForkManagedPreflightUsesExactProviderPromptAndExecutesEligibleMCPToolCall(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := runForkTestContext(t)
	container := buildSelectedForkProofContainer(t, ctx, db)

	manager := runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:  executionposture.Live,
		LLMBackend:        llmselection.BackendClaudeCLI,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	agentCfg := selectedContractTestAgentConfig(t, runtimeactors.AgentConfig{ID: "selected-health-agent", Role: "selected_health", Model: llmselection.ModelAliasRegular})
	topology, err := runtimeagenttopology.NewEphemeralAdmission(uuid.NewString(), "runtime_shard")
	if err != nil {
		t.Fatalf("construct selected fork test topology: %v", err)
	}
	if err := manager.MaterializeAdmittedAgentForExecution(ctx, runtimemanager.PersistedAgent{
		Config: agentCfg, Status: "ephemeral", HiredBy: "selected-fork-test", Topology: topology,
	}); err != nil {
		t.Fatalf("materialize selected fork test agent: %v", err)
	}
	executor := &selectedForkStartupProbeExecutor{}
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	const gatewayToken = "selected-fork-startup-token"
	gateway := runtimemcp.NewGateway(executor, gatewayToken, swaruntime.RuntimeMCPGatewayHooks(nil, nil, func(agentID string) (runtimeactors.AgentConfig, bool) {
		cfg, err := manager.ResolveAgentConfig(agentID, "")
		return cfg, err == nil
	}, nil, turns))
	server := httptest.NewServer(gateway.Handler())
	defer server.Close()
	binding, err := toolgateway.NewRuntimeOwnedBinding(
		toolgateway.TransportHTTP, server.URL, "http://host.docker.internal:8081", gatewayToken,
		toolgateway.LifecycleOwnerSelectedForkRuntime, toolgateway.SourceSelectedForkEphemeralGateway,
	)
	if err != nil {
		t.Fatalf("NewRuntimeOwnedBinding: %v", err)
	}
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	cfg := &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendClaudeCLI}}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		t.Fatalf("resolve selected-fork llm profile: %v", err)
	}
	var startupPrompts []string
	runtimes, err := runtimellm.NewAgentRuntimeSet(profile, runtimellm.RuntimeFactory{}, selectedForkStartupRuntime{prompts: &startupPrompts})
	if err != nil {
		t.Fatalf("build selected-fork runtime set: %v", err)
	}
	proof := container.Proof()
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx = runtimeeffects.WithAuthority(ctx, container.authority)
	ctx = runtimeeffects.WithController(ctx, liveTestEffectController(pg))
	ctx = managedexecution.WithAdmission(ctx, container.admission)
	surfaceIDs, err := swaruntime.ValidateManagedProviderPreflight(
		ctx, cfg, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), binding,
		runtimes, turns, executor, manager,
		swaruntime.ManagedProviderPreflightAuthority{
			ExecutionKind:        managedcapabilities.ExecutionSelectedContractFork,
			ExecutionAuthorityID: proof.RuntimeExecutionID,
			RunID:                proof.ForkRunID,
			StartupOwnerID:       proof.AuthorityExecutionOwner,
			StartupGeneration:    proof.RuntimeGeneration,
			EffectController:     liveTestEffectController(pg),
			CapabilityStore:      pg,
			EffectAuthority: func(string, string) (runtimeeffects.Authority, error) {
				return container.authority, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("ValidateManagedProviderPreflight: %v", err)
	}
	if !slices.Equal(executor.executed, []string{"health_check"}) {
		t.Fatalf("selected-fork startup tools/call executions = %#v, want [health_check]", executor.executed)
	}
	expectedPrompt, err := agentCfg.ProviderPrompt(runtimeagentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatalf("assemble selected-fork expected provider prompt: %v", err)
	}
	expectedText, err := expectedPrompt.Text()
	if err != nil {
		t.Fatalf("render selected-fork expected provider prompt: %v", err)
	}
	if !slices.Equal(startupPrompts, []string{expectedText}) {
		t.Fatalf("selected-fork startup prompts = %#v, want exact canonical prompt %q", startupPrompts, expectedText)
	}
	if len(surfaceIDs) != 1 {
		t.Fatalf("selected-fork startup surfaces = %#v, want one", surfaceIDs)
	}
	var persisted int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM managed_agent_capability_surfaces
		WHERE surface_id = $1::uuid
		  AND authority_kind = 'startup_probe'
		  AND execution_kind = 'selected_contract_fork'
		  AND execution_authority_id = $2::text
		  AND run_id = $3::uuid
		  AND actor_id = 'selected-health-agent'
		  AND surface->'tools'->0->'evidence' @> '[{"kind":"mcp_listed","status":"confirmed"}]'::jsonb
	`, surfaceIDs[0], proof.RuntimeExecutionID, proof.ForkRunID).Scan(&persisted); err != nil {
		t.Fatalf("count selected-fork eligible-call capability surface: %v", err)
	}
	if persisted != 1 {
		t.Fatalf("selected-fork eligible-call capability surfaces = %d, want 1", persisted)
	}
}

type selectedForkStartupProbeExecutor struct {
	executed []string
}

func (e *selectedForkStartupProbeExecutor) Execute(_ context.Context, name string, _ any) (any, error) {
	e.executed = append(e.executed, strings.TrimSpace(name))
	return map[string]any{"ok": true}, nil
}

func (*selectedForkStartupProbeExecutor) ToolDefinitionsForActor(runtimeactors.AgentConfig) []runtimellm.ToolDefinition {
	return selectedForkStartupProbeDefinitions()
}

func (*selectedForkStartupProbeExecutor) ToolDefinitionsForActorInContext(context.Context, runtimeactors.AgentConfig) []runtimellm.ToolDefinition {
	return selectedForkStartupProbeDefinitions()
}

func (*selectedForkStartupProbeExecutor) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	return selectedForkStartupProbeCapabilities(names)
}

func (*selectedForkStartupProbeExecutor) ToolCapabilitiesForActorInContext(_ context.Context, _ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	return selectedForkStartupProbeCapabilities(names)
}

func selectedForkStartupProbeDefinitions() []runtimellm.ToolDefinition {
	return []runtimellm.ToolDefinition{{
		Name: "health_check", Description: "Verify selected-fork MCP callability",
		Schema: map[string]any{"type": "object", "properties": map[string]any{}},
	}}
}

func selectedForkStartupProbeCapabilities(names []string) toolcapabilities.Set {
	capabilities := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		capabilities = append(capabilities, toolcapabilities.Capability{
			Name: strings.TrimSpace(name), Visible: true, Callable: true,
			ContextRequirement: toolcapabilities.ContextRequirementActorContext,
			StartupProbeMode:   toolcapabilities.StartupProbeModeCallEmptyObject,
		})
	}
	return toolcapabilities.NewSet(capabilities)
}

type selectedForkStartupVisibleSurfaceProbe struct{}

type selectedForkStartupRuntime struct {
	runtimellm.NoopRuntime
	prompts *[]string
}

func (selectedForkStartupRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.ClaudeCLIProviderContract()
}

func (r selectedForkStartupRuntime) ProbeStartupVisibleToolSurface(ctx context.Context, actor runtimeactors.AgentConfig, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Response, error) {
	if r.prompts != nil {
		*r.prompts = append(*r.prompts, systemPrompt)
	}
	return selectedForkStartupVisibleSurfaceProbe{}.ProbeStartupVisibleToolSurface(ctx, actor, systemPrompt, tools)
}

func (selectedForkStartupVisibleSurfaceProbe) ProbeStartupVisibleToolSurface(ctx context.Context, _ runtimeactors.AgentConfig, _ string, _ []runtimellm.ToolDefinition) (*runtimellm.Response, error) {
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, errors.New("selected-fork startup capability surface missing")
	}
	response := &runtimellm.Response{
		MCPServers:      map[string]string{"runtime-tools": "connected"},
		MCPVisibleTools: surface.PlannedBindingNames(managedcapabilities.BindingMCPProvider),
	}
	observed, err := runtimellm.ObserveCLIResponseCapabilitySurface(surface, response)
	if err != nil {
		return nil, err
	}
	response.CapabilitySurface = &observed
	return response, runtimellm.ValidateCLIProviderCapabilitySurface(observed, response)
}

func buildSelectedForkProofContainer(t testing.TB, ctx context.Context, db *sql.DB) selectedContractForkLocalRuntimeContainer {
	t.Helper()
	now := time.Now().UTC()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	forkEventID := uuid.NewString()
	bindingID := uuid.NewString()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: sourceRunID, StartedAt: now})
	storetest.RequirePausedRun(t, ctx, storetest.AdmitPostgresRuntimeStore(t, db), forkRunID, now)
	storetest.InsertExistingRunRootEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres, forkEventID, sourceRunID, "selected.proof",
		eventtest.Producer(events.EventProducerExternal, "selected-proof"), []byte(`{}`), events.EventEnvelope{Scope: events.EventScopeGlobal}, now)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings
			(binding_id,fork_run_id,source_run_id,fork_event_id,mode,contracts_root,workflow_name,workflow_version,created_at)
		VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,'selected_contracts','/tmp/contracts','workflow','v1',$5)
	`, bindingID, forkRunID, sourceRunID, forkEventID, now); err != nil {
		t.Fatalf("seed selected-fork proof binding: %v", err)
	}
	selection := runfork.RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: "/tmp/contracts", WorkflowName: "workflow", WorkflowVersion: "v1"}
	selectedSource := testSelectedSource(selection)
	deferredWorkAdmission := selectedContractDeferredWorkAdmissionForTest(t, sourceRunID, forkEventID, selectedSource)
	admission := runfork.RunForkSelectedContractExecutionAdmission{
		Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
		NonMutating: true, ForkRunID: forkRunID, SourceRunID: sourceRunID, ForkEventID: forkEventID,
		ContractSelection: selection, ContractBindingOwner: runfork.RunForkSelectedContractBindingOwner,
		AdmissionOwner: runfork.RunForkContractFrontierAdmissionOwner, AdmissionUse: runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner: runfork.RunForkSelectedContractExecutionModelOwner, SourceWorkflowName: "workflow", SourceWorkflowVersion: "v1",
		DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
	}
	planning := runfork.RunForkSelectedContractRecipientPlanning{
		Owner: runfork.RunForkSelectedContractRecipientPlanningOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
		NonMutating: true, RecipientPlanningSupported: true, ContractSelection: selection,
	}
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	container, err := buildSelectedContractForkLocalRuntimeContainer(ctx, publishSelectedContractForkEventsRequest{
		Admission: admission, RecipientPlanning: planning, Owner: selectedContractExecutionOwnerForTest(t, selected),
		LoadedSource: LoadedSelectedContractSource{
			Selection:        selection,
			Source:           selectedSource,
			BundleSourceFact: testEphemeralBundleSourceFact(runForkTestBundleHash),
		},
		SourceRunID: sourceRunID, ForkRunID: forkRunID, ForkEventID: forkEventID, SourceEvents: []string{forkEventID},
		ExecutionOwner: runfork.RunForkSelectedContractExecutionOwner, DeferredWorkAdmission: deferredWorkAdmission,
		AgentRuntime: selectedContractAgentRuntimePlan{Options: SelectedContractAgentRuntimeOptions{
			ExecutionPosture: executionposture.Live,
		}},
	})
	if err != nil {
		t.Fatalf("buildSelectedContractForkLocalRuntimeContainer: %v", err)
	}
	return container
}

func TestSelectedContractForkAuthoredHTTPToolPersistsCapabilityAndRejectsHostileAdmission(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := runForkTestContext(t)
	container := buildSelectedForkProofContainer(t, ctx, db)
	proof := container.Proof()
	var requests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer target.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"selected_http": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass("write_or_unknown")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: target.URL, TimeoutSeconds: 5})),
	}})
	executor := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{WorkflowSource: source})
	actorIdentity := selectedContractTestAgentIdentity(t, "selected-tool-agent", "global")
	actor := runtimeactors.AgentConfig{
		ID: "selected-tool-agent", Identity: actorIdentity, FlowPath: actorIdentity.FlowInstance(),
		Role: "selected_tool", Tools: []string{"selected_http"},
	}
	turnID := uuid.NewString()
	sessionID := uuid.NewString()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: actorIdentity, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "selected-http-proof",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
			ExecutionKind: managedcapabilities.ExecutionSelectedContractFork, ExecutionAuthorityID: proof.RuntimeExecutionID,
			RunID: proof.ForkRunID, SessionID: sessionID, TurnOrdinal: 1,
		},
		Tools: []managedcapabilities.PlannedTool{{
			Name: "selected_http", DefinitionHash: "selected-http-definition-v1",
			Capability: toolcapabilities.Capability{Name: "selected_http", Visible: true, Callable: true},
			Bindings: []managedcapabilities.DeliveryBinding{{
				Kind: managedcapabilities.BindingLocalRuntime, ExactName: "selected_http", RequiredEvidenceKind: "local_runtime_registered",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("build selected HTTP capability surface: %v", err)
	}
	surface, err = surface.Observe(managedcapabilities.DeliveryEvidence{
		BindingKind: managedcapabilities.BindingLocalRuntime, ExactName: "selected_http",
		Kind: "local_runtime_registered", Status: managedcapabilities.EvidenceConfirmed,
	})
	if err != nil {
		t.Fatalf("observe selected HTTP capability surface: %v", err)
	}

	effectCtx := runtimeactors.WithActor(ctx, actor)
	effectCtx = runtimeeffects.WithLogicalOperationIdentity(effectCtx, "selected-http-tool-call")
	effectCtx = runtimeeffects.WithAuthority(effectCtx, container.authority)
	effectCtx = runtimeeffects.WithUsageTarget(effectCtx, runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: proof.ForkRunID, AgentID: actor.ID,
		AgentIdentity: actorIdentity, SessionID: sessionID, Memory: agentmemory.PlatformDefault(),
		FlowInstance: actorIdentity.FlowInstance(),
	})
	effectCtx = runtimeeffects.WithController(effectCtx, liveTestEffectController(storetest.AdmitPostgresRuntimeStore(t, db)))
	effectCtx = managedexecution.WithAdmission(effectCtx, container.admission)
	effectCtx = managedcapabilities.WithContext(effectCtx, surface)
	if _, err := executor.Execute(effectCtx, "selected_http", map[string]any{}); err != nil {
		t.Fatalf("execute selected-fork authored HTTP tool: %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("selected-fork authored HTTP requests = %d, want 1", requests.Load())
	}

	var persisted int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND o.authority_kind = 'selected_contract_fork'
		  AND o.effect_kind = 'http_tool_target'
		  AND a.adapter = 'authored_http_tool'
		  AND a.capability_surface_id = $2::uuid
		  AND a.state = 'settled'
	`, proof.RuntimeExecutionID, surface.ID).Scan(&persisted); err != nil {
		t.Fatalf("count selected-fork HTTP effect evidence: %v", err)
	}
	if persisted != 1 {
		t.Fatalf("selected-fork HTTP effect evidence = %d, want 1", persisted)
	}

	hostileAdmission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork, uuid.NewString(), proof.RuntimeGeneration, uuid.NewString(),
		proof.ActorCensusFingerprint, runForkTestBundleHash, nil,
	)
	if err != nil {
		t.Fatalf("build hostile selected-fork admission: %v", err)
	}
	hostileCtx := managedexecution.WithAdmission(runtimeeffects.WithLogicalOperationIdentity(effectCtx, "hostile-selected-http-tool-call"), hostileAdmission)
	if _, err := executor.Execute(hostileCtx, "selected_http", map[string]any{}); err == nil || !strings.Contains(err.Error(), "managed_effect_execution_authority_mismatch") {
		t.Fatalf("hostile selected-fork HTTP tool error = %v, want managed authority mismatch", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("hostile selected-fork dispatch reached HTTP target; requests=%d", requests.Load())
	}

	crossRunSurface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: actorIdentity, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "selected-http-proof",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: uuid.NewString(),
			ExecutionKind: managedcapabilities.ExecutionSelectedContractFork, ExecutionAuthorityID: proof.RuntimeExecutionID,
			RunID: uuid.NewString(), SessionID: uuid.NewString(), TurnOrdinal: 1,
		},
		Tools: []managedcapabilities.PlannedTool{{
			Name: "selected_http", DefinitionHash: "selected-http-definition-v1",
			Capability: toolcapabilities.Capability{Name: "selected_http", Visible: true, Callable: true},
			Bindings: []managedcapabilities.DeliveryBinding{{
				Kind: managedcapabilities.BindingLocalRuntime, ExactName: "selected_http", RequiredEvidenceKind: "local_runtime_registered",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("build hostile cross-run selected HTTP capability surface: %v", err)
	}
	crossRunSurface, err = crossRunSurface.Observe(managedcapabilities.DeliveryEvidence{
		BindingKind: managedcapabilities.BindingLocalRuntime, ExactName: "selected_http",
		Kind: "local_runtime_registered", Status: managedcapabilities.EvidenceConfirmed,
	})
	if err != nil {
		t.Fatalf("observe hostile cross-run selected HTTP capability surface: %v", err)
	}
	crossRunCtx := runtimeeffects.WithLogicalOperationIdentity(effectCtx, "hostile-cross-run-selected-http-tool-call")
	crossRunCtx = managedcapabilities.WithContext(crossRunCtx, crossRunSurface)
	if _, err := executor.Execute(crossRunCtx, "selected_http", map[string]any{}); err == nil || !strings.Contains(err.Error(), "managed_effect_execution_authority_mismatch") {
		t.Fatalf("hostile cross-run selected-fork HTTP tool error = %v, want managed authority mismatch", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("hostile cross-run selected-fork dispatch reached HTTP target; requests=%d", requests.Load())
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND o.effect_kind = 'http_tool_target'
		  AND a.adapter = 'authored_http_tool'
	`, proof.RuntimeExecutionID).Scan(&persisted); err != nil {
		t.Fatalf("count selected-fork HTTP effect evidence after hostile cross-run attempt: %v", err)
	}
	if persisted != 1 {
		t.Fatalf("selected-fork HTTP effect evidence after hostile cross-run attempt = %d, want 1", persisted)
	}
}

func TestExecuteSelectedContractRunForkProviderFailurePreservesEvidenceThroughCleanup(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier7-composition/test-agent-emits-to-node")
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: contractsRoot})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	processCapability := selectedContractTestProcessCapability(t, ctx, pg, loaded)
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"provider failed"}}`))
	}))
	defer provider.Close()
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := credentials.Set(ctx, llmselection.OpenAICompatibleCredentialEnv, "test-key"); err != nil {
		t.Fatalf("store provider credential: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002403, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at, loaded.BundleSourceFact)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID: sourceRunID, At: sourceEventID, ConfirmSourceFreeze: true,
		Owner: selectedContractExecutionOwnerForTest(t, pg), SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot),
		AgentRuntime: SelectedContractAgentRuntimeOptions{
			Config:              selectedForkAPIProviderConfig(llmselection.BackendOpenAICompatible, "gpt-selected-fork", provider.URL),
			ProviderCredentials: credentials, ProcessCapability: processCapability,
			QuiescenceTimeout: selectedForkCapabilityProofQuiescenceTimeout,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "fork_selected_contract_agent_delivery_incomplete") {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want failed activation after terminal selected-agent delivery", err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider failure dispatches = %d, want exactly 1", providerCalls.Load())
	}
	proof := result.ForkLocalRuntimeContainer
	if proof == nil {
		t.Fatal("selected-fork failure runtime proof is missing")
	}

	var attemptState, operationState, failure, surfaceID, turnSurfaceID, executionState string
	if err := db.QueryRowContext(ctx, `
		SELECT a.state, o.state, a.failure::text, a.capability_surface_id::text,
		       t.capability_surface_id::text, e.state
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		JOIN agent_turns t ON t.completion_attempt_id = a.attempt_id
		JOIN run_fork_selected_contract_runtime_executions e ON e.execution_id = o.selected_execution_id
		WHERE o.selected_execution_id = $1::uuid
		  AND o.authority_kind = 'selected_contract_fork'
		  AND a.adapter = 'openai_compatible'
	`, proof.RuntimeExecutionID).Scan(&attemptState, &operationState, &failure, &surfaceID, &turnSurfaceID, &executionState); err != nil {
		t.Fatalf("load selected-fork failure cleanup evidence: %v", err)
	}
	if attemptState != string(runtimeeffects.StateOutcomeUncertain) || operationState != string(runtimeeffects.StateOutcomeUncertain) || failure == "" {
		t.Fatalf("selected-fork provider failure attempt=%s operation=%s failure=%q", attemptState, operationState, failure)
	}
	if surfaceID == "" || surfaceID != turnSurfaceID {
		t.Fatalf("selected-fork failure surfaces attempt=%q turn=%q", surfaceID, turnSurfaceID)
	}
	if executionState != "closed" {
		t.Fatalf("selected-fork runtime state after failure cleanup = %q, want closed", executionState)
	}
	var active int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND a.state IN ('authorized','launched','response_observed')
	`, proof.RuntimeExecutionID).Scan(&active); err != nil {
		t.Fatalf("count selected-fork active failure attempts: %v", err)
	}
	if active != 0 {
		t.Fatalf("selected-fork active attempts after failure cleanup = %d, want 0", active)
	}
}

func TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx := runForkTestContext(t)
	now := time.Now().UTC()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	forkEventID := uuid.NewString()
	bindingID := uuid.NewString()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: sourceRunID, StartedAt: now})
	storetest.RequirePausedRun(t, ctx, storetest.AdmitPostgresRuntimeStore(t, db), forkRunID, now)
	storetest.InsertExistingRunRootEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres, forkEventID, sourceRunID, "selected.test",
		eventtest.Producer(events.EventProducerExternal, "selected-test"), []byte(`{}`), events.EventEnvelope{Scope: events.EventScopeGlobal}, now)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings
			(binding_id,fork_run_id,source_run_id,fork_event_id,mode,contracts_root,workflow_name,workflow_version,created_at)
		VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,'selected_contracts','/tmp/contracts','workflow','v1',$5)
	`, bindingID, forkRunID, sourceRunID, forkEventID, now); err != nil {
		t.Fatalf("seed selected-contract container competition binding: %v", err)
	}

	servedDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open served PostgreSQL handle: %v", err)
	}
	defer servedDB.Close()
	standaloneDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open standalone PostgreSQL handle: %v", err)
	}
	defer standaloneDB.Close()
	if err := servedDB.PingContext(ctx); err != nil {
		t.Fatalf("ping served PostgreSQL handle: %v", err)
	}
	if err := standaloneDB.PingContext(ctx); err != nil {
		t.Fatalf("ping standalone PostgreSQL handle: %v", err)
	}
	servedStore := storetest.AdmitPostgresRuntimeStore(t, servedDB)
	standaloneStore := storetest.AdmitPostgresRuntimeStore(t, standaloneDB)

	selection := runfork.RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/contracts",
		WorkflowName:    "workflow",
		WorkflowVersion: "v1",
	}
	selectedSource := testSelectedSource(selection)
	deferredWorkAdmission := selectedContractDeferredWorkAdmissionForTest(t, sourceRunID, forkEventID, selectedSource)
	admission := runfork.RunForkSelectedContractExecutionAdmission{
		Owner:                      runfork.RunForkSelectedContractExecutionAdmissionOwner,
		FutureExecutionOwner:       runfork.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		ExecutionSupported:         false,
		ForkRunID:                  forkRunID,
		SourceRunID:                sourceRunID,
		ForkEventID:                forkEventID,
		ContractSelection:          selection,
		ContractBindingOwner:       runfork.RunForkSelectedContractBindingOwner,
		AdmissionOwner:             runfork.RunForkContractFrontierAdmissionOwner,
		AdmissionUse:               runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner:        runfork.RunForkSelectedContractExecutionModelOwner,
		DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
		SourceWorkflowName:         "workflow",
		SourceWorkflowVersion:      "v1",
	}
	planning := runfork.RunForkSelectedContractRecipientPlanning{
		Owner:                      runfork.RunForkSelectedContractRecipientPlanningOwner,
		FutureExecutionOwner:       runfork.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		RecipientPlanningSupported: true,
		ContractSelection:          selection,
	}
	baseRequest := publishSelectedContractForkEventsRequest{
		Admission: admission,
		LoadedSource: LoadedSelectedContractSource{
			Selection:        selection,
			Source:           selectedSource,
			BundleSourceFact: testEphemeralBundleSourceFact("bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
		},
		RecipientPlanning:     planning,
		SourceRunID:           sourceRunID,
		ForkRunID:             forkRunID,
		ForkEventID:           forkEventID,
		SourceEvents:          []string{forkEventID},
		ExecutionOwner:        runfork.RunForkSelectedContractExecutionOwner,
		DeferredWorkAdmission: deferredWorkAdmission,
		AgentRuntime: selectedContractAgentRuntimePlan{Options: SelectedContractAgentRuntimeOptions{
			ExecutionPosture: executionposture.Live,
		}},
	}
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		runForkTestRuntimeInstanceID,
		baseRequest.LoadedSource.BundleSourceFact.BundleHash(),
	))
	type contenderResult struct {
		surface   string
		container selectedContractForkLocalRuntimeContainer
		store     *store.PostgresStore
		err       error
	}
	start := make(chan struct{})
	results := make(chan contenderResult, 2)
	contenders := []struct {
		surface string
		store   *store.PostgresStore
	}{
		{surface: "served", store: servedStore},
		{surface: "standalone", store: standaloneStore},
	}
	for _, contender := range contenders {
		contender := contender
		go func() {
			<-start
			req := baseRequest
			req.Owner = selectedContractExecutionOwnerForTest(t, contender.store)
			container, buildErr := buildSelectedContractForkLocalRuntimeContainer(ctx, req)
			results <- contenderResult{surface: contender.surface, container: container, store: contender.store, err: buildErr}
		}()
	}
	close(start)
	first, second := <-results, <-results
	var winner contenderResult
	var loser contenderResult
	if first.err == nil {
		winner, loser = first, second
	} else {
		winner, loser = second, first
	}
	if winner.err != nil || loser.err == nil {
		t.Fatalf("served/standalone authority race first=%s/%v second=%s/%v, want exactly one winner", first.surface, first.err, second.surface, second.err)
	}
	if winner.surface == loser.surface {
		t.Fatalf("served/standalone authority race returned duplicate surface %q", winner.surface)
	}
	proof := winner.container.Proof()
	if proof.RuntimeExecutionID == "" || proof.RuntimeGeneration != 1 || proof.AuthorityExecutionOwner == "" {
		t.Fatalf("winning %s authority proof = %#v", winner.surface, proof)
	}

	authority := winner.container.authority
	targetIdentity := selectedContractTestAgentIdentity(t, "selected-agent", "selected-authority-race")
	authority.Target = runtimeeffects.UsageTarget{
		Kind:          runtimeeffects.UsageTargetAgentTurn,
		ID:            uuid.NewString(),
		RunID:         forkRunID,
		AgentID:       "selected-agent",
		AgentIdentity: targetIdentity,
		SessionID:     uuid.NewString(),
		Memory:        agentmemory.PlatformDefault(),
		FlowInstance:  targetIdentity.FlowInstance(),
	}
	providerCtx := runtimeeffects.WithLogicalOperationIdentity(
		runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), liveTestCompletionController(winner.store, winner.store, winner.store, selectedForkDiscardSpendProjection{})),
		"served-standalone-authority-race",
	)
	providerCtx = managedexecution.WithAdmission(providerCtx, winner.container.admission)
	capabilitySurface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Target.AgentIdentity, RuntimeMode: "task",
		Provider: "openai", Transport: "api", ProviderContract: "run-fork-race-test",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID,
			ExecutionKind:        managedcapabilities.ExecutionSelectedContractFork,
			ExecutionAuthorityID: authority.SelectedFork.ExecutionID, RunID: authority.SelectedFork.ForkRunID,
			SessionID: authority.Target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build winning %s capability surface: %v", winner.surface, err)
	}
	providerCtx = managedcapabilities.WithContext(providerCtx, capabilitySurface)
	handle, err := runtimeeffects.BeginCompletion(providerCtx, "openai_compatible", []byte("request"), nil)
	if err != nil {
		t.Fatalf("winning %s authorize provider completion: %v", winner.surface, err)
	}
	if err := handle.MarkLaunched(providerCtx); err != nil {
		t.Fatalf("winning %s launch provider completion: %v", winner.surface, err)
	}
	var providerLaunches atomic.Int32
	providerLaunches.Add(1)
	if err := handle.MarkResponseObserved(providerCtx, map[string]any{"surface": winner.surface}); err != nil {
		t.Fatalf("winning %s observe provider response: %v", winner.surface, err)
	}
	inputTokens, outputTokens := int64(1), int64(1)
	capabilitySurfaceJSON, err := json.Marshal(capabilitySurface)
	if err != nil {
		t.Fatalf("marshal winning %s capability surface: %v", winner.surface, err)
	}
	if _, err := handle.SettleCompletion(providerCtx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"surface": winner.surface}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "test-model",
			Exactness:     runtimeeffects.CompletionUsageExact,
			InputTokens:   &inputTokens,
			OutputTokens:  &outputTokens,
		},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: authority.Target.ID, RunID: forkRunID, AgentID: authority.Target.AgentID,
			Identity:  agentmemory.Identity{RunID: forkRunID, Agent: targetIdentity},
			SessionID: authority.Target.SessionID, Memory: authority.Target.Memory,
			FlowInstance: authority.Target.FlowInstance, ParseOK: true,
			CapabilitySurfaceID: capabilitySurface.ID, CapabilitySurface: capabilitySurfaceJSON,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: authority.Target.FlowInstance, AgentID: authority.Target.AgentID,
			AgentIdentity: targetIdentity,
			Model:         "test-model", ModelAlias: "regular", BackendProfile: "test",
			Provider: "test", Transport: "http", ResolvedModel: "test-model",
			InvocationType: "test",
		},
		Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("winning %s settle provider completion: %v", winner.surface, err)
	}
	if providerLaunches.Load() != 1 {
		t.Fatalf("provider launches = %d, want 1", providerLaunches.Load())
	}
	if err := winner.container.Quiesce(ctx); err != nil {
		t.Fatalf("quiesce winning %s container: %v", winner.surface, err)
	}
	if err := winner.container.Close(ctx); err != nil {
		t.Fatalf("close winning %s container: %v", winner.surface, err)
	}

	var authorities, attempts, reservations int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1::uuid AND generation=1 AND state='closed'`, forkRunID).Scan(&authorities); err != nil {
		t.Fatalf("count served/standalone authority rows: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1::uuid AND a.state='settled'`, proof.RuntimeExecutionID).Scan(&attempts); err != nil {
		t.Fatalf("count served/standalone completion attempts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, handle.Attempt().AttemptID).Scan(&reservations); err != nil {
		t.Fatalf("count no-cap completion reservations: %v", err)
	}
	if authorities != 1 || attempts != 1 || reservations != 0 {
		t.Fatalf("served/standalone authority evidence authorities=%d attempts=%d reservations=%d, want 1/1/0", authorities, attempts, reservations)
	}
}

func TestStartSelectedContractAgentRuntimeGatewayReturnsGeneratedBinding(t *testing.T) {
	const staleHostURL = "http://127.0.0.1:9998"
	const staleContainerURL = "http://host.docker.internal:9998"
	t.Setenv("SWARM_TOOL_GATEWAY_URL", staleHostURL)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", staleContainerURL)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{})
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, turns, testGatewayWorkOwner(t), nil)
	if err != nil {
		t.Fatalf("startSelectedContractAgentRuntimeGateway: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup is nil")
	}
	defer cleanup()
	if binding.HostMCPURL() == "" || binding.WorkspaceMCPURL() == "" {
		t.Fatalf("binding endpoints were not populated: %#v", binding)
	}
	if binding.AuthToken() == "" {
		t.Fatalf("binding token was not generated: %#v", binding)
	}
	if strings.Contains(binding.HostEndpoint, ":9998") || strings.Contains(binding.WorkspaceEndpoint, ":9998") {
		t.Fatalf("binding endpoints used stale env: %#v", binding)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL")); got != staleHostURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want stale operator value unchanged %q", got, staleHostURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL")); got != staleContainerURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want stale operator value unchanged %q", got, staleContainerURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN = %q, want generated binding token to remain typed-only", got)
	}

	req, err := http.NewRequest(http.MethodPost, binding.HostMCPURL(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+binding.AuthToken())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post selected-fork gateway initialize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("selected-fork gateway status = %d, want 200", resp.StatusCode)
	}
}

func TestStartSelectedContractAgentRuntimeGatewayRejectsRetiredTokenEnv(t *testing.T) {
	const staleHostURL = "http://127.0.0.1:9998"
	const staleContainerURL = "http://host.docker.internal:9998"
	t.Setenv("SWARM_TOOL_GATEWAY_URL", staleHostURL)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", staleContainerURL)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")

	exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{})
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, turns, testGatewayWorkOwner(t), nil)
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_TOKEN is retired") || !strings.Contains(err.Error(), "ToolGatewayBinding") {
		t.Fatalf("startSelectedContractAgentRuntimeGateway error = %v, want retired token env rejection", err)
	}
	if cleanup != nil {
		cleanup()
		t.Fatal("cleanup was returned for rejected retired token env")
	}
	if !binding.Empty() {
		t.Fatalf("binding = %#v, want empty rejected binding", binding)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL")); got != staleHostURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want unchanged %q", got, staleHostURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL")); got != staleContainerURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want unchanged %q", got, staleContainerURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")); got != "operator-token" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN = %q, want explicit token env preserved", got)
	}
}

func TestStartSelectedContractAgentRuntimeCleansGatewayOnRegistrationFailure(t *testing.T) {
	const staleHostURL = "http://127.0.0.1:9998"
	const staleContainerURL = "http://host.docker.internal:9998"
	t.Setenv("SWARM_TOOL_GATEWAY_URL", staleHostURL)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", staleContainerURL)
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	owner := testGatewayWorkOwner(t)
	eventBus, err := bus.NewEphemeralEventBusWithOptions(nil, bus.EventBusOptions{ExecutionPosture: executionposture.Live, WorkOwner: owner, ReceiverExecution: eventreceiver.NormalExecution()})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthoritySelectedContractFork, ID: "00000000-0000-0000-0000-000000000301",
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{
			ExecutionID: "00000000-0000-0000-0000-000000000301", ForkRunID: "00000000-0000-0000-0000-000000000302", Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
		},
		ExecutionOwner: "cleanup-test-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
	}
	ctx := selectedForkExecutionTestContext(t, context.Background(), authority)
	badIdentity := selectedContractTestRootAgentIdentity(t, "bad-agent")

	_, _, err = startSelectedContractAgentRuntime(ctx, publishSelectedContractForkEventsRequest{
		Owner: SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{}},
		AgentRuntime: selectedContractAgentRuntimePlan{
			Proof: SelectedContractAgentRuntimeMaterialization{
				AgentRecipients: []agentidentity.Identity{badIdentity},
			},
			Records: []runtimemanager.PersistedAgent{{
				Config: runtimeactors.AgentConfig{
					ExecutionMode: "live",
					ID:            "bad-agent",
					Identity:      badIdentity,
					Role:          "worker",
					LLMBackend:    llmselection.BackendAnthropic,
					Intent:        selectedContractTestIntent(t, "bad-agent"),
					Model:         "regular",
					Subscriptions: []string{"item.received"},
				},
				Topology: selectedContractTestDeclarationTopology(t),
			}},
			Options: SelectedContractAgentRuntimeOptions{
				ExecutionPosture: executionposture.Live,
				Config:           &config.Config{},
				LLMRuntime:       selectedContractCleanupRuntime{},
				AgentManagerOptions: runtimemanager.AgentManagerOptions{
					WorkOwner: owner, ReceiverExecution: eventreceiver.NormalExecution(),
				},
			},
		},
	}, eventBus, &runtimepipeline.PipelineCoordinator{})
	if err == nil || !strings.Contains(err.Error(), "cannot reconstruct its derived prompt without a semantic source") {
		t.Fatalf("startSelectedContractAgentRuntime error = %v, want registration failure", err)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL")); got != staleHostURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want unchanged %q", got, staleHostURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL")); got != staleContainerURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want unchanged %q", got, staleContainerURL)
	}
}

type selectedContractCleanupRuntime struct{ runtimellm.NoopRuntime }

func (selectedContractCleanupRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

func TestExecuteSelectedContractRunForkTreatsDiagnosticPlatformOutcomeAsLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, loaded.BundleSourceFact)
	ctx = runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.BundleSourceFact.BundleHash()),
	)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	diagnosticEventID := uuid.NewString()
	at := time.Unix(1700002215, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, loaded.BundleSourceFact)
	seedSelectedExecutionDiagnosticPlatformDeadLetter(t, db, sourceRunID, diagnosticEventID, at.Add(-time.Second))
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated || result.ExecutedEventCount != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	if result.SelectedContractExecutionAdmission == nil || result.SelectedContractExecutionAdmission.FrontierEventCount != 1 {
		t.Fatalf("selected execution admission = %#v, want only selected source frontier", result.SelectedContractExecutionAdmission)
	}
	if selectedExecutionResultHasBlocker(result, runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("selected execution retained unresolved route blocker: materialization=%#v activation=%#v", result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
	}

	var diagnosticCopies int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND (
			event_id = $2::uuid
			OR COALESCE(source_event_id::text, '') = $2::text
		  )
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticCopies); err != nil {
		t.Fatalf("count copied diagnostic events: %v", err)
	}
	if diagnosticCopies != 0 {
		t.Fatalf("diagnostic platform events copied into fork = %d, want 0", diagnosticCopies)
	}

	var diagnosticExecutionLineage int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticExecutionLineage); err != nil {
		t.Fatalf("count diagnostic execution lineage: %v", err)
	}
	if diagnosticExecutionLineage != 0 {
		t.Fatalf("diagnostic platform execution lineage rows = %d, want 0", diagnosticExecutionLineage)
	}

	var selectedExecutionLineage int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&selectedExecutionLineage); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if selectedExecutionLineage != 1 {
		t.Fatalf("selected source execution lineage rows = %d, want 1", selectedExecutionLineage)
	}
}

func TestActivateSelectedContractRunForkExecutesReplayReadyContractSwapThroughSelectedRecipients(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	selection := runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002600, 0).UTC()
	seedSelectedExecutionSourceRunWithPrimaryRoute(t, db, sourceRunID, entityID, sourceEventID, "item.received", at,
		selectedExecutionTestAgentRoute(t, "source-agent-that-must-not-route", "flow-a/1"), nil, loaded.BundleSourceFact)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	materialized := materializeSelectedExecutionForkForTest(t, ctx, pg, loaded, selection, sourceRunID, sourceEventID)

	result, err := activateLiveSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
		ForkRunID:           materialized.ForkRunID,
		ConfirmSourceFreeze: true,
		Store:               pg,
		ExecutionOwner:      selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
	})
	if err != nil {
		t.Fatalf("ActivateSelectedContractRunFork: %v", err)
	}
	if result.ContractSwapBootResumeExecution == nil ||
		result.ContractSwapBootResumeExecution.Owner != runfork.RunForkHistoricalReplayContractSwapBootResumeOwner ||
		result.ContractSwapBootResumeExecution.ParentHistoricalReplayExecutionOwner != runfork.RunForkHistoricalReplayExecutionOwner ||
		len(result.ContractSwapBootResumeExecution.ExecutableWork) != 1 {
		t.Fatalf("contract-swap execution = %#v", result.ContractSwapBootResumeExecution)
	}
	if result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 || !result.Activated {
		t.Fatalf("activation result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		runfork.RunForkHistoricalReplayContractSwapBootResumeOwner,
		sourceRunID,
		materialized.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	forkEventID := result.ForkEvents[0].ForkEventID

	var sourceSubscriberDeliveries, forkEventDeliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_id = 'source-agent-that-must-not-route'
	`, materialized.ForkRunID, forkEventID).Scan(&sourceSubscriberDeliveries); err != nil {
		t.Fatalf("count source subscriber deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, materialized.ForkRunID, forkEventID).Scan(&forkEventDeliveries); err != nil {
		t.Fatalf("count fork event deliveries: %v", err)
	}
	if sourceSubscriberDeliveries != 0 || forkEventDeliveries == 0 {
		t.Fatalf("fork delivery recipients source=%d total=%d, want selected recipient planning without source subscriber", sourceSubscriberDeliveries, forkEventDeliveries)
	}

	var genericReplayRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_delivery_event_replays
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&genericReplayRows); err != nil {
		t.Fatalf("count generic delivery replay rows: %v", err)
	}
	if genericReplayRows != 0 {
		t.Fatalf("generic delivery replay rows = %d, want contract-swap execution to avoid source-subscriber writer", genericReplayRows)
	}

	var forkReceipts, emittedFollowUps int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&forkReceipts); err != nil {
		t.Fatalf("count fork receipts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'item.processed'
		  AND source_event_id = $2::uuid
	`, materialized.ForkRunID, forkEventID).Scan(&emittedFollowUps); err != nil {
		t.Fatalf("count emitted follow-ups: %v", err)
	}
	if forkReceipts == 0 || emittedFollowUps != 1 {
		t.Fatalf("fork outcomes receipts=%d followUps=%d, want selected handler execution", forkReceipts, emittedFollowUps)
	}
}

func TestActivateSelectedContractRunForkFailsBeforePublishForPostTReplayScopeMarker(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	selection := runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	afterEventID := uuid.NewString()
	at := time.Unix(1700002605, 0).UTC()
	seedSelectedExecutionSourceRunWithPrimaryRoute(t, db, sourceRunID, entityID, sourceEventID, "item.received", at,
		selectedExecutionTestAgentRoute(t, "source-agent-that-must-not-route", "flow-a/1"), nil, loaded.BundleSourceFact)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	materialized := materializeSelectedExecutionForkForTest(t, ctx, pg, loaded, selection, sourceRunID, sourceEventID)
	seedSelectedExecutionPostForkSourceEvent(t, db, sourceRunID, afterEventID, entityID, at.Add(time.Second))
	seedSelectedExecutionSourceReplayScopeMarker(t, db, sourceRunID, afterEventID, "replay_scope_direct", at.Add(time.Second))
	captureSelectedExecutionSourceRevision(t, db, sourceRunID,
		runforkrevision.FamilyEvents,
		runforkrevision.FamilyEventDeliveries,
		runforkrevision.FamilyCommittedReplayScopes,
	)

	result, err := activateLiveSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
		ForkRunID:      materialized.ForkRunID,
		Store:          pg,
		ExecutionOwner: selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:   loader,
	})
	if err == nil || !strings.Contains(err.Error(), "source_committed_replay_scope_advanced_after_fork_point") {
		t.Fatalf("ActivateSelectedContractRunFork error = %v, want post-T marker blocker", err)
	}
	if result.ExecutedEventCount != 0 || len(result.ForkEvents) != 0 || result.Activated {
		t.Fatalf("result = %#v, want no fork publish before marker block", result)
	}
	assertNoForkExecutionRowsForRun(t, db, materialized.ForkRunID)
}

func TestExecuteSelectedContractRunForkTreatsSourceConversationHistoryAsLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002300, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, loaded.BundleSourceFact)
	agentIdentity := selectedContractTestAgentIdentity(t, "agent-a", "flow-a/1")
	agentFields := selectedExecutionTestAgentFields(t, agentIdentity)
	seedSelectedExecutionTestAgent(t, ctx, db, agentIdentity, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'active', $10, $10)
	`, sessionID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath, at); err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, entity_id, flow_instance, memory_enabled, memory_source,
			runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9::uuid, $10, FALSE, 'authored',
			'{}'::jsonb, 'active', $11, $11)
	`, auditID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, entityID, agentFields.FlowInstancePath, at); err != nil {
		t.Fatalf("seed source conversation audit: %v", err)
	}
	capabilitySurfaceID := seedRunForkAgentTurnCapabilitySurface(t, ctx, pg, sourceRunID, turnID, sessionID, agentIdentity, "session_per_entity")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id,
			session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, execution_mode, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8,
			$9::uuid, $10, TRUE, 'authored', $11::uuid,
			$12::uuid, 'item.received', 'task-a', $13::uuid, 'live', $14)
	`, turnID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID,
		sessionID, agentFields.FlowInstancePath, entityID, sourceEventID, capabilitySurfaceID, at); err != nil {
		t.Fatalf("seed source turn: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	for _, code := range []string{
		runfork.RunForkBlockerSessionHistoryUnproven,
		runfork.RunForkBlockerConversationAuditUnproven,
		runfork.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
	}
	var copiedSessions, copiedAudits, copiedTurns int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, sessionID).Scan(&copiedSessions); err != nil {
		t.Fatalf("count session copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, auditID).Scan(&copiedAudits); err != nil {
		t.Fatalf("count audit copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid OR turn_id = $2::uuid
	`, result.Materialization.ForkRunID, turnID).Scan(&copiedTurns); err != nil {
		t.Fatalf("count turn copies: %v", err)
	}
	if copiedSessions != 1 || copiedAudits != 1 || copiedTurns != 1 {
		t.Fatalf("copied conversation rows sessions=%d audits=%d turns=%d, want source-only 1/1/1", copiedSessions, copiedAudits, copiedTurns)
	}
}

func TestExecuteSelectedContractRunForkAdmitsSameSourceActiveDeliveryForkPointEmission(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	forkPointEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002303, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	agentRoute := selectedExecutionTestAgentRoute(t, "validation-coordinator", "flow-a/1")
	sourceEvent := seedSelectedExecutionSourceRunWithRoutes(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, []events.DeliveryRoute{agentRoute}, loaded.BundleSourceFact)
	agentIdentity := agentRoute.AgentIdentity
	agentFields := selectedExecutionTestAgentFields(t, agentIdentity)
	seedSelectedExecutionTestAgent(t, ctx, db, agentIdentity, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'active', $10, $10)
	`, sessionID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath, at); err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, entity_id, flow_instance, memory_enabled, memory_source,
			runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9::uuid, $10, FALSE, 'authored',
			'{}'::jsonb, 'active', $11, $11)
	`, auditID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, entityID, agentFields.FlowInstancePath, at); err != nil {
		t.Fatalf("seed source conversation audit: %v", err)
	}
	capabilitySurfaceID := seedRunForkAgentTurnCapabilitySurface(t, ctx, pg, sourceRunID, turnID, sessionID, agentIdentity, "session_per_entity")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id,
			session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, execution_mode, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8,
			$9::uuid, $10, TRUE, 'authored', $11::uuid,
			$12::uuid, 'item.received', 'task-a', $13::uuid, 'live', $14)
	`, turnID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID,
		sessionID, agentFields.FlowInstancePath, entityID, sourceEventID, capabilitySurfaceID, at); err != nil {
		t.Fatalf("seed source turn: %v", err)
	}
	claimed, err := storetest.ClaimDelivery(ctx, pg, sourceEvent, agentRoute)
	if err != nil {
		t.Fatalf("claim in-progress source delivery: %v", err)
	}
	if claimed.Snapshot.ActiveSessionID != "" {
		t.Fatalf("in-progress source delivery active session = %q, want unbound #678 lineage case", claimed.Snapshot.ActiveSessionID)
	}
	storetest.InsertChildEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres, forkPointEventID, sourceRunID, sourceEventID,
		"item.received", eventtest.Producer(events.EventProducerAgent, "validation-coordinator"), []byte(`{}`),
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1"), forkAt)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  forkPointEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	for _, code := range []string{
		runfork.RunForkBlockerDeliveryHistoryUnproven,
		runfork.RunForkBlockerSessionHistoryUnproven,
		runfork.RunForkBlockerConversationAuditUnproven,
		runfork.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
	}
	if !result.Activation.SourceAdvancedAfterFork ||
		result.Activation.BranchDivergence == nil ||
		!containsString(result.Activation.BranchDivergence.SourceAdvancedFacts, runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification) {
		t.Fatalf("activation branch divergence = %#v, want #678 same-source active delivery fact", result.Activation.BranchDivergence)
	}

	var copiedSessions, copiedAudits, copiedTurns, copiedSourceSubscriberDeliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, sessionID).Scan(&copiedSessions); err != nil {
		t.Fatalf("count session copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, auditID).Scan(&copiedAudits); err != nil {
		t.Fatalf("count audit copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid OR turn_id = $2::uuid
	`, result.Materialization.ForkRunID, turnID).Scan(&copiedTurns); err != nil {
		t.Fatalf("count turn copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND subscriber_id = 'validation-coordinator'
		  AND status = 'in_progress'
	`, result.Materialization.ForkRunID).Scan(&copiedSourceSubscriberDeliveries); err != nil {
		t.Fatalf("count copied source delivery: %v", err)
	}
	if copiedSessions != 1 || copiedAudits != 1 || copiedTurns != 1 || copiedSourceSubscriberDeliveries != 0 {
		t.Fatalf("copied source rows sessions=%d audits=%d turns=%d sourceDeliveries=%d, want source-only conversation rows and no source delivery copies", copiedSessions, copiedAudits, copiedTurns, copiedSourceSubscriberDeliveries)
	}
}

func TestExecuteSelectedContractRunForkTreatsPostTSourceConversationHistoryAsBranchDivergence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002305, 0).UTC()
	after := at.Add(time.Minute)
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, loaded.BundleSourceFact)
	agentIdentity := selectedContractTestAgentIdentity(t, "agent-a", "flow-a/1")
	agentFields := selectedExecutionTestAgentFields(t, agentIdentity)
	seedSelectedExecutionTestAgent(t, ctx, db, agentIdentity, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'active', $10, $10)
	`, sessionID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, agentFields.FlowInstancePath, after); err != nil {
		t.Fatalf("seed post-T source session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, entity_id, flow_instance, memory_enabled, memory_source,
			runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9::uuid, $10, FALSE, 'authored',
			'{}'::jsonb, 'active', $11, $11)
	`, auditID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID, entityID, agentFields.FlowInstancePath, after); err != nil {
		t.Fatalf("seed post-T source conversation audit: %v", err)
	}
	capabilitySurfaceID := seedRunForkAgentTurnCapabilitySurface(t, ctx, pg, sourceRunID, turnID, sessionID, agentIdentity, "session_per_entity")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id,
			session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, capability_surface_id, execution_mode, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8,
			$9::uuid, $10, TRUE, 'authored', $11::uuid,
			$12::uuid, 'item.received', 'task-a', $13::uuid, 'live', $14)
	`, turnID, sourceRunID, agentFields.AgentID, agentFields.NameOwner, agentFields.NameSource,
		agentFields.RoutePresence, agentFields.FlowScopeKey, agentFields.FlowInstanceID,
		sessionID, agentFields.FlowInstancePath, entityID, sourceEventID, capabilitySurfaceID, after); err != nil {
		t.Fatalf("seed post-T source turn: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID,
		runforkrevision.FamilyAgentSessions,
		runforkrevision.FamilyAgentConversationAudits,
		runforkrevision.FamilyAgentTurns,
	)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	if !result.Activation.SourceAdvancedAfterFork || result.Activation.BranchDivergence == nil {
		t.Fatalf("activation = %#v, want source-advanced branch divergence", result.Activation)
	}
	for _, code := range []string{
		"source_sessions_advanced_after_fork_point",
		"source_conversation_audits_advanced_after_fork_point",
		"source_turns_advanced_after_fork_point",
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
		if !containsString(result.Activation.BranchDivergence.SourceAdvancedFacts, code) {
			t.Fatalf("branch facts = %#v, want %s", result.Activation.BranchDivergence.SourceAdvancedFacts, code)
		}
	}
	for _, code := range []string{
		runfork.RunForkBlockerSessionHistoryUnproven,
		runfork.RunForkBlockerConversationAuditUnproven,
		runfork.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained old conversation-history blocker %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
	}

	var copiedSessions, copiedAudits, copiedTurns int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, sessionID).Scan(&copiedSessions); err != nil {
		t.Fatalf("count copied source sessions: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, auditID).Scan(&copiedAudits); err != nil {
		t.Fatalf("count copied source audits: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid OR turn_id = $2::uuid
	`, result.Materialization.ForkRunID, turnID).Scan(&copiedTurns); err != nil {
		t.Fatalf("count copied source turns: %v", err)
	}
	if copiedSessions != 1 || copiedAudits != 1 || copiedTurns != 1 {
		t.Fatalf("copied post-T conversation rows sessions=%d audits=%d turns=%d, want source-only 1/1/1", copiedSessions, copiedAudits, copiedTurns)
	}
}

func TestExecuteSelectedContractRunForkTreatsSourceReplayScopeMarkerAsLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002315, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, loaded.BundleSourceFact)
	seedSelectedExecutionSourceReplayScopeMarker(t, db, sourceRunID, sourceEventID, "replay_scope_subscribed", at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	if selectedExecutionResultHasBlocker(result, runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported) {
		t.Fatalf("selected execution retained committed replay-scope blocker: materialization=%#v activation=%#v", result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
	}

	var copiedSourceMarkers int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM committed_replay_scopes
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&copiedSourceMarkers); err != nil {
		t.Fatalf("count copied source replay-scope facts: %v", err)
	}
	if copiedSourceMarkers != 0 {
		t.Fatalf("copied source replay-scope facts into fork = %d, want 0", copiedSourceMarkers)
	}
	var forkLocalMarkers int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM committed_replay_scopes
		WHERE run_id = $1::uuid
		  AND event_id <> $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&forkLocalMarkers); err != nil {
		t.Fatalf("count fork-local replay-scope facts: %v", err)
	}
	if forkLocalMarkers == 0 {
		t.Fatalf("fork-local replay-scope fact missing for selected execution result")
	}
}

func TestExecuteSelectedContractRunForkRejectsSameEventReplayScopeWriteSkew(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002320, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, loaded.BundleSourceFact)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	if _, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION seed_conflicting_replay_scope_after_event_insert()
		RETURNS trigger AS $$
		BEGIN
			INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
			VALUES (NEW.event_id, NEW.run_id, 'direct', NEW.created_at, NEW.created_at)
			ON CONFLICT (event_id) DO NOTHING;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER seed_conflicting_replay_scope_after_event_insert
		AFTER INSERT ON events
		FOR EACH ROW EXECUTE FUNCTION seed_conflicting_replay_scope_after_event_insert();
	`); err != nil {
		t.Fatalf("install conflicting replay-scope trigger: %v", err)
	}

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil || !strings.Contains(err.Error(), "committed pipeline scope conflicts") {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want atomic same-event replay-scope conflict", err)
	}
	if result.Activation.Activated {
		t.Fatalf("activation = %#v, want rejection before activation", result.Activation)
	}
	assertSelectedContractExecutionCleanup(t, db, sourceRunID, result.Materialization.ForkRunID)
}

func TestExecuteSelectedContractRunForkRejectsUnresolvedFrontierBeforeMaterialization(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002325, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "ghost.event", at, loaded.BundleSourceFact)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want unresolved frontier blocker", err)
	}
	if result.Materialization.ForkRunID != "" || result.ExecutedEventCount != 0 {
		t.Fatalf("result mutated before unresolved frontier rejection: %#v", result)
	}

	var forkRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`, sourceRunID).Scan(&forkRows); err != nil {
		t.Fatalf("count fork rows: %v", err)
	}
	if forkRows != 0 {
		t.Fatalf("fork rows after unresolved frontier rejection = %d, want 0", forkRows)
	}
}

func TestExecuteSelectedContractRunForkCleansUpBeforeActivationOnPublishFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION fail_selected_contract_execution_lineage_insert()
		RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'forced selected execution lineage failure';
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER fail_selected_contract_execution_lineage_insert
		BEFORE INSERT ON run_fork_selected_contract_executions
		FOR EACH ROW EXECUTE FUNCTION fail_selected_contract_execution_lineage_insert();
	`); err != nil {
		t.Fatalf("install lineage failure trigger: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002335, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, loaded.BundleSourceFact)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil || !strings.Contains(err.Error(), "forced selected execution lineage failure") {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want forced lineage publish failure", err)
	}
	if result.Materialization.ForkRunID == "" {
		t.Fatalf("expected materialization before publish failure, got %#v", result.Materialization)
	}
	if result.Activation.SourceFrozen || result.Activation.ForkRunStatus == runfork.RunForkActivatedStatus {
		t.Fatalf("activation mutated before publish failure cleanup: %#v", result.Activation)
	}

	assertSelectedContractExecutionCleanup(t, db, sourceRunID, result.Materialization.ForkRunID)
}

func TestExecuteSelectedContractRunForkBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	ctx := runForkTestContext(t)
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, loaded.BundleSourceFact)
	ctx = runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.BundleSourceFact.BundleHash()),
	)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	afterEventID := uuid.NewString()
	at := time.Unix(1700002350, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at, loaded.BundleSourceFact)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	seedSelectedExecutionDiagnosticPlatformDeadLetter(t, db, sourceRunID, afterEventID, at.Add(time.Second))
	if _, _, err := storetest.TerminalizeRun(ctx, pg, storerunlifecycle.TerminalRequest{
		RunID:   sourceRunID,
		State:   storerunlifecycle.StateCancelled,
		EndedAt: at.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("mark source cancelled after fork point: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID,
		runforkrevision.FamilyEvents,
		runforkrevision.FamilyEventDeliveries,
		runforkrevision.FamilyEventReceipts,
		runforkrevision.FamilyDeadLetters,
	)

	result, err := executeLiveSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  sourceEventID,
		ConfirmSourceFreeze: true,
		Owner:               selectedContractExecutionOwnerForTest(t, pg),
		SourceLoader:        loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if !result.Activation.Activated || result.Activation.ForkRunStatus != runfork.RunForkActivatedStatus {
		t.Fatalf("activation = %#v, want activated fork", result.Activation)
	}
	if result.Activation.SourceFrozen || !result.Activation.SourceAdvancedAfterFork {
		t.Fatalf("branch activation flags = frozen:%v advanced:%v", result.Activation.SourceFrozen, result.Activation.SourceAdvancedAfterFork)
	}
	if result.Activation.BranchDivergence == nil {
		t.Fatalf("branch divergence missing from result: %#v", result.Activation)
	}
	if result.Activation.BranchDivergence.Owner != runfork.RunForkSelectedContractBranchDivergenceOwner ||
		result.Activation.BranchDivergence.Policy != runfork.RunForkSelectedContractSourceAdvancedBranchPolicy ||
		result.Activation.BranchDivergence.SourceFrozen {
		t.Fatalf("branch divergence = %#v", result.Activation.BranchDivergence)
	}
	for _, fact := range []string{
		"source_events_advanced_after_fork_point",
		"source_run_terminal_at_activation",
		"source_receipts_advanced_after_fork_point",
	} {
		if !containsString(result.Activation.BranchDivergence.SourceAdvancedFacts, fact) {
			t.Fatalf("branch facts = %#v, want %s", result.Activation.BranchDivergence.SourceAdvancedFacts, fact)
		}
	}

	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, result.Materialization.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "cancelled" || forkStatus != runfork.RunForkActivatedStatus {
		t.Fatalf("branch statuses source/fork = %s/%s, want cancelled/running", sourceStatus, forkStatus)
	}

	var branchRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND fork_event_id = $3::uuid
		  AND policy = $4
		  AND source_frozen = false
		  AND source_run_status_at_activation = 'cancelled'
		  AND source_run_status_after_activation = 'cancelled'
		  AND source_advanced_facts @> ARRAY[
				'source_events_advanced_after_fork_point',
				'source_run_terminal_at_activation',
				'source_receipts_advanced_after_fork_point'
		  ]::text[]
	`, result.Materialization.ForkRunID, sourceRunID, sourceEventID, runfork.RunForkSelectedContractSourceAdvancedBranchPolicy).Scan(&branchRows); err != nil {
		t.Fatalf("count branch divergence rows: %v", err)
	}
	if branchRows != 1 {
		t.Fatalf("branch divergence rows = %d, want 1", branchRows)
	}

	forkEventID := result.ForkEvents[0].ForkEventID
	var copiedPostTEvents, copiedPostTDeliveries, forkReceipts, emittedFollowUps int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_id = $2::uuid`, result.Materialization.ForkRunID, afterEventID).Scan(&copiedPostTEvents); err != nil {
		t.Fatalf("count copied post-T source event: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND event_id = $2::uuid`, result.Materialization.ForkRunID, afterEventID).Scan(&copiedPostTDeliveries); err != nil {
		t.Fatalf("count copied post-T source delivery: %v", err)
	}
	if copiedPostTEvents != 0 || copiedPostTDeliveries != 0 {
		t.Fatalf("copied post-T source rows into fork events=%d deliveries=%d", copiedPostTEvents, copiedPostTDeliveries)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&forkReceipts); err != nil {
		t.Fatalf("count fork receipts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'item.processed'
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, forkEventID).Scan(&emittedFollowUps); err != nil {
		t.Fatalf("count branch follow-ups: %v", err)
	}
	if forkReceipts == 0 || emittedFollowUps != 1 {
		t.Fatalf("branch fork-local outcomes receipts=%d followUps=%d, want receipts and one follow-up", forkReceipts, emittedFollowUps)
	}
}

func TestSelectedContractRecipientPlanPublishGuardAuthorizesCanonicalPlan(t *testing.T) {
	frontier := testContractFrontierAdmission(testContractSelection())
	sourceEventID := frontier.FrontierEvents[0].SourceEventID
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning, nil)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", sourceEventID)

	err = guard.AuthorizeEvent(context.Background(), selectedContractGuardEvent(t, "fork-event",
		"work.begin", runfork.RunForkSelectedContractExecutionOwner, sourceEventID))
	if err != nil {
		t.Fatalf("AuthorizeEvent canonical recipient plan: %v", err)
	}

	err = guard.Authorize(context.Background(), selectedContractGuardEvent(t, "fork-event",
		"work.begin", runfork.RunForkSelectedContractExecutionOwner, sourceEventID),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          mustRunForkNode("flow-a", "alpha-intake").Key(),
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
		})
	if err != nil {
		t.Fatalf("Authorize canonical recipient plan: %v", err)
	}
}

func TestSelectedContractRecipientPlanPublishGuardScopesPathDriftToFreshCreateProjection(t *testing.T) {
	planning := runfork.RunForkSelectedContractRecipientPlanning{
		Owner:                      runfork.RunForkSelectedContractRecipientPlanningOwner,
		FutureExecutionOwner:       runfork.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		RecipientPlanningSupported: true,
		DeliveryWritesSupported:    false,
		RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "source-event",
			EventName:     "validation.requested",
			Recipients: []runfork.RunForkContractFrontierRecipient{
				testNodeFrontierRecipient("validator-node", "validator/source-instance", "canonical_connect"),
			},
			Disposition: runfork.RunForkSelectedContractDispositionForkLocalTruth,
		}},
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning, nil)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", "source-event")
	evt := selectedContractGuardEvent(t, "fork-event",
		"validation.requested", runfork.RunForkSelectedContractExecutionOwner, "source-event")
	projection, err := events.NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "fork-case"})
	if err != nil {
		t.Fatalf("NewDeliveryPayloadProjection: %v", err)
	}
	base := bus.PublishRecipientPlan{
		RoutedRecipients: []bus.PublishDiagnosticRecipient{{
			Type:        "node",
			ID:          mustRunForkNode("validator", "validator-node").Key(),
			Path:        "validator/fork-instance",
			RouteSource: "canonical_connect",
		}},
	}

	tests := []struct {
		name    string
		routes  []events.DeliveryRoute
		wantErr bool
	}{
		{
			name: "create fresh projected route accepts fork-local path",
			routes: []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(mustRunForkNode("validator", "validator-node")), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/fork-instance", EntityID: "fork-case"}),
				PayloadProjection: projection,
			}},
		},
		{
			name:    "select canonical path drift is rejected",
			routes:  []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(mustRunForkNode("validator", "validator-node")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/fork-instance", EntityID: "fork-case"})}},
			wantErr: true,
		},
		{
			name:    "select-or-create canonical path drift is rejected",
			routes:  []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(mustRunForkNode("validator", "validator-node")), Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/fork-instance", EntityID: "fork-case"})}},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := base
			actual.DeliveryRoutes = tc.routes
			err := guard.Authorize(context.Background(), evt, actual)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "routed recipients do not match") {
					t.Fatalf("Authorize error = %v, want concrete-path mismatch", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Authorize fresh create projection: %v", err)
			}
		})
	}
}

func TestSelectedContractRecipientPlanPublishGuardMaterializesTargetNodeDeliveryRoutes(t *testing.T) {
	node := runtimecontracts.SystemNodeContract{
		ID: "test-node", ExecutionType: "system_node", SubscribesTo: []string{"item.received"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"item.received": {}},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "selected-workflow", Version: "v1"},
		Nodes:     map[string]runtimecontracts.SystemNodeContract{"test-node": node},
		Events:    map[string]runtimecontracts.EventCatalogEntry{"item.received": {}},
	})
	planning := runfork.RunForkSelectedContractRecipientPlanning{
		Owner:                      runfork.RunForkSelectedContractRecipientPlanningOwner,
		FutureExecutionOwner:       runfork.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		RecipientPlanningSupported: true,
		DeliveryWritesSupported:    false,
		RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "source-event",
			EventName:     "item.received",
			Recipients: []runfork.RunForkContractFrontierRecipient{
				testAgentFrontierRecipient("target-agent", "", "selected_contracts", agentidentity.Identity{}),
				testNodeFrontierRecipient("test-node", "", "selected_contracts"),
			},
			Disposition: runfork.RunForkSelectedContractDispositionForkLocalTruth,
		}},
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning, source)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", "source-event")

	routes, err := guard.MaterializeNodeDeliveryRoutes(context.Background(), selectedContractGuardEvent(t, "fork-event",
		"item.received", runfork.RunForkSelectedContractExecutionOwner, "source-event"),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{
				{
					Type:        "agent",
					ID:          "target-agent",
					RouteSource: "selected_contracts",
				},
				{
					Type:        "node",
					ID:          mustRunForkRootNode("test-node").Key(),
					RouteSource: "selected_contracts",
				},
			},
		})
	if err != nil {
		t.Fatalf("MaterializeNodeDeliveryRoutes: %v", err)
	}
	if len(routes) != 1 ||
		!routes[0].Recipient.IsNode() ||
		routes[0].Recipient.ID() != mustRunForkRootNode("test-node").Key() ||
		!routes[0].Target.Empty() ||
		routes[0].Handler.Empty() ||
		!routes[0].Handler.Node().Equal(mustRunForkRootNode("test-node")) {
		t.Fatalf("materialized routes = %#v, want target node route only", routes)
	}
}

func TestSelectedContractRecipientPlanPublishGuardAuthorizesContractSwapOwner(t *testing.T) {
	frontier := testContractFrontierAdmission(testContractSelection())
	sourceEventID := frontier.FrontierEvents[0].SourceEventID
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning, nil, runfork.RunForkHistoricalReplayContractSwapBootResumeOwner)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", sourceEventID)

	err = guard.Authorize(context.Background(), selectedContractGuardEvent(t, "fork-event",
		"work.begin", runfork.RunForkHistoricalReplayContractSwapBootResumeOwner, sourceEventID),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          mustRunForkNode("flow-a", "alpha-intake").Key(),
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
		})
	if err != nil {
		t.Fatalf("Authorize contract-swap owner recipient plan: %v", err)
	}
}

func TestSelectedContractRecipientPlanPublishGuardRejectsBypassAndSubscriptions(t *testing.T) {
	frontier := testContractFrontierAdmission(testContractSelection())
	sourceEventID := frontier.FrontierEvents[0].SourceEventID
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning, nil)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}

	err = guard.Authorize(context.Background(), selectedContractGuardEvent(t, "fork-event",
		"work.begin", runfork.RunForkSelectedContractExecutionOwner, sourceEventID),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          mustRunForkNode("flow-a", "alpha-intake").Key(),
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
		})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkSelectedContractRecipientPlanningOwner) {
		t.Fatalf("Authorize without expectation error = %v, want recipient-planning evidence failure", err)
	}

	guard.ExpectForkEvent("fork-event-subscription", sourceEventID)
	err = guard.Authorize(context.Background(), selectedContractGuardEvent(t, "fork-event-subscription",
		"work.begin", runfork.RunForkSelectedContractExecutionOwner, sourceEventID),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          mustRunForkNode("flow-a", "alpha-intake").Key(),
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
			SubscriptionRecipients: []string{"legacy-subscription"},
		})
	if err == nil || !strings.Contains(err.Error(), "live subscription") {
		t.Fatalf("Authorize subscription recipient error = %v, want live subscription rejection", err)
	}

	guard.ExpectForkEvent("fork-event-wrong-recipient", sourceEventID)
	err = guard.Authorize(context.Background(), selectedContractGuardEvent(t, "fork-event-wrong-recipient",
		"work.begin", runfork.RunForkSelectedContractExecutionOwner, sourceEventID),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          mustRunForkNode("flow-a", "other-node").Key(),
				Path:        "flow-a/other-node",
				RouteSource: "selected_contracts",
			}},
		})
	if err == nil || !strings.Contains(err.Error(), "routed recipients do not match") {
		t.Fatalf("Authorize wrong recipient error = %v, want recipient-plan mismatch", err)
	}
}

func selectedContractGuardEvent(t *testing.T, eventID string, eventType events.EventType, producerID, sourceEventID string) events.Event {
	t.Helper()
	lineage, err := events.NewSelectedForkLineage(
		"fork-run",
		"source-run",
		sourceEventID,
		"selected-contract-test",
		"",
		executionmode.Live,
	)
	if err != nil {
		t.Fatalf("NewSelectedForkLineage: %v", err)
	}
	return eventtest.SelectedForkReplay(
		eventID,
		eventType,
		eventtest.Producer(events.EventProducerPlatform, producerID),
		"",
		nil,
		0,
		lineage,
		events.EventEnvelope{},
		time.Time{},
	)
}

func assertSelectedContractRuntimeContainerProof(t *testing.T, proof *SelectedContractForkLocalRuntimeContainer, executionOwner, sourceRunID, forkRunID, forkEventID string, sourceEventIDs []string) {
	t.Helper()
	if proof == nil {
		t.Fatal("fork-local runtime container proof missing")
	}
	if proof.Owner != runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner ||
		proof.ExecutionOwner != executionOwner ||
		proof.SourceRunID != sourceRunID ||
		proof.ForkRunID != forkRunID ||
		proof.ForkEventID != forkEventID {
		t.Fatalf("runtime container identity = %#v", proof)
	}
	if proof.RecipientPlanningOwner != runfork.RunForkSelectedContractRecipientPlanningOwner ||
		proof.DeferredWorkAdmissionOwner != runfork.RunForkSelectedContractDeferredWorkAdmissionOwner ||
		proof.AuthoritativeAgentDeliveryMaterializationOwner != runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner ||
		proof.RuntimePlatformEventLineagePolicyOwner != runfork.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner ||
		proof.TypedRuntimeLineageOwner != runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner ||
		proof.RouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner ||
		proof.ActivationGateOwner != runfork.RunForkSelectedContractExecutionActivationGateOwner {
		t.Fatalf("runtime container owner consumption = %#v", proof)
	}
	if !proof.EventBusRecipientPlanGuard ||
		!proof.RuntimeActiveAgentDescriptorsEphemeral ||
		!proof.EphemeralAgentRuntime ||
		!proof.QuiescenceRequired ||
		!proof.CleanupRequired {
		t.Fatalf("runtime container lifecycle proof = %#v", proof)
	}
	for _, sourceEventID := range sourceEventIDs {
		if !containsString(proof.SourceEventIDs, sourceEventID) {
			t.Fatalf("runtime container source events = %#v, want %s", proof.SourceEventIDs, sourceEventID)
		}
	}
	if !executionBoundaryHas(proof.InvalidPaths, "source_row_copy_as_execution_truth", runfork.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("runtime container invalid paths = %#v, want source-row-copy invalid", proof.InvalidPaths)
	}
	if executionBoundaryHas(proof.SplitSiblings, "typed_runtime_lineage", runfork.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("runtime container split siblings = %#v, typed lineage should be implemented by #708", proof.SplitSiblings)
	}
}

func assertSelectedContractExecutionCleanup(t *testing.T, db *sql.DB, sourceRunID, forkRunID string) {
	t.Helper()
	ctx := context.Background()
	var sourceStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if sourceStatus != "running" {
		t.Fatalf("source status = %q, want running", sourceStatus)
	}
	var forkRows, forkEvents, forkDeliveries, forkReceipts, forkState, forkMutations, bindingRows, lineageRows, routeRecoveryRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`, sourceRunID).Scan(&forkRows); err != nil {
		t.Fatalf("count fork rows: %v", err)
	}
	if forkRunID != "" {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, forkRunID).Scan(&forkEvents); err != nil {
			t.Fatalf("count fork events: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`, forkRunID).Scan(&forkDeliveries); err != nil {
			t.Fatalf("count fork deliveries: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts`).Scan(&forkReceipts); err != nil {
			t.Fatalf("count event receipts after cleanup: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid`, forkRunID).Scan(&forkState); err != nil {
			t.Fatalf("count fork state: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`, forkRunID).Scan(&forkMutations); err != nil {
			t.Fatalf("count fork mutations: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`, forkRunID).Scan(&bindingRows); err != nil {
			t.Fatalf("count fork binding: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id = $1::uuid`, forkRunID).Scan(&lineageRows); err != nil {
			t.Fatalf("count fork lineage: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1::uuid`, forkRunID).Scan(&routeRecoveryRows); err != nil {
			t.Fatalf("count fork route recoveries: %v", err)
		}
	}
	var forkStatus string
	if forkRunID != "" {
		if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, forkRunID).Scan(&forkStatus); err != nil {
			t.Fatalf("load retained fork tombstone: %v", err)
		}
	}
	if forkRows != 1 || forkStatus != "cancelled" || forkEvents != 0 || forkDeliveries != 0 || forkReceipts != 0 || forkState != 0 || forkMutations != 0 || bindingRows != 1 || lineageRows != 0 || routeRecoveryRows != 0 {
		t.Fatalf("cleanup left fork rows runs:%d events:%d deliveries:%d receipts:%d state:%d mutations:%d bindings:%d lineage:%d route_recoveries:%d",
			forkRows, forkEvents, forkDeliveries, forkReceipts, forkState, forkMutations, bindingRows, lineageRows, routeRecoveryRows)
	}
}

type selectedContractForkTestAgent struct {
	mu       sync.Mutex
	cfg      runtimeactors.AgentConfig
	runIDs   []string
	eventIDs []string
}

type selectedForkDiscardSpendProjection struct{}

func (selectedForkDiscardSpendProjection) ProjectCommittedCompletionSpend(context.Context, runtimeeffects.CompletionSpendProjection) {
}

func (a *selectedContractForkTestAgent) Configure(cfg runtimeactors.AgentConfig) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cfg = cfg
}

func (a *selectedContractForkTestAgent) ID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.ID
}

func (a *selectedContractForkTestAgent) Type() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.Type
}

func (a *selectedContractForkTestAgent) Subscriptions() []events.EventType {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]events.EventType, 0, len(a.cfg.Subscriptions))
	for _, raw := range a.cfg.Subscriptions {
		if eventType := strings.TrimSpace(raw); eventType != "" {
			out = append(out, events.EventType(eventType))
		}
	}
	return out
}

func (a *selectedContractForkTestAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.mu.Lock()
	a.runIDs = append(a.runIDs, strings.TrimSpace(evt.RunID()))
	a.eventIDs = append(a.eventIDs, strings.TrimSpace(evt.ID()))
	agentID := strings.TrimSpace(a.cfg.ID)
	a.mu.Unlock()

	return []events.Event{
		eventtest.Child("", events.EventType("task.completed"), agentID, "", json.RawMessage(`{}`), 0, evt, evt.NormalizedEnvelope(), time.Now().UTC()),
	}, nil
}

func (a *selectedContractForkTestAgent) SeenRunIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.runIDs...)
}

func (a *selectedContractForkTestAgent) SeenEventIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.eventIDs...)
}

func runForkExecutionRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	return root
}

func captureSelectedExecutionSourceRevision(t *testing.T, db *sql.DB, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin selected execution source revision: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	revision, err := runforkrevision.Capture(context.Background(), tx, runID, families...)
	if err != nil {
		t.Fatalf("capture selected execution source revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit selected execution source revision: %v", err)
	}
	return revision
}

func materializeSelectedExecutionForkForTest(
	t *testing.T,
	ctx context.Context,
	pg *store.PostgresStore,
	loaded LoadedSelectedContractSource,
	selection runfork.RunForkContractSelection,
	sourceRunID string,
	sourceEventID string,
) runfork.RunForkMaterialization {
	t.Helper()
	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: sourceEventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: selection,
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan: plan, Source: loaded.Source, ContractSelection: selection, FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission: frontier, RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission: frontier, RouteAdmission: routeAdmission, RouteTopology: topology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, loaded.BundleSourceFact)
	workflowStates, err := selectedContractWorkflowStateProjection(plan, loaded.Source, *model.RecipientPlanning)
	if err != nil {
		t.Fatalf("selectedContractWorkflowStateProjection: %v", err)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID, At: sourceEventID, ContractSelection: selection, BundleSourceFact: loaded.BundleSourceFact,
		FrontierAdmission: frontier, RouteTopology: topology, RecipientPlanning: *model.RecipientPlanning, WorkflowStates: workflowStates,
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	return materialized
}

func seedSelectedExecutionSourceRun(
	t *testing.T,
	db *sql.DB,
	sourceRunID, entityID, sourceEventID, eventName string,
	at time.Time,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) {
	seedSelectedExecutionSourceRunWithRoutes(t, db, sourceRunID, entityID, sourceEventID, eventName, at, nil, sourceFacts...)
}

func selectedExecutionTestAgentRoute(t testing.TB, agentID, flowInstance string) events.DeliveryRoute {
	t.Helper()
	return events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: selectedContractTestAgentIdentity(t, agentID, flowInstance)}
}

func selectedExecutionTestAgentFields(t testing.TB, identity agentidentity.Identity) agentidentity.StorageFields {
	t.Helper()
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("selected-execution test agent identity: %v", err)
	}
	return fields
}

func seedSelectedExecutionTestAgent(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	identity agentidentity.Identity,
	at time.Time,
) {
	t.Helper()
	fields := selectedExecutionTestAgentFields(t, identity)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance,
			role, model, memory_enabled, memory_source, status, created_at,
			topology_authority_kind, topology_admission, execution_lifetime
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'test-agent', 'tier1', TRUE, 'authored', 'active', $8,
			'static_declaration_plan', '{"authority":{"kind":"static_declaration_plan","static_declaration_plan":{"source_set_revision":"test-source-set-v1","bundle_hash":"bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bundle_source":"ephemeral"}},"execution_lifetime":"durable_managed"}'::jsonb, 'durable_managed')
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, at); err != nil {
		t.Fatalf("seed selected-execution test agent: %v", err)
	}
}

func seedSelectedExecutionStateOnlySourceRun(
	t *testing.T,
	db *sql.DB,
	sourceRunID, sourceEventID, eventName string,
	at time.Time,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) {
	t.Helper()
	ctx := runForkTestContext(t)
	sourceFact := testEphemeralBundleSourceFact(runForkTestBundleHash)
	if len(sourceFacts) > 0 {
		sourceFact = sourceFacts[0]
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
	ctx = runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, sourceFact.BundleHash()),
	)
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
		RunID: sourceRunID, StartedAt: at.Add(-time.Minute), Source: sourceFact,
	})
	event := eventtest.ExistingRunRootIngress(
		sourceEventID,
		events.EventType(eventName),
		"source-runtime",
		"",
		json.RawMessage(`{}`),
		0,
		sourceRunID,
		events.EventEnvelope{Scope: events.EventScopeGlobal},
		at,
	)
	commitRunForkTestEvent(t, ctx, storetest.AdmitPostgresRuntimeStore(t, db), event, nil)
}

func seedSelectedExecutionSourceRunWithRoutes(
	t *testing.T,
	db *sql.DB,
	sourceRunID, entityID, sourceEventID, eventName string,
	at time.Time,
	extraRoutes []events.DeliveryRoute,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) events.Event {
	return seedSelectedExecutionSourceRunWithPrimaryRoute(t, db, sourceRunID, entityID, sourceEventID, eventName, at,
		selectedExecutionEntitylessNodeRoute("test-node"), extraRoutes, sourceFacts...)
}

func selectedExecutionEntitylessNodeRoute(nodeID string) events.DeliveryRoute {
	return events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(mustRunForkRootNode(nodeID)),
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
			FlowID: "fixture", FlowInstance: "fixture/" + strings.TrimSpace(nodeID),
		}),
	}
}

func seedSelectedExecutionRootSourceRun(
	t *testing.T,
	db *sql.DB,
	sourceRunID, entityID, sourceEventID, eventName string,
	at time.Time,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) events.Event {
	agentRoute := selectedExecutionTestAgentRoute(t, "test-agent", "worker")
	agentRoute.Target = events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "worker", FlowInstance: "worker", EntityID: entityID,
	})
	event := seedSelectedExecutionSourceRunWithPrimaryRouteAndSource(
		t, db, sourceRunID, entityID, sourceEventID, eventName, at,
		agentRoute, nil,
		eventtest.RootRoutingSource(entityID), events.EventEnvelope{Scope: events.EventScopeGlobal}, sourceFacts...,
	)
	if _, err := db.ExecContext(runForkTestContext(t), `
		UPDATE entity_state
		SET flow_instance = 'worker'
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID); err != nil {
		t.Fatalf("bind selected source entity to worker: %v", err)
	}
	return event
}

func seedSelectedExecutionSourceRunWithPrimaryRoute(
	t *testing.T,
	db *sql.DB,
	sourceRunID, entityID, sourceEventID, eventName string,
	at time.Time,
	primaryRoute events.DeliveryRoute,
	extraRoutes []events.DeliveryRoute,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) events.Event {
	routingSource := eventtest.ConcreteTemplateRoutingSource("flow_a", "flow-a/1", entityID)
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1")
	return seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource(
		t, db, sourceRunID, entityID, sourceEventID, eventName, at,
		executionmode.Live, primaryRoute, extraRoutes, routingSource, envelope, sourceFacts...,
	)
}

func seedSelectedExecutionSourceRunWithPrimaryRouteAndMode(
	t *testing.T,
	db *sql.DB,
	sourceRunID, entityID, sourceEventID, eventName string,
	at time.Time,
	mode executionmode.Mode,
	primaryRoute events.DeliveryRoute,
	extraRoutes []events.DeliveryRoute,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) events.Event {
	routingSource := eventtest.ConcreteTemplateRoutingSource("flow_a", "flow-a/1", entityID)
	envelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1")
	return seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource(
		t, db, sourceRunID, entityID, sourceEventID, eventName, at,
		mode, primaryRoute, extraRoutes, routingSource, envelope, sourceFacts...,
	)
}

func seedSelectedExecutionSourceRunWithPrimaryRouteAndSource(
	t *testing.T,
	db *sql.DB,
	sourceRunID, entityID, sourceEventID, eventName string,
	at time.Time,
	primaryRoute events.DeliveryRoute,
	extraRoutes []events.DeliveryRoute,
	routingSource events.RoutingSource,
	envelope events.EventEnvelope,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) events.Event {
	return seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource(
		t, db, sourceRunID, entityID, sourceEventID, eventName, at,
		executionmode.Live, primaryRoute, extraRoutes, routingSource, envelope, sourceFacts...,
	)
}

func seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource(
	t *testing.T,
	db *sql.DB,
	sourceRunID, entityID, sourceEventID, eventName string,
	at time.Time,
	mode executionmode.Mode,
	primaryRoute events.DeliveryRoute,
	extraRoutes []events.DeliveryRoute,
	routingSource events.RoutingSource,
	envelope events.EventEnvelope,
	sourceFacts ...runtimecorrelation.BundleSourceFact,
) events.Event {
	t.Helper()
	ctx := runForkTestContext(t)
	sourceFact := testEphemeralBundleSourceFact(runForkTestBundleHash)
	if len(sourceFacts) > 0 {
		sourceFact = sourceFacts[0]
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
	ctx = runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, sourceFact.BundleHash()),
	)
	payload, _ := json.Marshal(map[string]any{"entity_id": entityID})
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),
		RunID: sourceRunID, StartedAt: at.Add(-time.Minute), Source: sourceFact,
	})
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(sourceEventID, events.EventType(eventName), "source-runtime", "", payload, 0, sourceRunID,
		envelope, routingSource, at, mode)
	routes := append([]events.DeliveryRoute{primaryRoute}, extraRoutes...)
	commitRunForkTestEvent(t, ctx, storetest.AdmitPostgresRuntimeStore(t, db), event, routes)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'lifecycle_state', '', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'selected-execution-test', 'seed', $4),
			($1::uuid, $2::uuid, 'authored_field', 'name', 'null'::jsonb, '"Selected Execution Entity"'::jsonb, $3::uuid, 'platform', 'selected-execution-test', 'seed', $4)
	`, sourceRunID, entityID, sourceEventID, at); err != nil {
		t.Fatalf("seed source mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Selected Execution Entity',
			'pending', '{}'::jsonb, '{"name":"Selected Execution Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
	return event
}

func seedSourceOutcomeThatMustNotSuppressFork(t *testing.T, db *sql.DB, sourceEventID, entityID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassConnectorFailure, "source_dead_letter", "run-fork-test", "seed", nil), "run-fork-test", "seed")
	failureRaw, err := json.Marshal(failure)
	if err != nil {
		t.Fatalf("marshal source dead-letter failure: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'platform', 'old-source-node', $2::uuid, 'flow-a/1', 'success', 'source_outcome_must_not_suppress_fork', '{}'::jsonb, $3)
	`, sourceEventID, entityID, at); err != nil {
		t.Fatalf("seed source receipt: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (
			original_event_id, original_event, entity_id, flow_instance, failure, handler_node, created_at
		)
		VALUES ($1::uuid, 'item.received', $2::uuid, 'flow-a/1', $3::jsonb, 'old-source-node', $4)
	`, sourceEventID, entityID, string(failureRaw), at); err != nil {
		t.Fatalf("seed source dead letter: %v", err)
	}
}

func seedSelectedExecutionDiagnosticPlatformDeadLetter(t *testing.T, db *sql.DB, sourceRunID, diagnosticEventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]any{
		"level":   "info",
		"message": "diagnostic platform row must remain lineage-only",
	})
	storetest.InsertDiagnosticDirectEventRecordForRun(t, ctx, db, authoractivityfixture.DialectPostgres, diagnosticEventID, sourceRunID, "", "runtime", payload, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance, outcome, reason_code, side_effects, processed_at
		)
		VALUES (
			$1::uuid, 'platform', 'pipeline', NULL, NULL,
			'dead_letter', 'runtime_log_pipeline_dead_letter', '{}'::jsonb, $2
		)
	`, diagnosticEventID, at); err != nil {
		t.Fatalf("seed diagnostic platform receipt: %v", err)
	}
}

func seedSelectedExecutionSourceReplayScopeMarker(t *testing.T, db *sql.DB, sourceRunID, sourceEventID, reasonCode string, at time.Time) {
	t.Helper()
	var scope runtimepipelineobligation.CommittedScope
	switch strings.TrimSpace(reasonCode) {
	case "replay_scope_direct":
		scope = runtimepipelineobligation.ScopeDirect
	case "replay_scope_subscribed":
		scope = runtimepipelineobligation.ScopeSubscribed
	default:
		t.Fatalf("unsupported replay-scope fixture reason %q", reasonCode)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, $4)
		ON CONFLICT (event_id) DO UPDATE
		SET created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at
		WHERE committed_replay_scopes.scope = EXCLUDED.scope
	`, sourceEventID, sourceRunID, string(scope), at); err != nil {
		t.Fatalf("seed source replay-scope marker: %v", err)
	}
}

func seedSelectedExecutionPostForkSourceEvent(t *testing.T, db *sql.DB, sourceRunID, sourceEventID, entityID string, at time.Time) {
	t.Helper()
	storetest.InsertExistingRunRootEventRecord(t, context.Background(), db, authoractivityfixture.DialectPostgres, sourceEventID, sourceRunID, "source.after",
		eventtest.Producer(events.EventProducerExternal, "source-runtime"), []byte(`{}`),
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1"), at)
}

func assertNoForkExecutionRowsForRun(t *testing.T, db *sql.DB, forkRunID string) {
	t.Helper()
	ctx := context.Background()
	for name, query := range map[string]string{
		"events":                                  `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`,
		"event_deliveries":                        `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`,
		"run_fork_selected_contract_executions":   `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id = $1::uuid`,
		"run_fork_selected_contract_divergences":  `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id = $1::uuid`,
		"run_fork_selected_contract_route_rows":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1::uuid`,
		"run_fork_delivery_event_replay_lineages": `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, forkRunID).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s rows for blocked selected fork = %d, want 0", name, count)
		}
	}
}

func assertNoSelectedContractExecutionMutationForSource(t *testing.T, db *sql.DB, sourceRunID, sourceEventID string) {
	t.Helper()
	ctx := context.Background()
	for name, query := range map[string]string{
		"fork_runs":                            `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`,
		"selected_contract_bindings":           `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE source_run_id = $1::uuid`,
		"selected_contract_executions":         `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE source_run_id = $1::uuid`,
		"selected_contract_branch_divergences": `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE source_run_id = $1::uuid`,
		"selected_contract_route_recoveries":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE source_run_id = $1::uuid`,
		"delivery_event_replays":               `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE source_run_id = $1::uuid`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, sourceRunID).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s rows for blocked selected fork source = %d, want 0", name, count)
		}
	}

	var forkEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE source_event_id = $1::uuid
		  AND run_id <> $2::uuid
	`, sourceEventID, sourceRunID).Scan(&forkEvents); err != nil {
		t.Fatalf("count fork events: %v", err)
	}
	if forkEvents != 0 {
		t.Fatalf("fork event rows for blocked selected fork source event = %d, want 0", forkEvents)
	}
}

func assertNoCopiedSourceReplayScopeMarkers(t *testing.T, db *sql.DB, forkRunID, sourceEventID string) {
	t.Helper()
	var copied int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM committed_replay_scopes
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, forkRunID, sourceEventID).Scan(&copied); err != nil {
		t.Fatalf("count copied source replay-scope facts: %v", err)
	}
	if copied != 0 {
		t.Fatalf("copied source replay-scope facts = %d, want 0", copied)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func selectedExecutionResultHasBlocker(result SelectedContractExecutionResult, code string) bool {
	for _, blocker := range result.Materialization.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	for _, blocker := range result.Activation.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func TestSelectedContractForkEventPreservesSourceExecutionMode(t *testing.T) {
	sourceRunID, forkRunID, sourceEventID, forkEventID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	evt, err := selectedContractForkEvent(sourceRunID, forkRunID, forkEventID, runfork.RunForkSelectedContractSourceEvent{
		SourceEventID: sourceEventID,
		EventName:     "task.started",
		ExecutionMode: runtimeeffects.ExecutionModeMock,
		Payload:       json.RawMessage(`{"ok":true}`),
	}, "selected-contract")
	if err != nil {
		t.Fatalf("selectedContractForkEvent: %v", err)
	}
	if evt.ExecutionMode() != runtimeeffects.ExecutionModeMock {
		t.Fatalf("fork event execution mode = %q, want mock", evt.ExecutionMode())
	}
}

func commitRunForkTestEvent(t testing.TB, ctx context.Context, pg *store.PostgresStore, event events.Event, routes []events.DeliveryRoute) {
	t.Helper()
	storetest.CommitSemanticEventWithRoutes(t, ctx, pg, event, routes, runtimepipelineobligation.ScopeSubscribed)
}
