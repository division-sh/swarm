package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeexecutionmode "github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type lifecycleSourceSetRebindStore interface {
	agentfixture.Store
	runtimemanager.AgentLifecycleCellCensus
	runtimemanager.AgentLifecycleStateReader
}

type lifecycleCensusDriftStore interface {
	lifecycleSourceSetRebindStore
	runtimestartupownership.Store
}

func TestAgentLifecycleSourceSetRebindParity(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		proveAgentLifecycleSourceSetRebind(t, newBootstrappedSQLiteRuntimeStoreForTest(t))
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		proveAgentLifecycleSourceSetRebind(t, admitTestPostgresStore(t, db))
	})
}

func TestAgentLifecycleProcessBindingReadbackParity(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		proveAgentLifecycleProcessBindingReadback(t, newBootstrappedSQLiteRuntimeStoreForTest(t))
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		proveAgentLifecycleProcessBindingReadback(t, admitTestPostgresStore(t, db))
	})
}

func TestAgentLifecycleCensusRejectsCanonicalAdmissionDriftParity(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, drift := range []string{"json execution lifetime", "canonical authority kind"} {
			t.Run(backend+"/"+drift, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				var store lifecycleCensusDriftStore
				var db *sql.DB
				postgres := backend == "postgres"
				if postgres {
					_, db, _ = testutil.StartPostgres(t)
					store = admitTestPostgresStore(t, db)
				} else {
					selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
					store = selected
					db = selected.backend.ConstructionHandle()
				}
				identity := testAgentIdentity(t, "census-drift-agent", "")
				requireRunningRunForTest(t, ctx, store, identity.RunID, time.Now().UTC())
				seedTestAgentRow(t, ctx, db, postgres, identity, "active")
				fields := testAgentIdentityStorageFields(t, identity)
				identityArgs := []any{
					fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
					fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
				}

				switch drift {
				case "json execution lifetime":
					admission := testAgentTopologyAdmission(t)
					admission.Lifetime = runtimeagenttopology.LifetimeEphemeral
					raw, err := canonicaljson.Bytes(admission)
					if err != nil {
						t.Fatalf("encode drifted topology admission: %v", err)
					}
					if postgres {
						_, err = db.ExecContext(ctx, `UPDATE agents SET topology_admission=$1::jsonb WHERE run_id=$2::uuid AND agent_id=$3 AND agent_name_owner=$4 AND agent_name_source=$5 AND agent_route_presence=$6 AND flow_scope_key=$7 AND flow_instance_id=$8 AND flow_instance=$9`, append([]any{string(raw)}, identityArgs...)...)
					} else {
						_, err = db.ExecContext(ctx, `UPDATE agents SET topology_admission=? WHERE run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=?`, append([]any{string(raw)}, identityArgs...)...)
					}
					if err != nil {
						t.Fatalf("persist execution-lifetime drift: %v", err)
					}
				case "canonical authority kind":
					query := `UPDATE agents SET topology_authority_kind='flow_readiness_plan' WHERE run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=?`
					if postgres {
						query = `UPDATE agents SET topology_authority_kind='flow_readiness_plan' WHERE run_id=$1::uuid AND agent_id=$2 AND agent_name_owner=$3 AND agent_name_source=$4 AND agent_route_presence=$5 AND flow_scope_key=$6 AND flow_instance_id=$7 AND flow_instance=$8`
					}
					if _, err := db.ExecContext(ctx, query, identityArgs...); err != nil {
						t.Fatalf("persist authority-kind drift: %v", err)
					}
				}

				var predecessorBinding string
				bindingQuery := `SELECT lifecycle_process_authority_id FROM agents WHERE run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=?`
				if postgres {
					bindingQuery = `SELECT lifecycle_process_authority_id::text FROM agents WHERE run_id=$1::uuid AND agent_id=$2 AND agent_name_owner=$3 AND agent_name_source=$4 AND agent_route_presence=$5 AND flow_scope_key=$6 AND flow_instance_id=$7 AND flow_instance=$8`
				}
				if err := db.QueryRowContext(ctx, bindingQuery, identityArgs...).Scan(&predecessorBinding); err != nil {
					t.Fatalf("read predecessor lifecycle binding: %v", err)
				}

				request := testStartupAcquireRequest("census-drift-successor")
				capability, err := store.AcquireProcessCapability(ctx, request)
				if err != nil {
					t.Fatalf("acquire census-drift capability: %v", err)
				}
				t.Cleanup(func() { _ = capability.Release(context.Background()) })
				plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{{
					BundleHash: testAgentTopologyBundleHash, BundleSource: "ephemeral",
				}}, nil)
				if err != nil {
					t.Fatalf("construct census-drift source set: %v", err)
				}
				if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
					t.Fatalf("install census-drift source set: %v", err)
				}
				grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
					BundleHash: testAgentTopologyBundleHash, BundleSource: "ephemeral",
					RuntimeInstanceID: request.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
				})
				if err != nil {
					t.Fatalf("issue census-drift successor grant: %v", err)
				}
				manager := runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{
					LifecycleStore: grant, ReceiverExecution: eventreceiver.NormalExecution(),
					PersistenceRoles: runtimemanager.PersistenceRoles{LifecycleCensus: store, LifecycleState: store},
				}, store)
				if err := manager.RebindLifecycleExecutionForStartup(ctx); err == nil || !strings.Contains(err.Error(), "differs from topology admission") {
					t.Fatalf("startup reconciliation error = %v, want canonical/admission drift refusal", err)
				}
				var afterBinding string
				if err := db.QueryRowContext(ctx, bindingQuery, identityArgs...).Scan(&afterBinding); err != nil {
					t.Fatalf("read lifecycle binding after failed census: %v", err)
				}
				if afterBinding != predecessorBinding {
					t.Fatalf("failed census rebound predecessor cell: before=%q after=%q", predecessorBinding, afterBinding)
				}
			})
		}
	}
}

func proveAgentLifecycleProcessBindingReadback(t *testing.T, store lifecycleSourceSetRebindStore) {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Now().UTC()
	staticIdentity := testAgentIdentity(t, "process-static-agent", "")
	runID := uuid.NewString()
	readinessIdentity := mustTestAgentIdentityForRun(runID, "process-readiness-agent", "readiness/instance-1")
	requireRunningRunForTest(t, ctx, store, staticIdentity.RunID, now)
	requireRunningRunForTest(t, ctx, store, runID, now)
	if err := agentfixture.UpsertStatic(t, ctx, store, runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: "process-static-agent", Identity: staticIdentity,
			Role: "worker", Type: "sonnet", Model: "regular", Memory: agentmemory.PlatformDefault(),
		}),
		Status: "active", HiredBy: "process-binding-proof", StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed static lifecycle cell: %v", err)
	}
	predecessor, err := agentfixture.ProcessCapability(t, ctx, store)
	if err != nil {
		t.Fatalf("load predecessor process capability: %v", err)
	}
	plan, exists, err := predecessor.CurrentSourceSet(ctx)
	if err != nil || !exists || len(plan.Sources) != 1 {
		t.Fatalf("load complete predecessor source set: plan=%#v exists=%v err=%v", plan, exists, err)
	}
	predecessorAuthority, err := predecessor.Evidence()
	if err != nil {
		t.Fatalf("load predecessor authority: %v", err)
	}
	coordinate := plan.Sources[0]
	readinessGrant, err := predecessor.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
		RuntimeInstanceID: predecessorAuthority.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue readiness predecessor grant: %v", err)
	}
	readinessRecord := runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: "process-readiness-agent", Identity: readinessIdentity,
			Role: "worker", Type: "sonnet", Model: "regular", Memory: agentmemory.Authored(true),
			FlowPath: readinessIdentity.FlowInstance(),
		}),
		Status: "active", HiredBy: "process-binding-proof", StartedAt: time.Now().UTC(),
	}
	readinessRevision, err := canonicaljson.Hash(readinessRecord.Config)
	if err != nil {
		t.Fatal(err)
	}
	readinessRevision = strings.TrimPrefix(readinessRevision, "sha256:")
	readinessPlan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: "readiness", ScopeKey: "readiness", InstanceID: "instance-1",
			InstancePath: "readiness/instance-1", EntityID: uuid.NewString(), HasStoredPath: true,
		},
		RunID: runID, BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
		WorkflowVersion: "1.0.0", ExecutionMode: runtimeexecutionmode.Live,
		Agents: []runtimepipeline.DynamicFlowRuntimeAgentExpectation{{
			Identity: readinessIdentity, ConfigRevision: readinessRevision,
		}},
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize readiness owner: %v", err)
	}
	readinessPlanJSON, err := canonicaljson.Bytes(readinessPlan)
	if err != nil {
		t.Fatalf("encode readiness owner: %v", err)
	}
	readinessFingerprint, err := canonicaljson.HashRaw(readinessPlanJSON)
	if err != nil {
		t.Fatalf("fingerprint readiness owner: %v", err)
	}
	seedLifecycleReadinessOwner(t, ctx, store, runID, readinessPlan.Identity.InstancePath, readinessPlanJSON, now)
	readinessTopology, err := runtimeagenttopology.FlowReadinessAdmission(
		runID, readinessPlan.Identity.InstancePath, readinessFingerprint,
	)
	if err != nil {
		t.Fatal(err)
	}
	readinessRecord.Topology = readinessTopology
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(),
		Identity: readinessIdentity, AgentID: readinessIdentity.AgentID(), Trigger: "readiness_fixture",
		TargetEpoch: 1, TargetGeneration: 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
		ConfigRevision: readinessRevision, RunMode: runtimemanager.AgentRunModeStopped,
		Agent: &readinessRecord, Topology: readinessTopology, Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed readiness lifecycle cell: %v", err)
	}
	readinessState, found, err := store.LoadAgentLifecycleState(ctx, readinessIdentity)
	if err != nil || !found {
		t.Fatalf("load readiness lifecycle before termination: found=%v err=%v", found, err)
	}
	terminateLifecycleReadinessOwnerForTest(t, ctx, store, runID, readinessPlan.Identity.InstancePath)
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "teardown", RequestHash: uuid.NewString(),
		Identity: readinessIdentity, AgentID: readinessIdentity.AgentID(), Trigger: "terminated_census_fixture",
		ExpectedEpoch: readinessState.RuntimeEpoch, ExpectedGeneration: readinessState.Generation, ExpectedPhase: readinessState.Phase,
		TargetEpoch: readinessState.RuntimeEpoch, TargetGeneration: readinessState.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleTerminated,
		ConfigRevision: readinessState.ConfigRevision, RunMode: runtimemanager.AgentRunModeStopped,
		Subordinate: runtimesessions.LifecycleMutationPlan{
			Action: runtimesessions.LifecycleMutationTerminateCurrentSet, TerminationReason: runtimesessions.TerminationReasonCancelled,
		},
		Topology: readinessState.Topology, Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("terminate readiness lifecycle cell: %v", err)
	}

	before := make(map[string]runtimemanager.AgentLifecycleState, 2)
	for _, identity := range []runtimeagentidentity.Identity{staticIdentity, readinessIdentity} {
		state, found, stateErr := store.LoadAgentLifecycleState(ctx, identity)
		if stateErr != nil || !found {
			t.Fatalf("load predecessor lifecycle %s: found=%v err=%v", identity.Description(), found, stateErr)
		}
		key, _ := identity.Fingerprint()
		before[key] = state
	}

	if err := predecessor.Release(ctx); err != nil {
		t.Fatalf("release predecessor process capability: %v", err)
	}
	acquire := runtimestartupownership.AcquireRequest{
		OwnerID: "process-binding-successor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}
	successor, err := store.AcquireProcessCapability(ctx, acquire)
	if err != nil {
		t.Fatalf("acquire successor process capability: %v", err)
	}
	t.Cleanup(func() { _ = successor.Release(context.Background()) })
	grant, err := successor.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: coordinate.BundleHash, BundleSource: coordinate.BundleSource,
		RuntimeInstanceID: acquire.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue successor generation grant: %v", err)
	}
	target, err := grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatalf("read successor process binding: %v", err)
	}
	manager := runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{
		LifecycleStore:    grant,
		ReceiverExecution: eventreceiver.NormalExecution(),
		PersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: store,
			LifecycleState:  store,
		},
	}, store)
	if err := manager.RebindLifecycleExecutionForStartup(ctx); err != nil {
		t.Fatalf("rebind lifecycle execution before hydration: %v", err)
	}
	if err := manager.RebindLifecycleExecutionForStartup(ctx); err != nil {
		t.Fatalf("replay completed lifecycle rebind: %v", err)
	}

	for _, identity := range []runtimeagentidentity.Identity{staticIdentity, readinessIdentity} {
		key, _ := identity.Fingerprint()
		previous := before[key]
		after, found, stateErr := store.LoadAgentLifecycleState(ctx, identity)
		if stateErr != nil || !found {
			t.Fatalf("load rebound lifecycle %s: found=%v err=%v", identity.Description(), found, stateErr)
		}
		if !after.ProcessBinding.Equal(target) || after.RuntimeEpoch != previous.RuntimeEpoch+1 ||
			after.Generation != previous.Generation+1 || after.Phase != previous.Phase ||
			after.ConfigRevision != previous.ConfigRevision || after.RunMode != previous.RunMode ||
			!after.Topology.Equal(previous.Topology) {
			t.Fatalf("rebound lifecycle %s=%#v, predecessor=%#v target=%#v", identity.Description(), after, previous, target)
		}
	}
	records, err := store.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("load persisted agents after lifecycle rebind: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("hydration projection after lifecycle rebind=%d, want only the active static cell", len(records))
	}
	for _, record := range records {
		if !record.ProcessBinding.Equal(target) {
			t.Fatalf("persisted agent %s binding=%#v, want %#v", record.Config.ID, record.ProcessBinding, target)
		}
		if record.Config.ID == readinessIdentity.AgentID() {
			t.Fatalf("terminated readiness cell leaked into hydration projection: %#v", record)
		}
	}
}

func terminateLifecycleReadinessOwnerForTest(t testing.TB, ctx context.Context, store any, runID, instancePath string) {
	t.Helper()
	var db *sql.DB
	placeholder := "?"
	switch selected := store.(type) {
	case *PostgresStore:
		db = selected.backend.ConstructionHandle()
		placeholder = "$1"
	case *SQLiteRuntimeStore:
		db = selected.backend.ConstructionHandle()
	default:
		t.Fatalf("unsupported lifecycle readiness fixture store %T", store)
	}
	query := `UPDATE flow_instances SET status='terminated' WHERE run_id=? AND instance_path=?`
	args := []any{runID, instancePath}
	if placeholder == "$1" {
		query = `UPDATE flow_instances SET status='terminated' WHERE run_id=$1::uuid AND instance_path=$2`
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("terminate lifecycle readiness owner: %v", err)
	}
}

func seedLifecycleReadinessOwner(
	t testing.TB,
	ctx context.Context,
	store lifecycleSourceSetRebindStore,
	runID string,
	instancePath string,
	plan []byte,
	now time.Time,
) {
	t.Helper()
	var db *sql.DB
	postgres := false
	switch selected := store.(type) {
	case *PostgresStore:
		db = selected.backend.ConstructionHandle()
		postgres = true
	case *SQLiteRuntimeStore:
		db = selected.backend.ConstructionHandle()
	default:
		t.Fatalf("unsupported lifecycle readiness fixture store %T", store)
	}
	if postgres {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
			VALUES ($1::uuid, $2, 'readiness', 'template', '{}'::jsonb, 'active', $3)
		`, runID, instancePath, now); err != nil {
			t.Fatalf("seed postgres flow instance: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, created_at, updated_at)
			VALUES ($1::uuid, $2, $3::jsonb, $4, $4)
		`, runID, instancePath, plan, now); err != nil {
			t.Fatalf("seed postgres readiness owner: %v", err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
		VALUES (?, ?, 'readiness', 'template', '{}', 'active', ?)
	`, runID, instancePath, now); err != nil {
		t.Fatalf("seed sqlite flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (run_id, instance_path, plan, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, runID, instancePath, plan, now, now); err != nil {
		t.Fatalf("seed sqlite readiness owner: %v", err)
	}
}

func proveAgentLifecycleSourceSetRebind(t *testing.T, store lifecycleSourceSetRebindStore) {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	agentID := "source-set-rebind-agent"
	identity := testAgentIdentity(t, agentID, "global")
	requireRunningRunForTest(t, ctx, store, identity.RunID, now)
	record := runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ID: agentID, Identity: identity, Role: "worker", Type: "sonnet", Model: "regular", FlowID: "global",
			ExecutionMode: runtimeeffects.ExecutionModeLive, Memory: agentmemory.Authored(true), FlowPath: "global",
			Config: []byte(`{}`),
		}),
		Status: "active", HiredBy: "source-set-rebind-test", StartedAt: now,
	}
	if err := agentfixture.UpsertStatic(t, ctx, store, record); err != nil {
		t.Fatalf("seed lifecycle cell: %v", err)
	}
	before, exists, err := store.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !exists {
		t.Fatalf("load lifecycle cell before rebind: exists=%v err=%v", exists, err)
	}

	request := runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "source_set_rebind", RequestHash: "source-set-rebind-v1",
		Identity: identity, AgentID: agentID, Trigger: "source_set_rebind",
		ExpectedEpoch: before.RuntimeEpoch, ExpectedGeneration: before.Generation, ExpectedPhase: before.Phase,
		TargetEpoch: before.RuntimeEpoch, TargetGeneration: before.Generation, TargetPhase: before.Phase,
		ConfigRevision: before.ConfigRevision, RunMode: before.RunMode, Topology: before.Topology, Now: now.Add(time.Second),
	}
	result, err := agentfixture.CommitExact(t, ctx, store, request)
	if err != nil {
		t.Fatalf("commit source-set rebind: %v", err)
	}
	if result.RuntimeEpoch != before.RuntimeEpoch || result.Generation != before.Generation || result.Phase != before.Phase ||
		result.ConfigRevision != before.ConfigRevision || result.RunMode != before.RunMode || !result.Topology.Equal(before.Topology) {
		t.Fatalf("source-set rebind result = %#v, want unchanged lifecycle with exact topology", result)
	}
	replayed, err := agentfixture.CommitExact(t, ctx, store, request)
	if err != nil || !replayed.Replayed || replayed.TransitionID != result.TransitionID {
		t.Fatalf("exact source-set rebind replay = %#v err=%v, want transition %s", replayed, err, result.TransitionID)
	}
	changed := request
	changed.RequestHash = "source-set-rebind-conflict"
	if _, err := agentfixture.CommitExact(t, ctx, store, changed); err == nil {
		t.Fatal("changed source-set rebind duplicate was accepted")
	} else {
		var failure *runtimefailures.Error
		if !errors.As(err, &failure) || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
			t.Fatalf("changed source-set rebind duplicate error = %v, want conflicting duplicate", err)
		}
	}
	after, exists, err := store.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !exists {
		t.Fatalf("load lifecycle cell after rebind: exists=%v err=%v", exists, err)
	}
	if after.RuntimeEpoch != before.RuntimeEpoch || after.Generation != before.Generation || after.Phase != before.Phase ||
		after.ConfigRevision != before.ConfigRevision || after.RunMode != before.RunMode || !after.Topology.Equal(before.Topology) {
		t.Fatalf("lifecycle readback after source-set rebind = %#v, want %#v", after, before)
	}
}
