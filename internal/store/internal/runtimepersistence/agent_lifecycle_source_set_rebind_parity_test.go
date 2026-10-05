package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeexecutionmode "github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func TestAgentLifecycleRemovedSourceRetirementParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var store lifecycleSourceSetRebindStore
			if backend == "sqlite" {
				store = newBootstrappedSQLiteRuntimeStoreForTest(t)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				store = admitTestPostgresStore(t, db)
			}
			ctx := testAuthorActivityContext()
			identity := testAgentIdentity(t, "removed-source-agent", "")
			requireRunningRunForTest(t, ctx, store, identity.RunID, time.Now().UTC())
			record := runtimemanager.PersistedAgent{
				Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
					ID: identity.AgentID(), Identity: identity, Role: "worker", Type: "sonnet", Model: "regular",
					ExecutionMode: "live", Memory: agentmemory.Plan{},
				}),
				Status: "active", HiredBy: "retirement-proof", StartedAt: time.Now().UTC(),
			}
			if err := agentfixture.UpsertStatic(t, ctx, store, record); err != nil {
				t.Fatal(err)
			}
			before, found, err := store.LoadAgentLifecycleState(ctx, identity)
			if err != nil || !found {
				t.Fatalf("load predecessor: %v %v", found, err)
			}
			capability, err := agentfixture.ProcessCapability(t, ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			hash := "bundle-v2:sha256:" + strings.Repeat("e", 64)
			plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{{BundleHash: hash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.RestoreSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: before.Topology.Authority.Static.SourceSetRevision, Plan: plan}); err != nil {
				t.Fatal(err)
			}
			authority, err := capability.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
				BundleHash: hash, RuntimeInstanceID: authority.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision,
			})
			if err != nil {
				t.Fatal(err)
			}
			if ownership, err := grant.InspectRunExecutionOwnership(ctx, identity.RunID); err != nil || ownership != runtimemanager.RunExecutionOtherNormalSource {
				t.Fatalf("removed source acquired execution: %v %v", ownership, err)
			}
			topology, err := runtimeagenttopology.StaticAdmission(plan.Revision, hash, runtimeagenttopology.LifetimeDurableManaged)
			if err != nil {
				t.Fatal(err)
			}
			req := runtimemanager.AgentLifecycleTransition{
				DiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation},
				OperationID:      uuid.NewString(), OperationKind: "source_set_retire", RequestHash: uuid.NewString(),
				Identity: identity, AgentID: identity.AgentID(), Trigger: "source_set_retire",
				ExpectedEpoch: before.RuntimeEpoch, ExpectedGeneration: before.Generation, ExpectedPhase: before.Phase,
				TargetEpoch: before.RuntimeEpoch, TargetGeneration: before.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
				ConfigRevision: before.ConfigRevision, RunMode: before.RunMode, Topology: topology, Now: time.Now().UTC(),
			}
			if _, err := grant.CommitAgentLifecycleTransition(ctx, req); !errors.Is(err, runtimemanager.ErrRunExecutionNotOwned) {
				t.Fatalf("nonterminal removed-source mutation: %v", err)
			}
			req.TargetPhase = runtimemanager.AgentLifecycleTerminated
			if _, err := grant.CommitAgentLifecycleTransition(ctx, req); err != nil {
				t.Fatalf("terminalize removed source: %v", err)
			}
			after, found, err := store.LoadAgentLifecycleState(ctx, identity)
			binding, bindingErr := grant.ProcessExecutionBinding()
			if err != nil || bindingErr != nil || !found || after.Phase != runtimemanager.AgentLifecycleTerminated ||
				after.Generation != before.Generation+1 || !after.Topology.Equal(topology) || !after.ProcessBinding.Equal(binding) {
				t.Fatalf("terminal readback: %+v found=%v err=%v bindingErr=%v", after, found, err, bindingErr)
			}
		})
	}
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
					BundleHash: testAgentTopologyBundleHash,
				}}, nil)
				if err != nil {
					t.Fatalf("construct census-drift source set: %v", err)
				}
				if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
					t.Fatalf("install census-drift source set: %v", err)
				}
				grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
					BundleHash:        testAgentTopologyBundleHash,
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

func TestAgentLifecycleReadinessSourceSetRebindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected lifecycleSourceSetRebindStore
			if backend == "sqlite" {
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			}
			proveAgentLifecycleProcessBindingReadback(t, selected, true)
		})
	}
}

func TestAgentLifecyclePreparedReadinessRebindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected lifecycleSourceSetRebindStore
			if backend == "sqlite" {
				selected = newBootstrappedSQLiteRuntimeStoreForTest(t)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				selected = admitTestPostgresStore(t, db)
			}
			proveAgentLifecycleProcessBindingReadback(t, selected, true, true)
		})
	}
}

func proveAgentLifecycleProcessBindingReadback(t *testing.T, store lifecycleSourceSetRebindStore, sourceSetRebind ...bool) {
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
			Role: "worker", Type: "sonnet", Model: "regular", Memory: agentmemory.Plan{},
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
		BundleHash:        coordinate.BundleHash,
		RuntimeInstanceID: predecessorAuthority.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue readiness predecessor grant: %v", err)
	}
	readinessRecord := runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: "process-readiness-agent", Identity: readinessIdentity,
			Role: "worker", Type: "sonnet", Model: "regular", Memory: agentmemory.Plan{Enabled: true},
			FlowPath: readinessIdentity.FlowInstance(),
		}),
		Status: "active", HiredBy: "process-binding-proof", StartedAt: time.Now().UTC(),
	}
	readinessRevision, err := canonicaljson.Hash(readinessRecord.Config)
	if err != nil {
		t.Fatal(err)
	}
	readinessRevision = strings.TrimPrefix(readinessRevision, "sha256:")
	readinessRecords := []runtimemanager.PersistedAgent{readinessRecord}
	actorExpectations := []runtimepipeline.DynamicFlowRuntimeAgentExpectation{{Identity: readinessIdentity, ConfigRevision: readinessRevision, EntityID: readinessRecord.Config.EntityID}}
	if len(sourceSetRebind) > 0 && sourceSetRebind[0] {
		sibling := readinessRecord
		sibling.Config.ID = "process-readiness-sibling"
		sibling.Config.Identity = mustTestAgentIdentityForRun(runID, sibling.Config.ID, readinessIdentity.FlowInstance())
		sibling.Config = withRuntimePersistenceTestIntent(t, sibling.Config)
		siblingRevision, err := canonicaljson.Hash(sibling.Config)
		if err != nil {
			t.Fatal(err)
		}
		readinessRecords = append(readinessRecords, sibling)
		actorExpectations = append(actorExpectations, runtimepipeline.DynamicFlowRuntimeAgentExpectation{Identity: sibling.Config.Identity, ConfigRevision: strings.TrimPrefix(siblingRevision, "sha256:"), EntityID: sibling.Config.EntityID})
	}
	readinessPlan, err := (runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: runtimeflowidentity.Instance{
			TemplateID: "readiness", ScopeKey: "readiness", InstanceID: "instance-1",
			InstancePath: "readiness/instance-1", EntityID: uuid.NewString(), HasStoredPath: true,
		},
		RunID: runID, BundleHash: coordinate.BundleHash,
		WorkflowVersion: "1.0.0", ExecutionMode: runtimeexecutionmode.Live,
		Agents: actorExpectations,
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize readiness owner: %v", err)
	}
	readinessFingerprint, err := readinessPlan.Hash()
	if err != nil {
		t.Fatalf("fingerprint readiness owner: %v", err)
	}
	seedLifecycleReadinessOwner(t, ctx, store, readinessPlan, now)
	activationStore, ok := store.(interface {
		LoadDynamicFlowRuntimeReadiness(context.Context, string, runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error)
		BeginDynamicFlowRuntimeActivation(context.Context, runtimepipeline.DynamicFlowRuntimeActivationRequest) (runtimepipeline.DynamicFlowRuntimeActivationAdmissionResult, error)
	})
	if !ok {
		t.Fatal("process takeover fixture requires exact flow activation owner")
	}
	readiness, found, err := activationStore.LoadDynamicFlowRuntimeReadiness(ctx, runID, readinessPlan.Identity.Route())
	if err != nil || !found {
		t.Fatalf("load readiness for lifecycle seed: found=%v err=%v", found, err)
	}
	preparedTopology, err := runtimeagenttopology.FlowReadinessAdmission(runID, readinessPlan.Identity.InstancePath, readinessFingerprint)
	if err != nil {
		t.Fatal(err)
	}
	preparedTopology, err = preparedTopology.WithFlowPreparationAttempt(readiness.AttemptOrdinal)
	if err != nil {
		t.Fatal(err)
	}
	readinessRecord.Topology = preparedTopology
	preparation := runtimemanager.AgentLifecycleTransition{
		DiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation},
		OperationID:      uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(),
		Identity: readinessIdentity, AgentID: readinessIdentity.AgentID(), Trigger: "readiness_preparation_fixture",
		TargetEpoch: 1, TargetGeneration: 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
		ConfigRevision: readinessRevision, RunMode: runtimemanager.AgentRunModeStopped,
		Agent: &readinessRecord, Topology: preparedTopology, Now: time.Now().UTC(),
	}
	staleTopology, err := preparedTopology.WithFlowPreparationAttempt(readiness.AttemptOrdinal + 1)
	if err != nil {
		t.Fatal(err)
	}
	stalePreparation := preparation
	stalePreparation.OperationID = uuid.NewString()
	stalePreparation.RequestHash = uuid.NewString()
	stalePreparation.Topology = staleTopology
	staleRecord := readinessRecord
	staleRecord.Topology = staleTopology
	stalePreparation.Agent = &staleRecord
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, stalePreparation); err == nil || !strings.Contains(err.Error(), "readiness_preparation_plan_not_current") {
		t.Fatalf("stale preparation revision admitted topology: %v", err)
	}
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, preparation); err != nil {
		t.Fatalf("prepare stopped readiness agent before execution admission: %v", err)
	}
	for index := 1; index < len(readinessRecords); index++ {
		record := readinessRecords[index]
		record.Topology = preparedTopology
		request := preparation
		request.OperationID, request.RequestHash = uuid.NewString(), uuid.NewString()
		request.Identity, request.AgentID = record.Config.Identity, record.Config.ID
		request.ConfigRevision, request.Agent = actorExpectations[index].ConfigRevision, &record
		if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, request); err != nil {
			t.Fatalf("prepare sibling readiness actor: %v", err)
		}
	}
	preparedState, found, err := store.LoadAgentLifecycleState(ctx, readinessIdentity)
	if err != nil || !found || preparedState.Phase != runtimemanager.AgentLifecycleRegistered || preparedState.RunMode != runtimemanager.AgentRunModeStopped || !preparedState.Topology.Equal(preparedTopology) {
		t.Fatalf("prepared topology readback: state=%+v found=%v err=%v", preparedState, found, err)
	}
	forgedPreparation := preparation
	forgedPreparation.OperationID = uuid.NewString()
	forgedPreparation.RequestHash = uuid.NewString()
	forgedPreparation.OperationKind = "reconfigure"
	forgedPreparation.ExpectedEpoch = preparedState.RuntimeEpoch
	forgedPreparation.ExpectedGeneration = preparedState.Generation
	forgedPreparation.ExpectedPhase = preparedState.Phase
	forgedPreparation.TargetGeneration = preparedState.Generation + 1
	forgedPreparation.TargetPhase = runtimemanager.AgentLifecycleRunning
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, forgedPreparation); err == nil || !strings.Contains(err.Error(), "readiness_preparation_must_remain_non_executable") {
		t.Fatalf("prepared grant launched agent: %v", err)
	}
	if _, err := readinessGrant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("settle readiness grant probes: %v", err)
	}
	if _, err := readinessGrant.AdmitExecution(ctx); err != nil {
		t.Fatalf("admit readiness grant: %v", err)
	}
	readinessBinding, err := readinessGrant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	forgedPreparation.TargetPhase = runtimemanager.AgentLifecycleRegistered
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, forgedPreparation); err == nil || !strings.Contains(err.Error(), "flow topology preparation requires a current pre-admission") {
		t.Fatalf("admitted grant reused preparation authority: %v", err)
	}
	admitted, err := activationStore.BeginDynamicFlowRuntimeActivation(ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(readinessPlan, readiness.AttemptOrdinal, readiness.AttemptState, readinessBinding))
	if err != nil || !admitted.Acknowledged {
		t.Fatalf("admit readiness lifecycle attempt: result=%+v err=%v", admitted, err)
	}
	readinessTopology, err := runtimeagenttopology.FlowReadinessAdmission(
		runID, readinessPlan.Identity.InstancePath, readinessFingerprint,
	)
	if err != nil {
		t.Fatal(err)
	}
	readinessTopology, err = readinessTopology.WithFlowActivationAttempt(admitted.Attempt.ID())
	if err != nil {
		t.Fatal(err)
	}
	readinessRecord.Topology = readinessTopology
	if len(sourceSetRebind) < 2 || !sourceSetRebind[1] {
		if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
			DiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation},
			OperationID:      uuid.NewString(), OperationKind: "reconfigure", RequestHash: uuid.NewString(),
			Identity: readinessIdentity, AgentID: readinessIdentity.AgentID(), Trigger: "readiness_fixture",
			ExpectedEpoch: preparedState.RuntimeEpoch, ExpectedGeneration: preparedState.Generation, ExpectedPhase: preparedState.Phase,
			TargetEpoch: preparedState.RuntimeEpoch, TargetGeneration: preparedState.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRegistered,
			ConfigRevision: readinessRevision, RunMode: runtimemanager.AgentRunModeStopped,
			Agent: &readinessRecord, Topology: readinessTopology, Now: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("seed readiness lifecycle cell: %v", err)
		}
		for index := 1; index < len(readinessRecords); index++ {
			record := readinessRecords[index]
			record.Topology = readinessTopology
			previous, found, err := store.LoadAgentLifecycleState(ctx, record.Config.Identity)
			if err != nil || !found {
				t.Fatalf("load prepared sibling: found=%t err=%v", found, err)
			}
			if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
				DiagnosticOrigin: preparation.DiagnosticOrigin,
				OperationID:      uuid.NewString(), OperationKind: "reconfigure", RequestHash: uuid.NewString(),
				Identity: record.Config.Identity, AgentID: record.Config.ID, Trigger: "readiness_fixture",
				ExpectedEpoch: previous.RuntimeEpoch, ExpectedGeneration: previous.Generation, ExpectedPhase: previous.Phase,
				TargetEpoch: previous.RuntimeEpoch, TargetGeneration: previous.Generation + 1, TargetPhase: previous.Phase,
				ConfigRevision: previous.ConfigRevision, RunMode: previous.RunMode, Agent: &record, Topology: readinessTopology, Now: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("admit sibling readiness actor: %v", err)
			}
		}
	}
	readinessState, found, err := store.LoadAgentLifecycleState(ctx, readinessIdentity)
	if err != nil || !found {
		t.Fatalf("load readiness lifecycle before termination: found=%v err=%v", found, err)
	}
	if len(sourceSetRebind) != 0 && sourceSetRebind[0] {
		// Isolate durable readiness admission from runtime cleanup so the
		// contested generation handoff has deterministic both-store evidence.
		successor, err := predecessor.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
			BundleHash: coordinate.BundleHash, RuntimeInstanceID: predecessorAuthority.RuntimeInstanceID,
			RuntimeGeneration: 3, SourceSetRevision: plan.Revision,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := successor.Retire(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		if _, err := successor.MarkProbesSettled(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := successor.AdmitExecution(ctx); err != nil {
			t.Fatal(err)
		}
		request := runtimemanager.FlowReadinessSourceSetRebindRequest{
			Attempt: admitted.Attempt, PlanHash: readinessFingerprint,
			Transitions: []runtimemanager.AgentLifecycleTransition{{
				DiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation},
				OperationID:      uuid.NewString(), OperationKind: "source_set_rebind", RequestHash: uuid.NewString(),
				Identity: readinessIdentity, AgentID: readinessIdentity.AgentID(), Trigger: "readiness_source_set_rebind_proof",
				ExpectedEpoch: readinessState.RuntimeEpoch, ExpectedGeneration: readinessState.Generation, ExpectedPhase: readinessState.Phase,
				TargetEpoch: readinessState.RuntimeEpoch, TargetGeneration: readinessState.Generation, TargetPhase: readinessState.Phase,
				ConfigRevision: readinessState.ConfigRevision, RunMode: readinessState.RunMode, Topology: readinessState.Topology, Now: time.Now().UTC(),
			}},
		}
		for _, record := range readinessRecords[1:] {
			state, found, err := store.LoadAgentLifecycleState(ctx, record.Config.Identity)
			if err != nil || !found {
				t.Fatalf("load sibling before rebind: found=%t err=%v", found, err)
			}
			request.Transitions = append(request.Transitions, runtimemanager.AgentLifecycleTransition{
				DiagnosticOrigin: preparation.DiagnosticOrigin,
				OperationID:      uuid.NewString(), OperationKind: "source_set_rebind", RequestHash: uuid.NewString(),
				Identity: record.Config.Identity, AgentID: record.Config.ID, Trigger: "readiness_source_set_rebind_proof",
				ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
				TargetEpoch: state.RuntimeEpoch, TargetGeneration: state.Generation, TargetPhase: state.Phase,
				ConfigRevision: state.ConfigRevision, RunMode: state.RunMode, Topology: state.Topology, Now: time.Now().UTC(),
			})
		}
		if _, err := successor.CommitAgentLifecycleTransition(ctx, request.Transitions[0]); err == nil || !strings.Contains(err.Error(), "atomic") {
			t.Fatalf("individual actor rebind bypassed the instance transaction: %v", err)
		}
		owner := successor.(runtimemanager.FlowReadinessSourceSetRebindPersistence)
		if _, err := owner.RebindFlowReadinessSourceSet(ctx, runtimemanager.FlowReadinessSourceSetRebindRequest{Attempt: request.Attempt, PlanHash: request.PlanHash}); err == nil {
			t.Fatal("empty census omitted a live instance actor")
		}
		proveReadinessRebindAtomicRollback(t, ctx, store, owner, request)
		rebound, err := owner.RebindFlowReadinessSourceSet(ctx, request)
		if err != nil {
			t.Fatalf("same-process readiness survivor cannot bind its adjacent generation: %v", err)
		}
		if len(rebound.Transitions) != len(readinessRecords) || rebound.Attempt.ID() != admitted.Attempt.ID() {
			t.Fatalf("readiness rebind lost its exact attempt: %+v", rebound)
		}
		result := rebound.Transitions[0]
		successorBinding, err := successor.ProcessExecutionBinding()
		if err != nil || !result.ProcessBinding.Equal(successorBinding) || !result.Topology.Equal(readinessState.Topology) ||
			result.RuntimeEpoch != readinessState.RuntimeEpoch || result.Generation != readinessState.Generation || result.Phase != readinessState.Phase {
			t.Fatalf("readiness survivor changed construction/lifecycle authority: result=%+v err=%v", result, err)
		}
		for _, transition := range request.Transitions {
			state, found, err := store.LoadAgentLifecycleState(ctx, transition.Identity)
			if err != nil || !found || !state.ProcessBinding.Equal(successorBinding) || !state.Topology.Equal(transition.Topology) || state.RuntimeEpoch != transition.ExpectedEpoch || state.Generation != transition.ExpectedGeneration || state.Phase != transition.ExpectedPhase {
				t.Fatalf("rebound actor did not retain exact attachment: %+v found=%t err=%v", state, found, err)
			}
			if _, err := successor.CommitAgentLifecycleTransition(ctx, transition); err == nil {
				t.Fatal("cached whole-instance operation allowed an individual same-attempt restamp")
			}
		}
		again, err := owner.RebindFlowReadinessSourceSet(ctx, request)
		if err != nil || len(again.Transitions) != len(readinessRecords) || again.Attempt.ID() != rebound.Attempt.ID() {
			t.Fatalf("whole-instance restamp retry lost exact evidence: %+v %v", again, err)
		}
		return
	}
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		DiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation},
		OperationID:      uuid.NewString(), OperationKind: "process_takeover", RequestHash: uuid.NewString(),
		Identity: readinessIdentity, AgentID: readinessIdentity.AgentID(), Trigger: "forged_takeover",
		ExpectedEpoch: readinessState.RuntimeEpoch, ExpectedGeneration: readinessState.Generation, ExpectedPhase: readinessState.Phase,
		TargetEpoch: readinessState.RuntimeEpoch + 1, TargetGeneration: readinessState.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
		ConfigRevision: readinessState.ConfigRevision, RunMode: readinessState.RunMode,
		Topology: readinessState.Topology, Now: time.Now().UTC(),
	}); err == nil || !strings.Contains(err.Error(), "readiness_takeover_must_only_rebind_execution") {
		t.Fatalf("forged takeover phase change was not rejected: %v", err)
	}
	terminateLifecycleReadinessOwnerForTest(t, ctx, store, runID, readinessPlan.Identity.InstancePath)
	if _, err := readinessGrant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		DiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation},
		OperationID:      uuid.NewString(), OperationKind: "teardown", RequestHash: uuid.NewString(),
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
		BundleHash:        coordinate.BundleHash,
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

func proveReadinessRebindAtomicRollback(t *testing.T, ctx context.Context, store lifecycleSourceSetRebindStore, owner runtimemanager.FlowReadinessSourceSetRebindPersistence, request runtimemanager.FlowReadinessSourceSetRebindRequest) {
	t.Helper()
	var db *sql.DB
	postgres := false
	switch selected := store.(type) {
	case *PostgresStore:
		db, postgres = selected.backend.ConstructionHandle(), true
	case *SQLiteRuntimeStore:
		db = selected.backend.ConstructionHandle()
	default:
		t.Fatalf("unsupported readiness census fixture %T", store)
	}
	before := snapshotForkHistoricalExecutionTables(t, db, postgres)
	for _, failure := range []string{"omitted_sibling", "wrong_plan", "cancelled", "second_actor_write"} {
		bad, callCtx := request, ctx
		var remove []string
		switch failure {
		case "omitted_sibling":
			bad.Transitions = request.Transitions[:1]
		case "wrong_plan":
			bad.PlanHash = strings.Repeat("0", 64)
		case "cancelled":
			var cancel context.CancelFunc
			callCtx, cancel = context.WithCancel(ctx)
			cancel()
		case "second_actor_write":
			operation := request.Transitions[1].OperationID
			install := []string{"CREATE TRIGGER rebind_second_actor BEFORE INSERT ON agent_lifecycle_operations WHEN NEW.operation_id='" + operation + "' BEGIN SELECT RAISE(ABORT,'second_actor_write'); END"}
			remove = []string{"DROP TRIGGER rebind_second_actor"}
			if postgres {
				install = []string{"CREATE FUNCTION rebind_second_actor_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.operation_id='" + operation + "' THEN RAISE EXCEPTION 'second_actor_write'; END IF; RETURN NEW; END $$", "CREATE TRIGGER rebind_second_actor BEFORE INSERT ON agent_lifecycle_operations FOR EACH ROW EXECUTE FUNCTION rebind_second_actor_fn()"}
				remove = []string{"DROP TRIGGER rebind_second_actor ON agent_lifecycle_operations", "DROP FUNCTION rebind_second_actor_fn()"}
			}
			for _, statement := range install {
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
		}
		result, err := owner.RebindFlowReadinessSourceSet(callCtx, bad)
		for _, statement := range remove {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		if err == nil || len(result.Transitions) != 0 || result.Attempt.ID() != "" {
			t.Fatalf("%s returned rebind evidence: %+v %v", failure, result, err)
		}
		if failure == "second_actor_write" && !strings.Contains(err.Error(), failure) {
			t.Fatalf("wrong second-actor failure boundary: %v", err)
		}
		if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, postgres)) {
			t.Fatalf("%s changed the grant stamp, predecessor actors, or durable evidence", failure)
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
	t *testing.T,
	ctx context.Context,
	store lifecycleSourceSetRebindStore,
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	now time.Time,
) {
	t.Helper()
	selected, ok := store.(agentFixtureFlowStore)
	if !ok {
		t.Fatalf("lifecycle readiness fixture requires the constructor owner: %T", store)
	}
	flowID := plan.Identity.TemplateID
	bundle := loadLifecyclePersistenceFixtureForTest(t, map[string]string{
		"schema.yaml":             "name: lifecycle-readiness-proof\n",
		flowID + "/schema.yaml":   "name: " + flowID + "\ninstance: fixture_key\nstages:\n  pending: {initial: true}\npins:\n  inputs: [lifecycle.constructed]\n",
		flowID + "/events.yaml":   "lifecycle.constructed:\n  fixture_key: text\n",
		flowID + "/entities.yaml": "item:\n  fixture_key: text\n",
	})
	bundle.Semantics.Version = plan.WorkflowVersion
	fact := mustStoreTestSourceArtifactFact(plan.BundleHash)
	ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(storeTestWorkContext(t, ctx), plan.RunID), fact)
	ctx = runtimeeffects.WithExecutionMode(ctx, plan.ExecutionMode)
	bus := &sqliteFlowActivationBus{}
	workflows := configureAgentFixtureFlowLifecycle(t, selected, bus, bundle)
	manager := ownStoreTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact,
		SemanticSource: semanticview.Wrap(bundle), WorkflowInstances: workflows, WorkOwner: storeTestWorkOwner(t),
		ReceiverExecution: eventreceiver.NormalExecution(),
	}, selected))
	construction, err := manager.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle), Instance: plan.Identity, OccurredAt: now,
		ConstructorInput: "lifecycle.constructed", ResolvedKey: plan.Identity.InstanceID,
		Config:       map[string]any{"fixture_key": plan.Identity.InstanceID},
		TriggerEvent: eventtest.ExistingRunRootIngress(uuid.NewString(), "lifecycle.constructed", "fixture", "", []byte(`{"fixture_key":"`+plan.Identity.InstanceID+`"}`), 0, plan.RunID, events.EventEnvelope{}, now),
	})
	if err != nil {
		t.Fatalf("prepare lifecycle readiness construction: %v", err)
	}
	// These store-admission controls supply their exact actor expectations but
	// still commit the header, entry evidence and receipt through O2 atomically.
	construction.Readiness = plan
	committed, err := (agentFixtureFlowActivationCommitter{store: selected}).CommitFlowInstanceActivation(ctx, construction)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct lifecycle readiness owner: result=%+v err=%v", committed, err)
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
			ExecutionMode: runtimeeffects.ExecutionModeLive, Memory: agentmemory.Plan{Enabled: true}, FlowPath: "global",
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
