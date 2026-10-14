package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestMaterializingSenderReachesExistingRequiredReceiverBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, required := range []bool{false, true} {
			name := "entityless_control"
			if required {
				name = "existing_required"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyMaterializingSenderExistingReceiver(t, required))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "exact"}, "idempotency_key": "seed"})
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				var count int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 AND e.event_name='work.ready' AND d.status='delivered'`, seed.RunID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("receiver completions=%d: %s", count, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
				}
			})
		}
	}
}

func TestProspectiveReceiverUpdatesAndCompanionRepairBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, rejectMissingHeader := range []bool{false, true} {
			name := "complete"
			if rejectMissingHeader {
				name = "missing_header_refused"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				if rejectMissingHeader {
					requireProspectiveMissingHeaderRefusal(t, backend)
				}
				rt, _, restart := newRetainedMailboxCompletionRuntime(t, backend, canonicalrouting.CopyMaterializingSenderExistingReceiver(t, true))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "first"}, "idempotency_key": "seed"})
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				var entityID, instance string
				if err := rt.DB.QueryRow(`SELECT entity_id,flow_instance FROM entity_state WHERE run_id=$1`, seed.RunID).Scan(&entityID, &instance); err != nil {
					t.Fatal(err)
				}
				params := map[string]any{"event_name": "start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"case_id": "second"}, "idempotency_key": "update"}
				updated := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != updated.EventID {
					t.Fatal("duplicate publication changed identity")
				}
				var fields string
				if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance=$3`, seed.RunID, entityID, instance).Scan(&fields); err != nil {
					t.Fatal(err)
				}
				var values map[string]any
				if err := json.Unmarshal([]byte(fields), &values); err != nil || values["case_id"] != "second" {
					t.Fatalf("updated state=%s err=%v", fields, err)
				}
				var deliveries, companions int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 AND e.event_name='work.ready' AND d.status='delivered'`, seed.RunID).Scan(&deliveries); err != nil {
					t.Fatal(err)
				}
				if err := rt.DB.QueryRow(`SELECT count(*) FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, seed.RunID, instance).Scan(&companions); err != nil {
					t.Fatal(err)
				}
				if deliveries != 2 || companions != 1 {
					t.Fatalf("delivered=%d companions=%d; %s", deliveries, companions, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
				}
				before := mailboxCompletionRunEffects(t, rt, seed.RunID)
				rt, _ = restart()
				duplicate = requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != updated.EventID || duplicate.RunID != seed.RunID {
					t.Fatal("restart changed the committed prospective-state publication")
				}
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				if after := mailboxCompletionRunEffects(t, rt, seed.RunID); !reflect.DeepEqual(before, after) {
					t.Fatal("restart/duplicate changed state, companion, publication or receiver delivery")
				}
			})
		}
	}
}

func requireProspectiveMissingHeaderRefusal(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	type nativeStore interface {
		storetest.RunFixtureStore
		sourceartifactfixture.Writer
		pipeline.WorkflowEngineMutationOwner
		receiverProofStateReader
		CommitFlowInstanceActivation(context.Context, bus.FlowInstanceActivationCommand) (pipeline.CommittedFlowInstanceActivation, error)
	}
	var selected nativeStore
	if backend == servedparity.BackendDefaultSQLite {
		selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
	} else {
		selected = storetest.StartPostgresRuntimeStore(t)
	}
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repoRootForTest(), canonicalrouting.CopyMaterializingSenderExistingReceiver(t, true), contracts.DefaultPlatformSpecFile(repoRootForTest()))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	fact := sourceartifactfixture.RequireArtifact(t, t.Context(), selected, bundle.SourceArtifact)
	work := newSupervisorTestRuntimeOccurrence(t, fact.BundleHash())
	runID := uuid.NewString()
	ctx := worklifetime.WithOccurrence(effects.WithExecutionMode(correlation.WithRunID(correlation.WithSourceArtifactFact(t.Context(), fact), runID), executionmode.Live), work)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(work.Identity().RuntimeInstanceID, fact.BundleHash()))
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact})
	flow := semanticview.RootExecutionFlowID(source)
	constructor, err := pipeline.CompileFlowConstructor(source, flow, "")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := constructor.InitialFields(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := constructor.InitialStoredStage()
	if err != nil || !stage.IsStatelessPosture() {
		t.Fatalf("isolated companion fixture requires the original stateless constructor: %v", err)
	}
	entity, found := entityruntime.ResolveForFlow(source, flow)
	if !found {
		t.Fatal("prospective fixture lost its declared field owner")
	}
	schema, found := source.FlowSchemaByID(flow)
	if !found {
		t.Fatal("prospective fixture lost its root declaration")
	}
	identity := flowidentity.Stored(source, flow, runID, runID, runID, "")
	at := time.Now().UTC().Truncate(time.Microsecond)
	readiness := pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live}
	instance := pipeline.WorkflowInstance{InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID, EntityType: entity.EntityType,
		WorkflowName: flow, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(), CurrentState: stage.ID(), StageDefined: false,
		Fields: fields, RuntimeReadiness: &readiness, CreatedAt: at, EnteredStageAt: at}
	command, err := flowactivationfixture.Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	constructed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !constructed.Acknowledged || !constructed.Created {
		t.Fatalf("native isolated construction: acknowledged=%v created=%v err=%v", constructed.Acknowledged, constructed.Created, err)
	}
	record, err := command.Plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	state := record.State
	state.Transition, state.ExpectedState, state.ExpectedRevision = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion, instance.CurrentState, 1
	state.Fields, state.UpdatedAt = json.RawMessage(`{"case_id":"first"}`), at.Add(time.Second)
	updated, err := selected.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: state})
	if err != nil || !updated.Committed {
		t.Fatalf("constructed prospective-update control: committed=%v err=%v", updated.Committed, err)
	}
	_, current := requireReceiverTargetState(t, selected, runID, flow, identity.InstancePath, identity.EntityID)
	if current.Revision != 2 || current.Fields["case_id"] != "first" {
		t.Fatalf("native update lost its exact field/revision: %+v", current)
	}
	// This owner was never activated; faulting it cannot strand live retirement.
	removed, err := storetest.RemoveWorkflowProjectionHeader(ctx, selected, runID, identity.InstancePath)
	if err != nil || removed != 1 {
		t.Fatalf("exact isolated header fault: removed=%d err=%v", removed, err)
	}
	before := requireReceiverApplicationSnapshot(t, selected)
	for _, table := range []string{"flow_instances", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness"} {
		if rows, exists := before[table]; !exists || len(rows) != 0 {
			t.Fatalf("missing-header cut retained %s evidence: %v", table, before[table])
		}
	}
	if len(before["entity_state"]) != 1 {
		t.Fatal("missing-header cut lost surviving business fields")
	}
	state.ExpectedRevision, state.Fields, state.UpdatedAt = current.Revision, json.RawMessage(`{"case_id":"second"}`), at.Add(2*time.Second)
	refused, err := selected.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: state})
	if err == nil || !strings.Contains(err.Error(), "workflow engine state route is missing: "+identity.InstancePath) || refused.Committed {
		t.Fatalf("prospective update did not refuse the exact missing header: committed=%v err=%v", refused.Committed, err)
	}
	if after := requireReceiverApplicationSnapshot(t, selected); !reflect.DeepEqual(before, after) {
		t.Fatal("refused prospective update reconstructed evidence or changed surviving state, publications or deliveries")
	}
}

func TestProspectiveTerminalReceiverRefusesWithoutMutationOrPublicationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyProspectiveTerminalSender(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "terminal"}, "idempotency_key": "seed"})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			var states, publications, failed int
			if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1`, seed.RunID).Scan(&states); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT count(*) FROM events WHERE run_id=$1 AND event_name='work.ready'`, seed.RunID).Scan(&publications); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT count(*) FROM event_deliveries WHERE event_id=$1 AND status='dead_letter'`, seed.EventID).Scan(&failed); err != nil {
				t.Fatal(err)
			}
			// Construction precedes execution. A rejected handler must preserve
			// its initial header/fields, not erase the successfully constructed tree.
			if states != 1 || publications != 0 || failed != 1 {
				t.Fatalf("terminal planning states=%d publications=%d refused=%d; %s", states, publications, failed, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
			}
			var entityID, stage, fields string
			var headerRevision, fieldRevision, mutations int
			if err := rt.DB.QueryRow(`SELECT fi.entity_id,fi.current_state,fi.revision,CAST(es.fields AS TEXT),es.revision
				FROM flow_instances fi JOIN entity_state es ON es.run_id=fi.run_id AND es.entity_id=fi.entity_id AND es.flow_instance=fi.instance_path
				WHERE fi.run_id=$1 AND fi.instance_path=$2 AND fi.flow_template='.'`, seed.RunID, seed.RunID).Scan(&entityID, &stage, &headerRevision, &fields, &fieldRevision); err != nil {
				t.Fatal(err)
			}
			var values map[string]any
			if err := json.Unmarshal([]byte(fields), &values); err != nil || len(values) != 0 || entityID != seed.RunID || stage != "waiting" || headerRevision != 1 || fieldRevision != 1 {
				t.Fatalf("rejected handler changed construction: entity=%s stage=%s revisions=%d/%d fields=%s err=%v", entityID, stage, headerRevision, fieldRevision, fields, err)
			}
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND domain='authored_field'`, seed.RunID).Scan(&mutations); err != nil || mutations != 0 {
				t.Fatalf("rejected handler journaled business mutations=%d err=%v", mutations, err)
			}
		})
	}
}
