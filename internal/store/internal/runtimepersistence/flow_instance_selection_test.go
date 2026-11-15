package runtimepersistence

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	swaruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func newR7SelectionProof(t *testing.T, backend string) (receiverConfigActivationFixture, pipeline.FlowInstanceSelectionRequest) {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":        "stages: []\n",
		"worker/schema.yaml": "instance: item_id\nstages:\n  pending: {}\n  done: {final: true}\ninputs:\n  - item.created\n",
		"worker/state.yaml":  "item_id: text\n",
		"worker/events.yaml": "item.created:\n  item_id: text\nitem.finished:\n",
		"worker/nodes.yaml":  "progress:\n  item.finished:\n    advances_to: done\n", "manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "worker/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
	}, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	rootRequest := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	rootRequest.Instance = flowidentity.Stored(rootRequest.ContractBundle, ".", runID, runID, runID, "")
	root, err := f.manager.PrepareFlowInstanceActivation(f.ctx, rootRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
	if !present {
		t.Fatal("selection proof requires admitted source")
	}
	keys, err := pipeline.AdmitFlowInstanceKeyMaterial(rootRequest.ContractBundle, "worker", "business-key")
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(rootRequest.ContractBundle, fact, runID, "worker", root.Identity, keys)
	if err != nil {
		t.Fatal(err)
	}
	constructor := sqliteFlowActivationRequest(f.bundle, "worker", "", "", "")
	constructor.Instance = flowidentity.Instance{}
	constructor.ConstructorInput, constructor.ResolvedKey = "item.created", "business-key"
	constructor.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"business-key"}`), 0, runID, events.EventEnvelope{}, constructor.OccurredAt)
	return f, pipeline.FlowInstanceSelectionRequest{
		Lookup: lookup, Mode: contracts.FlowInputResolutionModeSelectOrCreate,
		MissingInstanceID: "proposed-receiver", Constructor: constructor,
	}
}

func requireR7SelectionNoMutation(t *testing.T, f receiverConfigActivationFixture, operation func()) {
	t.Helper()
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	operation()
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("selection preparation or refusal mutated durable evidence")
	}
}

func TestR7SelectCreateSelectOrCreateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, change := range [][2]string{{"select-or-create", "select"}, {"select", "create"}} {
				t.Run("selected_"+change[0]+"_to_"+change[1], func(t *testing.T) { proveSelectedResolutionMode(t, backend, change[0], change[1]) })
			}
			f, request := newR7SelectionProof(t, backend)
			index := f.store.(pipeline.FlowInstanceIndexReader)
			for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelectOrCreate} {
				t.Run("absent_"+contracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
					request := request
					request.Mode = mode
					requireR7SelectionNoMutation(t, f, func() {
						selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if _, unavailable := selected.UnavailableReceiver(); unavailable {
							t.Fatal("absence produced unavailable occupancy")
						}
						if mode == contracts.FlowInputResolutionModeSelect {
							var missing *pipeline.WorkflowInstanceLookupMiss
							if !errors.As(err, &missing) || selected.Activation != nil || selected.Observation.Valid() {
								t.Fatalf("select created an absent receiver: %+v %v", selected, err)
							}
							return
						}
						if err != nil || selected.Activation == nil || selected.Observation.Valid() || selected.Identity().InstanceID != request.MissingInstanceID || selected.Activation.Instance.InstanceKey != "business-key" {
							t.Fatalf("missing receiver preparation: %+v %v", selected, err)
						}
					})
				})
			}
			storedRequest := request.Constructor
			var err error
			storedRequest.Instance, err = flowidentity.KeyedChild(request.Lookup.Source(), request.Lookup.ParentIdentity(), "worker", "actual-stored-coordinate")
			if err != nil {
				t.Fatal(err)
			}
			storedRequest.Instance.EntityID = uuid.NewString()
			winner, err := f.manager.PrepareFlowInstanceActivation(f.ctx, storedRequest)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, winner); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelectOrCreate} {
				t.Run("occupied_"+contracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
					request := request
					request.Mode = mode
					requireR7SelectionNoMutation(t, f, func() {
						selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if _, unavailable := selected.UnavailableReceiver(); unavailable {
							t.Fatal("active occupancy produced lifecycle refusal")
						}
						if mode == contracts.FlowInputResolutionModeCreate {
							var conflict *pipeline.FlowInstanceActivationConflict
							if !errors.As(err, &conflict) || conflict.Owner.Route != winner.Identity.Route() || selected.Activation != nil {
								t.Fatalf("create hid occupied actual identity: %+v %v", selected, err)
							}
							return
						}
						if err != nil || selected.Activation != nil || !selected.Observation.Valid() || selected.Identity() != winner.Identity {
							t.Fatalf("selection reconstructed an existing receiver: %+v %v", selected, err)
						}
					})
				})
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: request.Lookup.RunID(), Route: winner.Identity.Route()}
			persisted, err := winner.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			mutation := persisted.State
			mutation.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
			mutation.ExpectedRevision, mutation.ExpectedState = 1, mutation.CurrentState
			mutation.CurrentState = "done"
			mutation.EnteredStageAt, mutation.UpdatedAt = winner.OccurredAt.Add(time.Second), winner.OccurredAt.Add(time.Second)
			var claim deliverylifecycle.Claim
			mutation = workflowTargetMutationEntry(t, f, mutation, "item.finished", &claim)
			if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()}}); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelectOrCreate} {
				t.Run("final_stage_"+contracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
					request := request
					request.Mode = mode
					requireR7SelectionNoMutation(t, f, func() {
						selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if mode != contracts.FlowInputResolutionModeSelect {
							if err != nil || selected.Activation == nil || selected.Identity() != winner.Identity || selected.Activation.Instance.CurrentState != "pending" {
								t.Fatalf("finished receiver did not prepare same-identity restart: %+v %v", selected, err)
							}
							return
						}
						if err == nil || selected.Activation != nil || selected.Observation.Valid() {
							t.Fatalf("final-stage occupancy became creation or execution: %+v %v", selected, err)
						}
						unavailable, refused := selected.UnavailableReceiver()
						if !refused || unavailable != owner {
							t.Fatalf("final-stage refusal lost exact occupied receiver: %+v refused=%t", unavailable, refused)
						}
						var terminal *pipeline.NoHandlerError
						if !errors.As(err, &terminal) || terminal.FlowID != "worker" || terminal.Stage != "done" {
							t.Fatalf("final-stage selection lost proven availability refusal: %v", err)
						}
						observed, found, readErr := index.LookupFlowInstance(f.ctx, request.Lookup)
						if readErr != nil || !found || observed.Identity() != winner.Identity {
							t.Fatalf("final-stage receiver became absent: %+v found=%t err=%v", observed, found, readErr)
						}
						header, readErr := observed.WorkflowInstance()
						if readErr != nil || header.Status != "active" || !header.TerminatedAt.IsZero() || header.CurrentState != "done" {
							t.Fatalf("final-stage control accidentally terminated its receiver: %+v err=%v", header, readErr)
						}
					})
				})
			}
			t.Run("restart_exact_replay", func(t *testing.T) {
				var originalBirth []byte
				if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&originalBirth); err != nil {
					t.Fatal(err)
				}
				restart := request
				restart.Mode = contracts.FlowInputResolutionModeCreate
				restart.Constructor.OccurredAt = winner.OccurredAt.Add(2 * time.Second)
				restart.Constructor.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"business-key"}`), 0, owner.RunID, events.EventEnvelope{}, restart.Constructor.OccurredAt)
				selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, restart)
				if err != nil || selected.Activation == nil || selected.Identity() != winner.Identity {
					t.Fatalf("prepare same-identity restart: %+v %v", selected, err)
				}
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				if _, err := committer.CommitFlowInstanceActivation(f.ctx, *selected.Activation); err != nil {
					t.Fatalf("commit restart: %v", err)
				}
				if _, err := committer.CommitFlowInstanceActivation(f.ctx, *selected.Activation); err != nil {
					t.Fatalf("replay restart: %v", err)
				}
				observed, found, err := index.LookupFlowInstance(f.ctx, restart.Lookup)
				if err != nil || !found || observed.Identity() != winner.Identity {
					t.Fatalf("restart changed identity: %+v found=%t err=%v", observed, found, err)
				}
				header, err := observed.WorkflowInstance()
				if err != nil || header.CurrentState != "pending" || header.Status != "active" || !header.TerminatedAt.IsZero() || header.Fields["item_id"] != "business-key" {
					t.Fatalf("restart did not restore initial state: %+v %v", header, err)
				}
				var life, births int
				if err := f.db.QueryRowContext(f.ctx, `SELECT life FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&life); err != nil || life != 2 {
					t.Fatalf("restart/replay life=%d want=2 err=%v", life, err)
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&births); err != nil || births != 1 {
					t.Fatalf("restart changed immutable construction: births=%d err=%v", births, err)
				}
				var retainedBirth []byte
				if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&retainedBirth); err != nil || !bytes.Equal(originalBirth, retainedBirth) || !header.CreatedAt.Equal(winner.Instance.CreatedAt) {
					t.Fatalf("restart rewrote physical construction evidence: %v", err)
				}
			})
			if err := f.workflows.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(winner.Identity.EntityID), winner.OccurredAt.AddDate(0, 0, 1)); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelectOrCreate} {
				t.Run("terminal_"+contracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
					request := request
					request.Mode = mode
					requireR7SelectionNoMutation(t, f, func() {
						selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if err == nil || selected.Activation != nil || selected.Observation.Valid() {
							t.Fatalf("terminal occupancy became creation or execution: %+v %v", selected, err)
						}
						unavailable, refused := selected.UnavailableReceiver()
						if refused != (mode != contracts.FlowInputResolutionModeCreate) || refused && unavailable != owner {
							t.Fatalf("terminal refusal lost exact occupied receiver: %+v refused=%t", unavailable, refused)
						}
						if mode == contracts.FlowInputResolutionModeCreate {
							var conflict *pipeline.FlowInstanceActivationConflict
							if !errors.As(err, &conflict) || conflict.Owner != owner {
								t.Fatalf("terminal create lost occupied identity: %v", err)
							}
						} else {
							var terminated *pipeline.TerminatedReceiverError
							if !errors.As(err, &terminated) || terminated.FlowID != "worker" {
								t.Fatalf("terminal selection lost proven availability refusal: %v", err)
							}
						}
						observed, found, readErr := index.LookupFlowInstance(f.ctx, request.Lookup)
						if readErr != nil || !found || observed.Identity() != winner.Identity {
							t.Fatalf("refused receiver became absent: %+v found=%t err=%v", observed, found, readErr)
						}
					})
				})
			}
		})
	}
}

func proveSelectedResolutionMode(t *testing.T, backend, originalMode, selectedMode string) {
	t.Helper()
	documents := map[string]string{
		"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":          fmt.Sprintf("stages: []\ninputs: [item.created, item.requested, checkpoint]\nconnect:\n  - {event: item.created, from: ., to: worker, resolution: select-or-create}\n  - {event: item.requested, from: ., to: worker, resolution: %s}\n", originalMode),
		"events.yaml":          "item.created:\n  item_id: text\nitem.requested:\n  item_id: text\ncheckpoint:\n",
		"nodes.yaml":           "checkpoint:\n  checkpoint: {}\n",
		"worker/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"worker/schema.yaml":   "instance: item_id\nstages:\n  pending: {}\ninputs: [item.created, item.requested]\n",
		"worker/state.yaml":    "item_id: text\n",
		"worker/nodes.yaml":    "receiver:\n  item.created: {}\n  item.requested: {}\n",
	}
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
	source := semanticview.Wrap(f.bundle)
	runID := correlation.RunIDFromContext(f.ctx)
	rootRequest := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	rootRequest.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
	rootPlan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, rootRequest)
	if err != nil {
		t.Fatal(err)
	}
	committer := agentFixtureFlowActivationCommitter{store: f.store}
	if result, err := committer.CommitFlowInstanceActivation(f.ctx, rootPlan); err != nil || !result.Acknowledged {
		t.Fatalf("mode source root: %+v %v", result, err)
	}
	child, err := flowidentity.KeyedChild(source, rootPlan.Identity, "worker", "stored-worker")
	if err != nil {
		t.Fatal(err)
	}
	construct := sqliteFlowActivationRequest(f.bundle, "worker", "", "", "")
	construct.Instance, construct.ConstructorInput, construct.ResolvedKey = child, "item.created", "business-key"
	construct.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "fixture", "", []byte(`{"item_id":"business-key"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if originalMode == "select" {
		construct.ConstructorInput = "item.requested"
		construct.TriggerEvent = eventtest.ExistingRunRootIngress(construct.TriggerEvent.ID(), "item.requested", "fixture", "", construct.TriggerEvent.Payload(), 0, runID, events.EventEnvelope{}, construct.TriggerEvent.CreatedAt())
	}
	fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
	if !present {
		t.Fatal("mode proof lacks original source fact")
	}
	admit := swaruntime.NewRuntimePayloadAdmitter(nil, source, fact)
	admission, err := admit(f.ctx, construct.TriggerEvent, events.EventPayloadProducer(construct.TriggerEvent, "."))
	if err != nil {
		t.Fatal(err)
	}
	construct.TriggerEvent, err = events.ApplyPayloadAdmission(construct.TriggerEvent, admission)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, construct)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := committer.CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
		t.Fatalf("mode source worker: %+v %v", result, err)
	}
	eb, err := newStoreTestEventBus(t, f.store.(storeTestDurableEventBusStore), runtimebus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, PayloadAdmitter: admit})
	if err != nil {
		t.Fatal(err)
	}
	commitPending := func(event events.Event) {
		t.Helper()
		admission, err := admit(f.ctx, event, events.EventPayloadProducer(event, "."))
		if err != nil {
			t.Fatal(err)
		}
		event, err = events.ApplyPayloadAdmission(event, admission)
		if err != nil {
			t.Fatal(err)
		}
		routes, err := eb.CheckPublishRecipientPlan(f.ctx, event)
		if err != nil || routes.TargetFailure != "" || len(routes.DeliveryRoutes) == 0 {
			t.Fatalf("mode original admission: %+v %v", routes, err)
		}
		if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, routes.DeliveryRoutes); err != nil {
			t.Fatal(err)
		}
	}
	if originalMode == "select" {
		commitPending(construct.TriggerEvent)
	} else {
		pending := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.requested", "fixture", "", []byte(`{"item_id":"business-key"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
		commitPending(pending)
	}
	checkpoint := eventtest.ExistingRunRootIngress(uuid.NewString(), "checkpoint", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	commitPending(checkpoint)
	captureFanOutBarrierForkRevision(t, f.ctx, f.db, runID, backend == "postgres")
	members := map[string][]byte{}
	for _, entry := range f.bundle.SourceArtifact.Entries() {
		members[entry.Label()] = entry.Bytes()
	}
	old := "resolution: " + originalMode + "}"
	rootBody := string(members["schema.yaml"])
	needle := "event: item.requested, from: ., to: worker, " + old
	if strings.Count(rootBody, needle) != 1 {
		t.Fatal("mode proof lost exact selected input")
	}
	members["schema.yaml"] = []byte(strings.Replace(rootBody, needle, "event: item.requested, from: ., to: worker, resolution: "+selectedMode+"}", 1))
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "selected-mode", artifact)
	repo := canonicalrouting.RepoRoot(t)
	target, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.(selectedSourceArtifactStore).EnsureRootedSource(f.ctx, rooted); err != nil {
		t.Fatal(err)
	}
	targetFact, err := correlation.NewSourceArtifactFact(artifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	targetCtx := correlation.WithRootAdmission(correlation.WithSourceArtifactFact(testAuthorActivityContextForBundle(artifact.BundleHash()), targetFact), rooted.Admission())
	targetScope, err := authoractivity.BundleScopeForTarget(f.ctx, artifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	targetCtx = authoractivity.WithScope(targetCtx, targetScope)
	targetCtx = originalStoreFixtureContext(t, targetCtx, f.store)
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: artifact.BundleHash()}
	request := prepareSelectedStoreMaterializationForTest(t, targetCtx, f.store, runID, checkpoint.ID(), selection, artifact.BundleHash())
	owner := f.store.(runforkexecution.SelectedContractForkLifecycle)
	materialized, err := owner.MaterializeRunForkForSelectedContractExecution(targetCtx, request)
	if err != nil || materialized.ForkRunID == "" {
		t.Fatalf("harmless resolution change %s -> %s refused: %+v %v", originalMode, selectedMode, materialized, err)
	}
	targetSource := semanticview.Wrap(target)
	graph := pinrouting.CompileConnectGraph(targetSource)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatal(issues)
	}
	var mode contracts.FlowInputResolutionMode
	for _, connect := range graph.Plans() {
		if connect.SourceEndpoint().Readback().LocalEvent == "item.requested" && connect.ReceiverEndpoint().Readback().LocalEvent == "item.requested" && connect.InstanceKey() != nil {
			mode = connect.InstanceKey().Mode()
		}
	}
	if contracts.FlowInputResolutionModeCode(mode) != selectedMode {
		t.Fatalf("selected compiled edge mode=%s want=%s", contracts.FlowInputResolutionModeCode(mode), selectedMode)
	}
	parent := flowidentity.Stored(targetSource, ".", materialized.ForkRunID, materialized.ForkRunID, materialized.ForkRunID, "")
	keys, err := pipeline.AdmitFlowInstanceKeyMaterial(targetSource, "worker", "business-key")
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(targetSource, targetFact, materialized.ForkRunID, "worker", parent, keys)
	if err != nil {
		t.Fatal(err)
	}
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(targetCtx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := pipeline.PrepareFlowInstanceSelection(targetCtx, f.store.(pipeline.FlowInstanceIndexReader), f.manager, pipeline.FlowInstanceSelectionRequest{Lookup: lookup, Mode: mode, MissingInstanceID: "must-not-create"})
	if selectedMode == "select" {
		if err != nil || selected.Activation != nil || !selected.Observation.Valid() || selected.Identity().EntityID != plan.Identity.EntityID {
			t.Fatalf("changed select lost retained owner: %+v %v", selected, err)
		}
	} else {
		var conflict *pipeline.FlowInstanceActivationConflict
		if !errors.As(err, &conflict) || selected.Activation != nil {
			t.Fatalf("genuinely new create hid occupied receiver: %+v %v", selected, err)
		}
	}
	reused, err := owner.RequireMaterializedRunForkForSelectedContractExecution(targetCtx, materialized.ForkRunID, request)
	if err != nil || reused.ForkRunID != materialized.ForkRunID {
		t.Fatalf("exact continuation re-created occupied receiver: %+v %v", reused, err)
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(targetCtx, f.store)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("mode admission/replay mutated retained state: %v", err)
	}
	if selectedMode == "create" {
		// The original publication owned this construction; a distinct later
		// publication does not acquire that recorded creation admission.
		other := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.requested", "fixture", "", []byte(`{"item_id":"business-key"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
		commitPending(other)
		captureFanOutBarrierForkRevision(t, f.ctx, f.db, runID, backend == "postgres")
		before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
		if err != nil {
			t.Fatal(err)
		}
		_, err = prepareSelectedStoreForkForTest(t, targetCtx, f.store, runID, other.ID(), selection, artifact.BundleHash())
		if err == nil || !strings.Contains(err.Error(), "historical create selector does not own the captured constructor") {
			t.Fatalf("new duplicate was not refused at counterfactual admission: %v", err)
		}
		after, readErr := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
		if readErr != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("refused new duplicate mutated native state: %v", readErr)
		}
	}
}

func TestR7ConstructionCommitRechecksSelectorAndPathBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request := newR7SelectionProof(t, backend)
			index := f.store.(pipeline.FlowInstanceIndexReader)
			first, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
			if err != nil || first.Activation == nil {
				t.Fatalf("first pure creation: %+v %v", first, err)
			}
			requireR7SelectionNoMutation(t, f, func() {
				preview := request
				preview.Prepared = []pipeline.FlowInstanceActivationPlan{*first.Activation}
				for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeSelectOrCreate} {
					preview.Mode = mode
					selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, preview)
					if err != nil || selected.Activation != nil || selected.Observation.Valid() || selected.Proposed == nil || selected.Identity() != first.Identity() {
						t.Fatalf("prepared creation did not feed later selection: %+v %v", selected, err)
					}
				}
				preview.Mode = contracts.FlowInputResolutionModeCreate
				if _, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, preview); err == nil {
					t.Fatal("second explicit create reused an operation-local proposal")
				}
			})
			request.MissingInstanceID = "different-proposed-path"
			second, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
			if err != nil || second.Activation == nil {
				t.Fatalf("second pure creation: %+v %v", second, err)
			}
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			if _, err := committer.CommitFlowInstanceActivation(f.ctx, *first.Activation); err != nil {
				t.Fatal(err)
			}
			requireR7SelectionNoMutation(t, f, func() {
				committed, err := committer.CommitFlowInstanceActivation(f.ctx, *second.Activation)
				var conflict *pipeline.FlowInstanceActivationConflict
				if !errors.As(err, &conflict) || committed.Acknowledged {
					t.Fatalf("stale absent selection created another coordinate: %+v %v", committed, err)
				}
			})
			selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
			if err != nil || selected.Activation != nil || selected.Identity() != first.Identity() {
				t.Fatalf("rolled-back preparation did not reuse durable winner: %+v %v", selected, err)
			}
			request.Mode = contracts.FlowInputResolutionModeCreate
			requireR7SelectionNoMutation(t, f, func() {
				if _, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request); err == nil {
					t.Fatal("explicit create became implicit winner reuse")
				}
			})
		})
	}
}

func TestR7PreparedSelectionRechecksDurableAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"unchanged", "run_stopped", "source_replaced", "stored_winner", "terminal_winner"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				f, request := newR7SelectionProof(t, backend)
				index := f.store.(pipeline.FlowInstanceIndexReader)
				selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
				if err != nil || selected.Activation == nil || selected.Observation.Valid() {
					t.Fatalf("prepare from admitted absence: %+v err=%v", selected, err)
				}
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				var winner pipeline.FlowInstanceActivationPlan
				// The prepared operation stays unchanged while a durable owner wins.
				switch change {
				case "run_stopped":
					_, _, err = f.store.(runlifecycle.OperationOwner).MarkTerminalRun(f.ctx, runlifecycle.TerminalRequest{
						RunID: request.Lookup.RunID(), State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
					})
				case "source_replaced":
					replacement := loadLifecyclePersistenceFixtureForTest(t, map[string]string{"schema.yaml": "stages:\n  active: {}\n# replacement selection source\n", "manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n"})
					rooted, rootErr := replacement.RootedSource()
					if rootErr != nil {
						t.Fatal(rootErr)
					}
					if replacement.SourceArtifact.BundleHash() == f.bundle.SourceArtifact.BundleHash() {
						t.Fatal("replacement selection source reused the current artifact")
					}
					sourceartifactfixture.RequireArtifact(t, f.ctx, f.store.(sourceartifactfixture.Writer), replacement.SourceArtifact)
					_, err = f.store.(runlifecycle.OperationOwner).ReviseRunSource(f.ctx, runlifecycle.SourceRevisionRequest{
						RunID: request.Lookup.RunID(), Source: sourceartifactfixture.FactFor(replacement.SourceArtifact), RootAdmission: rooted.Admission(),
					})
				case "stored_winner", "terminal_winner":
					constructor := request.Constructor
					constructor.Instance, err = flowidentity.KeyedChild(request.Lookup.Source(), request.Lookup.ParentIdentity(), "worker", "different-stored-winner")
					if err != nil {
						t.Fatal(err)
					}
					constructor.Instance.EntityID = uuid.NewString()
					winner, err = f.manager.PrepareFlowInstanceActivation(f.ctx, constructor)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = committer.CommitFlowInstanceActivation(f.ctx, winner); err != nil {
						t.Fatal(err)
					}
					if winner.Identity.InstancePath == selected.Identity().InstancePath || winner.Identity.EntityID == flowidentity.EntityID(winner.Identity.InstancePath) {
						t.Fatal("winner must have a different path and non-derived entity identity")
					}
					if change == "terminal_winner" {
						owner := flowidentity.RunScopedFlowInstance{RunID: request.Lookup.RunID(), Route: winner.Identity.Route()}
						err = f.workflows.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(winner.Identity.EntityID), winner.OccurredAt.Add(time.Hour))
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if change == "unchanged" {
					committed, err := committer.CommitFlowInstanceActivation(f.ctx, *selected.Activation)
					if err != nil || !committed.Acknowledged || !committed.Created || committed.Plan.Identity != selected.Identity() {
						t.Fatalf("unchanged preparation lost exact construction: %+v err=%v", committed, err)
					}
					observed, found, err := index.LookupFlowInstance(f.ctx, request.Lookup)
					if err != nil || !found || observed.Identity() != selected.Identity() {
						t.Fatalf("construction lost admitted stored identity: %+v found=%t err=%v", observed, found, err)
					}
					return
				}
				requireR7SelectionNoMutation(t, f, func() {
					committed, err := committer.CommitFlowInstanceActivation(f.ctx, *selected.Activation)
					if err == nil || committed.Acknowledged || committed.Created {
						t.Fatalf("earlier absence granted commit after %s: %+v err=%v", change, committed, err)
					}
					if change == "stored_winner" || change == "terminal_winner" {
						var conflict *pipeline.FlowInstanceActivationConflict
						if !errors.As(err, &conflict) || conflict.Owner.Route != selected.Identity().Route() {
							t.Fatalf("stale creation lost original operation identity: %v", err)
						}
						observed, found, err := index.LookupFlowInstance(f.ctx, request.Lookup)
						if err != nil || !found || observed.Identity() != winner.Identity {
							t.Fatalf("conflict changed the actual winner: %+v found=%t err=%v", observed, found, err)
						}
						fresh, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if change == "stored_winner" {
							if err != nil || fresh.Activation != nil || fresh.Identity() != winner.Identity {
								t.Fatalf("new selection did not reuse the durable winner: %+v err=%v", fresh, err)
							}
						} else {
							var terminal *pipeline.TerminatedReceiverError
							if !errors.As(err, &terminal) || fresh.Activation != nil || fresh.Observation.Valid() {
								t.Fatalf("new selection resurrected the terminal winner: %+v err=%v", fresh, err)
							}
						}
					}
				})
			})
		}
	}
}
