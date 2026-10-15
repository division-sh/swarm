package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type constructedInputAgent struct {
	selectedContractForkTestAgent
	observe func(context.Context, events.Event) error
}

func (a *constructedInputAgent) OnEvent(ctx context.Context, event events.Event) ([]events.Event, error) {
	if a.observe != nil {
		if err := a.observe(ctx, event); err != nil {
			return nil, err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runIDs = append(a.runIDs, event.RunID())
	a.eventIDs = append(a.eventIDs, event.ID())
	return nil, nil
}

// The source headers are explicit constructor fixtures. Selected preparation,
// materialization, delivery, agent admission and settlement execute normally.
func TestSelectedConstructedAgentInputExecutesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSelectedConstructedAgentInput(t, backend, false, false) })
	}
}

func TestSelectedNestedConstructedAgentInputExecutesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fields := range []bool{false, true} {
			name := "fieldless"
			if fields {
				name = "field_bearing"
			}
			t.Run(backend+"/"+name, func(t *testing.T) { proveSelectedConstructedAgentInput(t, backend, true, fields) })
		}
	}
}

func TestSelectedHistoricalPredecessorScanBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveSelectedConstructedAgentInput(t, backend, false, false, selectedConstructedInputProof{scan: true})
		})
	}
}

func TestSelectedConstructedRecoveryKeepsCapturedVersionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveSelectedConstructedAgentInput(t, backend, false, false, selectedConstructedInputProof{changedVersionRecovery: true})
		})
	}
}

type selectedConstructedInputProof struct {
	scan, changedVersionRecovery bool
}

type selectedHistoricalScanProbe struct {
	SelectedContractReplayPersistence
	store deliverylifecycle.Store
}

var errSelectedHistoricalScanCut = errors.New("selected historical scan component cut")

// Component proof: commit an acknowledged prepared-publication handoff through
// its real owner, then inspect pending eligibility before execution consumes it.
func (p selectedHistoricalScanProbe) CommitSelectedForkEvent(ctx context.Context, req bus.CommitSelectedForkEventRequest) (bus.CommittedSelectedForkEvent, error) {
	disposition := pipelineobligation.Acknowledged("selected_predecessor_component")
	req.Commit.Disposition = &disposition
	result, err := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, req)
	if err != nil {
		return result, err
	}
	page, err := p.store.ScanDeliveryContinuations(ctx, req.Commit.DeliveryAuthority, deliverylifecycle.ContinuationCursor{}, 64)
	if err != nil {
		return result, err
	}
	for _, proof := range result.DeliveryHandoffs {
		found := slices.ContainsFunc(page.Items, func(item deliverylifecycle.ContinuationItem) bool {
			return item.DeliveryID == proof.DeliveryID() && item.Disposition == deliverylifecycle.ClaimAcquired && item.Invariant == nil && item.Snapshot.ClaimVersion == 0
		})
		if !found {
			return result, fmt.Errorf("historical pending continuation rejected: delivery=%s page=%+v", proof.DeliveryID(), page)
		}
	}
	if len(result.DeliveryHandoffs) == 0 {
		return result, fmt.Errorf("historical scan proof never reached a durable receiver")
	}
	// Stop this component before ordinary publication dispatch: its initial
	// disposition was already committed, so it must not be settled a second time.
	return result, errSelectedHistoricalScanCut
}

func proveSelectedConstructedAgentInput(t *testing.T, backend string, nested, fields bool, proofs ...selectedConstructedInputProof) {
	t.Helper()
	var proof selectedConstructedInputProof
	if len(proofs) != 0 {
		proof = proofs[0]
	}
	var db *sql.DB
	var selected startupownership.Store
	var owner SelectedContractExecutionOwner
	if backend == "sqlite" {
		s := storetest.StartSQLiteRuntimeStore(t)
		selected, db, owner = s, storetest.Database(s), selectedContractSQLiteExecutionOwnerForTest(t, s)
	} else {
		_, db, _ = testutil.StartPostgres(t)
		s := storetest.AdmitPostgresRuntimeStore(t, db)
		selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
	}
	ctx, repo := runForkTestContext(t), runForkExecutionRepoRoot(t)
	root := canonicalrouting.CopySelectedConstructedAgentInput(t)
	if nested {
		root = canonicalrouting.CopySelectedNestedConstructedAgentInput(t, fields)
	}
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	runID, eventID := uuid.NewString(), uuid.NewString()
	at := time.Unix(1700002200, 0).UTC()
	bundle, _ := semanticview.Bundle(loaded.Source)
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID,
		BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)})
	payload := []byte(`{}`)
	anchorType := events.EventType("task.assigned")
	if nested {
		payload = []byte(`{"work_id":"one"}`)
		anchorType = "construction.started"
	}
	input := eventtest.OperatorInjectedWithRoutingSource(eventID, anchorType, "operator", "", payload, 0, runID, nil,
		events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
	input, err = eventtest.AdmitPayload(input, ".", string(anchorType))
	if err != nil {
		t.Fatal(err)
	}
	rootIdentity := flowidentity.Stored(loaded.Source, ".", runID, runID, runID, "")
	instances := []flowidentity.Instance{rootIdentity}
	var child flowidentity.Instance
	if !nested {
		child, err = flowidentity.KeylessChild(loaded.Source, rootIdentity, "child")
		if err != nil {
			t.Fatal(err)
		}
		instances = append(instances, child)
	} else {
		for _, key := range []string{"one", "two"} {
			parent := flowidentity.Derive(loaded.Source, "templ", key)
			parent.ParentRoute = flowidentity.ParentRoute{FlowID: ".", FlowInstance: runID, EntityID: runID}
			parent.ParentEntityID = runID
			instances = append(instances, parent)
			for _, branch := range []string{"child", "audit"} {
				current := parent
				for _, flow := range []string{"templ/" + branch, "templ/" + branch + "/leaf"} {
					current, err = flowidentity.KeylessChild(loaded.Source, current, flow)
					if err != nil {
						t.Fatal(err)
					}
					instances = append(instances, current)
					if key == "one" && flow == "templ/child/leaf" {
						child = current
					}
				}
			}
		}
	}
	writeCtx := effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact), input.ExecutionMode())
	for _, identity := range instances {
		var constructorInput []string
		constructorEvent := input
		if nested && identity.TemplateID == "templ" {
			constructorInput = []string{string(anchorType)}
			payload, err := json.Marshal(map[string]any{"work_id": identity.InstanceID})
			if err != nil {
				t.Fatal(err)
			}
			constructorEvent = eventtest.OperatorInjectedWithRoutingSource(uuid.NewString(), anchorType, "operator", "", payload, 0, runID, nil,
				events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
			constructorEvent, err = eventtest.AdmitPayload(constructorEvent, ".", string(anchorType))
			if err != nil {
				t.Fatal(err)
			}
		}
		command := selectedExecutionSourceFlowCommand(t, writeCtx, loaded, constructorEvent, identity, constructorInput...)
		result, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(writeCtx, command)
		if err != nil || !result.Acknowledged || !result.Created {
			t.Fatalf("component source construction: result=%#v err=%v", result, err)
		}
	}
	if nested {
		// This fixture anchor carries lineage, not admitted subscriber work.
		acknowledged := pipelineobligation.Acknowledged("construction_fixture_anchor")
		storetest.CommitSemanticEventWithInitialFacts(t, writeCtx, selected, input, nil, pipelineobligation.ScopeDirect, &acknowledged)
		lineage := events.EventLineage{RunID: runID, ParentEventID: input.ID(), ExecutionMode: input.ExecutionMode()}
		routingSource := eventtest.StaticFlowRoutingSource(child.TemplateID, child.InstancePath, child.EntityID)
		publication, err := pinrouting.AdmitPublicationIdentity(child.TemplateID, "work.ready", routingSource)
		if err != nil {
			t.Fatal(err)
		}
		input = eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), publication, eventtest.Producer(events.EventProducerPlatform, "workflow"), "", payload, 1, lineage,
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, child.EntityID), child.InstancePath),
			routingSource, at.Add(time.Second))
		input, err = eventtest.AdmitPayload(input, child.TemplateID, "work.ready")
		if err != nil {
			t.Fatal(err)
		}
		eventID = input.ID()
	}
	materialized, err := manager.ConstructedFlowMaterialization(loaded.Source, runID, child)
	if err != nil || len(materialized.Agents) != 1 {
		t.Fatalf("exact constructed child declaration: plan=%#v err=%v", materialized, err)
	}
	record, err := materialized.Agents[0].Materialize(runID)
	if err != nil {
		t.Fatal(err)
	}
	storetest.CommitSemanticEventWithRoutes(t, writeCtx, selected, input, []events.DeliveryRoute{{
		Recipient: events.MustAgentDeliveryRecipient(record.Config.ID), AgentIdentity: record.Config.Identity,
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: child.TemplateID, FlowInstance: child.InstancePath}),
	}}, pipelineobligation.ScopeSubscribed)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if backend == "sqlite" {
		_, err = runforkrevision.CaptureSQLite(ctx, tx, runID, runforkrevision.AllFamilies()...)
	} else {
		_, err = runforkrevision.Capture(ctx, tx, runID, runforkrevision.AllFamilies()...)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if nested {
		plan, err := selected.(interface {
			PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
		}).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID})
		if err != nil {
			t.Fatal(err)
		}
		frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: loaded.Source})
		if err != nil {
			t.Fatal(err)
		}
		history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: loaded.Source, FrontierAdmission: frontier})
		if err != nil {
			t.Fatal(err)
		}
		topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: history})
		if err != nil || !topology.DynamicTopologySupported {
			t.Fatalf("exact constructed route proof: frontier=%+v history=%+v topology=%+v err=%v", frontier, history, topology, err)
		}
	}
	before := selectedPreparationDatabaseSnapshot(t, db, backend)
	capturedVersion := loaded.Source.WorkflowVersion()
	var sourceLoader SelectedContractSourceLoader = loader
	if proof.changedVersionRecovery {
		if err := os.WriteFile(filepath.Join(root, "child/prompts/worker.md"), []byte("Consume the assigned task using the compatible selected version.\n"), 0600); err != nil {
			t.Fatal(err)
		}
		loaded, err = loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
		if err != nil || loaded.Source.WorkflowVersion() == capturedVersion {
			t.Fatalf("recovery proof needs distinct captured and selected versions: captured=%s err=%v", capturedVersion, err)
		}
		selectedBundle, _ := semanticview.Bundle(loaded.Source)
		sourceartifactfixture.RequireArtifact(t, ctx, selected.(sourceartifactfixture.Writer), selectedBundle.SourceArtifact)
		sourceLoader = SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected.(SourceArtifactSelectedContractSourceStore)}
	}
	if proof.scan {
		owner.ports.replay = selectedHistoricalScanProbe{SelectedContractReplayPersistence: owner.ports.replay, store: selected.(deliverylifecycle.Store)}
	}
	agents := map[agentidentity.Identity]*constructedInputAgent{}
	request := SelectedContractExecutionRequest{
		SourceRunID: runID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: sourceLoader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live,
			ProcessCapability: selectedContractTestProcessCapability(t, ctx, selected),
			Config:            &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
			AgentFactory: func(cfg actors.AgentConfig) (manager.Agent, error) {
				identity, err := cfg.ConcreteIdentity()
				if err != nil {
					return nil, err
				}
				agent := &constructedInputAgent{observe: func(ctx context.Context, event events.Event) error {
					err := observeSelectedConstructedInput(ctx, selected, event)
					if err != nil {
						t.Logf("selected admission observation: %v", err)
					}
					return err
				}}
				agent.Configure(cfg)
				agents[identity] = agent
				return agent, nil
			},
		},
	}
	var proveRecovery func(string)
	if proof.changedVersionRecovery {
		request.ContractSelection = runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: loaded.SourceArtifactFact.BundleHash()}
		proveRecovery = prepareSelectedConstructedRecovery(t, ctx, owner, request, loaded)
	}
	result, err := ExecuteSelectedContractRunFork(ctx, request)
	if proof.scan {
		if !errors.Is(err, errSelectedHistoricalScanCut) {
			t.Fatalf("historical scan did not reach its exact admitted component cut: %v", err)
		}
	} else {
		if err != nil || !result.Activation.Activated || result.ExecutedEventCount != 1 {
			t.Fatalf("actual selected constructed-input execution: result=%#v err=%v", result, err)
		}
		wantActors := 1
		if nested {
			wantActors = 8
		}
		if result.AgentRuntimeMaterialization == nil || len(result.AgentRuntimeMaterialization.ConfiguredAgentIdentities) != wantActors || len(agents) != wantActors {
			t.Fatalf("selected runtime omitted known actors: proof=%+v agents=%d", result.AgentRuntimeMaterialization, len(agents))
		}
		var executions int
		for identity, agent := range agents {
			if identity.RunID != result.Materialization.ForkRunID {
				t.Fatal("source/sibling actor adopted into selected runtime")
			}
			if nested && (identity.Route.InstancePath == "templ/child" || identity.Route.InstancePath == "templ/audit") {
				t.Fatal("authored-path phantom prepared")
			}
			if seen := agent.SeenRunIDs(); len(seen) != 0 {
				if len(seen) != 1 || seen[0] != result.Materialization.ForkRunID {
					t.Fatalf("actor escaped exact selected owner: %v", seen)
				}
				executions++
			}
		}
		if executions != 1 {
			t.Fatalf("fixed frontier executed %d actors, want one", executions)
		}
		workflow := pipeline.NewWorkflowPersistence(selected.(pipeline.WorkflowPersistenceOwner))
		for _, source := range instances[1:] {
			childOwner := flowidentity.RunScopedFlowInstance{RunID: result.Materialization.ForkRunID, Route: source.Route()}
			header, found, err := workflow.LoadWorkflowInstance(ctx, childOwner)
			parent, parentErr := runfork.ProjectParentRoute(runID, childOwner.RunID, source.ParentRoute)
			if err != nil || parentErr != nil || !found || header.ParentFlowID != parent.FlowID || header.ParentFlowInstance != parent.FlowInstance || header.ParentEntityID != parent.EntityID {
				t.Fatalf("fork header lost exact constructor parent: found=%v header=%#v err=%v parentErr=%v", found, header, err, parentErr)
			}
			if fields && source.TemplateID != "templ" && header.Fields["value"] != "retained" {
				t.Fatalf("fork companion lost retained business field: %+v", header.Fields)
			}
			if proof.changedVersionRecovery && header.WorkflowVersion != capturedVersion {
				t.Fatalf("selected execution rewrote captured header version: got=%s want=%s", header.WorkflowVersion, capturedVersion)
			}
		}
		if proof.changedVersionRecovery {
			proveRecovery(result.Materialization.ForkRunID)
		}
	}
	after := selectedPreparationDatabaseSnapshot(t, db, backend)
	for table, rows := range before {
		if table == "runs" {
			continue
		}
		for _, row := range rows {
			if strings.Contains(row, runID) && !slices.Contains(after[table], row) {
				t.Fatalf("fork changed source evidence in %s: %s", table, row)
			}
		}
	}

}

func prepareSelectedConstructedRecovery(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, request SelectedContractExecutionRequest, loaded LoadedSelectedContractSource) func(string) {
	t.Helper()
	ctx = correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
	plan, err := owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: request.SourceRunID, At: request.At})
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: loaded.Source, ContractSelection: request.ContractSelection})
	if err != nil {
		t.Fatal(err)
	}
	history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: loaded.Source, ContractSelection: request.ContractSelection, FrontierAdmission: frontier})
	if err != nil {
		t.Fatal(err)
	}
	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: history})
	if err != nil {
		t.Fatal(err)
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{Admission: frontier, RouteAdmission: history, RouteTopology: topology})
	if err != nil {
		t.Fatal(err)
	}
	runtime, readiness, err := prepareSelectedContractWorkflowReadiness(ctx, owner.ports.replay, loaded, *model.RecipientPlanning, plan, frontier, []string{request.At}, request.AgentRuntime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.releaseWorkspaceProjection(); err != nil {
			t.Error(err)
		}
	})
	projection, err := readiness.Projection()
	if err != nil {
		t.Fatal(err)
	}
	states, err := projection.MaterializationStates()
	if err != nil {
		t.Fatal(err)
	}
	return func(forkRunID string) {
		ctx := correlation.WithRunID(ctx, forkRunID)
		before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, owner.ports.events)
		if err != nil {
			t.Fatal(err)
		}
		for _, variant := range []string{"captured_header_current_readiness", "stale_selected_version", "foreign_entity", "foreign_mode"} {
			t.Run(variant, func(t *testing.T) {
				selectedStates := slices.Clone(states)
				for i := range selectedStates {
					if selectedStates[i].AddressKind != runfork.RunForkSelectedContractWorkflowStateExact {
						continue
					}
					switch variant {
					case "stale_selected_version":
						selectedStates[i].WorkflowVersion = "stale-selected-version"
					case "foreign_entity":
						selectedStates[i].EntityID = uuid.NewString()
					case "foreign_mode":
						selectedStates[i].Mode = "template"
					}
				}
				bound, err := bindRecoveredSelectedContractAgentRuntime(ctx, owner.ports.workflow, forkRunID, loaded, selectedStates, runtime)
				if variant == "captured_header_current_readiness" {
					if err != nil || len(bound.Records) != 1 || bound.Records[0].Config.Identity.RunID != forkRunID || bound.Records[0].Config.EffectiveEntityID() != "" {
						t.Fatalf("exact changed-version recovery did not bind the original entityless actor: records=%+v err=%v", bound.Records, err)
					}
				} else {
					want := "disagrees with exact entity/type/workflow descriptor"
					if variant == "stale_selected_version" {
						want = "disagrees with regenerated fork state"
					}
					if err == nil || len(bound.Records) != 0 || !strings.Contains(err.Error(), want) {
						t.Fatalf("hostile regenerated recovery state missed its exact gate: variant=%s err=%v want=%s", variant, err, want)
					}
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, owner.ports.events)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("recovery binding changed construction, fields, delivery or execution evidence: %v", err)
				}
			})
		}
	}
}

func observeSelectedConstructedInput(ctx context.Context, selected startupownership.Store, event events.Event) error {
	rows, err := storetest.ReadReceiverDeliveryStorage(ctx, selected, event.RunID())
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.EventID == event.ID() {
			return observeHistoricalClaimEntrances(ctx, selected.(deliverylifecycle.Store), row.DeliveryID)
		}
	}
	return fmt.Errorf("admitted selected receiver lacks its exact delivery")
}

func observeHistoricalClaimEntrances(ctx context.Context, store deliverylifecycle.Store, deliveryID string) error {
	before, err := store.Snapshot(ctx, deliveryID)
	if err != nil {
		return err
	}
	batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{deliveryID})
	if err != nil || len(batch) != 1 || batch[0].Invariant != nil || batch[0].Disposition != deliverylifecycle.ClaimBusy {
		return fmt.Errorf("historical batch rejected admitted receiver: %+v err=%v", batch, err)
	}
	single, err := store.ObserveDeliveryContinuation(ctx, before.Authority, deliveryID)
	if err != nil || single.Invariant != nil || single.Disposition != deliverylifecycle.ClaimBusy {
		return fmt.Errorf("historical continuation rejected admitted receiver: %+v err=%v", single, err)
	}
	after, err := store.Snapshot(ctx, deliveryID)
	if err != nil || !reflect.DeepEqual(before, after) {
		return fmt.Errorf("historical observation mutated exact claim: err=%v", err)
	}
	return nil
}
