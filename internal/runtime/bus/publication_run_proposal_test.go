package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type publicationRunPreflightTestStore struct {
	InMemoryEventStore
	runlifecycle.OperationOwner
	runID string
	fault error
}

func (s *publicationRunPreflightTestStore) RequireActiveRun(ctx context.Context, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.fault != nil {
		return s.fault
	}
	if runID == "" || runID != s.runID {
		return &runlifecycle.RunNotFoundError{RunID: runID}
	}
	return nil
}

func admitRunProposalEvent(t testing.TB, creating bool) events.AdmittedEvent {
	t.Helper()
	event := eventtest.RunCreatingRootIngress(eventtest.UUID("run-proposal-event"), "task.requested", "operator", "", []byte(`{}`), 0, busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
	if !creating {
		event = eventtest.ExistingRunRootIngress(event.ID(), event.Type(), "operator", "", event.Payload(), 0, busInternalTestRunID, events.EventEnvelope{}, event.CreatedAt())
	}
	event, err := eventtest.AdmitPayload(event, ".", "task.requested")
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	return admitted
}

func TestPublicationRunFixtureRequiresExactKnownRun(t *testing.T) {
	owner := &publicationRunPreflightTestStore{runID: busInternalTestRunID}
	if err := owner.RequireActiveRun(context.Background(), busInternalTestRunID); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []string{"", eventtest.UUID("foreign-fixture-run")} {
		var missing *runlifecycle.RunNotFoundError
		if err := owner.RequireActiveRun(context.Background(), runID); !errors.As(err, &missing) || missing.RunID != runID {
			t.Fatalf("unknown run accepted by fixture owner: run=%q err=%v", runID, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := owner.RequireActiveRun(ctx, busInternalTestRunID); !errors.Is(err, context.Canceled) {
		t.Fatalf("fixture owner lost cancellation: %v", err)
	}
}

func TestRunCreatingRecipientPreviewSharesProposalWithOrdinaryConsumers(t *testing.T) {
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, map[string]string{
		"schema.yaml": "name: root\npins:\n  inputs: [task.requested]\n  outputs: [task.requested]\n",
		"events.yaml": "task.requested:\n",
		"nodes.yaml":  "worker:\n  execution_type: system_node\n  subscribes_to: [task.requested]\n  event_handlers:\n    task.requested:\n      guard: {id: admitted, check: true}\n",
	}))
	missing := &runlifecycle.RunNotFoundError{RunID: busInternalTestRunID}
	independent := errors.New("independent run observation failure")
	for _, test := range []struct {
		name     string
		creating bool
		fault    error
	}{
		{name: "new run", creating: true, fault: missing},
		{name: "existing-run carrier", fault: missing},
		{name: "joined independent failure", creating: true, fault: errors.Join(missing, independent)},
	} {
		t.Run(test.name, func(t *testing.T) {
			selected := newConnectRoutePlanStaticStore()
			bus, err := newScopedTestEventBus(selected, EventBusOptions{
				ContractBundle: source,
				Durable: DurableDependencies{
					RunLifecycle: &publicationRunPreflightTestStore{fault: test.fault},
					Instances:    constructionIndexTestReader{err: errors.New("preview consulted uncommitted native instance evidence")},
				},
				TemplateInstancePlanner: newTestFlowInstanceActivationOwner(nil),
			})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := bus.CheckPublishRecipientPlan(context.Background(), admitRunProposalEvent(t, test.creating).Event())
			if test.name == "new run" {
				if err != nil || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target.Route().FlowInstance != busInternalTestRunID || !plan.DeliveryRoutes[0].ConnectClaim.Empty() {
					t.Fatalf("ordinary root consumer lost its operation-private proposal: plan=%+v err=%v", plan, err)
				}
			} else if !errors.Is(err, test.fault) {
				t.Fatalf("preview changed the original refusal: got=%v want=%v", err, test.fault)
			}
			if len(selected.events) != 0 || len(selected.routes) != 0 {
				t.Fatal("recipient preview persisted construction or delivery")
			}
		})
	}
}

func TestPublicationRunProposalRequiresExactIsolatedNativeAbsence(t *testing.T) {
	missing := &runlifecycle.RunNotFoundError{RunID: busInternalTestRunID}
	independent := errors.New("independent preflight failure")
	for _, test := range []struct {
		name     string
		creating bool
		fault    error
		proposed bool
	}{
		{"existing run with create permission", true, nil, false},
		{"new run", true, missing, true},
		{"wrapped absence", true, fmt.Errorf("preflight: %w", missing), true},
		{"single joined absence", true, errors.Join(missing), true},
		{"existing-run carrier", false, missing, false},
		{"foreign run", true, &runlifecycle.RunNotFoundError{RunID: eventtest.UUID("foreign-run")}, false},
		{"unqualified sentinel", true, runlifecycle.ErrRunNotFound, false},
		{"independent failure", true, independent, false},
		{"joined independent failure", true, errors.Join(missing, independent), false},
		{"joined cancellation", true, errors.Join(missing, context.Canceled), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := &publicationRunPreflightTestStore{runID: busInternalTestRunID, fault: test.fault}
			bus := &EventBus{store: &InMemoryEventStore{}, durable: DurableDependencies{RunLifecycle: owner}}
			proposed, err := bus.requireExistingRunActive(context.Background(), admitRunProposalEvent(t, test.creating))
			if proposed != test.proposed {
				t.Fatalf("proposal = %t, want %t; error = %v", proposed, test.proposed, err)
			}
			if test.proposed || test.fault == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if err != test.fault {
				t.Fatalf("preflight changed the original failure: %v, want %v", err, test.fault)
			}
		})
	}
	admitted := admitRunProposalEvent(t, true)
	if proposed, err := (&EventBus{store: &InMemoryEventStore{}}).requireExistingRunActive(context.Background(), admitted); proposed || err != nil {
		t.Fatalf("unknown run presence became a proposal: %t %v", proposed, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if proposed, err := (&EventBus{store: &InMemoryEventStore{}, durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{}}}).requireExistingRunActive(ctx, admitted); proposed || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation became a proposal: %t %v", proposed, err)
	}
}

func TestProposedRunSelectionConsumesOnlyExactPreparedConstruction(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	ctx := constructionIndexContext(t, source)
	fact, _ := correlation.SourceArtifactFactFromContext(ctx)
	admitted := admitRunProposalEvent(t, true)
	proposal, err := pipeline.NewFlowInstanceRunProposal(fact, admitted)
	if err != nil {
		t.Fatal(err)
	}
	root := flowidentity.Stored(source, ".", busInternalTestRunID, busInternalTestRunID, busInternalTestRunID, "")
	child, err := flowidentity.KeylessChild(source, root, "producer")
	if err != nil {
		t.Fatal(err)
	}
	planner := newTestFlowInstanceActivationOwner(nil)
	prepare := func(identity flowidentity.Instance) pipeline.FlowInstanceActivationPlan {
		t.Helper()
		plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: identity, TriggerEvent: admitted.Event(), OccurredAt: admitted.Event().CreatedAt()})
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	rootPlan, childPlan := prepare(root), prepare(child)
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(source, fact, busInternalTestRunID, "producer", root, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := pipeline.FlowInstanceSelectionRequest{Lookup: lookup, Mode: contracts.FlowInputResolutionModeSelect, Prepared: []pipeline.FlowInstanceActivationPlan{rootPlan, childPlan}, RunProposal: proposal}
	reader := constructionIndexTestReader{err: errors.New("native observation requested before run commit")}
	selected, err := pipeline.PrepareFlowInstanceSelection(ctx, reader, nil, request)
	if err != nil || selected.Proposed == nil || selected.Identity() != child || selected.Observation.Valid() || selected.Activation != nil {
		t.Fatalf("pure selection invented native evidence: %+v %v", selected, err)
	}
	request.Mode = contracts.FlowInputResolutionModeCreate
	if _, err := pipeline.PrepareFlowInstanceSelection(ctx, reader, planner, request); err == nil {
		t.Fatal("explicit create reused an occupied proposed coordinate")
	}
	request.Mode, request.Prepared = contracts.FlowInputResolutionModeSelect, []pipeline.FlowInstanceActivationPlan{rootPlan}
	if _, err := pipeline.PrepareFlowInstanceSelection(ctx, reader, nil, request); !isolatedInstanceLookupMiss(err) {
		t.Fatalf("absent proposed child acquired a native observation: %v", err)
	}
	request.Prepared = []pipeline.FlowInstanceActivationPlan{childPlan}
	if _, err := pipeline.PrepareFlowInstanceSelection(ctx, reader, nil, request); err == nil || isolatedInstanceLookupMiss(err) {
		t.Fatalf("missing proposed parent became ordinary absence: %v", err)
	}
	request.Prepared = []pipeline.FlowInstanceActivationPlan{rootPlan, childPlan}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := pipeline.PrepareFlowInstanceSelection(canceled, reader, nil, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("pure planning concealed cancellation: %v", err)
	}
}

func TestRunConstructionProposalRefusesForeignAndRestoredAuthority(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	fact, _ := correlation.SourceArtifactFactFromContext(constructionIndexContext(t, source))
	admitted := admitRunProposalEvent(t, true)
	proposal, err := pipeline.NewFlowInstanceRunProposal(fact, admitted)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatal(err)
	}
	if err := proposal.Validate(eventtest.UUID("foreign-run"), fact); err == nil {
		t.Fatal("proposal crossed its admitted run")
	}
	if err := proposal.Validate(busInternalTestRunID, foreign); err == nil {
		t.Fatal("proposal crossed its admitted source")
	}
	for _, existing := range []events.AdmittedEvent{{}, admitRunProposalEvent(t, false)} {
		if _, err := pipeline.NewFlowInstanceRunProposal(fact, existing); err == nil {
			t.Fatal("unadmitted or existing-run carrier acquired creation context")
		}
	}
	restored, err := events.RevalidatePersistedEvent(admitted.Event())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.NewFlowInstanceRunProposal(fact, restored); err == nil {
		t.Fatal("durable replay regained creation context")
	}
}
