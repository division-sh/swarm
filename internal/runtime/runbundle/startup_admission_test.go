package runbundle

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type admissionAvailabilityReader func(context.Context) ([]Availability, error)

func (r admissionAvailabilityReader) ActiveNonStandingRunBundleAvailabilities(ctx context.Context) ([]Availability, error) {
	return r(ctx)
}

func (r admissionAvailabilityReader) ReadBundleUpgradeState(_ context.Context, runID string, _ semanticview.Source) (UpgradeState, error) {
	return UpgradeState{RunID: runID}, nil
}

func admittedUpgradeSource(t *testing.T, label string) semanticview.Source {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\n# " + label + "\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, label, artifact)
	repo := filepath.Join("..", "..", "..")
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func TestAdmitPinnedSourcesRequiresCompleteObservation(t *testing.T) {
	candidate := admittedUpgradeSource(t, "candidate")
	readErr := errors.New("private selected-store address")
	for _, test := range []struct {
		name   string
		reader UpgradeReader
	}{
		{name: "missing reader"},
		{name: "failed read", reader: admissionAvailabilityReader(func(context.Context) ([]Availability, error) { return nil, readErr })},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := AdmitPinnedSources(context.Background(), test.reader, candidate, func(context.Context, sourceartifact.RootAdmissionReference) (semanticview.Source, error) {
				return candidate, nil
			})
			failure, ok := runtimefailures.As(err)
			if !ok || failure.Failure.Class != runtimefailures.ClassDependencyUnavailable {
				t.Fatalf("error = %v, want dependency refusal", err)
			}
			if test.name == "failed read" && (!errors.Is(err, readErr) || strings.Contains(err.Error(), readErr.Error())) {
				t.Fatalf("read failure must retain its cause without leaking private detail: %v", err)
			}
		})
	}
}

func TestAdmitPinnedSourcesUsesExactActiveSourceOwner(t *testing.T) {
	candidate, current := admittedUpgradeSource(t, "candidate"), admittedUpgradeSource(t, "current")
	candidateBundle, _ := semanticview.Bundle(candidate)
	first, err := candidateBundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	currentBundle, _ := semanticview.Bundle(current)
	second, err := currentBundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	hash := first.Admission().BundleHash()
	other := second.Admission().BundleHash()
	for _, state := range []string{"running", "live", "paused"} {
		for _, mismatch := range []bool{false, true} {
			calls := 0
			reader := admissionAvailabilityReader(func(context.Context) ([]Availability, error) {
				calls++
				active := Availability{RunID: "ordinary-run", Status: state, BundleHash: hash, RootReference: first.Admission().Reference(), RootAdmissionPresent: true, SourceArtifactPresent: true}
				if mismatch {
					active.BundleHash = other
					active.RootReference = second.Admission().Reference()
				}
				return []Availability{active}, nil
			})
			loads := 0
			plan, err := AdmitPinnedSources(context.Background(), reader, candidate, func(_ context.Context, reference sourceartifact.RootAdmissionReference) (semanticview.Source, error) {
				loads++
				if reference != second.Admission().Reference() {
					t.Fatalf("loader lost exact retained root: %+v", reference)
				}
				return current, nil
			})
			if calls != 1 || err != nil {
				t.Fatalf("state %s mismatch %t: reads %d, error %v", state, mismatch, calls, err)
			}
			if _, upgraded := plan.Runs["ordinary-run"]; upgraded != (mismatch && state != "paused") {
				t.Fatalf("state %s mismatch %t: wrong upgrade population %v", state, mismatch, plan.Runs)
			}
			if (loads != 0) != (mismatch && state != "paused") {
				t.Fatalf("state %s mismatch %t: unexpected retained loads %d", state, mismatch, loads)
			}
		}
	}
	foreignCandidate, _ := sourceartifactfixture.Rooted(t, "foreign-candidate", candidateBundle.SourceArtifact)
	foreignCurrent, _ := sourceartifactfixture.Rooted(t, "foreign-current", currentBundle.SourceArtifact)
	for _, test := range []struct {
		name       string
		references []sourceartifact.RootAdmissionReference
		loads      int
	}{
		{"candidate equal hash foreign root", []sourceartifact.RootAdmissionReference{foreignCandidate.Admission().Reference()}, 0},
		{"loaded source foreign root", []sourceartifact.RootAdmissionReference{foreignCurrent.Admission().Reference()}, 1},
		{"cached source foreign root", []sourceartifact.RootAdmissionReference{second.Admission().Reference(), foreignCurrent.Admission().Reference()}, 1},
		{"missing root", []sourceartifact.RootAdmissionReference{{BundleHash: other}}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := admissionAvailabilityReader(func(context.Context) ([]Availability, error) {
				var active []Availability
				for _, reference := range test.references {
					active = append(active, Availability{RunID: "ordinary-run", Status: "running", BundleHash: reference.BundleHash, RootReference: reference, RootAdmissionPresent: true, SourceArtifactPresent: true})
				}
				return active, nil
			})
			loads := 0
			plan, err := AdmitPinnedSources(context.Background(), reader, candidate, func(_ context.Context, reference sourceartifact.RootAdmissionReference) (semanticview.Source, error) {
				if reference != test.references[loads] {
					t.Fatalf("loader replaced persisted root: %+v", reference)
				}
				loads++
				return current, nil
			})
			if err == nil || plan != nil || loads != test.loads {
				t.Fatalf("contradictory root admitted: plan=%v loads=%d error=%v", plan, loads, err)
			}
		})
	}
	t.Run("upgrade preserves held instance state", testUpgradeRetainedInstances)
}

func testUpgradeRetainedInstances(t *testing.T) {
	source := func(stage, fieldType string, edits ...func(*contracts.WorkflowContractBundle)) semanticview.Source {
		bundle := &contracts.WorkflowContractBundle{
			RootSchema: &contracts.FlowSchemaDocument{Name: "upgrade", StageDeclarations: contracts.FlowStageDeclarations{
				Declared: true, Entries: []contracts.FlowStageDeclaration{{ID: stage, Gate: &contracts.FlowStageGateDeclaration{
					Decision: "review", Title: "Review", Outcomes: map[string]contracts.FlowStageGateOutcomeDeclaration{
						"approve": {Label: "Approve", AdvancesTo: stage},
					},
				}}},
			}},
			FlowSources: map[string]contracts.FlowSource{".": {Schema: "schema.yaml", Entities: "entities.yaml"}},
			RootEntities: contracts.EntityContractsDocument{"record": {Fields: map[string]contracts.EntityFieldDecl{
				"count": {Type: fieldType},
			}}},
			Events: map[string]contracts.EventCatalogEntry{"work.queued": {Payload: contracts.EventPayloadSpec{
				Type: "object", Properties: map[string]contracts.EventFieldSpec{"count": {Type: fieldType}},
			}}},
		}
		root := &contracts.FlowContractView{Paths: contracts.FlowContractPaths{FlowPath: "."},
			Schema: *bundle.RootSchema, Events: bundle.Events}
		bundle.FlowTree = contracts.FlowTree{Root: root, ByID: map[string]*contracts.FlowContractView{".": root}}
		for _, edit := range edits {
			edit(bundle)
		}
		if err := contracts.CompileWorkflowSemantics(bundle); err != nil {
			t.Fatal(err)
		}
		return semanticview.Wrap(bundle)
	}
	current := source("ready", "integer")
	identity := flowidentity.Derive(current, ".", "one")
	state := UpgradeState{RunID: "run-1", Instances: []UpgradeInstance{{
		Route:    flowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath),
		EntityID: identity.EntityID, FlowID: ".", Status: "active", Stage: "ready", Fields: map[string]any{"count": int64(5)},
	}}}
	for _, test := range []struct {
		name, stage, fieldType, kind string
	}{
		{"unchanged declarations", "ready", "integer", ""},
		{"numeric widening", "ready", "double", ""},
		{"removed occupied stage", "later", "integer", "stage_removed"},
		{"incompatible held value", "ready", "string", "field_value_incompatible"},
	} {
		t.Run(test.name, func(t *testing.T) {
			blockers, err := CheckUpgrade(context.Background(), current, source(test.stage, test.fieldType), state)
			if err != nil {
				t.Fatal(err)
			}
			if test.kind == "" {
				if len(blockers) != 0 {
					t.Fatalf("compatible upgrade blocked: %+v", blockers)
				}
			} else if len(blockers) != 1 || blockers[0].Kind != test.kind || blockers[0].FlowInstance != identity.InstancePath || blockers[0].EntityID != identity.EntityID || blockers[0].Location == "" {
				t.Fatalf("upgrade did not name its exact retained blocker: %+v", blockers)
			}
			if state.Instances[0].Fields["count"] != int64(5) || state.Instances[0].Stage != "ready" {
				t.Fatalf("admission changed retained state: %+v", state.Instances[0])
			}
		})
	}
	t.Run("queued event keeps its admitted schema", func(t *testing.T) {
		event, err := events.NewOperatorInjectedEvent(events.OperatorInjectedEventInput{
			RunID: state.RunID, Facts: events.EventFacts{
				ID: "queued-1", Type: "work.queued", Payload: []byte(`{"count":5}`), ExecutionMode: executionmode.Live,
				Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: "operator"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		queued := UpgradeState{RunID: state.RunID, Events: []events.Event{event}}
		for _, fieldType := range []string{"integer", "double"} {
			blockers, err := CheckUpgrade(context.Background(), current, source("ready", fieldType), queued)
			if err != nil {
				t.Fatal(err)
			}
			if fieldType == "integer" && len(blockers) != 0 {
				t.Fatalf("unchanged queued schema blocked: %+v", blockers)
			}
			if fieldType == "double" && (len(blockers) != 1 || blockers[0].Kind != "queued_event_schema_changed") {
				t.Fatalf("changed queued schema admitted: %+v", blockers)
			}
		}
		t.Run("all retained blockers are reported together", func(t *testing.T) {
			routingSource, err := events.NewRootRoutingSource(identity.EntityID)
			if err != nil {
				t.Fatal(err)
			}
			anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
				Route: state.Instances[0].Route, FlowID: ".", EntityID: identity.EntityID,
				Stage: "ready", StageActivationID: "stage-entry-1", Source: routingSource,
			})
			if err != nil {
				t.Fatal(err)
			}
			queued.Instances = state.Instances
			queued.Deliveries = []deliverylifecycle.Snapshot{{
				RunID: state.RunID, EventID: event.ID(), SubscriberID: "removed-agent", Status: deliverylifecycle.StatusPending,
				Route: events.DeliveryRoute{
					Recipient: events.MustAgentDeliveryRecipient("removed-agent"),
					Target:    events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: identity.InstancePath, EntityID: identity.EntityID}),
				},
			}}
			queued.Cards = []decisioncard.ListItem{{RunID: state.RunID, Status: "pending", Anchor: anchor}}
			queued.Barriers = []fanoutbarrier.Barrier{{Status: fanoutbarrier.StatusArmed, Registration: fanoutbarrier.Registration{
				IntentKey: fanoutobligation.IntentKey{RunID: state.RunID, TriggeringDeliveryID: "delivery-1", ElementRef: contracts.FanOutElementRef{
					FlowPath: ".", Family: "fan_out", SemanticPath: `nodes["producer"].handlers["work.queued"].fan_out`,
				}},
				Route: state.Instances[0].Route, EntityID: identity.EntityID,
			}}}
			candidate := source("later", "string", func(bundle *contracts.WorkflowContractBundle) {
				bundle.FlowSources["."] = contracts.FlowSource{Schema: "candidate/schema.yaml", Entities: "candidate/entities.yaml", Nodes: "candidate/nodes.yaml", Events: "candidate/events.yaml"}
			})
			blockers, err := CheckUpgrade(context.Background(), current, candidate, queued)
			if err != nil {
				t.Fatal(err)
			}
			kinds := map[string]bool{}
			for _, blocker := range blockers {
				if blocker.RunID != state.RunID || !strings.HasPrefix(blocker.Location, "candidate/") {
					t.Fatalf("blocker lost its instance or candidate location: %+v", blocker)
				}
				if blocker.Kind != "queued_event_schema_changed" && (blocker.FlowInstance != identity.InstancePath || blocker.EntityID != identity.EntityID) {
					t.Fatalf("instance-owned blocker lost its exact target: %+v", blocker)
				}
				kinds[blocker.Kind] = true
			}
			for _, kind := range []string{"stage_removed", "field_value_incompatible", "queued_event_schema_changed", "delivery_target_removed", "gate_removed", "join_in_flight"} {
				if !kinds[kind] {
					t.Fatalf("missing %s in aggregate blockers: %+v", kind, blockers)
				}
			}
			refusal := &UpgradeConflictError{Blockers: blockers}
			failure, typed := runtimefailures.As(refusal)
			if !typed || failure.Failure.Class != runtimefailures.ClassLifecycleConflict || !strings.Contains(refusal.Error(), identity.EntityID) || !strings.Contains(refusal.Error(), "candidate/schema.yaml") {
				t.Fatalf("upgrade refusal lost its type or actionable blockers: %v", refusal)
			}
			queued.Deliveries[0].RunID = "another-run"
			if partial, err := CheckUpgrade(context.Background(), current, candidate, queued); err == nil || partial != nil {
				t.Fatalf("foreign delivery returned a partial admission: %+v, %v", partial, err)
			}
			queued.Deliveries[0].RunID = state.RunID
			queued.Deliveries[0].Status = deliverylifecycle.StatusDelivered
			queued.Cards[0].Status = "resolved"
			queued.Barriers[0].Status = fanoutbarrier.StatusFired
			if blockers, err := CheckUpgrade(context.Background(), current, source("ready", "integer"), queued); err != nil || len(blockers) != 0 {
				t.Fatalf("settled obligations blocked a compatible upgrade: %+v, %v", blockers, err)
			}
		})
	})
	t.Run("removed occupied flow names the candidate root", func(t *testing.T) {
		withChild := source("ready", "integer", func(bundle *contracts.WorkflowContractBundle) {
			root := bundle.FlowTree.Root
			root.Children = []contracts.FlowContractView{{Paths: contracts.FlowContractPaths{FlowPath: "child"}, Schema: *bundle.RootSchema, Parent: root}}
			bundle.FlowTree.ByID["child"] = &root.Children[0]
			bundle.FlowSources["child"] = contracts.FlowSource{Schema: "child/schema.yaml"}
		})
		child := flowidentity.Derive(withChild, "child", "one")
		retained := UpgradeState{RunID: state.RunID, Instances: []UpgradeInstance{{
			Route:    flowidentity.StoredRoute(child.ScopeKey, child.InstanceID, child.InstancePath),
			EntityID: child.EntityID, FlowID: "child", Status: "active", Stage: "ready",
		}}}
		blockers, err := CheckUpgrade(context.Background(), withChild, source("ready", "integer"), retained)
		if err != nil || len(blockers) != 1 || blockers[0].Kind != "stage_removed" || blockers[0].Location != "schema.yaml" || blockers[0].FlowInstance != child.InstancePath {
			t.Fatalf("removed flow lost its candidate location or exact instance: %+v, %v", blockers, err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CheckUpgrade(ctx, current, source("ready", "integer"), state); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled upgrade = %v", err)
	}
}

func TestAdmitPinnedSourcesCancellationWins(t *testing.T) {
	candidate := admittedUpgradeSource(t, "candidate")
	ctx, cancel := context.WithCancel(context.Background())
	reader := admissionAvailabilityReader(func(context.Context) ([]Availability, error) {
		cancel()
		return nil, errors.New("late dependency failure")
	})
	if _, err := AdmitPinnedSources(ctx, reader, candidate, func(context.Context, sourceartifact.RootAdmissionReference) (semanticview.Source, error) {
		return candidate, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled observation = %v", err)
	}
	if _, err := AdmitPinnedSources(ctx, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was treated as non-applicability: %v", err)
	}
	if _, err := AdmitPinnedSources(nil, nil, nil, nil); err == nil {
		t.Fatal("missing context admitted")
	}
}
