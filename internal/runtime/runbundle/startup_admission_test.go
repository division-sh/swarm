package runbundle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type admissionAvailabilityReader func(context.Context) ([]Availability, error)

func (r admissionAvailabilityReader) ActiveNonStandingRunBundleAvailabilities(ctx context.Context) ([]Availability, error) {
	return r(ctx)
}

func TestAdmitPinnedSourcesRequiresCompleteObservation(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	readErr := errors.New("private selected-store address")
	for _, test := range []struct {
		name   string
		reader ActiveAvailabilityReader
		pins   []string
		boot   string
		class  runtimefailures.Class
	}{
		{name: "not applicable"},
		{name: "missing reader", pins: []string{hash}, boot: hash, class: runtimefailures.ClassDependencyUnavailable},
		{name: "failed read", reader: admissionAvailabilityReader(func(context.Context) ([]Availability, error) { return nil, readErr }), pins: []string{hash}, boot: hash, class: runtimefailures.ClassDependencyUnavailable},
		{name: "missing identity", pins: []string{hash}, class: runtimefailures.ClassSchemaInvalid},
		{name: "invalid hash", pins: []string{"invalid"}, boot: hash, class: runtimefailures.ClassSchemaInvalid},
		{name: "empty hash is not omission", pins: []string{""}, boot: hash, class: runtimefailures.ClassSchemaInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := AdmitPinnedSources(context.Background(), test.reader, test.boot, test.pins)
			if test.class == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			failure, ok := runtimefailures.As(err)
			if !ok || failure.Failure.Class != test.class {
				t.Fatalf("error = %v, want %s", err, test.class)
			}
			if test.name == "failed read" && (!errors.Is(err, readErr) || strings.Contains(err.Error(), readErr.Error())) {
				t.Fatalf("read failure must retain its cause without leaking private detail: %v", err)
			}
		})
	}
}

func TestAdmitPinnedSourcesUsesExactActiveSourceOwner(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	other := "bundle-v2:sha256:" + strings.Repeat("b", 64)
	for _, state := range []string{"running", "live", "paused"} {
		for _, mismatch := range []bool{false, true} {
			calls := 0
			reader := admissionAvailabilityReader(func(context.Context) ([]Availability, error) {
				calls++
				active := Availability{RunID: "ordinary-run", Status: state, BundleHash: hash, SourceArtifactPresent: true}
				if mismatch {
					active.BundleHash = other
				}
				return []Availability{active, {RunID: "missing-artifact", BundleHash: other}}, nil
			})
			err := AdmitPinnedSources(context.Background(), reader, hash, []string{hash, hash})
			wantConflict := mismatch && state != "paused"
			if calls != 1 || (err != nil) != wantConflict {
				t.Fatalf("state %s mismatch %t: reads %d, error %v", state, mismatch, calls, err)
			}
			if wantConflict {
				failure, ok := runtimefailures.As(err)
				if !ok || failure.Failure.Class != runtimefailures.ClassLifecycleConflict {
					t.Fatalf("conflict classification lost: %v", err)
				}
				for _, detail := range []string{"active non-standing run pinned bundle_hash conflict", hash, other, "ordinary-run"} {
					if !strings.Contains(err.Error(), detail) {
						t.Fatalf("conflict detail %q lost: %v", detail, err)
					}
				}
				if strings.Contains(err.Error(), "missing-artifact") {
					t.Fatalf("source integrity was reinterpreted as a pin mismatch: %v", err)
				}
			}
		}
	}
	t.Run("upgrade preserves held instance state", testUpgradeRetainedInstances)
}

func testUpgradeRetainedInstances(t *testing.T) {
	source := func(stage, fieldType string) semanticview.Source {
		bundle := &contracts.WorkflowContractBundle{
			RootSchema: &contracts.FlowSchemaDocument{Name: "upgrade", StageDeclarations: contracts.FlowStageDeclarations{
				Declared: true, Entries: []contracts.FlowStageDeclaration{{ID: stage}},
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
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CheckUpgrade(ctx, current, source("ready", "integer"), state); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled upgrade = %v", err)
	}
}

func TestAdmitPinnedSourcesCancellationWins(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	ctx, cancel := context.WithCancel(context.Background())
	reader := admissionAvailabilityReader(func(context.Context) ([]Availability, error) {
		cancel()
		return nil, errors.New("late dependency failure")
	})
	if err := AdmitPinnedSources(ctx, reader, hash, []string{hash}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled observation = %v", err)
	}
	if err := AdmitPinnedSources(ctx, nil, "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was treated as non-applicability: %v", err)
	}
	if err := AdmitPinnedSources(nil, nil, "", nil); err == nil {
		t.Fatal("missing context admitted")
	}
}
