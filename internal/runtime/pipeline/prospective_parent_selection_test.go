package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type prospectiveSelectionReader struct {
	list   func(context.Context, FlowInstanceLookupScope) ([]FlowInstanceObservation, error)
	lookup func(context.Context, FlowInstanceLookupRequest) (FlowInstanceObservation, bool, error)
}

func (r prospectiveSelectionReader) ListFlowInstances(ctx context.Context, scope FlowInstanceLookupScope) ([]FlowInstanceObservation, error) {
	return r.list(ctx, scope)
}

func (r prospectiveSelectionReader) LookupFlowInstance(ctx context.Context, request FlowInstanceLookupRequest) (FlowInstanceObservation, bool, error) {
	return r.lookup(ctx, request)
}

func prospectiveSelectionObservation(t *testing.T) (FlowInstanceLookupRequest, FlowInstanceObservation) {
	t.Helper()
	source, fact := instanceIndexTestSource(t, "text")
	runID := uuid.NewString()
	parent, err := flowidentity.StandingForGeneration(source, ".", runID)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := AdmitFlowInstanceKeyMaterial(source, "worker", "business-key")
	if err != nil {
		t.Fatal(err)
	}
	declared, err := NewDeclaredFlowInstanceLookup(source, fact, runID, "worker", parent, keys)
	if err != nil {
		t.Fatal(err)
	}
	child, err := flowidentity.KeyedChild(source, parent, "worker", "actual-stored-child")
	if err != nil {
		t.Fatal(err)
	}
	child.EntityID = uuid.NewString()
	exact, err := NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: child.Route()})
	if err != nil {
		t.Fatal(err)
	}
	flow, _ := source.FlowSchemaByID("worker")
	at := time.Now().UTC()
	instance := WorkflowInstance{
		WorkflowName: "worker", WorkflowVersion: source.WorkflowVersion(), Mode: flow.EffectiveMode(), Status: "active",
		InstanceID: child.InstanceID, StorageRef: child.InstancePath, EntityID: child.EntityID, EntityType: "item",
		ParentFlowID: parent.TemplateID, ParentFlowInstance: parent.InstancePath, ParentEntityID: parent.EntityID,
		InstanceKey: "business-key", CurrentState: "open", Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: fact.BundleHash(), StartedAt: at}
	receipt := FlowConstructionPublicationEvidence{Identity: child, InstanceKey: instance.InstanceKey, WorkflowVersion: instance.WorkflowVersion, BundleHash: fact.BundleHash()}
	readiness := DynamicFlowRuntimeReadiness{
		Plan:            DynamicFlowRuntimeReadinessPlan{Identity: child, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active",
	}
	observation, err := AdmitNativeFlowInstanceObservation(exact, instance, run, 3, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	return declared, observation
}

func TestProspectiveParentSelectionBindsExactObservationBeforeAvailability(t *testing.T) {
	lookup, inventory := prospectiveSelectionObservation(t)
	current := inventory
	current.instance.Revision = 2
	current.instance.Status = "terminated"
	current.instance.TerminatedAt = inventory.instance.UpdatedAt.Add(time.Second)
	var calls []string
	reader := prospectiveSelectionReader{
		list: func(ctx context.Context, scope FlowInstanceLookupScope) ([]FlowInstanceObservation, error) {
			calls = append(calls, "inventory")
			if scope.RunID() != lookup.RunID() || !reflect.DeepEqual(scope.FlowIDs(), []string{"worker"}) || len(scope.Coordinates()) != 0 {
				t.Fatalf("inventory broadened the admitted declaration: %+v", scope)
			}
			return []FlowInstanceObservation{inventory}, nil
		},
		lookup: func(ctx context.Context, exact FlowInstanceLookupRequest) (FlowInstanceObservation, bool, error) {
			calls = append(calls, "exact")
			if exact.DeclaredSelection() || exact.ExactPath() != inventory.Owner().Route.InstancePath || exact.RunID() != lookup.RunID() || !exact.SourceFact().Matches(lookup.SourceFact()) {
				t.Fatalf("selection did not bind the actual stored coordinate: %+v", exact)
			}
			return current, true, nil
		},
	}
	observation, found, err := readFlowInstanceSelection(context.Background(), reader, lookup, true)
	if err != nil || !found || observation.HeaderRevision() != 2 || !reflect.DeepEqual(calls, []string{"inventory", "exact"}) {
		t.Fatalf("selection reused inventory as mutable evidence: found=%t revision=%d calls=%v err=%v", found, observation.HeaderRevision(), calls, err)
	}
	selected, err := selectObservedFlowInstance(FlowInstanceSelectionRequest{Lookup: lookup, Mode: contracts.FlowInputResolutionModeSelect}, observation)
	var terminated *TerminatedReceiverError
	if !errors.As(err, &terminated) || selected.Observation.Valid() || selected.Activation != nil {
		t.Fatalf("exact unavailable receiver became executable: %+v err=%v", selected, err)
	}
	if owner, found := selected.UnavailableReceiver(); !found || owner != inventory.Owner() {
		t.Fatalf("exact unavailable occupancy lost its coordinate: %+v", selected)
	}
}

func TestProspectiveParentSelectionRefusesContradictoryExactResults(t *testing.T) {
	lookup, inventory := prospectiveSelectionObservation(t)
	for _, name := range []string{"absent", "invalid", "other_path", "other_entity", "other_run", "other_source", "other_key", "read_failure", "joined_cancellation"} {
		t.Run(name, func(t *testing.T) {
			current, found := inventory, true
			readFailure := errors.New("independent exact read failure")
			var readErr error
			switch name {
			case "absent":
				current, found = FlowInstanceObservation{}, false
			case "invalid":
				current = FlowInstanceObservation{}
			case "other_path":
				current.identity.InstancePath += "-other"
			case "other_entity":
				current.identity.EntityID = uuid.NewString()
			case "other_run":
				current.request.runID = uuid.NewString()
			case "other_source":
				_, current.request.fact = instanceIndexTestSource(t, "integer")
			case "other_key":
				current.instance.InstanceKey = "another-business-key"
			case "read_failure":
				readErr = readFailure
			case "joined_cancellation":
				readErr = errors.Join(readFailure, context.Canceled)
			}
			exactCalls := 0
			reader := prospectiveSelectionReader{
				list: func(context.Context, FlowInstanceLookupScope) ([]FlowInstanceObservation, error) {
					return []FlowInstanceObservation{inventory}, nil
				},
				lookup: func(context.Context, FlowInstanceLookupRequest) (FlowInstanceObservation, bool, error) {
					exactCalls++
					return current, found, readErr
				},
			}
			observed, present, err := readFlowInstanceSelection(context.Background(), reader, lookup, true)
			if err == nil || present || observed.Valid() || exactCalls != 1 {
				t.Fatalf("contradictory exact result escaped: present=%t observed=%+v calls=%d err=%v", present, observed, exactCalls, err)
			}
			if readErr != nil && !errors.Is(err, readFailure) || name == "joined_cancellation" && !errors.Is(err, context.Canceled) {
				t.Fatalf("exact read/cancellation cause was lost: %v", err)
			}
		})
	}
}

func TestProspectiveParentSelectionPreservesInventoryAbsenceAndFailures(t *testing.T) {
	lookup, inventory := prospectiveSelectionObservation(t)
	for _, name := range []string{"absent", "nonselected", "ambiguous", "inventory_failure", "joined_cancellation"} {
		t.Run(name, func(t *testing.T) {
			var observations []FlowInstanceObservation
			readFailure := errors.New("independent inventory failure")
			var listErr error
			switch name {
			case "nonselected":
				other := inventory
				other.instance.InstanceKey = "another-business-key"
				observations = []FlowInstanceObservation{other}
			case "ambiguous":
				observations = []FlowInstanceObservation{inventory, inventory}
			case "inventory_failure":
				listErr = readFailure
			case "joined_cancellation":
				listErr = errors.Join(readFailure, context.Canceled)
			}
			reader := prospectiveSelectionReader{
				list: func(context.Context, FlowInstanceLookupScope) ([]FlowInstanceObservation, error) {
					return observations, listErr
				},
				lookup: func(context.Context, FlowInstanceLookupRequest) (FlowInstanceObservation, bool, error) {
					t.Fatal("absent or refused inventory borrowed parent lookup permission")
					return FlowInstanceObservation{}, false, nil
				},
			}
			observed, found, err := readFlowInstanceSelection(context.Background(), reader, lookup, true)
			wantErr := name == "ambiguous" || listErr != nil
			if (err != nil) != wantErr || found || observed.Valid() {
				t.Fatalf("inventory result changed absence/refusal: found=%t observed=%+v err=%v", found, observed, err)
			}
			if listErr != nil && !errors.Is(err, readFailure) || name == "joined_cancellation" && !errors.Is(err, context.Canceled) {
				t.Fatalf("inventory error lost its independent causes: %v", err)
			}
		})
	}
}
