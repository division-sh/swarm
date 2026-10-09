package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func instanceObservationTestFixture(t *testing.T) (FlowInstanceLookupRequest, WorkflowInstance, runlifecycle.Snapshot, FlowConstructionPublicationEvidence, DynamicFlowRuntimeReadiness) {
	t.Helper()
	source, fact := instanceIndexTestSource(t, "text")
	runID := uuid.NewString()
	identity, err := flowidentity.StandingForGeneration(source, ".", runID)
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	instance := WorkflowInstance{
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), Mode: "static", Status: "active",
		InstanceID: runID, StorageRef: runID, EntityID: runID, CurrentState: "open", Revision: 1,
		CreatedAt: at, UpdatedAt: at, Fields: map[string]any{"nested": map[string]any{"value": "original"}},
	}
	run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: fact.BundleHash(), StartedAt: at}
	receipt := FlowConstructionPublicationEvidence{Identity: identity, Fields: instance.Fields}
	readiness := DynamicFlowRuntimeReadiness{
		Plan:            DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active",
	}
	return request, instance, run, receipt, readiness
}

func TestFlowInstanceObservationIsDetachedData(t *testing.T) {
	request, instance, run, receipt, readiness := instanceObservationTestFixture(t)
	observation, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	instance.Fields["nested"].(map[string]any)["value"] = "changed input"
	for range 2 {
		fields, err := observation.WorkflowInstance()
		if err != nil || fields.Fields["nested"].(map[string]any)["value"] != "original" {
			t.Fatalf("mutable header observation: %+v %v", fields, err)
		}
		fields.Fields["nested"].(map[string]any)["value"] = "changed output"
		construction, found, err := observation.NativeConstruction()
		if err != nil || !found || construction.Fields["nested"].(map[string]any)["value"] != "original" {
			t.Fatalf("mutable receipt observation: %+v %v", construction, err)
		}
		construction.Fields["nested"].(map[string]any)["value"] = "changed receipt"
	}
	if !observation.Valid() || observation.RunRevision() != 3 || observation.HeaderRevision() != 1 || observation.Owner().Key() != run.RunID+"\x00"+instance.StorageRef {
		t.Fatal("observation lost exact snapshot coordinates")
	}
	if _, found := observation.HistoricalConstruction(); found {
		t.Fatal("native observation acquired historical provenance")
	}
	readiness.Plan.Identity.EntityID = uuid.NewString()
	if _, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness); err == nil {
		t.Fatal("native observation accepted crossed readiness")
	}
}

func TestFlowInstanceObservationHistoricalAbsenceIsNotNativeFallback(t *testing.T) {
	request, instance, run, receipt, readiness := instanceObservationTestFixture(t)
	sourceRunID := uuid.NewString()
	origin, err := runlifecycle.ForkMaterializationRunOrigin(sourceRunID, runlifecycle.ForkOriginPointDeploymentRevision, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	run.Origin = origin
	historical := HistoricalFlowInstanceConstruction{
		Identity: receipt.Identity, SourceRevision: 2,
		SourceOwner: flowidentity.RunScopedFlowInstance{RunID: sourceRunID, Route: flowidentity.StoredRoute(".", sourceRunID, sourceRunID)},
	}
	observation, err := AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, historical, nil)
	if err != nil || !observation.Valid() {
		t.Fatalf("admitted fixed construction rejected: %+v %v", observation, err)
	}
	if _, found := observation.Readiness(); found {
		t.Fatal("materialize-only history fabricated readiness")
	}
	if _, found, err := observation.NativeConstruction(); err != nil || found {
		t.Fatal("history fabricated a native receipt")
	}
	if _, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, DynamicFlowRuntimeReadiness{}); err == nil {
		t.Fatal("fork origin alone excused missing native readiness")
	}
	if _, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness); err != nil {
		t.Fatalf("genuine post-fork native construction rejected: %v", err)
	}
	historical.SourceOwner.Route = flowidentity.StoredRoute("worker", "other", "worker/other")
	if _, err := AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, historical, nil); err == nil {
		t.Fatal("history accepted an unrelated fixed receiver")
	}
	historical.SourceRevision = 1
	if _, err := AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, historical, nil); err == nil {
		t.Fatal("history accepted a different cut")
	}
}

func TestFlowInstanceObservationKeepsTerminalOccupancy(t *testing.T) {
	request, instance, run, receipt, readiness := instanceObservationTestFixture(t)
	ended := run.StartedAt.Add(time.Second)
	run.State, run.EndedAt = runlifecycle.StateCompleted, &ended
	instance.Status, instance.TerminatedAt = "terminated", ended
	readiness.RunStatus, readiness.InstanceStatus, readiness.InstanceTerminatedAt = "completed", "terminated", ended
	observation, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness)
	if err != nil || !observation.Valid() || observation.RunState() != runlifecycle.StateCompleted {
		t.Fatalf("terminal occupied receiver disappeared: %+v %v", observation, err)
	}
	if _, err := AdmitNativeFlowInstanceObservation(request, instance, run, 0, receipt, readiness); err == nil {
		t.Fatal("observation accepted a missing snapshot revision")
	}
}

func TestFlowInstanceObservationRefusesCrossedHeaderAndRoute(t *testing.T) {
	request, instance, run, receipt, readiness := instanceObservationTestFixture(t)
	wrongOwner := flowidentity.RunScopedFlowInstance{RunID: run.RunID, Route: receipt.Identity.Route()}
	wrongOwner.Route.InstanceID = "different-instance"
	crossed, err := NewExactFlowInstanceLookup(request.Source(), request.SourceFact(), wrongOwner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AdmitNativeFlowInstanceObservation(crossed, instance, run, 3, receipt, readiness); err == nil {
		t.Fatal("exact coordinate accepted a different instance discriminator")
	}
	instance.EntityType = "foreign-type"
	if _, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness); err == nil {
		t.Fatal("observed header accepted a different declared entity owner")
	}
}
