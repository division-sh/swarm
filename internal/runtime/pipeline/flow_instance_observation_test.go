package pipeline

import (
	"strings"
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
	receipt := FlowConstructionPublicationEvidence{Identity: identity, WorkflowVersion: instance.WorkflowVersion, BundleHash: fact.BundleHash(), Fields: instance.Fields}
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
		Identity: receipt.Identity, SourceRevision: 2, WorkflowVersion: instance.WorkflowVersion,
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

func TestFlowInstanceObservationHistoricalVersionUsesFixedCut(t *testing.T) {
	request, instance, run, receipt, readiness := instanceObservationTestFixture(t)
	sourceRunID := uuid.NewString()
	origin, err := runlifecycle.ForkMaterializationRunOrigin(sourceRunID, runlifecycle.ForkOriginPointDeploymentRevision, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	run.Origin = origin
	instance.WorkflowVersion = "captured-v0"
	historical := HistoricalFlowInstanceConstruction{
		Identity: receipt.Identity, SourceRevision: 2, WorkflowVersion: instance.WorkflowVersion,
		SourceOwner: flowidentity.RunScopedFlowInstance{RunID: sourceRunID, Route: flowidentity.StoredRoute(".", sourceRunID, sourceRunID)},
	}
	observation, err := AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, historical, nil)
	if err != nil || !observation.Valid() {
		t.Fatalf("captured historical version was replaced by selected execution version: %+v %v", observation, err)
	}
	stored, err := observation.WorkflowInstance()
	if err != nil || stored.WorkflowVersion != instance.WorkflowVersion {
		t.Fatalf("historical readback rewrote captured version: %+v %v", stored, err)
	}
	if _, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness); err == nil {
		t.Fatal("native construction accepted a foreign source version")
	}
	if _, err := AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, historical, &readiness); err != nil {
		t.Fatalf("current selected readiness contradicted captured historical header version: %v", err)
	}
	readiness.Plan.WorkflowVersion = instance.WorkflowVersion
	if _, err := AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, historical, &readiness); err == nil {
		t.Fatal("captured header version substituted for current selected execution readiness")
	}
	for _, version := range []string{"", request.Source().WorkflowVersion(), "foreign-fixed-version"} {
		historical.WorkflowVersion = version
		if _, err := AdmitHistoricalFlowInstanceObservation(request, instance, run, 3, historical, nil); err == nil {
			t.Fatalf("historical header accepted crossed or absent fixed version %q", version)
		}
	}
}

func TestFlowInstanceObservationNativeVersionUsesOriginalConstruction(t *testing.T) {
	request, instance, run, receipt, readiness := instanceObservationTestFixture(t)
	instance.WorkflowVersion = "captured-native-v0"
	receipt.WorkflowVersion = instance.WorkflowVersion
	receipt.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
	if receipt.BundleHash == request.SourceFact().BundleHash() || receipt.WorkflowVersion == request.Source().WorkflowVersion() {
		t.Fatal("proof requires original construction to differ from current executable source")
	}
	observation, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness)
	if err != nil || !observation.Valid() {
		t.Fatalf("current source replaced original native construction: %+v %v", observation, err)
	}
	stored, err := observation.WorkflowInstance()
	if err != nil || stored.WorkflowVersion != receipt.WorkflowVersion {
		t.Fatalf("native observation rewrote the original header: %+v %v", stored, err)
	}
	construction, native, err := observation.NativeConstruction()
	if err != nil || !native || construction.WorkflowVersion != receipt.WorkflowVersion || construction.BundleHash != receipt.BundleHash {
		t.Fatalf("native observation lost original source provenance: %+v %v", construction, err)
	}
	for _, fault := range []string{"header_version", "receipt_version", "missing_receipt_version", "missing_receipt_source", "malformed_receipt_source", "readiness_version", "readiness_source", "run_source"} {
		t.Run(fault, func(t *testing.T) {
			header, original, desired, active := instance, receipt, readiness, run
			switch fault {
			case "header_version":
				header.WorkflowVersion = request.Source().WorkflowVersion()
			case "receipt_version":
				original.WorkflowVersion = request.Source().WorkflowVersion()
			case "missing_receipt_version":
				original.WorkflowVersion = ""
			case "missing_receipt_source":
				original.BundleHash = ""
			case "malformed_receipt_source":
				original.BundleHash = "not-an-artifact"
			case "readiness_version":
				desired.Plan.WorkflowVersion = receipt.WorkflowVersion
			case "readiness_source":
				desired.Plan.BundleHash = receipt.BundleHash
			case "run_source":
				active.BundleHash = receipt.BundleHash
			}
			if got, err := AdmitNativeFlowInstanceObservation(request, header, active, 3, original, desired); err == nil || got.Valid() {
				t.Fatalf("crossed %s produced native authority: %+v %v", fault, got, err)
			}
		})
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

func TestFlowInstanceObservationTransitionHistoryIsDetached(t *testing.T) {
	request, instance, run, receipt, readiness := instanceObservationTestFixture(t)
	instance.TransitionHistory = []WorkflowTransitionRecord{{GuardsEvaluated: []string{"original"}}}
	observation, err := AdmitNativeFlowInstanceObservation(request, instance, run, 3, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	instance.TransitionHistory[0].GuardsEvaluated[0] = "mutated-input"
	for range 2 {
		stored, err := observation.WorkflowInstance()
		if err != nil {
			t.Fatal(err)
		}
		if stored.TransitionHistory[0].GuardsEvaluated[0] != "original" {
			t.Fatalf("detached observation changed through history: %s", stored.TransitionHistory[0].GuardsEvaluated[0])
		}
		stored.TransitionHistory[0].GuardsEvaluated[0] = "mutated-output"
	}
}
