package bus

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func derivedRouteTableFixture(t testing.TB, source semanticview.Source) *RouteTable {
	t.Helper()
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

// These wrappers require explicit declaration/instance data and delegate only
// to the compiled owners. They do not observe existence or grant readiness.
func (rt *RouteTable) PubsubDeclarationDefinitionsFixture(t testing.TB, flowID string, keys ...string) []Subscriber {
	t.Helper()
	definitions, err := rt.PubsubDeclarationDefinitions(flowID, keys)
	if err != nil {
		t.Fatal(err)
	}
	return definitions
}

func AdmittedFlowInstanceObservationFixture(t testing.TB, source semanticview.Source, runID string, instance flowidentity.Instance, key string) pipeline.FlowInstanceObservation {
	return constructionIndexObservation(t, source, runID, instance, key)
}

// This is detached component evidence, not a native lifecycle transition.
func TerminatedFlowInstanceObservationFixture(t testing.TB, source semanticview.Source, runID string, instance flowidentity.Instance) pipeline.FlowInstanceObservation {
	t.Helper()
	base := constructionIndexObservation(t, source, runID, instance, "")
	header, err := base.WorkflowInstance()
	if err != nil {
		t.Fatal(err)
	}
	header.Status = "terminated"
	header.TerminatedAt = header.CreatedAt.Add(time.Second)
	header.UpdatedAt = header.TerminatedAt
	header.Revision++
	receipt, found, err := base.NativeConstruction()
	if err != nil || !found {
		t.Fatalf("terminal component lacks original construction: found=%t err=%v", found, err)
	}
	readiness, found := base.Readiness()
	if !found {
		t.Fatal("terminal component lacks original attachment evidence")
	}
	readiness.InstanceStatus, readiness.InstanceTerminatedAt = header.Status, header.TerminatedAt
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("terminal component lacks its compiled source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	run := runlifecycle.Snapshot{
		RunID: runID, State: base.RunState(), Origin: runlifecycle.DeploymentRunOrigin(),
		BundleHash: base.SourceFact().BundleHash(), RootReference: rooted.Admission().Reference(), StartedAt: header.CreatedAt,
	}
	observed, err := pipeline.AdmitNativeFlowInstanceObservation(base.Selection(), header, run, base.RunRevision()+1, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func FlowInstanceIndexFixture(observations ...pipeline.FlowInstanceObservation) pipeline.FlowInstanceIndexReader {
	return constructionIndexTestReader{observations: observations}
}

func (rt *RouteTable) PubsubReceiverDefinitionsFixture(t testing.TB, runID string, instance flowidentity.Instance, keys ...string) []Subscriber {
	t.Helper()
	definitions, err := rt.PubsubReceiverDefinitions(runID, instance, keys)
	if err != nil {
		t.Fatal(err)
	}
	return definitions
}
