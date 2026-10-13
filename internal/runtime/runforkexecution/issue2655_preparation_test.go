package runforkexecution

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestIssue2655MaterializationPreservesOriginalSelector(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "omitted", true: "explicit"}[explicit], func(t *testing.T) {
			prepared := issue2655PreparedProjection(t)
			if explicit {
				prepared.plan.ForkPoint.Input = prepared.plan.ForkPoint.EventID
			}
			request, err := prepared.MaterializationRequest()
			if err != nil {
				t.Fatal(err)
			}
			if request.At != prepared.plan.ForkPoint.Input {
				t.Fatalf("original selector changed: got=%q want=%q", request.At, prepared.plan.ForkPoint.Input)
			}
		})
	}
}

func TestIssue2655MaterializationOperationIsDetached(t *testing.T) {
	prepared := issue2655PreparedProjection(t)
	point := prepared.plan.ForkPoint
	prepared.forkOperation = &runfork.ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "operator", TransportHash: "request",
		SourceRunID: prepared.plan.SourceRunID, TargetBundleHash: runForkTestBundleHash,
		ContractSelection: testContractSelection(), ResolvedPoint: &point,
		DataPinOverrides: []durabledata.ExplicitPin{{
			Declaration: durabledata.DeclarationRef{FlowPath: ".", EventName: "work.ready"},
			VersionID:   durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("a", 64)),
		}},
	}
	want, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	// The expected value must not itself share the operation under test.
	wantOperation := *want.ForkOperation
	wantPoint := *want.ForkOperation.ResolvedPoint
	wantOperation.ResolvedPoint = &wantPoint
	wantOperation.DataPinOverrides = append([]durabledata.ExplicitPin(nil), wantOperation.DataPinOverrides...)
	want.ForkOperation = &wantOperation
	copy, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	copy.ForkOperation.Actor = "different operator"
	copy.ForkOperation.ResolvedPoint.Revision++
	copy.ForkOperation.DataPinOverrides[0].VersionID = durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("b", 64))
	got, err := prepared.MaterializationRequest()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("projection changed retained operation: err=%v got=%+v want=%+v", err, got.ForkOperation, want.ForkOperation)
	}
}

func issue2655PreparedProjection(t *testing.T) *PreparedSelectedFork {
	t.Helper()
	ctx := runForkTestContext(t)
	selection := testContractSelection()
	frontier := testContractFrontierAdmission(selection)
	routes := testSelectedContractRouteAdmission(frontier)
	prepared := &PreparedSelectedFork{
		operation: selectedContractOperationForTest(t, ctx), loadedSource: testLoadedSelectedSource(selection),
		frontier: frontier, routeTopology: testSelectedContractRouteTopologyFromAdmission(t, frontier, routes),
		model: testSelectedContractExecutionModel(t, frontier),
		plan: runfork.RunForkPlan{SourceRunID: uuid.NewString(), ForkPoint: runfork.RunForkPoint{
			Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 3,
		}},
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	return prepared
}
