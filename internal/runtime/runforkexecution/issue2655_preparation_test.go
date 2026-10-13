package runforkexecution

import (
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestIssue2655PreparedOperationPointBinding(t *testing.T) {
	plan := runfork.RunForkPlan{SourceRunID: uuid.NewString(), ForkPoint: runfork.RunForkPoint{
		Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), EventName: "work.ready", Revision: 3,
		Timestamp: time.Now().UTC(), ProducedBy: "producer", ProducedByType: "node",
	}}
	base := runfork.ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "operator", TransportHash: "request",
		SourceRunID: plan.SourceRunID, TargetBundleHash: runForkTestBundleHash, ContractSelection: testContractSelection(),
	}
	for _, supplied := range []bool{false, true} {
		t.Run(map[bool]string{false: "unresolved", true: "equal"}[supplied], func(t *testing.T) {
			operation := base
			if supplied {
				point := plan.ForkPoint
				operation.ResolvedPoint = &point
			}
			_, beforeHash, err := operation.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			bound, err := bindPreparedSelectedForkOperation(&operation, plan)
			if err != nil || bound == &operation || bound.ResolvedPoint == nil || *bound.ResolvedPoint != plan.ForkPoint {
				t.Fatalf("bind admitted point: bound=%+v err=%v", bound, err)
			}
			_, afterHash, err := bound.Canonical()
			if err != nil || beforeHash != afterHash || (!supplied && operation.ResolvedPoint != nil) || (supplied && operation.ResolvedPoint == bound.ResolvedPoint) {
				t.Fatalf("binding changed caller/hash or retained its pointer: before=%s after=%s err=%v", beforeHash, afterHash, err)
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func(*runfork.ForkOperationRequest)
	}{
		{"source", func(o *runfork.ForkOperationRequest) { o.SourceRunID = uuid.NewString() }},
		{"selector", func(o *runfork.ForkOperationRequest) { o.ForkEventID = uuid.NewString(); o.ResolvedPoint = nil }},
		{"revision", func(o *runfork.ForkOperationRequest) { o.ResolvedPoint.Revision++ }},
		{"timestamp", func(o *runfork.ForkOperationRequest) {
			o.ResolvedPoint.Timestamp = o.ResolvedPoint.Timestamp.Add(time.Nanosecond)
		}},
		{"input", func(o *runfork.ForkOperationRequest) { o.ResolvedPoint.Input = o.ResolvedPoint.EventID }},
		{"event_name", func(o *runfork.ForkOperationRequest) { o.ResolvedPoint.EventName = "work.changed" }},
		{"producer", func(o *runfork.ForkOperationRequest) { o.ResolvedPoint.ProducedBy = "another producer" }},
		{"cause", func(o *runfork.ForkOperationRequest) { o.ResolvedPoint.SourceEventID = uuid.NewString() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			operation := base
			point := plan.ForkPoint
			operation.ResolvedPoint = &point
			test.change(&operation)
			if _, err := bindPreparedSelectedForkOperation(&operation, plan); err == nil {
				t.Fatal("conflicting operation replaced the admitted point")
			}
		})
	}
	if operation, err := bindPreparedSelectedForkOperation(nil, plan); err != nil || operation != nil {
		t.Fatalf("event without operation minted an invocation: operation=%+v err=%v", operation, err)
	}
}
