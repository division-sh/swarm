package runforkexecution

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/google/uuid"
)

func TestSelectedForkCommitOutcomeRequiresIsolatedTypedError(t *testing.T) {
	cause := errors.New("postcommit cleanup")
	materializationErr := selectedMaterializationCommitTestError{value: runfork.RunForkMaterialization{ForkRunID: "fork"}, cause: cause}
	if got, ok := isolatedSelectedForkMaterializationCommit(materializationErr); !ok || got.ForkRunID != "fork" {
		t.Fatalf("isolated materialization commit=%+v acknowledged=%t", got, ok)
	}
	if _, ok := isolatedSelectedForkMaterializationCommit(errors.Join(materializationErr, context.Canceled)); ok {
		t.Fatal("joined cancellation authorized materialization continuation")
	}
	materializationErr.cause = errors.Join(cause, errors.New("second native cleanup failure"))
	if got, ok := isolatedSelectedForkMaterializationCommit(materializationErr); !ok || got.ForkRunID != "fork" {
		t.Fatalf("native cleanup diagnostics displaced acknowledged materialization: %+v, %t", got, ok)
	}
	if _, ok := isolatedSelectedForkMaterializationCommit(cause); ok {
		t.Fatal("untyped error authorized materialization continuation")
	}
	sourceErr := selectedSourceEventsCommitTestError{
		sourceRunID: "source", forkRunID: "fork",
		value: []runfork.RunForkSelectedContractSourceEvent{{SourceEventID: "source-event"}}, cause: cause,
	}
	if source, fork, got, ok := isolatedSelectedForkSourceEventsCommit(sourceErr); !ok || source != "source" || fork != "fork" || len(got) != 1 {
		t.Fatalf("isolated source commit scope=%s/%s events=%+v acknowledged=%t", source, fork, got, ok)
	}
	if _, _, _, ok := isolatedSelectedForkSourceEventsCommit(errors.Join(sourceErr, context.Canceled)); ok {
		t.Fatal("joined cancellation authorized source-event continuation")
	}
	sourceErr.cause = errors.Join(cause, errors.New("second native cleanup failure"))
	if source, fork, events, ok := isolatedSelectedForkSourceEventsCommit(sourceErr); !ok || source != "source" || fork != "fork" || len(events) != 1 {
		t.Fatalf("native cleanup diagnostics displaced acknowledged source events: %s/%s %+v, %t", source, fork, events, ok)
	}
	if _, _, _, ok := isolatedSelectedForkSourceEventsCommit(cause); ok {
		t.Fatal("untyped error authorized source-event continuation")
	}
}

func TestSelectedForkCommittedOutputsRequireExactAdmittedScope(t *testing.T) {
	eventID := uuid.NewString()
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: eventID, Revision: 2}
	req := runforkreadiness.MaterializeRequest{SourceRunID: "source", At: eventID, Preparation: runfork.SelectedForkPreparation{ForkPoint: point}, ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"}}
	valid := runfork.RunForkMaterialization{
		SourceRunID: "source", ForkRunID: "fork", ForkPoint: point,
		SelectedContractBinding: &runfork.RunForkSelectedContractBinding{
			BindingID: "binding", ForkRunID: "fork", SourceRunID: "source", ForkPoint: point, ForkEventID: eventID, ContractSelection: req.ContractSelection,
		},
	}
	if err := requireExactSelectedForkMaterialization(req, valid); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*runfork.RunForkMaterialization){
		func(value *runfork.RunForkMaterialization) { value.SourceRunID = "foreign" },
		func(value *runfork.RunForkMaterialization) { value.ForkRunID = "foreign" },
		func(value *runfork.RunForkMaterialization) { value.ForkPoint.EventID = "foreign" },
		func(value *runfork.RunForkMaterialization) { value.ForkPoint.Revision++ },
		func(value *runfork.RunForkMaterialization) { value.SelectedContractBinding = nil },
		func(value *runfork.RunForkMaterialization) {
			copy := *value.SelectedContractBinding
			copy.ContractSelection.Mode = "foreign"
			value.SelectedContractBinding = &copy
		},
	} {
		changed := valid
		change(&changed)
		if err := requireExactSelectedForkMaterialization(req, changed); err == nil {
			t.Fatalf("foreign materialization accepted: %+v", changed)
		}
	}
	deploymentPoint := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 3}
	deploymentReq := req
	deploymentReq.At = ""
	deploymentReq.Preparation.ForkPoint = deploymentPoint
	deployment := valid
	deployment.ForkPoint = deploymentPoint
	binding := *valid.SelectedContractBinding
	binding.ForkPoint = deploymentPoint
	binding.ForkEventID = ""
	deployment.SelectedContractBinding = &binding
	if err := requireExactSelectedForkMaterialization(deploymentReq, deployment); err != nil {
		t.Fatalf("exact eventless materialization rejected: %v", err)
	}
	deployment.SelectedContractBinding.ForkPoint.Revision++
	if err := requireExactSelectedForkMaterialization(deploymentReq, deployment); err == nil {
		t.Fatal("materialization accepted a different deployment revision")
	}
	if err := requireExactSelectedForkSourceEvents([]string{"first", "second"}, []runfork.RunForkSelectedContractSourceEvent{{SourceEventID: "second"}, {SourceEventID: "first"}}); err != nil {
		t.Fatal(err)
	}
	for _, events := range [][]runfork.RunForkSelectedContractSourceEvent{
		{{SourceEventID: "first"}},
		{{SourceEventID: "first"}, {SourceEventID: "first"}},
		{{SourceEventID: "first"}, {SourceEventID: "foreign"}},
	} {
		if err := requireExactSelectedForkSourceEvents([]string{"first", "second"}, events); err == nil {
			t.Fatalf("foreign or incomplete source events accepted: %+v", events)
		}
	}
}
