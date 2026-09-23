package runforkexecution

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
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
	materializationErr.cause = errors.Join(cause, context.Canceled)
	if _, ok := isolatedSelectedForkMaterializationCommit(materializationErr); ok {
		t.Fatal("typed materialization error with joined cancellation authorized continuation")
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
	sourceErr.cause = errors.Join(cause, context.Canceled)
	if _, _, _, ok := isolatedSelectedForkSourceEventsCommit(sourceErr); ok {
		t.Fatal("typed source-event error with joined cancellation authorized continuation")
	}
	if _, _, _, ok := isolatedSelectedForkSourceEventsCommit(cause); ok {
		t.Fatal("untyped error authorized source-event continuation")
	}
}

func TestSelectedForkCommittedOutputsRequireExactAdmittedScope(t *testing.T) {
	req := runforkreadiness.MaterializeRequest{SourceRunID: "source", At: "event", ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"}}
	valid := runfork.RunForkMaterialization{
		SourceRunID: "source", ForkRunID: "fork", ForkPoint: runfork.RunForkPoint{EventID: "event"},
		SelectedContractBinding: &runfork.RunForkSelectedContractBinding{
			BindingID: "binding", ForkRunID: "fork", SourceRunID: "source", ForkEventID: "event", ContractSelection: req.ContractSelection,
		},
	}
	if err := requireExactSelectedForkMaterialization(req, valid); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*runfork.RunForkMaterialization){
		func(value *runfork.RunForkMaterialization) { value.SourceRunID = "foreign" },
		func(value *runfork.RunForkMaterialization) { value.ForkRunID = "foreign" },
		func(value *runfork.RunForkMaterialization) { value.ForkPoint.EventID = "foreign" },
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
