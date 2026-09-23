package runforkexecution

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
)

type selectedForkMaterializationCommit interface {
	SelectedForkMaterializationCommit() runfork.RunForkMaterialization
}

type selectedForkSourceEventsCommit interface {
	SelectedForkSourceEventsCommit() (string, string, []runfork.RunForkSelectedContractSourceEvent)
}

// Only a direct store postcommit carrier authorizes continuation. Its cause
// may contain multiple native cleanup failures; an outer join can contain an
// independent cancellation or fence failure and is not admitted.
func isolatedSelectedForkMaterializationCommit(err error) (runfork.RunForkMaterialization, bool) {
	if committed, ok := err.(selectedForkMaterializationCommit); ok {
		return committed.SelectedForkMaterializationCommit(), true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isolatedSelectedForkMaterializationCommit(wrapped.Unwrap())
	}
	return runfork.RunForkMaterialization{}, false
}

func requireExactSelectedForkMaterialization(req runforkreadiness.MaterializeRequest, result runfork.RunForkMaterialization) error {
	binding := result.SelectedContractBinding
	if binding == nil || binding.BindingID == "" || result.ForkRunID == "" ||
		binding.ForkRunID != result.ForkRunID ||
		result.SourceRunID != req.SourceRunID || result.ForkPoint.EventID != req.At ||
		binding.SourceRunID != req.SourceRunID || binding.ForkEventID != req.At ||
		binding.ContractSelection != req.ContractSelection {
		return fmt.Errorf("selected materialization returned no exact admitted binding")
	}
	return nil
}

func isolatedSelectedForkSourceEventsCommit(err error) (string, string, []runfork.RunForkSelectedContractSourceEvent, bool) {
	if committed, ok := err.(selectedForkSourceEventsCommit); ok {
		sourceRunID, forkRunID, events := committed.SelectedForkSourceEventsCommit()
		return sourceRunID, forkRunID, events, true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isolatedSelectedForkSourceEventsCommit(wrapped.Unwrap())
	}
	return "", "", nil, false
}

func requireExactSelectedForkSourceEvents(requested []string, events []runfork.RunForkSelectedContractSourceEvent) error {
	ids := normalizeSelectedContractRuntimeContainerSourceEvents(requested)
	if len(events) != len(ids) {
		return fmt.Errorf("selected-contract source event commit returned %d of %d requested events", len(events), len(ids))
	}
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	for _, event := range events {
		id := strings.TrimSpace(event.SourceEventID)
		if _, ok := want[id]; !ok {
			return fmt.Errorf("selected-contract source event commit returned unrequested or duplicate event %s", id)
		}
		delete(want, id)
	}
	return nil
}
