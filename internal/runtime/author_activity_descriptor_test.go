package runtime

import (
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestAuthorActivityEventDescriptorsIncludeInternalStageTimer(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Timers: []runtimecontracts.WorkflowTimerContract{
			{StageOwned: true, Event: runtimecontracts.WorkflowStageTimerInternalEvent},
		}},
	})
	descriptors, err := AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatalf("AuthorActivityEventDescriptors: %v", err)
	}
	for _, descriptor := range descriptors {
		if descriptor.EventType != runtimecontracts.WorkflowStageTimerInternalEvent {
			continue
		}
		if descriptor.Disposition != runtimeauthoractivity.StoryDifferent || descriptor.AuthorSummaryField != "" {
			t.Fatalf("internal stage timer descriptor = %#v", descriptor)
		}
		return
	}
	t.Fatalf("internal stage timer descriptor missing from %#v", descriptors)
}
