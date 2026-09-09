package runtime

import (
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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

func TestAuthorActivityEventDescriptorsIncludeCompiledJoinOccurrences(t *testing.T) {
	for _, cell := range []struct {
		name   string
		bundle func(testing.TB) string
		events []string
	}{
		{"root_arrival", func(t testing.TB) string { return canonicalrouting.CopyExactJoinEventBusProof(t, "") }, []string{"platform.join_complete", "platform.join_timeout"}},
		{"flow_arrival", func(t testing.TB) string { return canonicalrouting.CopyExactJoinEventBusProof(t, "orders") }, []string{"platform.join_complete", "platform.join_timeout"}},
		{"fanout_barrier", canonicalrouting.CopyForkFanOutCompletionConsumer, []string{"platform.join_complete"}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, cell.bundle(t), runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			if len(source.WorkflowJoins()) == 0 {
				t.Fatal("fixture did not compile any join")
			}
			descriptors, err := AuthorActivityEventDescriptors(source)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range cell.events {
				count := 0
				for _, descriptor := range descriptors {
					if descriptor.EventType != event {
						continue
					}
					count++
					if descriptor.Disposition != runtimeauthoractivity.StoryDifferent || descriptor.AuthorSummaryField != "" {
						t.Errorf("internal join occurrence became an authored story: %+v", descriptor)
					}
				}
				if count != 1 {
					t.Errorf("compiled join occurrence %s requires one descriptor, got %d", event, count)
				}
			}
		})
	}
}
