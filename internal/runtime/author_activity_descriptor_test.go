package runtime

import (
	"strings"
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

type joinDescriptorSource struct {
	semanticview.Source
	joins    []runtimecontracts.WorkflowJoinPlan
	conflict bool
}

func (s joinDescriptorSource) WorkflowJoins() []runtimecontracts.WorkflowJoinPlan { return s.joins }
func (s joinDescriptorSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	if s.conflict {
		return map[string]runtimecontracts.EventCatalogEntry{"platform.join_complete": {}}
	}
	return s.Source.ResolvedEventCatalog()
}
func (s joinDescriptorSource) AuthoredResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	if s.conflict {
		return s.ResolvedEventCatalog()
	}
	return s.Source.AuthoredResolvedEventCatalog()
}

func TestAuthorActivityEventDescriptorsJoinDemandAndConflict(t *testing.T) {
	base := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	for _, cell := range []struct {
		name              string
		joins             []runtimecontracts.WorkflowJoinPlan
		complete, timeout int
		conflict, reject  bool
	}{
		{name: "no_join"},
		{name: "untimed_arrival", joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival}}, complete: 1},
		{name: "timed_fanout", joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery, Spec: runtimecontracts.JoinSpec{TimeoutFound: true, Timeout: runtimecontracts.JoinTimeoutSpec{After: "1h"}}}}, complete: 1, timeout: 1},
		{name: "multiple_declarations", joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival}, {Mode: runtimecontracts.WorkflowJoinModeFanOutDelivery}, {Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: runtimecontracts.JoinSpec{Timeout: runtimecontracts.JoinTimeoutSpec{After: "1h"}}}}, complete: 1, timeout: 1},
		{name: "authored_conflict", joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival}}, conflict: true, reject: true},
		{name: "invalid_compiled_mode", joins: []runtimecontracts.WorkflowJoinPlan{{}}, reject: true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			descriptors, err := AuthorActivityEventDescriptors(joinDescriptorSource{Source: base, joins: cell.joins, conflict: cell.conflict})
			if cell.reject {
				if err == nil {
					t.Fatal("invalid descriptor authority accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, descriptor := range descriptors {
				counts[descriptor.EventType]++
				if descriptor.Disposition != runtimeauthoractivity.StoryDifferent || descriptor.AuthorSummaryField != "" {
					t.Fatalf("internal occurrence became authored: %+v", descriptor)
				}
			}
			if counts["platform.join_complete"] != cell.complete || counts["platform.join_timeout"] != cell.timeout {
				t.Fatalf("wrong compiled occurrence demand: %+v", descriptors)
			}
		})
	}
}

type joinDescriptorRegistrar struct {
	registry *runtimeauthoractivity.EventCatalogRegistry
}

func (r joinDescriptorRegistrar) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return r.registry.Register(scope, descriptors)
}

func TestAuthorActivityEventDescriptorsNormalCatalogLifetime(t *testing.T) {
	descriptors, err := AuthorActivityEventDescriptors(joinDescriptorSource{Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), joins: []runtimecontracts.WorkflowJoinPlan{{Mode: runtimecontracts.WorkflowJoinModeArrival, Spec: runtimecontracts.JoinSpec{Timeout: runtimecontracts.JoinTimeoutSpec{After: "1h"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	registry := runtimeauthoractivity.NewEventCatalogRegistry()
	scope := runtimeauthoractivity.BundleScope("runtime-a", "bundle-v2:sha256:"+strings.Repeat("a", 64))
	newRuntime := func(scope runtimeauthoractivity.Scope) *Runtime {
		return &Runtime{authorActivityScope: scope, authorActivityDescriptors: descriptors, authorActivityRegistrars: []AuthorActivityCatalogRegistrar{joinDescriptorRegistrar{registry}}}
	}
	first, overlap := newRuntime(scope), newRuntime(scope)
	otherScope := runtimeauthoractivity.BundleScope("runtime-b", scope.BundleHash)
	other := newRuntime(otherScope)
	for _, rt := range []*Runtime{first, overlap, other} {
		if err := rt.PrepareAuthorActivityCatalog(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(rt.releaseAuthorActivityCatalog)
	}
	first.releaseAuthorActivityCatalog()
	for _, s := range []runtimeauthoractivity.Scope{scope, otherScope} {
		for _, name := range []string{"platform.join_complete", "platform.join_timeout"} {
			if descriptor, ok := registry.Resolve(s, name); !ok || descriptor.Disposition != runtimeauthoractivity.StoryDifferent {
				t.Fatalf("exact live lease lost descriptor: %+v %v", descriptor, ok)
			}
		}
	}
	overlap.releaseAuthorActivityCatalog()
	if registry.HasScope(scope) || !registry.HasScope(otherScope) {
		t.Fatal("lease release crossed runtime identity")
	}
	restored := newRuntime(scope)
	if err := restored.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restored.releaseAuthorActivityCatalog)
	if _, ok := registry.Resolve(scope, "platform.join_complete"); !ok {
		t.Fatal("reconstructed catalog lost occurrence")
	}
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
