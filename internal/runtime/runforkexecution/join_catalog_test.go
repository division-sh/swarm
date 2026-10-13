package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// Observe the real selected preparation registration, without replacing its
// store catalog, lease, or later execution ownership.
type observedSelectedJoinCatalog struct {
	SelectedContractForkLifecycle
	scopes []authoractivity.Scope
}

type selectedJoinCatalogSource struct{ semanticview.Source }

func (s selectedJoinCatalogSource) WorkflowJoins() []contracts.WorkflowJoinPlan {
	return []contracts.WorkflowJoinPlan{{Mode: contracts.WorkflowJoinModeArrival, Spec: contracts.JoinSpec{Deadline: &contracts.JoinDeadlineSpec{After: "1h", From: contracts.JoinDeadlineFromStageEntry}}}}
}

type observedActivationJoinCatalog struct {
	*fakeSelectedContractActivationStore
	registry *authoractivity.EventCatalogRegistry
	scopes   []authoractivity.Scope
	t        *testing.T
}

func (s *observedActivationJoinCatalog) RegisterAuthorActivityEventCatalog(scope authoractivity.Scope, descriptors []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error) {
	lease, err := s.registry.Register(scope, descriptors)
	if err != nil {
		return nil, err
	}
	s.scopes = append(s.scopes, scope)
	for _, name := range []string{"platform.join_complete", "platform.join_timeout"} {
		if got, found := s.registry.Resolve(scope, name); !found || got.Disposition != authoractivity.StoryDifferent {
			s.t.Errorf("activation catalog lost %s: %+v %v", name, got, found)
		}
	}
	return lease, nil
}

func TestSelectedActivationReconstructsJoinCatalogWithRetainedOwner(t *testing.T) {
	forkID := uuid.NewString()
	binding := testSelectedContractBinding(forkID)
	loaded := testLoadedSelectedSource(binding.ContractSelection)
	loaded.Source = selectedJoinCatalogSource{loaded.Source}
	store := &observedActivationJoinCatalog{fakeSelectedContractActivationStore: &fakeSelectedContractActivationStore{
		binding: binding, bindingOK: true, bundleAvailability: testSelectedContractBundleAvailability(forkID), plan: testSelectedContractStateOnlyPlan(binding),
		activation: runfork.RunForkActivation{SourceRunID: binding.SourceRunID, ForkRunID: forkID, Activated: true},
	}, registry: authoractivity.NewEventCatalogRegistry(), t: t}
	loader := &fakeSelectedContractSourceLoader{loaded: loaded, original: originalActivationSourceFixture(t)}
	owner := selectedContractGateOwnerForTest(t)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
			ExecutionOwner: owner, AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: owner.ports.contexts.capability}, ForkRunID: forkID, Store: store, SourceLoader: loader,
		})
		if err != nil || !result.Activated || result.ForkRunID != forkID || result.SourceRunID != binding.SourceRunID {
			t.Fatalf("retained-owner activation was not acknowledged: %+v err=%v", result, err)
		}
		if !store.activateCalled || len(store.scopes) != attempt+1 {
			t.Fatal("activation did not consume its store owner and exact catalog")
		}
		for _, scope := range store.scopes {
			if scope.RuntimeInstanceID == "" || !strings.HasPrefix(scope.BundleHash, "bundle-v2:sha256:") || store.registry.HasScope(scope) {
				t.Fatalf("acknowledged activation retained or crossed catalog scope: %+v", scope)
			}
		}
	}
}

func (o *observedSelectedJoinCatalog) RegisterAuthorActivityEventCatalog(scope authoractivity.Scope, descriptors []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error) {
	lease, err := o.SelectedContractForkLifecycle.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		return nil, err
	}
	o.scopes = append(o.scopes, scope)
	return lease, nil
}
