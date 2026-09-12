package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// Observe the real selected preparation registration, without replacing its
// store catalog, lease, or later deferred-execution rejection.
type observedSelectedJoinCatalog struct {
	SelectedContractForkLifecycle
	t       *testing.T
	want    []string
	scopes  []authoractivity.Scope
	resolve func(authoractivity.Scope, string) (authoractivity.EventDescriptor, bool)
}

type selectedJoinCatalogSource struct{ semanticview.Source }

func (s selectedJoinCatalogSource) WorkflowJoins() []contracts.WorkflowJoinPlan {
	return []contracts.WorkflowJoinPlan{{Mode: contracts.WorkflowJoinModeArrival, Spec: contracts.JoinSpec{Timeout: contracts.JoinTimeoutSpec{After: "1h"}}}}
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
		if got, found := s.registry.Resolve(scope, name); !found || got.Disposition != authoractivity.StoryDifferent || got.AuthorSummaryField != "" {
			s.t.Errorf("activation catalog lost %s: %+v %v", name, got, found)
		}
	}
	return lease, nil
}

func TestSelectedActivationReconstructsJoinCatalogWithoutDeferredAuthority(t *testing.T) {
	forkID := uuid.NewString()
	binding := testSelectedContractBinding(forkID)
	loaded := testLoadedSelectedSource(binding.ContractSelection)
	loaded.Source = selectedJoinCatalogSource{loaded.Source}
	store := &observedActivationJoinCatalog{fakeSelectedContractActivationStore: &fakeSelectedContractActivationStore{
		binding: binding, bindingOK: true, bundleAvailability: testSelectedContractBundleAvailability(forkID), plan: testSelectedContractStateOnlyPlan(binding),
	}, registry: authoractivity.NewEventCatalogRegistry(), t: t}
	loader := &fakeSelectedContractSourceLoader{loaded: loaded, original: originalActivationSourceFixture(t)}
	owner := selectedContractGateOwnerForTest(t)
	for attempt := 0; attempt < 2; attempt++ {
		_, err := activateLiveSelectedContractRunFork(runForkTestContext(t), SelectedContractActivationGateRequest{
			ExecutionOwner: owner, AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: owner.ports.contexts.capability}, ForkRunID: forkID, Store: store, SourceLoader: loader,
		})
		failure, ok := failures.EnvelopeFromError(err)
		if !ok || failure.Detail.Code != selectedContractDeferredWorkOwnerUnavailable {
			t.Fatalf("activation changed deferred policy: %v", err)
		}
		if store.activateCalled || len(store.scopes) != attempt+1 {
			t.Fatal("catalog reconstruction bypassed policy or did not register")
		}
		for _, scope := range store.scopes {
			if scope.RuntimeInstanceID == "" || !strings.HasPrefix(scope.BundleHash, "bundle-v2:sha256:") || store.registry.HasScope(scope) {
				t.Fatalf("refusal retained or crossed activation scope: %+v", scope)
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
	for _, name := range o.want {
		got, found := o.resolve(scope, name)
		if !found || got.Disposition != authoractivity.StoryDifferent || got.AuthorSummaryField != "" {
			o.t.Errorf("selected preparation omitted exact internal descriptor %s: %+v found=%v", name, got, found)
		}
		other := scope
		other.RuntimeInstanceID += "-other"
		if _, found := o.resolve(other, name); found {
			o.t.Error("selected descriptor leaked to another runtime")
		}
		other = scope
		other.BundleHash += "-other"
		if _, found := o.resolve(other, name); found {
			o.t.Error("selected descriptor leaked to another bundle")
		}
	}
	return lease, nil
}
