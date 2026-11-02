package runtimepersistence

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/decisioncardtest"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestAuthorActivityDecisionCardAdapterCoversRegisteredAnchors(t *testing.T) {
	stageEntityID := uuid.NewString()
	stage, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		Route: runtimeflowidentity.RouteForInstancePath("root/review"), FlowID: "review", EntityID: stageEntityID, Stage: "pending", StageActivationID: uuid.NewString(),
		Source: eventtest.ConcreteTemplateRoutingSource("review", "root/review", stageEntityID),
	})
	if err != nil {
		t.Fatal(err)
	}
	human, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "reviewer", OperationID: decisioncardtest.HumanOperation(t, t.Name(), "human-op"), Category: "review",
		Scope:  decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "root/review"},
		Source: eventtest.RootRoutingSource("human-task-owner"),
	})
	if err != nil {
		t.Fatal(err)
	}
	proposed, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: uuid.NewString(), ActivityID: "send_reply", Decision: "support_reply",
		Scope:  decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: "root/review", EntityID: uuid.NewString()},
		Source: eventtest.RootRoutingSource("proposed-effect-owner"),
	})
	if err != nil {
		t.Fatal(err)
	}
	registered := map[decisioncard.AnchorKind]decisioncard.Anchor{
		decisioncard.AnchorKindStageGate: stage, decisioncard.AnchorKindHumanTask: human, decisioncard.AnchorKindProposedEffect: proposed,
	}
	if len(registered) != len(decisioncard.RegisteredAnchorKinds()) {
		t.Fatalf("author activity decision-card adapters = %d, registered anchors = %d", len(registered), len(decisioncard.RegisteredAnchorKinds()))
	}
	for _, kind := range decisioncard.RegisteredAnchorKinds() {
		anchor, ok := registered[kind]
		if !ok {
			t.Fatalf("registered decision-card anchor %q has no author activity fixture", kind)
		}
		anchorID, _, _, err := decisionCardAuthorActivityIdentity(anchor)
		if err != nil || anchorID == "" {
			t.Fatalf("decision-card anchor %q author activity identity = %q, %v", kind, anchorID, err)
		}
	}
}

func TestAuthorActivityPlatformEventDispositionCoversSpecCatalog(t *testing.T) {
	source, err := yamlsource.LoadFile(runtimecontracts.DefaultPlatformSpecFile(authorActivityRegistryRepoRoot(t)))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	document, err := runtimecontracts.AdmitPlatformSpecValue(source.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatalf("decode platform event catalog: %v", err)
	}
	if len(runtimecontracts.PlatformEventCatalogNames(document)) == 0 {
		t.Fatal("platform event catalog is empty")
	}
	for _, name := range runtimecontracts.PlatformEventCatalogNames(document) {
		disposition, ok := storeactivityjournal.PlatformEventDisposition[name]
		if !ok {
			t.Errorf("platform event %q has no author activity disposition", name)
			continue
		}
		switch disposition {
		case storeactivityjournal.PlatformDispositionRegistered, storeactivityjournal.PlatformDispositionHandled, storeactivityjournal.PlatformDispositionDifferent:
		default:
			t.Errorf("platform event %q has invalid author activity disposition %q", name, disposition)
		}
	}
	for name := range storeactivityjournal.PlatformEventDisposition {
		if !runtimecontracts.PlatformEventCatalogContains(document, name) {
			t.Errorf("author activity disposition names non-catalog platform event %q", name)
		}
	}
}

func TestAuthorActivityEffectDispositionCoversRegistrations(t *testing.T) {
	registrations := runtimeeffects.Registrations()
	seen := make(map[string]struct{}, len(registrations))
	for _, registration := range registrations {
		key := string(registration.Kind) + "/" + registration.Adapter
		if _, duplicate := seen[key]; duplicate {
			t.Errorf("duplicate effect registration %q", key)
		}
		seen[key] = struct{}{}
		if _, ok := externalEffectStoryDispositions[key]; !ok {
			t.Errorf("effect registration %q has no author activity disposition", key)
		}
	}
	var stale []string
	for key := range externalEffectStoryDispositions {
		if _, ok := seen[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Fatalf("author activity effect dispositions without live registrations: %v", stale)
	}
}

type dynamicAuthoredEventDescriptorResolver struct {
	registry *runtimeauthoractivity.EventCatalogRegistry
}

type rejectedAuthorActivityMutation struct{}

func (rejectedAuthorActivityMutation) Record(context.Context, runtimeauthoractivity.Draft) error {
	return errors.New("unexpected author activity record")
}

func (rejectedAuthorActivityMutation) PersistedOccurredAt(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (r dynamicAuthoredEventDescriptorResolver) ResolveAuthorActivityEventDescriptor(scope runtimeauthoractivity.Scope, name string) (runtimeauthoractivity.EventDescriptor, bool) {
	return r.registry.Resolve(scope, name)
}

func (r dynamicAuthoredEventDescriptorResolver) AuthorActivityEventCatalogRegistered(scope runtimeauthoractivity.Scope) bool {
	return r.registry.HasScope(scope)
}

func TestDynamicAuthorActivityEventDescriptorRejectsStaticConflict(t *testing.T) {
	scope := runtimeauthoractivity.BundleScope(uuid.NewString(), "bundle-v2:sha256:"+strings.Repeat("e", 64))
	registry := runtimeauthoractivity.NewEventCatalogRegistry()
	lease, err := registry.Register(scope, []runtimeauthoractivity.EventDescriptor{{EventType: "message.sent", Disposition: runtimeauthoractivity.StoryAuthored}})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	ctx := runtimeauthoractivity.WithScope(context.Background(), scope)
	ctx, err = runtimeauthoractivity.WithResolvedEventDescriptor(ctx, scope, runtimeauthoractivity.EventDescriptor{
		EventType: "message.sent", Disposition: runtimeauthoractivity.StoryDifferent,
	})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType("message.sent"), "sender", "", []byte(`{"text":"hello"}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now(),
	)
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := storeactivityjournal.RecordPersistedEvent(ctx, rejectedAuthorActivityMutation{}, dynamicAuthoredEventDescriptorResolver{registry: registry}, admitted, "sender", "agent"); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("descriptor conflict error = %v", err)
	}
}

func authorActivityRegistryRepoRoot(t *testing.T) string {
	t.Helper()
	return repoRootForRuntimeWriterGuard(t)
}
