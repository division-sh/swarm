package activityjournal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type receiverDescriptorRegistry struct {
	*authoractivity.EventCatalogRegistry
}

func (r receiverDescriptorRegistry) ResolveAuthorActivityEventDescriptor(scope authoractivity.Scope, name string) (authoractivity.EventDescriptor, bool) {
	return r.Resolve(scope, name)
}

func (r receiverDescriptorRegistry) AuthorActivityEventCatalogRegistered(scope authoractivity.Scope) bool {
	return r.HasScope(scope)
}

func TestReceiverDescriptorStillRequiresExactEvidenceAndLiveLease(t *testing.T) {
	const name = "worker-flow/worker-001/worker.inspect.requested"
	scope := authoractivity.BundleScope("runtime-1", "bundle-v2:sha256:"+strings.Repeat("1", 64))
	ctx := authoractivity.WithScope(context.Background(), scope)
	entityID := uuid.NewString()
	evt := eventtest.ChildWithLineageAndRoutingSource(uuid.NewString(), name, "manager", "", []byte(`{}`), 1,
		events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
		events.EventEnvelope{FlowInstance: "worker-flow/worker-001", EntityID: entityID, Scope: events.EventScopeEntity},
		eventtest.ConcreteTemplateRoutingSource("worker-flow", "worker-flow/worker-001", entityID), time.Now().UTC())
	admitted, err := events.AdmitForPersistence(evt, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	registry := receiverDescriptorRegistry{authoractivity.NewEventCatalogRegistry()}
	lease, err := registry.Register(scope, []authoractivity.EventDescriptor{{EventType: "worker-flow/worker.inspect.requested", Disposition: authoractivity.StoryDifferent}})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if _, ok, err := PersistedEventDraft(ctx, registry, admitted, "manager", "system"); err == nil || ok || !strings.Contains(err.Error(), "has no author activity descriptor") {
		t.Fatalf("flow-level declaration must not implicitly authorize an occurrence: ok=%t err=%v", ok, err)
	}
	exact, err := authoractivity.WithResolvedEventDescriptor(ctx, scope, authoractivity.EventDescriptor{EventType: name, Disposition: authoractivity.StoryDifferent})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := PersistedEventDraft(exact, registry, admitted, "manager", "system"); err != nil || ok {
		t.Fatalf("exact compiled receiver proof should be admitted without an authored story: ok=%t err=%v", ok, err)
	}
	wrong, err := authoractivity.WithResolvedEventDescriptor(ctx, scope, authoractivity.EventDescriptor{EventType: "worker-flow/sibling/worker.inspect.requested", Disposition: authoractivity.StoryDifferent})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := PersistedEventDraft(wrong, registry, admitted, "manager", "system"); err == nil || ok || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("sibling proof must not authorize this occurrence: ok=%t err=%v", ok, err)
	}
	lease.Release()
	if _, ok, err := PersistedEventDraft(exact, registry, admitted, "manager", "system"); err == nil || ok || !strings.Contains(err.Error(), "no live registry lease") {
		t.Fatalf("released catalog must not authorize an occurrence: ok=%t err=%v", ok, err)
	}
}
