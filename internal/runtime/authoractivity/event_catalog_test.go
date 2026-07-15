package authoractivity

import (
	"context"
	"strings"
	"testing"
)

func TestEventCatalogRegistryIsScopedConflictSafeAndReferenceCounted(t *testing.T) {
	registry := NewEventCatalogRegistry()
	first := BundleScope("runtime-a", "bundle-v1:sha256:"+strings.Repeat("a", 64))
	second := BundleScope("runtime-b", "bundle-v1:sha256:"+strings.Repeat("b", 64))
	descriptors := []EventDescriptor{{EventType: "message.sent", Disposition: StoryAuthored, AuthorSummaryField: "text"}}

	leaseA, err := registry.Register(first, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	leaseA2, err := registry.Register(first, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	leaseB, err := registry.Register(second, []EventDescriptor{{EventType: "message.sent", Disposition: StoryDifferent}})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.HasScope(first) || !registry.HasScope(second) {
		t.Fatal("registered exact scopes are not live")
	}
	if got, ok := registry.Resolve(first, "message.sent"); !ok || got.Disposition != StoryAuthored {
		t.Fatalf("first descriptor = %#v, %v", got, ok)
	}
	if got, ok := registry.Resolve(second, "message.sent"); !ok || got.Disposition != StoryDifferent {
		t.Fatalf("second descriptor = %#v, %v", got, ok)
	}
	if _, err := registry.Register(first, []EventDescriptor{{EventType: "message.sent", Disposition: StoryDifferent}}); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting registration error = %v", err)
	}

	leaseA.Release()
	if !registry.HasScope(first) {
		t.Fatal("first identical lease release removed a still-live scope")
	}
	leaseA.Release()
	if !registry.HasScope(first) {
		t.Fatal("repeated release was not idempotent")
	}
	leaseA2.Release()
	if registry.HasScope(first) {
		t.Fatal("last lease release left stale scope")
	}
	if !registry.HasScope(second) {
		t.Fatal("one scope teardown removed another scope")
	}
	leaseB.Release()
}

func TestResolvedEventDescriptorFactRequiresExactMatchingScopeAndName(t *testing.T) {
	scope := BundleScope("runtime-a", "bundle-v1:sha256:"+strings.Repeat("a", 64))
	descriptor := EventDescriptor{EventType: "flow/instance/message.sent", Disposition: StoryAuthored, AuthorSummaryField: "text"}
	ctx, err := WithResolvedEventDescriptor(context.Background(), scope, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ResolvedEventDescriptorFromContext(ctx, scope, descriptor.EventType); err != nil || !ok || got != descriptor {
		t.Fatalf("resolved descriptor = %#v, %v, %v", got, ok, err)
	}
	otherScope := BundleScope("runtime-b", scope.BundleHash)
	if _, _, err := ResolvedEventDescriptorFromContext(ctx, otherScope, descriptor.EventType); err == nil || !strings.Contains(err.Error(), "scope") {
		t.Fatalf("scope mismatch error = %v", err)
	}
	if _, _, err := ResolvedEventDescriptorFromContext(ctx, scope, "other.event"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("name mismatch error = %v", err)
	}
}

func TestResolvedEventDescriptorFactDoesNotCrossEventPublicationBoundary(t *testing.T) {
	scope := BundleScope("runtime-a", "bundle-a")
	ctx, err := WithResolvedEventDescriptor(context.Background(), scope, EventDescriptor{
		EventType:   "parent.started",
		Disposition: StoryDifferent,
	})
	if err != nil {
		t.Fatalf("WithResolvedEventDescriptor: %v", err)
	}

	ctx = WithoutResolvedEventDescriptor(ctx)
	if got, ok, err := ResolvedEventDescriptorFromContext(ctx, scope, "child.started"); err != nil || ok || got != (EventDescriptor{}) {
		t.Fatalf("descriptor after publication boundary = %#v, ok=%v, err=%v; want absent", got, ok, err)
	}
}
