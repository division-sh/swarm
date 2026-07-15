package authoractivity

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type StoryDisposition string

const (
	StoryAuthored  StoryDisposition = "authored"
	StoryDifferent StoryDisposition = "different"
)

type EventDescriptor struct {
	EventType          string
	Disposition        StoryDisposition
	AuthorSummaryField string
}

type resolvedEventDescriptorFact struct {
	scope      Scope
	descriptor EventDescriptor
}

type resolvedEventDescriptorContextKey struct{}

type catalogEntry struct {
	descriptors map[string]EventDescriptor
	refs        int
}

type EventCatalogRegistry struct {
	mu      sync.RWMutex
	entries map[string]catalogEntry
}

type EventCatalogLease struct {
	once    sync.Once
	release func()
}

func (l *EventCatalogLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(l.release)
}

func NewEventCatalogRegistry() *EventCatalogRegistry {
	return &EventCatalogRegistry{entries: map[string]catalogEntry{}}
}

func (r *EventCatalogRegistry) Register(scope Scope, descriptors []EventDescriptor) (*EventCatalogLease, error) {
	if r == nil {
		return nil, fmt.Errorf("author activity event catalog registry is required")
	}
	if scope.Kind != ScopeBundle || strings.TrimSpace(scope.RuntimeInstanceID) == "" || strings.TrimSpace(scope.BundleHash) == "" {
		return nil, fmt.Errorf("author activity event catalog requires exact bundle scope")
	}
	normalized, err := normalizeEventDescriptors(descriptors)
	if err != nil {
		return nil, err
	}
	key := catalogScopeKey(scope)
	r.mu.Lock()
	if existing, ok := r.entries[key]; ok {
		if !eventDescriptorsEqual(existing.descriptors, normalized) {
			r.mu.Unlock()
			return nil, fmt.Errorf("author activity event catalog conflicts for runtime %q bundle %q", scope.RuntimeInstanceID, scope.BundleHash)
		}
		existing.refs++
		r.entries[key] = existing
	} else {
		r.entries[key] = catalogEntry{descriptors: normalized, refs: 1}
	}
	r.mu.Unlock()
	return &EventCatalogLease{release: func() { r.release(key) }}, nil
}

func (r *EventCatalogRegistry) Resolve(scope Scope, eventType string) (EventDescriptor, bool) {
	if r == nil || scope.Kind != ScopeBundle {
		return EventDescriptor{}, false
	}
	r.mu.RLock()
	entry, ok := r.entries[catalogScopeKey(scope)]
	if !ok {
		r.mu.RUnlock()
		return EventDescriptor{}, false
	}
	descriptor, ok := entry.descriptors[strings.TrimSpace(eventType)]
	r.mu.RUnlock()
	return descriptor, ok
}

func (r *EventCatalogRegistry) HasScope(scope Scope) bool {
	if r == nil || scope.Kind != ScopeBundle {
		return false
	}
	r.mu.RLock()
	_, ok := r.entries[catalogScopeKey(scope)]
	r.mu.RUnlock()
	return ok
}

func WithResolvedEventDescriptor(ctx context.Context, scope Scope, descriptor EventDescriptor) (context.Context, error) {
	if scope.Kind != ScopeBundle || strings.TrimSpace(scope.RuntimeInstanceID) == "" || strings.TrimSpace(scope.BundleHash) == "" {
		return ctx, fmt.Errorf("resolved author activity event descriptor requires exact bundle scope")
	}
	normalized, err := normalizeEventDescriptors([]EventDescriptor{descriptor})
	if err != nil {
		return ctx, err
	}
	descriptor = normalized[strings.TrimSpace(descriptor.EventType)]
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, resolvedEventDescriptorContextKey{}, resolvedEventDescriptorFact{
		scope:      scope,
		descriptor: descriptor,
	}), nil
}

// WithoutResolvedEventDescriptor starts a new event-publication context. A
// resolved descriptor is evidence for exactly one event and must not flow into
// a deferred child publication.
func WithoutResolvedEventDescriptor(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, resolvedEventDescriptorContextKey{}, resolvedEventDescriptorFact{})
}

func ResolvedEventDescriptorFromContext(ctx context.Context, scope Scope, eventType string) (EventDescriptor, bool, error) {
	if ctx == nil {
		return EventDescriptor{}, false, nil
	}
	fact, ok := ctx.Value(resolvedEventDescriptorContextKey{}).(resolvedEventDescriptorFact)
	if !ok {
		return EventDescriptor{}, false, nil
	}
	if strings.TrimSpace(fact.descriptor.EventType) == "" {
		return EventDescriptor{}, false, nil
	}
	if fact.scope.Kind != scope.Kind || strings.TrimSpace(fact.scope.RuntimeInstanceID) != strings.TrimSpace(scope.RuntimeInstanceID) || strings.TrimSpace(fact.scope.BundleHash) != strings.TrimSpace(scope.BundleHash) {
		return EventDescriptor{}, false, fmt.Errorf("resolved author activity event descriptor scope does not match persisted event scope")
	}
	eventType = strings.TrimSpace(eventType)
	if fact.descriptor.EventType != eventType {
		return EventDescriptor{}, false, fmt.Errorf("resolved author activity event descriptor %q does not match persisted event %q", fact.descriptor.EventType, eventType)
	}
	return fact.descriptor, true, nil
}

func (r *EventCatalogRegistry) release(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[key]
	if !ok {
		return
	}
	entry.refs--
	if entry.refs == 0 {
		delete(r.entries, key)
		return
	}
	r.entries[key] = entry
}

func normalizeEventDescriptors(descriptors []EventDescriptor) (map[string]EventDescriptor, error) {
	out := make(map[string]EventDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		descriptor.EventType = strings.TrimSpace(descriptor.EventType)
		descriptor.AuthorSummaryField = strings.TrimSpace(descriptor.AuthorSummaryField)
		if descriptor.EventType == "" {
			return nil, fmt.Errorf("author activity event descriptor event_type is required")
		}
		if descriptor.Disposition != StoryAuthored && descriptor.Disposition != StoryDifferent {
			return nil, fmt.Errorf("author activity event %q disposition %q is not registered", descriptor.EventType, descriptor.Disposition)
		}
		if previous, ok := out[descriptor.EventType]; ok && previous != descriptor {
			return nil, fmt.Errorf("author activity event descriptor %q conflicts within one bundle", descriptor.EventType)
		}
		out[descriptor.EventType] = descriptor
	}
	return out, nil
}

func eventDescriptorsEqual(left, right map[string]EventDescriptor) bool {
	if len(left) != len(right) {
		return false
	}
	keys := make([]string, 0, len(left))
	for key := range left {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if left[key] != right[key] {
			return false
		}
	}
	return true
}

func catalogScopeKey(scope Scope) string {
	return strings.TrimSpace(scope.RuntimeInstanceID) + "\x00" + strings.TrimSpace(scope.BundleHash)
}
