package channelactivation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type Operation struct {
	Binding packs.OutboundBindingPlan
	Name    string
}

type snapshot struct {
	publication channelonboarding.ChannelActivationPublication
	bindings    []packs.OutboundBindingPlan
	runtime     map[string]Operation
	activities  map[string]Operation
	tools       map[string]runtimecontracts.ToolSchemaEntry
	leases      int
}

// Lease pins one exact executable activation snapshot until the caller has
// completed the operation that was admitted from it.
type Lease struct {
	owner     *Owner
	snapshot  *snapshot
	operation Operation
	authority *leaseAuthority
	borrowed  bool
	once      sync.Once
}

type leaseAuthority struct {
	released atomic.Bool
}

type executionLeaseContextKey struct{}

// WithExecutionLease carries one already-admitted process-local activation
// authority into child execution. It is never serialized or reconstructed.
func WithExecutionLease(ctx context.Context, lease *Lease) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if !lease.live() {
		return ctx
	}
	return context.WithValue(ctx, executionLeaseContextKey{}, lease)
}

func ExecutionLeaseFromContext(ctx context.Context) (*Lease, bool) {
	if ctx == nil {
		return nil, false
	}
	lease, ok := ctx.Value(executionLeaseContextKey{}).(*Lease)
	return lease, ok && lease.live()
}

// WithoutExecutionLease prevents an admitted child from forwarding its
// parent's authority into later publications.
func WithoutExecutionLease(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionLeaseContextKey{}, (*Lease)(nil))
}

func newOwnedLease(owner *Owner, current *snapshot, operation Operation) *Lease {
	return &Lease{owner: owner, snapshot: current, operation: operation, authority: &leaseAuthority{}}
}

func (l *Lease) live() bool {
	return l != nil && l.snapshot != nil && l.authority != nil && !l.authority.released.Load()
}

func (l *Lease) Operation() Operation {
	if l == nil {
		return Operation{}
	}
	return l.operation
}

func (l *Lease) Generation() channelonboarding.ChannelActivationGeneration {
	if l == nil || l.snapshot == nil {
		return channelonboarding.ChannelActivationGeneration{}
	}
	return l.snapshot.publication.Generation()
}

func (l *Lease) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	out := map[string]runtimecontracts.ToolSchemaEntry{}
	if l == nil || l.snapshot == nil {
		return out
	}
	for id, tool := range l.snapshot.tools {
		out[id] = tool
	}
	return out
}

func (l *Lease) RuntimeOperation(toolID string) (Operation, bool) {
	if l == nil || l.snapshot == nil {
		return Operation{}, false
	}
	operation, ok := l.snapshot.runtime[strings.TrimSpace(toolID)]
	return operation, ok
}

// BorrowRuntimeOperation binds one operation to an already-held presentation
// lease without acquiring or releasing another owner refcount.
func (l *Lease) BorrowRuntimeOperation(toolID string) (*Lease, bool) {
	if !l.live() {
		return nil, false
	}
	operation, ok := l.RuntimeOperation(toolID)
	if !ok {
		return nil, false
	}
	return &Lease{owner: l.owner, snapshot: l.snapshot, operation: operation, authority: l.authority, borrowed: true}, true
}

// BorrowActivityOperation admits a child activity from the exact snapshot
// already pinned by its parent operation. It does not reopen owner admission
// after replacement has fenced new consumers.
func (o *Owner) BorrowActivityOperation(parent *Lease, toolID string, generation channelonboarding.ChannelActivationGeneration) (*Lease, bool) {
	if o == nil || !parent.live() || parent.owner != o || !generation.Valid() || !parent.snapshot.publication.Generation().Equal(generation) {
		return nil, false
	}
	operation, ok := parent.snapshot.activities[strings.TrimSpace(toolID)]
	if !ok {
		return nil, false
	}
	return &Lease{owner: o, snapshot: parent.snapshot, operation: operation, authority: parent.authority, borrowed: true}, true
}

func (l *Lease) Activations() []channelonboarding.CompiledActivation {
	if l == nil || l.snapshot == nil {
		return nil
	}
	return l.snapshot.publication.Activations()
}

func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.borrowed {
			return
		}
		if l.authority != nil {
			l.authority.released.Store(true)
		}
		if l.owner == nil || l.snapshot == nil {
			return
		}
		l.owner.mu.Lock()
		l.snapshot.leases--
		if l.snapshot.leases == 0 {
			l.owner.changed.Broadcast()
		}
		l.owner.mu.Unlock()
	})
}

// Owner is the one process-local executable projection for declared and
// learned channel activations. Replacement fences new leases, waits for every
// predecessor execution lease, and then publishes one immutable successor.
type Owner struct {
	mu        sync.Mutex
	changed   *sync.Cond
	current   *snapshot
	accepting bool
}

func NewOwner(publication channelonboarding.ChannelActivationPublication) (*Owner, error) {
	owner := &Owner{accepting: true}
	owner.changed = sync.NewCond(&owner.mu)
	next, err := compileSnapshot(publication)
	if err != nil {
		return nil, err
	}
	owner.current = next
	return owner, nil
}

func (o *Owner) Replace(publication channelonboarding.ChannelActivationPublication) error {
	return o.ReplaceContext(context.Background(), publication)
}

func (o *Owner) ReplaceContext(ctx context.Context, publication channelonboarding.ChannelActivationPublication) error {
	if o == nil {
		return fmt.Errorf("channel activation owner is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	next, err := compileSnapshot(publication)
	if err != nil {
		return err
	}

	o.mu.Lock()
	if o.changed == nil {
		o.changed = sync.NewCond(&o.mu)
	}
	predecessor := o.current
	o.accepting = false
	stopWakeup := context.AfterFunc(ctx, func() {
		o.mu.Lock()
		o.changed.Broadcast()
		o.mu.Unlock()
	})
	defer stopWakeup()
	for predecessor != nil && predecessor.leases > 0 && ctx.Err() == nil {
		o.changed.Wait()
	}
	if err := ctx.Err(); err != nil {
		o.accepting = true
		o.changed.Broadcast()
		o.mu.Unlock()
		return fmt.Errorf("fence predecessor channel activations: %w", err)
	}
	o.current = next
	o.accepting = true
	o.changed.Broadcast()
	o.mu.Unlock()
	return nil
}

func compileSnapshot(publication channelonboarding.ChannelActivationPublication) (*snapshot, error) {
	if err := publication.Validate(); err != nil {
		return nil, err
	}
	if !publication.Executable() {
		return nil, fmt.Errorf("declared-only channel activation publication cannot grant executable authority")
	}
	bindings := publication.Bindings()
	next := &snapshot{
		publication: publication,
		bindings:    append([]packs.OutboundBindingPlan(nil), bindings...),
		runtime:     map[string]Operation{}, activities: map[string]Operation{},
		tools: map[string]runtimecontracts.ToolSchemaEntry{},
	}
	for _, binding := range next.bindings {
		tools, err := binding.RuntimeTools()
		if err != nil {
			return nil, fmt.Errorf("channel binding %q runtime tools: %w", binding.BindingID(), err)
		}
		for id, tool := range tools {
			if _, duplicate := next.tools[id]; duplicate {
				return nil, fmt.Errorf("duplicate channel runtime tool %q", id)
			}
			next.tools[id] = tool
		}
		for _, name := range binding.OperationNames() {
			operation := Operation{Binding: binding, Name: name}
			runtimeID := strings.TrimSpace(binding.RuntimeToolID(name))
			if runtimeID == "" {
				return nil, fmt.Errorf("channel binding %q operation %q has no runtime tool identity", binding.BindingID(), name)
			}
			if _, duplicate := next.runtime[runtimeID]; duplicate {
				return nil, fmt.Errorf("duplicate channel runtime tool %q", runtimeID)
			}
			if _, compiled := next.tools[runtimeID]; !compiled {
				return nil, fmt.Errorf("channel binding %q operation %q runtime tool %q is absent from its compiled projection", binding.BindingID(), name, runtimeID)
			}
			next.runtime[runtimeID] = operation
			activity, err := binding.RuntimeActivityTarget(name)
			if err != nil {
				return nil, fmt.Errorf("channel binding %q operation %q private target: %w", binding.BindingID(), name, err)
			}
			if _, duplicate := next.activities[activity.ToolID()]; duplicate {
				return nil, fmt.Errorf("duplicate private channel activity tool %q", activity.ToolID())
			}
			next.activities[activity.ToolID()] = operation
		}
	}
	return next, nil
}

// AcquirePresentation pins the complete current publication while a caller
// presents its tools or derives another activation-backed read model.
func (o *Owner) AcquirePresentation() (*Lease, bool) {
	if o == nil {
		return nil, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.accepting || o.current == nil {
		return nil, false
	}
	o.current.leases++
	return newOwnedLease(o, o.current, Operation{}), true
}

// AcquirePresentationContext waits through a fenced replacement and returns
// one exact current publication. It never turns temporary unavailability into
// an empty capability surface.
func (o *Owner) AcquirePresentationContext(ctx context.Context) (*Lease, error) {
	if o == nil {
		return nil, fmt.Errorf("channel activation owner is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o.mu.Lock()
	if o.changed == nil {
		o.changed = sync.NewCond(&o.mu)
	}
	stopWakeup := context.AfterFunc(ctx, func() {
		o.mu.Lock()
		o.changed.Broadcast()
		o.mu.Unlock()
	})
	defer stopWakeup()
	for !o.accepting && ctx.Err() == nil {
		o.changed.Wait()
	}
	if err := ctx.Err(); err != nil {
		o.mu.Unlock()
		return nil, fmt.Errorf("acquire channel activation presentation: %w", err)
	}
	if o.current == nil {
		o.mu.Unlock()
		return nil, fmt.Errorf("current channel activation publication is unavailable")
	}
	o.current.leases++
	lease := newOwnedLease(o, o.current, Operation{})
	o.mu.Unlock()
	return lease, nil
}

func (o *Owner) AcquireRuntimeOperation(toolID string) (*Lease, bool) {
	return o.acquire(strings.TrimSpace(toolID), false)
}

// HasRuntimeTool reports whether the current executable snapshot owns toolID,
// including while replacement has fenced new leases. Callers use this only to
// distinguish an ordinary non-channel tool from a temporarily fenced channel
// operation; executable authority still requires a Lease.
func (o *Owner) HasRuntimeTool(toolID string) bool {
	if o == nil {
		return false
	}
	toolID = strings.TrimSpace(toolID)
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.current == nil {
		return false
	}
	_, ok := o.current.runtime[toolID]
	return ok
}

func (o *Owner) AcquireActivityOperation(toolID string, generation channelonboarding.ChannelActivationGeneration) (*Lease, bool) {
	return o.acquireAt(strings.TrimSpace(toolID), generation, true)
}

func (o *Owner) acquire(toolID string, activity bool) (*Lease, bool) {
	if o == nil {
		return nil, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.accepting || o.current == nil {
		return nil, false
	}
	operations := o.current.runtime
	if activity {
		operations = o.current.activities
	}
	operation, ok := operations[toolID]
	if !ok {
		return nil, false
	}
	o.current.leases++
	return newOwnedLease(o, o.current, operation), true
}

func (o *Owner) acquireAt(toolID string, generation channelonboarding.ChannelActivationGeneration, activity bool) (*Lease, bool) {
	if o == nil || !generation.Valid() {
		return nil, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.accepting || o.current == nil || !o.current.publication.Generation().Equal(generation) {
		return nil, false
	}
	operations := o.current.runtime
	if activity {
		operations = o.current.activities
	}
	operation, ok := operations[toolID]
	if !ok {
		return nil, false
	}
	o.current.leases++
	return newOwnedLease(o, o.current, operation), true
}
