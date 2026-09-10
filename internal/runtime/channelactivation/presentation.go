package channelactivation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// PresentationScope identifies the admitted actor and input, not a tool name or
// a serialized capability. The lease also validates its issuing owner/occurrence.
type PresentationScope struct {
	Source, Actor, Flow, Run, Entity, Input string
	Identity                                agentidentity.Identity
}

type presentationContextKey struct{}

type presentation struct {
	lease *Lease
	scope PresentationScope
}

// PresentationFromContext reports presence even after revocation. Callers must
// never turn an invalid inherited capability into fresh current admission.
func PresentationFromContext(ctx context.Context) (*Lease, bool) {
	if ctx == nil {
		return nil, false
	}
	p, ok := ctx.Value(presentationContextKey{}).(presentation)
	return p.lease, ok
}

func (o *Owner) ValidatePresentation(ctx context.Context, scope PresentationScope) error {
	if ctx == nil {
		return fmt.Errorf("channel presentation context is missing")
	}
	p, ok := ctx.Value(presentationContextKey{}).(presentation)
	if !ok || o == nil {
		return fmt.Errorf("channel presentation authority is missing")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !p.lease.live() || p.lease.owner != o || p.lease.snapshot != o.current || p.scope != scope {
		return fmt.Errorf("channel presentation authority is revoked or mismatched")
	}
	return nil
}

// AcquirePresentationForContext admits roots through the replacement fence, but
// retains descendants from their exact predecessor. Release joins descendants.
func (o *Owner) AcquirePresentationForContext(ctx context.Context, scope PresentationScope) (context.Context, *Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, inherited := PresentationFromContext(ctx); inherited {
		if err := o.ValidatePresentation(ctx, scope); err != nil {
			return ctx, nil, err
		}
		p := ctx.Value(presentationContextKey{}).(presentation)
		childCtx, child, err := o.retainPresentation(ctx, p.lease)
		if err != nil {
			return ctx, nil, err
		}
		return context.WithValue(childCtx, presentationContextKey{}, presentation{child, scope}), child, nil
	}
	lease, err := o.AcquirePresentationContext(ctx)
	if err != nil {
		return ctx, nil, err
	}
	lease.authority.ctx, lease.authority.cancel = context.WithCancel(ctx)
	return context.WithValue(lease.authority.ctx, presentationContextKey{}, presentation{lease, scope}), lease, nil
}

func (o *Owner) retainPresentation(ctx context.Context, parent *Lease) (context.Context, *Lease, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if ctx.Err() != nil || !parent.live() || parent.owner != o || parent.snapshot != o.current {
		return ctx, nil, fmt.Errorf("channel presentation parent is unavailable")
	}
	child := newOwnedLease(o, parent.snapshot, Operation{})
	child.authority.ctx, child.authority.cancel = context.WithCancel(ctx)
	if parent.authority.ctx != nil {
		child.authority.stop = context.AfterFunc(parent.authority.ctx, child.authority.cancel)
	}
	child.authority.parent = parent.authority
	parent.authority.children++
	parent.snapshot.leases++
	return child.authority.ctx, child, nil
}

// PresentationBinding carries an existing turn's capability across transport.
// Registration holds no owner refcount: only admitted requests retain a child.
// Revocation cancels and joins requests without holding the turn registry mutex.
type PresentationBinding struct {
	mu       sync.Mutex
	changed  *sync.Cond
	parent   presentation
	ctx      context.Context
	cancel   context.CancelFunc
	stop     func() bool
	requests int
	revoked  bool
	expires  time.Time
}

func BindPresentation(ctx context.Context, expires time.Time) *PresentationBinding {
	if ctx == nil {
		return nil
	}
	p, ok := ctx.Value(presentationContextKey{}).(presentation)
	if !ok {
		return nil
	}
	b := &PresentationBinding{parent: p, expires: expires}
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.changed = sync.NewCond(&b.mu)
	b.stop = context.AfterFunc(b.ctx, b.Revoke)
	return b
}

func (b *PresentationBinding) Acquire(ctx context.Context) (context.Context, func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.revoked || b.ctx.Err() != nil || b.parent.lease == nil || b.expires.IsZero() || !time.Now().Before(b.expires) {
		return ctx, nil, fmt.Errorf("channel presentation transport binding is revoked")
	}
	childCtx, lease, err := b.parent.lease.owner.retainPresentation(ctx, b.parent.lease)
	if err != nil {
		return ctx, nil, err
	}
	stop := context.AfterFunc(b.ctx, lease.authority.cancel)
	b.requests++
	var once sync.Once
	release := func() {
		once.Do(func() {
			stop()
			lease.Release()
			b.mu.Lock()
			b.requests--
			b.changed.Broadcast()
			b.mu.Unlock()
		})
	}
	return context.WithValue(childCtx, presentationContextKey{}, presentation{lease, b.parent.scope}), release, nil
}

func (b *PresentationBinding) Revoke() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.revoked = true
	b.cancel()
	b.mu.Unlock()
}

func (b *PresentationBinding) Close() {
	if b == nil {
		return
	}
	b.Revoke()
	b.stop()
	b.mu.Lock()
	for b.requests > 0 {
		b.changed.Wait()
	}
	b.mu.Unlock()
}
