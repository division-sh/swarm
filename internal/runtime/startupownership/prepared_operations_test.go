package startupownership

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type preparedEffectProbe struct {
	call func(context.Context) error
}

func (s *preparedEffectProbe) IsExternalEffectAuthorityCurrent(ctx context.Context, _ effects.Authority) (bool, error) {
	return true, s.call(ctx)
}
func (s *preparedEffectProbe) AuthorizeExternalAttempt(ctx context.Context, _ effects.Authority, _ effects.AuthorizeRequest) (effects.Attempt, error) {
	return effects.Attempt{OperationID: "acknowledged"}, s.call(ctx)
}
func (s *preparedEffectProbe) MarkExternalAttemptLaunched(ctx context.Context, _ effects.Attempt, _ time.Time) (effects.ExternalAttemptLaunch, error) {
	return effects.ExternalAttemptLaunch{}, s.call(ctx)
}

func TestPreparedProcessOperationJoinsBeforeReleaseAndClosesItsExactScope(t *testing.T) {
	session := &retainedSessionProbe{authority: AuthorityFromLease(processbinding.LeaseStamp{
		LeaseIdentity: processbinding.LeaseIdentity{LeaseID: uuid.NewString(), OwnerID: "prepared-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend}, Generation: 1,
	})}
	capability, err := NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	var retained context.Context
	store := &preparedEffectProbe{}
	store.call = func(ctx context.Context) error {
		if !PreparedOperationEntered(ctx, PreparedEffectAuthorize, store) || PreparedOperationEntered(ctx, PreparedEffectLaunch, store) || PreparedOperationEntered(ctx, PreparedEffectAuthorize, &preparedEffectProbe{}) {
			return errors.New("process invocation lost exact operation/owner identity")
		}
		retained = context.WithoutCancel(ctx)
		close(entered)
		<-resume
		return nil
	}
	var result effects.Attempt
	operation := PreparedEffectAuthorizeOperation(store, effects.Authority{ExecutionOwner: session.authority.OwnerID}, effects.AuthorizeRequest{}, &result)
	completed := make(chan error, 1)
	go func() { completed <- capability.RunPreparedOperation(context.Background(), operation) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("store operation did not enter")
	}
	released := make(chan error, 1)
	go func() { released <- capability.Release(context.Background()) }()
	select {
	case err := <-released:
		t.Fatalf("release did not join admitted operation: %v", err)
	default:
	}
	unblock()
	if err := <-completed; err != nil || result.OperationID != "acknowledged" {
		t.Fatalf("admitted outcome lost: %+v %v", result, err)
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if PreparedOperationEntered(retained, PreparedEffectAuthorize, store) {
		t.Fatal("WithoutCancel preserved an ended operation admission")
	}
	if err := capability.RunPreparedOperation(retained, operation); !errors.Is(err, ErrProcessNotCurrent) {
		t.Fatalf("released/ended operation reused: %v", err)
	}
	if err := capability.RunPreparedOperation(context.Background(), operation); !errors.Is(err, ErrProcessNotCurrent) {
		t.Fatalf("new operation after Release admitted: %v", err)
	}
}

func TestPreparedProcessOperationRejectsNestedAndCanceledInvocationWithoutDeadlock(t *testing.T) {
	session := &retainedSessionProbe{authority: AuthorityFromLease(processbinding.LeaseStamp{
		LeaseIdentity: processbinding.LeaseIdentity{LeaseID: uuid.NewString(), OwnerID: "prepared-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend}, Generation: 1,
	})}
	capability, err := NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	store := &preparedEffectProbe{}
	var result effects.Attempt
	operation := PreparedEffectAuthorizeOperation(store, effects.Authority{ExecutionOwner: session.authority.OwnerID}, effects.AuthorizeRequest{}, &result)
	store.call = func(owned context.Context) error {
		cancel()
		if !PreparedOperationEntered(owned, PreparedEffectAuthorize, store) {
			return errors.New("caller cancellation retired admitted cleanup scope")
		}
		if err := capability.RunPreparedOperation(owned, operation); !errors.Is(err, ErrProcessNotCurrent) {
			return errors.New("nested operation reacquired process ownership")
		}
		return owned.Err()
	}
	if err := capability.RunPreparedOperation(ctx, operation); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation lost: %v", err)
	}
	if err := capability.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("canceled operation retired healthy possession: %v", err)
	}
}

func TestPreparedProcessOperationQueuedCancellationAndSemanticFailureKeepPossession(t *testing.T) {
	session := &retainedSessionProbe{authority: AuthorityFromLease(processbinding.LeaseStamp{
		LeaseIdentity: processbinding.LeaseIdentity{LeaseID: uuid.NewString(), OwnerID: "prepared-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend}, Generation: 1,
	})}
	capability, err := NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	sentinel := errors.New("exact semantic refusal")
	store := &preparedEffectProbe{call: func(context.Context) error {
		close(entered)
		<-resume
		return sentinel
	}}
	var result effects.Attempt
	operation := PreparedEffectAuthorizeOperation(store, effects.Authority{ExecutionOwner: session.authority.OwnerID}, effects.AuthorizeRequest{}, &result)
	first := make(chan error, 1)
	go func() { first <- capability.RunPreparedOperation(context.Background(), operation) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	queued := make(chan error, 1)
	go func() { queued <- capability.RunPreparedOperation(ctx, operation) }()
	cancel()
	unblock()
	if err := <-first; !errors.Is(err, sentinel) || result.OperationID != "acknowledged" {
		t.Fatalf("semantic outcome lost: %+v %v", result, err)
	}
	if err := <-queued; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation lost: %v", err)
	}
	if err := capability.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("neutral cancellation/refusal retired native possession: %v", err)
	}
}
