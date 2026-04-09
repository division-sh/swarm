package replayclaim

import (
	"context"
	"errors"
	"time"

	"swarm/internal/events"
	runtimeownership "swarm/internal/runtime/core/ownership"
)

var (
	ErrMissingReplayEventReader                  = errors.New("store does not support replay-eligible event reads")
	ErrMissingReplayClaimOwner                   = errors.New("store does not support explicit pipeline replay claims")
	ErrAuthoritativeRecipientManifestUnavailable = errors.New("authoritative delivery recipient manifest is unavailable for non-persistent event stores")
	ErrMissingCommittedReplayScope               = errors.New("store does not support authoritative committed replay scope for persisted replay")
)

type CommittedReplayScope string

const (
	CommittedReplayScopeDirect     CommittedReplayScope = "direct"
	CommittedReplayScopeSubscribed CommittedReplayScope = "subscribed"
)

type Participation interface {
	SupportsPersistedReplay() bool
}

type Lister interface {
	ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error)
}

type Owner interface {
	ClaimPipelineReplay(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error)
}

type Store interface {
	Lister
	Owner
}

type RecipientReader interface {
	ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error)
}

type ScopeReader interface {
	LoadCommittedReplayScope(ctx context.Context, eventID string) (CommittedReplayScope, error)
}

type composedStore struct {
	Lister
	Owner
}

func SupportsPersistedReplay(store any) bool {
	support, ok := store.(Participation)
	if !ok {
		return true
	}
	return support.SupportsPersistedReplay()
}

func RequireStore(store any) (Store, bool, error) {
	if !SupportsPersistedReplay(store) {
		return nil, false, nil
	}
	lister, ok := store.(Lister)
	if !ok {
		return nil, true, ErrMissingReplayEventReader
	}
	owner, ok := store.(Owner)
	if !ok {
		return nil, true, ErrMissingReplayClaimOwner
	}
	return composedStore{
		Lister: lister,
		Owner:  owner,
	}, true, nil
}
