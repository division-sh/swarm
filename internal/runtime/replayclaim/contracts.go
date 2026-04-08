package replayclaim

import (
	"context"
	"errors"
	"time"

	"swarm/internal/events"
	runtimeownership "swarm/internal/runtime/core/ownership"
)

var (
	ErrMissingReplayEventReader            = errors.New("store does not support replay-eligible event reads")
	ErrMissingReplayClaimOwner             = errors.New("store does not support explicit pipeline replay claims")
	ErrMissingAuthoritativeRecipientReader = errors.New("store does not support authoritative delivery recipient reads")
)

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

type composedStore struct {
	Lister
	Owner
}

func RequireStore(store any) (Store, error) {
	lister, ok := store.(Lister)
	if !ok {
		return nil, ErrMissingReplayEventReader
	}
	owner, ok := store.(Owner)
	if !ok {
		return nil, ErrMissingReplayClaimOwner
	}
	return composedStore{
		Lister: lister,
		Owner:  owner,
	}, nil
}

func RequireRecipientReader(store any) (RecipientReader, error) {
	reader, ok := store.(RecipientReader)
	if !ok {
		return nil, ErrMissingAuthoritativeRecipientReader
	}
	return reader, nil
}
