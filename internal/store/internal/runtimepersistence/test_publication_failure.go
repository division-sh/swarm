package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

type PublicationFailureCut uint8

const (
	PublicationAfterMaterialization PublicationFailureCut = iota + 1
	PublicationBeforeReplayScope
	PublicationBeforeAPICompletion
)

type PublicationFailure struct {
	mu              sync.Mutex
	selected        any
	cut             PublicationFailureCut
	template        string
	id              uuid.UUID
	cleanupRequired bool
}

func NewPublicationFailureForTest(selected any, cut PublicationFailureCut, template string) (*PublicationFailure, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	if template == "" || strings.TrimSpace(template) != template {
		return nil, fmt.Errorf("publication fault requires an exact template")
	}
	if cut < PublicationAfterMaterialization || cut > PublicationBeforeAPICompletion {
		return nil, fmt.Errorf("publication fault requires a named temporal cut")
	}
	return &PublicationFailure{selected: selected, cut: cut, template: template, id: uuid.New()}, nil
}

func (f *PublicationFailure) InstallForTest(ctx context.Context) error {
	if f == nil {
		return fmt.Errorf("publication fault requires its original selected owner")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cleanupRequired {
		return fmt.Errorf("publication fault already attempted installation")
	}
	// Prearm even if COMMIT succeeds but its acknowledgment is lost.
	f.cleanupRequired = true
	return f.set(ctx, true)
}

func (f *PublicationFailure) RestoreForTest(ctx context.Context) error {
	if f == nil {
		return fmt.Errorf("publication cleanup requires its captured fault")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cleanupRequired {
		return nil
	}
	if err := f.set(ctx, false); err != nil {
		return err
	}
	f.cleanupRequired = false
	return nil
}

func (f *PublicationFailure) set(ctx context.Context, enabled bool) error {
	_, postgres := f.selected.(*PostgresStore)
	return runWorkflowProjectionFault(ctx, f.selected, func(ctx context.Context, tx *sql.Tx) error {
		switch f.cut {
		case PublicationAfterMaterialization:
			return delivery.SetPublicationMaterializationFailureTx(ctx, tx, postgres, f.id, f.template, enabled)
		case PublicationBeforeReplayScope:
			return pipelinepersistence.SetPublicationReplayFailureTx(ctx, tx, postgres, f.id, f.template, enabled)
		case PublicationBeforeAPICompletion:
			return apiidempotency.SetPublicationCompletionFailureTx(ctx, tx, postgres, f.id, f.template, enabled)
		default:
			return fmt.Errorf("publication fault has no temporal cut")
		}
	})
}
