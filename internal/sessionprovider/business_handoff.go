//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

// This is the incoming handoff, not an outgoing retry queue. The private capture
// and selected publication owners retain all durable state and retirement rules.
type sessionBusinessHandoff struct {
	mu      sync.Mutex
	input   *sessionInputOwner
	trigger providertriggers.InboundAdmissionPlan
	alias   string
	bus     *bus.EventBus
	store   sessionBusinessStore
	posture executionposture.Posture
}

func (h *sessionBusinessHandoff) drain(ctx context.Context) error {
	if h == nil || h.input == nil || h.input.native == nil || h.bus == nil || h.store == nil || h.alias == "" || ctx == nil {
		return fmt.Errorf("native handoff requires its capture, input, runtime and selected publication owners")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	rows, err := h.input.native.spool.pendingPublications(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		// Setup capture has a separate exact claim-receipt consumer. Never turn it
		// into business authority or discard it while draining business work.
		if row.event.Scope.Kind != channelonboarding.SessionInputBusiness {
			continue
		}
		if err := h.publish(ctx, row.event); err != nil {
			return err
		}
	}
	return nil
}

func (h *sessionBusinessHandoff) publish(ctx context.Context, event capturedEvent) error {
	spool := h.input.native.spool
	settled, err := spool.reconcilePublished(ctx, event, h.store)
	if err != nil || settled {
		return err
	}
	admitted, err := h.input.recoverBusiness(ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
		OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
	if err != nil {
		return err
	}
	defer admitted.Close()
	prepared, err := prepareSessionBusinessPublication(ctx, admitted, h.trigger, h.alias, h.bus, h.store, h.posture)
	if err != nil {
		return err
	}
	if prepared.history == nil {
		if err := spool.stagePublication(ctx, event, prepared.command.Request); err != nil {
			return errors.Join(err, h.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(ctx), prepared.plan))
		}
	}
	result, err := prepared.commitAndDispatch()
	if err != nil {
		return err
	}
	if !result.Acknowledged {
		return errCapturePublicationPending
	}
	return spool.retirePublished(ctx, event, h.store)
}
