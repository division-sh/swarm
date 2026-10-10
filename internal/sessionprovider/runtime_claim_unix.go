//go:build linux || darwin

package sessionprovider

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func (i *runtimeIncoming) drain(ctx context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	rows, err := i.connection.captures.pendingPublications(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.event.Scope.Kind != channelonboarding.SessionInputOnboarding {
			continue
		}
		if err := i.settleClaim(ctx, row.event); err != nil {
			return err
		}
	}
	return i.handoff.drain(ctx)
}

func (i *runtimeIncoming) settleClaim(ctx context.Context, event capturedEvent) error {
	spool := i.connection.captures
	// Historical receipts may retire identical capture without granting today's
	// socket, operation or source any new claim authority.
	settled, err := spool.reconcileSessionClaim(ctx, event, i.store)
	if err != nil || settled {
		return err
	}
	admitted, err := i.handoff.input.recoverClaim(ctx, SessionInputReference{
		ConnectionID: event.Scope.Session.ConnectionID, OccurrenceID: event.OccurrenceID,
		Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
	if err != nil {
		return err
	}
	defer admitted.Close()
	prepared, err := prepareSessionSetup(ctx, admitted, i.handoff.trigger, i.connection.plan)
	if err != nil {
		return err
	}
	if !prepared.nonClaim.Empty() {
		return spool.settleNonClaim(ctx, prepared.nonClaim)
	}
	claim := prepared.inbound
	if _, err := i.store.SettleSessionChannelClaim(ctx, prepared.claim); err != nil {
		return err
	}
	receipt, found, err := i.store.LoadOperatorChannelClaimReceipt(ctx, claim.PublicationID)
	if err != nil {
		return err
	}
	if !found || !receipt.Matches(claim) {
		return fmt.Errorf("native claim settlement has no exact durable receipt")
	}
	settled, err = spool.reconcileSessionClaim(ctx, event, i.store)
	if err != nil {
		return err
	}
	if !settled {
		return errCapturePublicationPending
	}
	return nil
}
