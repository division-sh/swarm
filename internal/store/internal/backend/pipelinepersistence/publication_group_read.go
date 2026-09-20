package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func (g *publicationGroup) ReadPublicationSettlement(ctx context.Context, requests []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationSettlementSnapshot, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out pipelineobligation.PublicationSettlementSnapshot
	if !g.sealed {
		return out, errors.New("publication settlement observation requires a sealed attempt")
	}
	members, err := g.settlementMembers(requests)
	if err != nil {
		return out, err
	}
	operation := func(ctx context.Context, tx *sql.Tx) error {
		out.ObservedAt = time.Now().UTC()
		if err := g.validateCommittedMembersTx(ctx, tx, members); err != nil {
			return err
		}
		for i, member := range members {
			request := requests[i]
			row := pipelineobligation.PublicationSettlementObservation{Claim: member.claim}
			exact, found, err := exactStoredPipelineDisposition(ctx, tx, member.event.ID(), request.Disposition, g.postgres != nil)
			if err != nil {
				return err
			}
			switch {
			case !found:
				row.State, row.Reason = pipelineobligation.PublicationSettlementPending, "platform acknowledgement absent"
			case !exact:
				row.State, row.Reason = pipelineobligation.PublicationSettlementConflict, "platform disposition differs from submitted evidence"
			default:
				row.State, row.Reason = pipelineobligation.PublicationSettlementSatisfied, "exact durable disposition observed"
				var routeStatus string
				err := tx.QueryRowContext(ctx, `SELECT status FROM decision_card_route_obligations WHERE event_id=$1`, member.event.ID()).Scan(&routeStatus)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if err == nil {
					want := "quarantined"
					if request.Disposition.Successful() {
						want = "completed"
					}
					if routeStatus == "pending" {
						row.State, row.Reason = pipelineobligation.PublicationSettlementPending, "decision route remains pending"
					} else if routeStatus != want {
						row.State, row.Reason = pipelineobligation.PublicationSettlementConflict, "decision route disposition differs"
					}
				}
				if request.Disposition.Successful() {
					adapter := sqliteDeliveryAdapter
					if g.postgres != nil {
						adapter = postgresDeliveryAdapter
					}
					incomplete, err := adapter.PipelineHandoffIncomplete(ctx, tx, member.event.ID())
					if err != nil {
						return err
					}
					if incomplete && row.State != pipelineobligation.PublicationSettlementConflict {
						row.State, row.Reason = pipelineobligation.PublicationSettlementPending, "successful delivery handoff is incomplete"
					}
				}
			}
			out.Rows = append(out.Rows, row)
		}
		return nil
	}
	if g.postgres != nil {
		err = g.postgres.backend.RunReadTransaction(ctx, operation)
	} else {
		err = g.sqlite.backend.RunReadTransaction(ctx, operation)
	}
	if err != nil {
		return pipelineobligation.PublicationSettlementSnapshot{}, err
	}
	return out, nil
}
