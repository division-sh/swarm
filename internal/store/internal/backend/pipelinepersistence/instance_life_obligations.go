package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

// The native reset caller supplies its locked predecessor. This read neither
// settles old work nor grants a reset from an independently observed zero.
func readUnsettledInstanceLifeEventsTx(ctx context.Context, tx *sql.Tx, postgres bool, life events.InstanceLife) ([]string, error) {
	if tx == nil {
		return nil, fmt.Errorf("instance pipeline census requires its native transaction")
	}
	if err := life.Validate(); err != nil {
		return nil, err
	}
	diagnosticPredicate := sqliteDiagnosticDirectReplayExclusionSQL("e")
	if postgres {
		diagnosticPredicate = postgresDiagnosticDirectReplayExclusionSQL("e", 2)
	}
	args := append([]any{life.RunID}, diagnosticDirectReplayEventArgs()...)
	rows, err := tx.QueryContext(ctx, `SELECT e.event_id, e.routing_source_kind,
		COALESCE(e.routing_source_authority,''), e.source_route
		FROM events e LEFT JOIN event_receipts receipt ON receipt.event_id=e.event_id
		AND receipt.subscriber_type='platform' AND receipt.subscriber_id='pipeline'
		WHERE e.run_id=$1 AND (receipt.event_id IS NULL OR (receipt.outcome='success'
		AND EXISTS (SELECT 1 FROM decision_card_route_obligations route
		WHERE route.event_id=e.event_id AND route.status='pending')))
		AND `+diagnosticPredicate+` ORDER BY e.created_at,e.event_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("read instance pipeline obligations: %w", err)
	}
	defer rows.Close()
	var pending []string
	for rows.Next() {
		var record eventrecord.Record
		if err := rows.Scan(&record.EventID, &record.RoutingSourceKind, &record.RoutingSourceAuthority, &record.SourceRoute); err != nil {
			return nil, err
		}
		source, err := record.DecodeRoutingSource()
		if err != nil {
			return nil, err
		}
		path := source.Route().FlowInstance
		if source.Kind() == events.RoutingSourceRoot {
			path = life.RunID
		}
		if path == life.FlowInstance && source.Life() == life.Life {
			pending = append(pending, record.EventID)
		}
	}
	return pending, rows.Err()
}

func (s *PipelinePostgresOwner) ReadUnsettledInstanceLifeEventsTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife) ([]string, error) {
	return readUnsettledInstanceLifeEventsTx(ctx, tx, true, life)
}

func (s *PipelineSQLiteOwner) ReadUnsettledInstanceLifeEventsTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife) ([]string, error) {
	return readUnsettledInstanceLifeEventsTx(ctx, tx, false, life)
}
