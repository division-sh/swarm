package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
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

func readPendingInstanceLifeFanOutTx(ctx context.Context, tx *sql.Tx, postgres bool, life events.InstanceLife) (int, error) {
	if tx == nil {
		return 0, fmt.Errorf("instance fan-out census requires its native transaction")
	}
	if err := life.Validate(); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents
		WHERE run_id=$1 AND origin_kind='handler' ORDER BY triggering_delivery_id,flow_path,declaration_family,semantic_path`, life.RunID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var intents []fanoutobligation.Intent
	for rows.Next() {
		intent, err := scanFanOutIntent(rows)
		if err != nil {
			return 0, err
		}
		capsule := intent.Request.Capsule
		if capsule.Route.InstancePath == life.FlowInstance && capsule.ProducerSource.Life() == life.Life {
			intents = append(intents, intent)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	// Finish the source read before reusing the native transaction connection.
	if err := rows.Close(); err != nil {
		return 0, err
	}
	pending := 0
	for _, intent := range intents {
		if intent.Status == fanoutobligation.StatusOpen || intent.Status == fanoutobligation.StatusBlocked {
			pending += intent.Request.Cardinality - intent.Cursor
		}
		fold, err := foldFanOutIntentTerminalDispositions(ctx, tx, postgres, intent.Request.Key)
		if err != nil {
			return 0, err
		}
		pending += fold.PendingCommitted
		key := intent.Request.Key
		barriers, err := summarizeFanOutDeliveryBarriers(ctx, tx, life.RunID, &key)
		if err != nil {
			return 0, err
		}
		pending += barriers.Armed + barriers.ClosedPending
	}
	return pending, nil
}

func (s *PipelinePostgresOwner) ReadPendingInstanceLifeFanOutTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife) (int, error) {
	return readPendingInstanceLifeFanOutTx(ctx, tx, true, life)
}

func (s *PipelineSQLiteOwner) ReadPendingInstanceLifeFanOutTx(ctx context.Context, tx *sql.Tx, life events.InstanceLife) (int, error) {
	return readPendingInstanceLifeFanOutTx(ctx, tx, false, life)
}
