package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

type currentFanOutPublication struct {
	key                               fanoutobligation.IntentKey
	ordinal                           int
	kind, eventID, sourceID, retained string
}

func readFanOutSettlement(ctx context.Context, db pipelineQueryer, postgres bool, runID string, now time.Time) (fanoutobligation.RunSettlement, error) {
	out := fanoutobligation.RunSettlement{RunID: strings.TrimSpace(runID)}
	if err := out.Validate(); err != nil {
		return out, err
	}
	if now.IsZero() {
		return out, fmt.Errorf("fan-out settlement requires observation time")
	}
	if err := readCurrentFanOutIntents(ctx, db, &out); err != nil {
		return out, err
	}
	if err := db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN status='armed' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN status='closed_pending' THEN 1 ELSE 0 END),0)
		FROM fan_out_obligation_barriers WHERE run_id=$1 AND status IN ('armed','closed_pending')`, out.RunID).Scan(&out.BarrierArmed, &out.BarrierPending); err != nil {
		return out, err
	}
	publications, err := readCurrentFanOutPublications(ctx, db, out.RunID)
	if err != nil {
		return out, err
	}
	for start := 0; start < len(publications); start += 128 {
		pending, err := settleCurrentFanOutPublications(ctx, db, postgres, out.RunID, publications[start:min(start+128, len(publications))])
		if err != nil {
			return out, err
		}
		out.Unsettled += pending
	}
	return out, out.Validate()
}

func readCurrentFanOutIntents(ctx context.Context, db pipelineQueryer, out *fanoutobligation.RunSettlement) error {
	rows, err := db.QueryContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1 AND status IN ('open','blocked')`, out.RunID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		intent, err := scanFanOutIntent(rows)
		if err != nil {
			return err
		}
		if intent.Status == fanoutobligation.StatusOpen {
			out.Open++
		} else {
			out.Blocked++
		}
		out.Owed += intent.Request.Cardinality - intent.Cursor
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return rows.Close()
}

func readCurrentFanOutPublications(ctx context.Context, db pipelineQueryer, runID string) ([]currentFanOutPublication, error) {
	// Preserve the current delivery census before joins. A foreign run_id must
	// not hide an otherwise exact target ordinal. The existing partial delivery
	// index covers this current-only set; unique outcome event_id supplies probes.
	rows, err := db.QueryContext(ctx, `WITH active AS MATERIALIZED (
		SELECT DISTINCT event_id,run_id FROM event_deliveries WHERE status IN ('pending','failed','in_progress')
	)
	SELECT DISTINCT o.run_id,o.triggering_delivery_id,o.deployment_feed_id,o.flow_path,o.declaration_family,o.semantic_path,
		o.ordinal,o.outcome_kind,o.event_id,o.source_event_id,o.inherited_disposition,
		COALESCE(i.cardinality,-1),COALESCE(i.cursor,-1),COALESCE(i.status,'')
	FROM active d JOIN fan_out_outcomes o ON o.event_id=d.event_id
	LEFT JOIN fan_out_intents i ON i.run_id=o.run_id AND
		(i.deployment_feed_id=o.deployment_feed_id OR
		(i.triggering_delivery_id=o.triggering_delivery_id AND i.flow_path=o.flow_path AND i.declaration_family=o.declaration_family AND i.semantic_path=o.semantic_path))
	WHERE o.run_id=$1 OR d.run_id=$1 ORDER BY o.event_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var publications []currentFanOutPublication
	for rows.Next() {
		var p currentFanOutPublication
		var delivery, feed, flow, family, path, source, retained sql.NullString
		var cardinality, cursor int
		var status string
		if err := rows.Scan(&p.key.RunID, &delivery, &feed, &flow, &family, &path, &p.ordinal, &p.kind, &p.eventID, &source, &retained, &cardinality, &cursor, &status); err != nil {
			return nil, err
		}
		p.key.TriggeringDeliveryID, p.key.DeploymentFeedID = delivery.String, feed.String
		p.key.ElementRef = runtimecontracts.FanOutElementRef{FlowPath: flow.String, Family: family.String, SemanticPath: path.String}
		if p.key.RunID != runID {
			return nil, fmt.Errorf("current fan-out publication belongs to another run")
		}
		if err := p.key.Validate(); err != nil {
			return nil, err
		}
		if cursor < 0 || cursor > cardinality || p.ordinal < 0 || p.ordinal >= cursor ||
			(status != "open" && status != "blocked" && status != "closed" && status != "canceled") || (status == "closed" && cursor != cardinality) {
			return nil, fmt.Errorf("current fan-out ordinal has missing or contradictory intent progress")
		}
		p.sourceID, p.retained = source.String, retained.String
		if (p.kind != "committed" || source.Valid || retained.Valid) &&
			(p.kind != "continued" || !source.Valid || source.String == "" || source.String == p.eventID ||
				(retained.Valid && (!fanoutobligation.InheritedTerminalDisposition(retained.String).Valid() || retained.String == "no_route"))) {
			return nil, fmt.Errorf("current fan-out ordinal has contradictory publication evidence")
		}
		publications = append(publications, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return publications, rows.Close()
}

func settleCurrentFanOutPublications(ctx context.Context, db pipelineQueryer, postgres bool, runID string, publications []currentFanOutPublication) (int, error) {
	ids := make([]string, len(publications))
	for i, p := range publications {
		ids[i] = p.eventID
	}
	var admitted []eventrecord.AdmittedRecord
	var err error
	adapter := sqliteDeliveryAdapter
	if postgres {
		adapter = postgresDeliveryAdapter
		admitted, err = eventrecordpostgres.LoadAdmittedMany(ctx, db, ids)
	} else {
		admitted, err = eventrecordsqlite.LoadAdmittedMany(ctx, db, ids)
	}
	if err != nil {
		return 0, fmt.Errorf("load current fan-out publications: %w", err)
	}
	snapshots, err := adapter.SnapshotsForEvents(ctx, db, ids)
	if err != nil {
		return 0, fmt.Errorf("load current fan-out recipients: %w", err)
	}
	pending := 0
	for i, publication := range admitted {
		p := publications[i]
		if publication.Event.Event().RunID() != runID {
			return 0, fmt.Errorf("current fan-out event belongs to another run")
		}
		deliveries := snapshots[p.eventID]
		routes := make([]events.DeliveryRoute, len(deliveries))
		for j, delivery := range deliveries {
			if delivery.RunID != runID || delivery.EventID != p.eventID {
				return 0, fmt.Errorf("current fan-out recipient belongs to another run or event")
			}
			routes[j] = delivery.Route
		}
		if err := publication.Settlement.Validate(routes); err != nil {
			return 0, fmt.Errorf("current fan-out recipient membership: %w", err)
		}
		var terminal bool
		if p.kind == "continued" {
			_, terminal, err = continuedFanOutRecipientDisposition(runID, p.sourceID, publication, deliveries, fanoutobligation.InheritedTerminalDisposition(p.retained))
		} else {
			_, terminal, err = fanoutbarrier.RecipientDisposition(deliveries)
		}
		if err != nil {
			return 0, err
		}
		if !terminal {
			pending++
		}
	}
	return pending, nil
}

func (s *PipelinePostgresOwner) ReadFanOutSettlement(ctx context.Context, runID string, now time.Time) (fanoutobligation.RunSettlement, error) {
	var out fanoutobligation.RunSettlement
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		out, err = readFanOutSettlement(ctx, tx, true, runID, now)
		return err
	})
	return out, err
}

func (s *PipelineSQLiteOwner) ReadFanOutSettlement(ctx context.Context, runID string, now time.Time) (fanoutobligation.RunSettlement, error) {
	var out fanoutobligation.RunSettlement
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		out, err = readFanOutSettlement(ctx, tx, false, runID, now)
		return err
	})
	return out, err
}

func (s *PipelinePostgresOwner) ReadFanOutSettlementTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time) (fanoutobligation.RunSettlement, error) {
	return readFanOutSettlement(ctx, tx, true, runID, now)
}

func (s *PipelineSQLiteOwner) ReadFanOutSettlementTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time) (fanoutobligation.RunSettlement, error) {
	return readFanOutSettlement(ctx, tx, false, runID, now)
}
