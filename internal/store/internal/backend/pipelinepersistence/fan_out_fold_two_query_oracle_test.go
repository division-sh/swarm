package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

// Frozen before the per-intent physical join. This is a test-only oracle;
// keep its two queries and validation order independent of the production fold.
func foldFanOutIntentTwoQueryBefore(
	ctx context.Context,
	db pipelineQueryer,
	postgres bool,
	key fanoutobligation.IntentKey,
) (fanoutbarrier.Fold, error) {
	if err := key.Validate(); err != nil {
		return fanoutbarrier.Fold{}, err
	}
	var cardinality, cursor int
	var rawStatus string
	err := db.QueryRowContext(ctx, `
		SELECT cardinality, cursor, status
		FROM fan_out_intents
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5
	`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&cardinality, &cursor, &rawStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s is missing", key.String())
	}
	if err != nil {
		return fanoutbarrier.Fold{}, err
	}
	status := fanoutobligation.Status(strings.TrimSpace(rawStatus))
	if cardinality < 0 || cursor < 0 || cursor > cardinality {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s has invalid progress", key.String())
	}
	fold := fanoutbarrier.Fold{
		Summary:           fanoutbarrier.Summary{Total: cardinality},
		EnumerationClosed: status == fanoutobligation.StatusClosed || status == fanoutobligation.StatusCanceled,
	}
	if status == fanoutobligation.StatusCanceled {
		fold.Summary.Canceled = cardinality - cursor
	} else if status != fanoutobligation.StatusOpen && status != fanoutobligation.StatusClosed && status != fanoutobligation.StatusBlocked {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s has invalid status %q", key.String(), status)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT ordinal, outcome_kind, event_id, source_event_id, inherited_disposition
		FROM fan_out_outcomes
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5
		ORDER BY ordinal
	`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath)
	if err != nil {
		return fanoutbarrier.Fold{}, err
	}
	type outcomeFact struct {
		ordinal       int
		kind          string
		eventID       sql.NullString
		sourceEventID sql.NullString
		disposition   sql.NullString
	}
	facts := make([]outcomeFact, 0, cursor)
	for rows.Next() {
		var fact outcomeFact
		if err := rows.Scan(&fact.ordinal, &fact.kind, &fact.eventID, &fact.sourceEventID, &fact.disposition); err != nil {
			rows.Close()
			return fanoutbarrier.Fold{}, err
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fanoutbarrier.Fold{}, err
	}
	if err := rows.Close(); err != nil {
		return fanoutbarrier.Fold{}, err
	}
	if len(facts) != cursor {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s has %d ordinal outcomes, want contiguous cursor %d", key.String(), len(facts), cursor)
	}
	adapter := sqliteDeliveryAdapter
	if postgres {
		adapter = postgresDeliveryAdapter
	}
	// Bound retained decoded events as well as SQL parameters. The original
	// compact ordinal facts remain the complete membership proof for the fold.
	for start := 0; start < len(facts); start += 128 {
		batch := facts[start:min(start+128, len(facts))]
		var eventIDs []string
		for _, fact := range batch {
			if fanoutobligation.OutcomeKind(strings.TrimSpace(fact.kind)) == fanoutobligation.OutcomeCommitted &&
				!fact.sourceEventID.Valid && fact.eventID.Valid {
				eventIDs = append(eventIDs, fact.eventID.String)
			}
		}
		var admitted []eventrecord.AdmittedRecord
		if postgres {
			admitted, err = eventrecordpostgres.LoadAdmittedMany(ctx, db, eventIDs)
		} else {
			admitted, err = eventrecordsqlite.LoadAdmittedMany(ctx, db, eventIDs)
		}
		if err != nil {
			var corrupt *eventrecord.CorruptError
			if errors.As(err, &corrupt) {
				for _, fact := range batch {
					if fact.eventID.String == corrupt.EventID {
						return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out ordinal %d settlement: %w", fact.ordinal, err)
					}
				}
			}
			return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out settlement batch: %w", err)
		}
		settlements := make(map[string]eventrecord.AdmittedRecord, len(admitted))
		for i, record := range admitted {
			if record.Event.Event().RunID() != key.RunID {
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out outcome event %s belongs to another run", eventIDs[i])
			}
			settlements[eventIDs[i]] = record
		}
		deliveriesByEvent, err := adapter.SnapshotsForEvents(ctx, db, eventIDs)
		if err != nil {
			return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out delivery batch: %w", err)
		}
		for offset, fact := range batch {
			ordinal := start + offset
			if fact.ordinal != ordinal {
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s outcome ordinal %d is not contiguous at %d", key.String(), fact.ordinal, ordinal)
			}
			switch fanoutobligation.OutcomeKind(strings.TrimSpace(fact.kind)) {
			case fanoutobligation.OutcomeSemanticRejected:
				if fact.eventID.Valid || fact.sourceEventID.Valid || fact.disposition.Valid {
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s rejected ordinal %d carries settlement identity", key.String(), ordinal)
				}
				fold.Summary.SemanticRejected++
			case fanoutobligation.OutcomeCommitted:
				if fact.sourceEventID.Valid {
					if fact.eventID.Valid || !fact.disposition.Valid {
						return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s inherited ordinal %d has contradictory settlement evidence", key.String(), ordinal)
					}
					switch fanoutobligation.InheritedTerminalDisposition(strings.TrimSpace(fact.disposition.String)) {
					case fanoutobligation.InheritedSucceeded:
						fold.Summary.Succeeded++
					case fanoutobligation.InheritedDeadLettered:
						fold.Summary.DeadLettered++
					case fanoutobligation.InheritedNoRoute:
						fold.Summary.NoRoute++
					default:
						return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s inherited ordinal %d has invalid terminal disposition %q", key.String(), ordinal, fact.disposition.String)
					}
					continue
				}
				if !fact.eventID.Valid || fact.disposition.Valid {
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s owned ordinal %d has contradictory settlement evidence", key.String(), ordinal)
				}
				admitted := settlements[fact.eventID.String]
				settlement := admitted.Settlement
				deliveries := deliveriesByEvent[admitted.Event.ID()]
				for _, delivery := range deliveries {
					if delivery.RunID != key.RunID || delivery.EventID != admitted.Event.ID() {
						return fanoutbarrier.Fold{}, fmt.Errorf("fan-out ordinal %d delivery belongs to another run or event", ordinal)
					}
				}
				switch {
				case settlement.NoDelivery() && len(deliveries) == 0:
					fold.Summary.NoRoute++
				case settlement.Delivered() && len(deliveries) > 0:
					allTerminal := true
					deadLettered := false
					for _, delivery := range deliveries {
						if !delivery.Terminal() {
							allTerminal = false
						}
						if delivery.Status == runtimedelivery.StatusDeadLetter {
							deadLettered = true
						}
					}
					if !allTerminal {
						fold.PendingCommitted++
					} else if deadLettered {
						fold.Summary.DeadLettered++
					} else {
						fold.Summary.Succeeded++
					}
				default:
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out ordinal %d route and delivery settlement are contradictory", ordinal)
				}
			default:
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s ordinal %d has invalid outcome kind %q", key.String(), ordinal, fact.kind)
			}
		}
	}
	if err := fold.Validate(); err != nil {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s: %w", key.String(), err)
	}
	return fold, nil
}
