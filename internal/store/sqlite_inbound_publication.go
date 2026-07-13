package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
)

func (s *SQLiteRuntimeStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return runtimeinbound.Record{}, err
	}
	if fn == nil {
		return runtimeinbound.Record{}, fmt.Errorf("inbound publication mutation callback is required")
	}
	var result runtimeinbound.Record
	err := s.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		attemptResult := runtimeinbound.Record{}
		existing, found, err := loadSQLiteInboundPublicationTx(txctx, tx, request.Provider, request.EntityID, request.ProviderEventID)
		if err != nil {
			return err
		}
		if found {
			if err := validateInboundPublicationRetry(request, existing); err != nil {
				return err
			}
			if err := validateSQLiteInboundPublicationIntegrityTx(txctx, tx, &existing); err != nil {
				return err
			}
			result = existing
			return nil
		}
		if err := admitSQLiteInboundStandingTargetTx(txctx, tx, request); err != nil {
			return err
		}
		if err := insertSQLiteInboundPublicationPreparedTx(txctx, tx, request); err != nil {
			return err
		}
		mutation := newSQLInboundPublicationMutation(txctx, tx, s, s)
		mutation.request = request
		if err := fn(mutation); err != nil {
			return err
		}
		if !mutation.finalized {
			return fmt.Errorf("inbound publication callback returned without finalizing the operation")
		}
		attemptResult = mutation.record
		attemptResult.Created = true
		result = attemptResult
		return nil
	})
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	return result, nil
}

func (s *SQLiteRuntimeStore) LoadInboundPublicationByIdentity(ctx context.Context, provider, entityID, providerEventID string) (runtimeinbound.Record, bool, error) {
	if s == nil || s.DB == nil {
		return runtimeinbound.Record{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	record, found, err := loadSQLiteInboundPublicationTx(ctx, s.DB, provider, entityID, providerEventID)
	if err != nil || !found {
		return record, found, err
	}
	if err := validateSQLiteInboundPublicationIntegrityTx(ctx, s.DB, &record); err != nil {
		return runtimeinbound.Record{}, false, err
	}
	return record, true, nil
}

func (s *SQLiteRuntimeStore) ValidateInboundPublicationIntegrity(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT provider, entity_id, provider_event_id
		FROM inbound_publications
		ORDER BY provider, entity_id, provider_event_id
	`)
	if err != nil {
		return fmt.Errorf("list sqlite inbound publications for integrity validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var provider, entityID, providerEventID string
		if err := rows.Scan(&provider, &entityID, &providerEventID); err != nil {
			return fmt.Errorf("scan sqlite inbound publication identity: %w", err)
		}
		record, found, err := loadSQLiteInboundPublicationTx(ctx, s.DB, provider, entityID, providerEventID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("sqlite inbound publication disappeared during integrity validation")
		}
		if err := validateSQLiteInboundPublicationIntegrityTx(ctx, s.DB, &record); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read sqlite inbound publication identities: %w", err)
	}
	return nil
}

func loadSQLiteInboundPublicationTx(ctx context.Context, db inboundPublicationQueryer, provider, entityID, providerEventID string) (runtimeinbound.Record, bool, error) {
	record, err := scanSQLiteInboundPublication(db.QueryRowContext(ctx, sqliteInboundPublicationSelect+`
		WHERE provider = ? AND entity_id = ? AND provider_event_id = ?
	`, strings.ToLower(strings.TrimSpace(provider)), strings.TrimSpace(entityID), strings.TrimSpace(providerEventID)))
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeinbound.Record{}, false, nil
	}
	if err != nil {
		return runtimeinbound.Record{}, false, fmt.Errorf("load sqlite inbound publication: %w", err)
	}
	return record, true, nil
}

const sqliteInboundPublicationSelect = `
	SELECT
		p.publication_id, p.provider, p.entity_id, p.provider_event_id,
		p.semantic_fingerprint, p.semantic_projection_version,
		p.stable_service_id, p.package_key, p.flow_id, p.instance_id,
		p.target_alias, p.target_flow_instance, p.expected_publication_sequence,
		p.resolved_run_id, COALESCE(p.marker_event_id, ''),
		COALESCE(p.publication_event_id, ''), p.acknowledgement_mode,
		p.recipient_manifest, p.recipient_manifest_fingerprint, p.recipient_count,
		p.original_received_at, p.original_user_agent, p.original_transport_metadata,
		p.state, p.created_at, p.committed_at
	FROM inbound_publications p
`

func scanSQLiteInboundPublication(row *sql.Row) (runtimeinbound.Record, error) {
	var record runtimeinbound.Record
	var ackMode string
	var recipientManifest, transportMetadata any
	var originalReceivedAt, createdAt, committedAt any
	err := row.Scan(
		&record.PublicationID, &record.Provider, &record.EntityID, &record.ProviderEventID,
		&record.SemanticFingerprint, &record.SemanticProjectionVersion,
		&record.StableServiceID, &record.PackageKey, &record.FlowID, &record.InstanceID,
		&record.TargetAlias, &record.TargetFlowInstance, &record.ExpectedPublicationSequence,
		&record.ResolvedRunID, &record.MarkerEventID, &record.PublicationEventID, &ackMode,
		&recipientManifest, &record.RecipientFingerprint, &record.RecipientCount,
		&originalReceivedAt, &record.OriginalUserAgent, &transportMetadata,
		&record.State, &createdAt, &committedAt,
	)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	record.AcknowledgementMode = runtimeinbound.AcknowledgementMode(ackMode)
	record.RecipientManifest = jsonRawMessageValue(recipientManifest)
	record.OriginalTransportMetadata = jsonRawMessageValue(transportMetadata)
	if parsed, ok, err := sqliteTimeValue(originalReceivedAt); err != nil {
		return runtimeinbound.Record{}, err
	} else if ok {
		record.OriginalReceivedAt = parsed.UTC()
	}
	if parsed, ok, err := sqliteTimeValue(createdAt); err != nil {
		return runtimeinbound.Record{}, err
	} else if ok {
		record.CreatedAt = parsed.UTC()
	}
	if parsed, ok, err := sqliteTimeValue(committedAt); err != nil {
		return runtimeinbound.Record{}, err
	} else if ok {
		record.CommittedAt = parsed.UTC()
	}
	record.Request = record.Request.Normalized()
	return record, nil
}

func admitSQLiteInboundStandingTargetTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request) error {
	var packageKey, flowID, instanceID, entityID, runID, effectiveState, publicationState string
	var generation, publicationSequence int64
	err := tx.QueryRowContext(ctx, `
		SELECT package_key, flow_id, instance_id, entity_id, current_run_id,
		       current_generation, publication_sequence, effective_state, publication_state
		FROM standing_services
		WHERE service_id = ?
	`, request.StableServiceID).Scan(&packageKey, &flowID, &instanceID, &entityID, &runID, &generation, &publicationSequence, &effectiveState, &publicationState)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("standing service %s is not admitted", request.StableServiceID)
	}
	if err != nil {
		return fmt.Errorf("lock sqlite inbound standing service: %w", err)
	}
	if packageKey != request.PackageKey || flowID != request.FlowID || instanceID != request.InstanceID || entityID != request.EntityID || runID != request.ResolvedRunID || publicationSequence != request.ExpectedPublicationSequence {
		return fmt.Errorf("stale or conflicting sqlite inbound standing target")
	}
	if effectiveState != "active" || publicationState != "published" {
		return fmt.Errorf("standing service %s is %s/%s and cannot accept inbound publication", request.StableServiceID, effectiveState, publicationState)
	}
	var runStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, request.ResolvedRunID).Scan(&runStatus); err != nil {
		return fmt.Errorf("lock sqlite inbound target run: %w", err)
	}
	if runStatus != "running" && runStatus != "paused" {
		return fmt.Errorf("inbound target run %s has terminal status %s", request.ResolvedRunID, runStatus)
	}
	var generationRunID string
	if err := tx.QueryRowContext(ctx, `
		SELECT run_id FROM standing_service_generations
		WHERE service_id = ? AND generation = ? AND retired_at IS NULL
	`, request.StableServiceID, generation).Scan(&generationRunID); err != nil {
		return fmt.Errorf("lock sqlite inbound standing generation: %w", err)
	}
	if generationRunID != request.ResolvedRunID {
		return fmt.Errorf("sqlite standing generation run changed during inbound admission")
	}
	return nil
}

func insertSQLiteInboundPublicationPreparedTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO inbound_publications (
			publication_id, provider, entity_id, provider_event_id,
			semantic_fingerprint, semantic_projection_version, stable_service_id,
			package_key, flow_id, instance_id, target_alias, target_flow_instance,
			expected_publication_sequence, resolved_run_id, acknowledgement_mode,
			original_received_at, original_user_agent, original_transport_metadata,
			state, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?)
	`, request.PublicationID, request.Provider, request.EntityID, request.ProviderEventID,
		request.SemanticFingerprint, request.SemanticProjectionVersion, request.StableServiceID,
		request.PackageKey, request.FlowID, request.InstanceID, request.TargetAlias, request.TargetFlowInstance,
		request.ExpectedPublicationSequence, request.ResolvedRunID, string(request.AcknowledgementMode),
		request.OriginalReceivedAt, request.OriginalUserAgent, string(request.OriginalTransportMetadata), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert prepared sqlite inbound publication: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) appendInboundEvidenceTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	ctx = withDiagnosticDirectOwner(ctx, diagnosticDirectInboundRecord)
	if err := s.AppendEventTx(ctx, tx, evt); err != nil {
		return fmt.Errorf("append sqlite inbound evidence: %w", err)
	}
	return recordInboundAuthorActivity(ctx, evt, evt.SourceAgent())
}

func (s *SQLiteRuntimeStore) finalizeInboundPublicationTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request, finalization runtimeinbound.Finalization) (runtimeinbound.Record, error) {
	manifest, fingerprint, count, err := canonicalInboundRecipientManifest(finalization.RecipientManifest)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE inbound_publications
		SET marker_event_id = ?, publication_event_id = ?, recipient_manifest = ?,
		    recipient_manifest_fingerprint = ?, recipient_count = ?, state = 'committed', committed_at = ?
		WHERE publication_id = ? AND state = 'prepared'
	`, request.MarkerEventID, request.PublicationEventID, string(manifest), fingerprint, count, now, request.PublicationID)
	if err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("finalize sqlite inbound publication: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		return runtimeinbound.Record{}, fmt.Errorf("prepared sqlite inbound publication %s was not finalized", request.PublicationID)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs SET event_count = (SELECT COUNT(*) FROM events WHERE run_id = ?) WHERE run_id = ?
	`, request.ResolvedRunID, request.ResolvedRunID); err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("synchronize sqlite inbound publication event count: %w", err)
	}
	record, found, err := loadSQLiteInboundPublicationTx(ctx, tx, request.Provider, request.EntityID, request.ProviderEventID)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	if !found {
		return runtimeinbound.Record{}, errInboundPublicationNotFound
	}
	record.PublicationEvent = finalization.PublicationEvent
	return record, nil
}

func validateSQLiteInboundPublicationIntegrityTx(ctx context.Context, db inboundPublicationQueryer, record *runtimeinbound.Record) error {
	if record == nil {
		return fmt.Errorf("sqlite inbound publication record is required")
	}
	if record.State != "committed" || record.MarkerEventID == "" || record.PublicationEventID == "" || record.CommittedAt.IsZero() {
		return fmt.Errorf("sqlite inbound publication %s has incomplete committed coupling", record.PublicationID)
	}
	manifest, fingerprint, count, err := canonicalInboundRecipientManifest(record.RecipientManifest)
	if err != nil {
		return fmt.Errorf("sqlite inbound publication %s manifest: %w", record.PublicationID, err)
	}
	if fingerprint != record.RecipientFingerprint || count != record.RecipientCount || string(manifest) != string(record.RecipientManifest) {
		return fmt.Errorf("sqlite inbound publication %s recipient manifest is incoherent", record.PublicationID)
	}
	var markerCount, eventCount, scopeCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM events WHERE event_id = ? AND event_name = 'platform.inbound_recorded'),
			(SELECT COUNT(*) FROM events WHERE event_id = ?),
			(SELECT COUNT(*) FROM event_deliveries WHERE event_id = ? AND subscriber_type = ? AND subscriber_id = ?)
	`, record.MarkerEventID, record.PublicationEventID, record.PublicationEventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID).Scan(&markerCount, &eventCount, &scopeCount); err != nil {
		return fmt.Errorf("validate sqlite inbound publication coupling: %w", err)
	}
	if markerCount != 1 || eventCount != 1 || scopeCount != 1 {
		return fmt.Errorf("sqlite inbound publication %s is missing coupled marker/event/replay scope", record.PublicationID)
	}
	evt, err := loadSQLiteInboundPublicationEvent(ctx, db, record.PublicationEventID)
	if err != nil {
		return err
	}
	record.PublicationEvent = evt
	return nil
}

func loadSQLiteInboundPublicationEvent(ctx context.Context, db inboundPublicationQueryer, eventID string) (events.Event, error) {
	var eventType, sourceAgent, payload, runID, parentID, entityID, flowInstance, scope string
	var sourceRouteRaw, targetRouteRaw, targetSetRaw []byte
	var chainDepth int
	var createdAtValue any
	err := db.QueryRowContext(ctx, `
		SELECT event_name, COALESCE(produced_by, ''), payload, COALESCE(run_id, ''),
		       COALESCE(source_event_id, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, ''),
		       scope, source_route, target_route, target_set, chain_depth, created_at
		FROM events WHERE event_id = ?
	`, eventID).Scan(&eventType, &sourceAgent, &payload, &runID, &parentID, &entityID, &flowInstance, &scope, &sourceRouteRaw, &targetRouteRaw, &targetSetRaw, &chainDepth, &createdAtValue)
	if err != nil {
		return events.EmptyEvent(), fmt.Errorf("load sqlite inbound publication event: %w", err)
	}
	createdAt, ok, err := sqliteTimeValue(createdAtValue)
	if err != nil {
		return events.EmptyEvent(), fmt.Errorf("load sqlite inbound publication event created_at: %w", err)
	}
	if !ok {
		return events.EmptyEvent(), fmt.Errorf("load sqlite inbound publication event created_at is missing")
	}
	envelope, err := decodeInboundEventEnvelope(entityID, flowInstance, scope, sourceRouteRaw, targetRouteRaw, targetSetRaw)
	if err != nil {
		return events.EmptyEvent(), err
	}
	return events.NewProjectionEvent(eventID, events.EventType(eventType), sourceAgent, "", json.RawMessage(payload), chainDepth, runID, parentID, envelope, createdAt), nil
}

var _ runtimeinbound.Runner = (*SQLiteRuntimeStore)(nil)
