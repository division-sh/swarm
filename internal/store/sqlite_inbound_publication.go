package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
		allowGenerationRebind, err := admitSQLiteInboundStandingTargetTx(txctx, tx, request)
		if err != nil {
			return err
		}
		if allowGenerationRebind {
			txctx = runtimepipeline.WithStandingGenerationRebind(txctx)
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
		result = mutation.record
		result.Created = true
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
	rows, err := s.DB.QueryContext(ctx, `SELECT provider, entity_id, provider_event_id FROM inbound_publications ORDER BY provider, entity_id, provider_event_id`)
	if err != nil {
		return fmt.Errorf("list sqlite inbound publications for integrity validation: %w", err)
	}
	type identity struct{ provider, entityID, providerEventID string }
	identities := make([]identity, 0)
	for rows.Next() {
		var item identity
		if err := rows.Scan(&item.provider, &item.entityID, &item.providerEventID); err != nil {
			rows.Close()
			return fmt.Errorf("scan sqlite inbound publication identity: %w", err)
		}
		identities = append(identities, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read sqlite inbound publication identities: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close sqlite inbound publication identities: %w", err)
	}
	for _, item := range identities {
		record, found, err := loadSQLiteInboundPublicationTx(ctx, s.DB, item.provider, item.entityID, item.providerEventID)
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
	return nil
}

func loadSQLiteInboundPublicationTx(ctx context.Context, db inboundPublicationQueryer, provider, entityID, providerEventID string) (runtimeinbound.Record, bool, error) {
	record, err := scanSQLiteInboundPublication(db.QueryRowContext(ctx, sqliteInboundPublicationSelect+` WHERE provider = ? AND entity_id = ? AND provider_event_id = ?`, strings.ToLower(strings.TrimSpace(provider)), strings.TrimSpace(entityID), strings.TrimSpace(providerEventID)))
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeinbound.Record{}, false, nil
	}
	if err != nil {
		return runtimeinbound.Record{}, false, fmt.Errorf("load sqlite inbound publication: %w", err)
	}
	record.Events, err = loadSQLiteInboundPublicationChildren(ctx, db, record)
	if err != nil {
		return runtimeinbound.Record{}, false, err
	}
	return record, true, nil
}

const sqliteInboundPublicationSelect = `
	SELECT p.publication_id, p.provider, p.entity_id, p.provider_event_id,
	       p.request_fingerprint, p.request_projection_version,
	       p.stable_service_id, p.package_key, p.flow_id, p.instance_id,
	       p.target_alias, p.target_flow_instance, p.expected_publication_sequence,
	       p.resolved_run_id, COALESCE(p.marker_event_id, ''), p.acknowledgement_mode,
	       p.output_count, p.original_received_at, p.original_user_agent, p.original_transport_metadata,
	       p.state, p.created_at, p.committed_at
	FROM inbound_publications p
`

func scanSQLiteInboundPublication(row inboundPublicationRowScanner) (runtimeinbound.Record, error) {
	var record runtimeinbound.Record
	var ackMode string
	var transportMetadata any
	var originalReceivedAt, createdAt, committedAt any
	err := row.Scan(
		&record.PublicationID, &record.Provider, &record.EntityID, &record.ProviderEventID,
		&record.RequestFingerprint, &record.RequestProjectionVersion,
		&record.StableServiceID, &record.PackageKey, &record.FlowID, &record.InstanceID,
		&record.TargetAlias, &record.TargetFlowInstance, &record.ExpectedPublicationSequence,
		&record.ResolvedRunID, &record.MarkerEventID, &ackMode, &record.OutputCount,
		&originalReceivedAt, &record.OriginalUserAgent, &transportMetadata,
		&record.State, &createdAt, &committedAt,
	)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	record.AcknowledgementMode = runtimeinbound.AcknowledgementMode(ackMode)
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

func loadSQLiteInboundPublicationChildren(ctx context.Context, db inboundPublicationQueryer, record runtimeinbound.Record) ([]runtimeinbound.EventRecord, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT ordinal, event_id, event_name, output_kind,
		       COALESCE(pack_id, ''), COALESCE(pack_version, ''), COALESCE(manifest_hash, ''), COALESCE(observed_generation_id, ''),
		       event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count
		FROM inbound_publication_events WHERE publication_id = ? ORDER BY ordinal
	`, record.PublicationID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite inbound publication children: %w", err)
	}
	children := make([]runtimeinbound.EventRecord, 0, record.OutputCount)
	for rows.Next() {
		var child runtimeinbound.EventRecord
		var kind, packID, packVersion, manifestHash, generationID string
		if err := rows.Scan(&child.Ordinal, &child.EventID, &child.EventName, &kind, &packID, &packVersion, &manifestHash, &generationID, &child.EventIntegrityFingerprint, &child.RecipientManifestFingerprint, &child.RecipientCount); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan sqlite inbound publication child: %w", err)
		}
		child.Kind = runtimeprovideroutput.Kind(kind)
		if child.Kind == runtimeprovideroutput.KindNormalized {
			child.Authorization = runtimeprovideroutput.Authorization{Provider: record.Provider, Event: child.EventName, PackID: packID, PackVersion: packVersion, ManifestHash: manifestHash, GenerationID: generationID}.Normalized()
		}
		children = append(children, child)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("read sqlite inbound publication children: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close sqlite inbound publication children: %w", err)
	}
	for i := range children {
		children[i].Event, err = loadSQLiteInboundPublicationEvent(ctx, db, children[i].EventID)
		if err != nil {
			return nil, err
		}
	}
	return children, nil
}

func admitSQLiteInboundStandingTargetTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request) (bool, error) {
	var packageKey, flowID, instanceID, entityID, runID, effectiveState, publicationState string
	var generation, publicationSequence int64
	err := tx.QueryRowContext(ctx, `
		SELECT package_key, flow_id, instance_id, entity_id, current_run_id,
		       current_generation, publication_sequence, effective_state, publication_state
		FROM standing_services WHERE service_id = ?
	`, request.StableServiceID).Scan(&packageKey, &flowID, &instanceID, &entityID, &runID, &generation, &publicationSequence, &effectiveState, &publicationState)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("standing service %s is not admitted", request.StableServiceID)
	}
	if err != nil {
		return false, fmt.Errorf("lock sqlite inbound standing service: %w", err)
	}
	if packageKey != request.PackageKey || flowID != request.FlowID || instanceID != request.InstanceID || entityID != request.EntityID || runID != request.ResolvedRunID || publicationSequence != request.ExpectedPublicationSequence {
		return false, fmt.Errorf("stale or conflicting sqlite inbound standing target")
	}
	if effectiveState != "active" || publicationState != "published" {
		return false, fmt.Errorf("standing service %s is %s/%s and cannot accept inbound publication", request.StableServiceID, effectiveState, publicationState)
	}
	var runStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, request.ResolvedRunID).Scan(&runStatus); err != nil {
		return false, fmt.Errorf("lock sqlite inbound target run: %w", err)
	}
	if runStatus != "running" && runStatus != "paused" {
		return false, fmt.Errorf("inbound target run %s has terminal status %s", request.ResolvedRunID, runStatus)
	}
	var generationRunID string
	if err := tx.QueryRowContext(ctx, `SELECT run_id FROM standing_service_generations WHERE service_id = ? AND generation = ? AND retired_at IS NULL`, request.StableServiceID, generation).Scan(&generationRunID); err != nil {
		return false, fmt.Errorf("lock sqlite inbound standing generation: %w", err)
	}
	if generationRunID != request.ResolvedRunID {
		return false, fmt.Errorf("sqlite standing generation run changed during inbound admission")
	}
	return generation > 1, nil
}

func insertSQLiteInboundPublicationPreparedTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request) error {
	now := time.Now().UTC()
	_, err := tx.ExecContext(ctx, `
		INSERT INTO inbound_publications (
			publication_id, provider, entity_id, provider_event_id, request_fingerprint, request_projection_version,
			stable_service_id, package_key, flow_id, instance_id, target_alias, target_flow_instance,
			expected_publication_sequence, resolved_run_id, acknowledgement_mode,
			original_received_at, original_user_agent, original_transport_metadata, state, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?)
	`, request.PublicationID, request.Provider, request.EntityID, request.ProviderEventID, request.RequestFingerprint, request.RequestProjectionVersion,
		request.StableServiceID, request.PackageKey, request.FlowID, request.InstanceID, request.TargetAlias, request.TargetFlowInstance,
		request.ExpectedPublicationSequence, request.ResolvedRunID, string(request.AcknowledgementMode), request.OriginalReceivedAt, request.OriginalUserAgent, string(request.OriginalTransportMetadata), now)
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

func (s *SQLiteRuntimeStore) linkInboundPublicationEventTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request, child runtimeinbound.EventRecord) error {
	auth := child.Authorization.Normalized()
	_, err := tx.ExecContext(ctx, `
		INSERT INTO inbound_publication_events (
			publication_id, ordinal, event_id, event_name, output_kind,
			pack_id, pack_version, manifest_hash, observed_generation_id,
			event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count
		) VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?)
	`, request.PublicationID, child.Ordinal, child.EventID, child.EventName, string(child.Kind), auth.PackID, auth.PackVersion, auth.ManifestHash, auth.GenerationID, child.EventIntegrityFingerprint, child.RecipientManifestFingerprint, child.RecipientCount)
	if err != nil {
		return fmt.Errorf("link sqlite inbound publication child ordinal %d: %w", child.Ordinal, err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) finalizeInboundPublicationTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request, outputCount int) (runtimeinbound.Record, error) {
	var count, minOrdinal, maxOrdinal int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(ordinal), -1), COALESCE(MAX(ordinal), -1) FROM inbound_publication_events WHERE publication_id = ?`, request.PublicationID).Scan(&count, &minOrdinal, &maxOrdinal); err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("validate sqlite inbound publication child cardinality: %w", err)
	}
	if count != outputCount || minOrdinal != 0 || maxOrdinal != outputCount-1 {
		return runtimeinbound.Record{}, fmt.Errorf("sqlite inbound publication child ordinals are not contiguous: count=%d min=%d max=%d expected=%d", count, minOrdinal, maxOrdinal, outputCount)
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE inbound_publications SET marker_event_id = ?, output_count = ?, state = 'committed', committed_at = ? WHERE publication_id = ? AND state = 'prepared'`, request.MarkerEventID, outputCount, now, request.PublicationID)
	if err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("finalize sqlite inbound publication: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		return runtimeinbound.Record{}, fmt.Errorf("prepared sqlite inbound publication %s was not finalized", request.PublicationID)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET event_count = (SELECT COUNT(*) FROM events WHERE run_id = ?) WHERE run_id = ?`, request.ResolvedRunID, request.ResolvedRunID); err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("synchronize sqlite inbound publication event count: %w", err)
	}
	record, found, err := loadSQLiteInboundPublicationTx(ctx, tx, request.Provider, request.EntityID, request.ProviderEventID)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	if !found {
		return runtimeinbound.Record{}, errInboundPublicationNotFound
	}
	if err := validateSQLiteInboundPublicationIntegrityTx(ctx, tx, &record); err != nil {
		return runtimeinbound.Record{}, err
	}
	return record, nil
}

func validateSQLiteInboundPublicationIntegrityTx(ctx context.Context, db inboundPublicationQueryer, record *runtimeinbound.Record) error {
	if err := validateInboundPublicationRecordShape(record); err != nil {
		return err
	}
	var childCount, minOrdinal, maxOrdinal, markerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM inbound_publication_events WHERE publication_id = ?),
		       (SELECT COALESCE(MIN(ordinal), -1) FROM inbound_publication_events WHERE publication_id = ?),
		       (SELECT COALESCE(MAX(ordinal), -1) FROM inbound_publication_events WHERE publication_id = ?),
		       (SELECT COUNT(*) FROM events WHERE event_id = ? AND event_name = 'platform.inbound_recorded')
	`, record.PublicationID, record.PublicationID, record.PublicationID, record.MarkerEventID).Scan(&childCount, &minOrdinal, &maxOrdinal, &markerCount); err != nil {
		return fmt.Errorf("validate sqlite inbound publication cardinality: %w", err)
	}
	if childCount != record.OutputCount || minOrdinal != 0 || maxOrdinal != record.OutputCount-1 || markerCount != 1 {
		return fmt.Errorf("sqlite inbound publication %s is missing contiguous children or evidence", record.PublicationID)
	}
	marker, err := loadSQLiteInboundPublicationEvent(ctx, db, record.MarkerEventID)
	if err != nil {
		return err
	}
	if err := runtimeinbound.ValidateEvidenceEvent(record.Request, marker, record.EventIDs(), record.EventNames()); err != nil {
		return fmt.Errorf("sqlite inbound publication %s evidence integrity mismatch: %w", record.PublicationID, err)
	}
	for index := range record.Events {
		child := &record.Events[index]
		var scopeCount int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ? AND subscriber_type = ? AND subscriber_id = ?`, child.EventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID).Scan(&scopeCount); err != nil {
			return fmt.Errorf("validate sqlite inbound publication child replay scope: %w", err)
		}
		if scopeCount != 1 {
			return fmt.Errorf("sqlite inbound publication %s child ordinal %d is missing committed replay scope", record.PublicationID, index)
		}
		routes, err := loadSQLiteInboundPublicationRoutes(ctx, db, child.EventID)
		if err != nil {
			return err
		}
		_, fingerprint, count, err := runtimeinbound.CanonicalRecipientManifest(routes)
		if err != nil || fingerprint != child.RecipientManifestFingerprint || count != child.RecipientCount {
			return fmt.Errorf("sqlite inbound publication %s child ordinal %d recipient manifest mismatch", record.PublicationID, index)
		}
	}
	return nil
}

func loadSQLiteInboundPublicationRoutes(ctx context.Context, db inboundPublicationQueryer, eventID string) ([]events.DeliveryRoute, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT subscriber_type, subscriber_id, COALESCE(delivery_target_route, '{}'), COALESCE(delivery_context, '{}')
		FROM event_deliveries WHERE event_id = ? AND NOT (subscriber_type = ? AND subscriber_id = ?)
		ORDER BY created_at ASC, delivery_id ASC
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite inbound publication child routes: %w", err)
	}
	defer rows.Close()
	routes := make([]events.DeliveryRoute, 0)
	for rows.Next() {
		var route events.DeliveryRoute
		var targetValue, contextValue any
		if err := rows.Scan(&route.SubscriberType, &route.SubscriberID, &targetValue, &contextValue); err != nil {
			return nil, fmt.Errorf("scan sqlite inbound publication child route: %w", err)
		}
		route.Target = decodeRouteIdentityJSON(jsonRawMessageValue(targetValue))
		route.Context = decodeDeliveryContextJSON(jsonRawMessageValue(contextValue))
		routes = append(routes, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite inbound publication child routes: %w", err)
	}
	return events.NormalizeDeliveryRoutes(routes), nil
}

func loadSQLiteInboundPublicationEvent(ctx context.Context, db inboundPublicationQueryer, eventID string) (events.Event, error) {
	var row persistedEventIdentity
	var createdAtValue any
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(run_id, ''), event_name, COALESCE(task_id, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, ''),
		       scope, payload, COALESCE(chain_depth, 0), COALESCE(produced_by, ''), COALESCE(produced_by_type, ''),
		       COALESCE(source_event_id, ''), created_at, execution_mode, source_route, target_route, target_set
		FROM events WHERE event_id = ?
	`, eventID).Scan(&row.RunID, &row.EventName, &row.TaskID, &row.EntityID, &row.FlowInstance, &row.Scope, &row.Payload,
		&row.ChainDepth, &row.ProducedBy, &row.ProducedByType, &row.SourceEventID, &createdAtValue, &row.ExecutionMode,
		&row.SourceRoute, &row.TargetRoute, &row.TargetSet)
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
	row.CreatedAt = createdAt
	row.EventID = eventID
	return eventFromPersistedIdentity(row)
}

var _ runtimeinbound.Runner = (*SQLiteRuntimeStore)(nil)
