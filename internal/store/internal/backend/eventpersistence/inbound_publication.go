package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	storeactivityjournal "github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

var errInboundPublicationNotFound = errors.New("inbound publication not found")

type inboundPublicationTransactionStore interface {
	linkInboundPublicationEventTx(context.Context, *sql.Tx, runtimeinbound.Request, runtimeinbound.EventRecord) error
	finalizeInboundPublicationTx(context.Context, *sql.Tx, runtimeinbound.Request, int) (runtimeinbound.Record, error)
}

func commitInboundPublicationTx(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	eventStore eventCommitTxStore,
	publicationStore inboundPublicationTransactionStore,
	postgres bool,
	command runtimeinbound.CommitCommand,
	handoff *runLifecycleCandidateHandoffReservation,
) (runtimeinbound.CommitResult, error) {
	if tx == nil || story == nil {
		return runtimeinbound.CommitResult{}, fmt.Errorf("inbound publication requires private transaction and story owners")
	}
	if err := command.Validate(); err != nil {
		return runtimeinbound.CommitResult{}, err
	}
	request := command.Request.Normalized()
	committed := make([]runtimebus.CommittedPublication, len(command.Publications))
	children := make([]runtimeinbound.EventRecord, len(command.Finalization.Events))
	for index, publication := range command.Publications {
		var err error
		committed[index], err = commitPublicationTx(ctx, tx, story, eventStore, postgres, publication, handoff)
		if err != nil {
			return runtimeinbound.CommitResult{}, fmt.Errorf("commit inbound publication event %d: %w", index, err)
		}
		item := command.Finalization.Events[index]
		_, recipientFingerprint, recipientCount, err := canonicalInboundRecipientManifest(item.RecipientManifest)
		if err != nil {
			return runtimeinbound.CommitResult{}, err
		}
		eventFingerprint, err := runtimeinbound.EventIntegrityFingerprint(item.Event, item.Kind, item.Authorization)
		if err != nil {
			return runtimeinbound.CommitResult{}, err
		}
		children[index] = runtimeinbound.EventRecord{
			Ordinal: index, EventID: item.Event.ID(), EventName: string(item.Event.Type()), Kind: item.Kind,
			Authorization: item.Authorization, EventIntegrityFingerprint: eventFingerprint,
			RecipientManifestFingerprint: recipientFingerprint, RecipientCount: recipientCount, Event: item.Event,
		}
		if err := publicationStore.linkInboundPublicationEventTx(ctx, tx, request, children[index]); err != nil {
			return runtimeinbound.CommitResult{}, err
		}
	}
	evidence, err := events.AdmitForPersistence(command.Finalization.EvidenceEvent, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return runtimeinbound.CommitResult{}, fmt.Errorf("admit inbound evidence: %w", err)
	}
	committer := sqlPublishCommitter{tx: tx, store: eventStore, story: runtimeAuthorActivityMutation(story)}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteInboundEvidenceDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return runtimeinbound.CommitResult{}, err
	}
	if _, err := committer.commitNamedEvent(ctx, "finalize inbound publication evidence", events.EventAdmissionDiagnosticDirect, events.EventTypePlatformInboundRecord, runtimebus.CommitPublishRequest{Event: evidence, RouteSettlement: settlement, ReplayScope: runtimepipelineobligation.ScopeDirect}); err != nil {
		return runtimeinbound.CommitResult{}, fmt.Errorf("commit inbound evidence: %w", err)
	}
	if err := storeactivityjournal.RecordInbound(ctx, story, command.Finalization.EvidenceEvent, request.Provider, command.AuthorProjection); err != nil {
		return runtimeinbound.CommitResult{}, fmt.Errorf("record inbound author activity: %w", err)
	}
	record, err := publicationStore.finalizeInboundPublicationTx(ctx, tx, request, len(command.Finalization.Events))
	if err != nil {
		return runtimeinbound.CommitResult{}, err
	}
	record.Created = true
	return runtimeinbound.CommitResult{Record: record, Publications: committed}, nil
}

func (s *EventPostgresOwner) CommitInboundPublication(ctx context.Context, command runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	if err := command.Validate(); err != nil {
		return runtimeinbound.CommitResult{}, err
	}
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return runtimeinbound.CommitResult{}, err
	}
	defer handoff.Rollback()
	request := command.Request.Normalized()
	var result runtimeinbound.CommitResult
	err = s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		identityKey := inboundEventIdempotencyKey(request.ProviderEventID, request.EntityID, request.Provider)
		if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, identityKey); err != nil {
			return fmt.Errorf("lock inbound publication identity: %w", err)
		}
		existing, found, err := loadPostgresInboundPublicationTx(txctx, tx, request.Provider, request.EntityID, request.ProviderEventID, true)
		if err != nil {
			return err
		}
		if found {
			if err := validateInboundPublicationRetry(request, existing); err != nil {
				return err
			}
			if err := validatePostgresInboundPublicationIntegrityTx(txctx, tx, &existing); err != nil {
				return err
			}
			result.Record = existing
			return nil
		}
		if err := admitPostgresInboundStandingTargetTx(txctx, s, tx, request); err != nil {
			return err
		}
		if err := insertPostgresInboundPublicationPreparedTx(txctx, tx, request); err != nil {
			return err
		}
		result, err = commitInboundPublicationTx(txctx, tx, story, s, s, true, command, handoff)
		return err
	})
	if err != nil {
		return runtimeinbound.CommitResult{}, err
	}
	return result, handoff.Commit()
}

func (s *EventPostgresOwner) LoadInboundPublicationByIdentity(ctx context.Context, provider, entityID, providerEventID string) (runtimeinbound.Record, bool, error) {
	if s == nil || s.backend == nil {
		return runtimeinbound.Record{}, false, fmt.Errorf("postgres store is required")
	}
	record, found, err := loadPostgresInboundPublicationTx(ctx, s.backend, provider, entityID, providerEventID, false)
	if err != nil || !found {
		return record, found, err
	}
	if err := validatePostgresInboundPublicationIntegrityTx(ctx, s.backend, &record); err != nil {
		return runtimeinbound.Record{}, false, err
	}
	return record, true, nil
}

func (s *EventPostgresOwner) ValidateInboundPublicationIntegrity(ctx context.Context) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	rows, err := s.backend.QueryContext(ctx, `SELECT provider, entity_id::text, provider_event_id FROM inbound_publications ORDER BY provider, entity_id::text, provider_event_id`)
	if err != nil {
		return fmt.Errorf("list inbound publications for integrity validation: %w", err)
	}
	defer rows.Close()
	type identity struct{ provider, entityID, providerEventID string }
	identities := make([]identity, 0)
	for rows.Next() {
		var item identity
		if err := rows.Scan(&item.provider, &item.entityID, &item.providerEventID); err != nil {
			return fmt.Errorf("scan inbound publication identity: %w", err)
		}
		identities = append(identities, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read inbound publication identities: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close inbound publication identities: %w", err)
	}
	for _, item := range identities {
		record, found, err := loadPostgresInboundPublicationTx(ctx, s.backend, item.provider, item.entityID, item.providerEventID, false)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("inbound publication disappeared during integrity validation")
		}
		if err := validatePostgresInboundPublicationIntegrityTx(ctx, s.backend, &record); err != nil {
			return err
		}
	}
	return nil
}

type inboundPublicationQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type inboundPublicationRowScanner interface {
	Scan(...any) error
}

func loadPostgresInboundPublicationTx(ctx context.Context, db inboundPublicationQueryer, provider, entityID, providerEventID string, forUpdate bool) (runtimeinbound.Record, bool, error) {
	query := postgresInboundPublicationSelect + ` WHERE provider = $1 AND entity_id = $2::uuid AND provider_event_id = $3`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	record, err := scanPostgresInboundPublication(db.QueryRowContext(ctx, query, strings.ToLower(strings.TrimSpace(provider)), strings.TrimSpace(entityID), strings.TrimSpace(providerEventID)))
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeinbound.Record{}, false, nil
	}
	if err != nil {
		return runtimeinbound.Record{}, false, fmt.Errorf("load inbound publication: %w", err)
	}
	record.Events, err = loadPostgresInboundPublicationChildren(ctx, db, record)
	if err != nil {
		return runtimeinbound.Record{}, false, err
	}
	return record, true, nil
}

const postgresInboundPublicationSelect = `
	SELECT p.publication_id::text, p.provider, p.entity_id::text, p.provider_event_id,
	       p.request_fingerprint, p.request_projection_version,
	       p.stable_service_id::text, p.package_key, p.flow_id, p.instance_id,
	       p.target_alias, p.target_flow_instance, p.expected_generation, p.expected_publication_sequence,
	       p.resolved_run_id::text, COALESCE(p.marker_event_id::text, ''), p.acknowledgement_mode,
	       p.output_count, p.original_received_at, p.original_user_agent, p.original_transport_metadata,
	       p.state, p.created_at, p.committed_at
	FROM inbound_publications p
`

func scanPostgresInboundPublication(row inboundPublicationRowScanner) (runtimeinbound.Record, error) {
	var record runtimeinbound.Record
	var ackMode string
	var committedAt sql.NullTime
	err := row.Scan(
		&record.PublicationID, &record.Provider, &record.EntityID, &record.ProviderEventID,
		&record.RequestFingerprint, &record.RequestProjectionVersion,
		&record.StableServiceID, &record.PackageKey, &record.FlowID, &record.InstanceID,
		&record.TargetAlias, &record.TargetFlowInstance, &record.ExpectedGeneration, &record.ExpectedPublicationSequence,
		&record.ResolvedRunID, &record.MarkerEventID, &ackMode, &record.OutputCount,
		&record.OriginalReceivedAt, &record.OriginalUserAgent, &record.OriginalTransportMetadata,
		&record.State, &record.CreatedAt, &committedAt,
	)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	record.AcknowledgementMode = runtimeinbound.AcknowledgementMode(ackMode)
	if committedAt.Valid {
		record.CommittedAt = committedAt.Time.UTC()
	}
	record.Request = record.Request.Normalized()
	return record, nil
}

func loadPostgresInboundPublicationChildren(ctx context.Context, db inboundPublicationQueryer, record runtimeinbound.Record) ([]runtimeinbound.EventRecord, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT ordinal, event_id::text, event_name, output_kind,
		       COALESCE(pack_id, ''), COALESCE(pack_version, ''), COALESCE(manifest_hash, ''), COALESCE(observed_generation_id, ''),
		       event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count
		FROM inbound_publication_events WHERE publication_id = $1::uuid ORDER BY ordinal
	`, record.PublicationID)
	if err != nil {
		return nil, fmt.Errorf("list inbound publication children: %w", err)
	}
	children := make([]runtimeinbound.EventRecord, 0, record.OutputCount)
	for rows.Next() {
		var child runtimeinbound.EventRecord
		var kind, packID, packVersion, manifestHash, generationID string
		if err := rows.Scan(&child.Ordinal, &child.EventID, &child.EventName, &kind, &packID, &packVersion, &manifestHash, &generationID, &child.EventIntegrityFingerprint, &child.RecipientManifestFingerprint, &child.RecipientCount); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan inbound publication child: %w", err)
		}
		child.Kind = runtimeprovideroutput.Kind(kind)
		if child.Kind == runtimeprovideroutput.KindNormalized {
			child.Authorization, err = runtimeprovideroutput.ParseAuthorization(record.Provider, child.EventName, packID, packVersion, manifestHash, generationID)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("admit inbound publication child authorization: %w", err)
			}
		}
		children = append(children, child)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("read inbound publication children: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close inbound publication children: %w", err)
	}
	for i := range children {
		children[i].Event, err = loadPostgresInboundPublicationEvent(ctx, db, children[i].EventID)
		if err != nil {
			return nil, err
		}
	}
	return children, nil
}

func admitPostgresInboundStandingTargetTx(ctx context.Context, s *EventPostgresOwner, tx *sql.Tx, request runtimeinbound.Request) error {
	var packageKey, flowID, instanceID, entityID, runID, effectiveState, publicationState string
	var generation, publicationSequence int64
	err := tx.QueryRowContext(ctx, `
		SELECT package_key, flow_id, instance_id, entity_id::text, current_run_id::text,
		       current_generation, publication_sequence, effective_state, publication_state
		FROM standing_services WHERE service_id = $1::uuid FOR UPDATE
	`, request.StableServiceID).Scan(&packageKey, &flowID, &instanceID, &entityID, &runID, &generation, &publicationSequence, &effectiveState, &publicationState)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("standing service %s is not admitted", request.StableServiceID)
	}
	if err != nil {
		return fmt.Errorf("lock inbound standing service: %w", err)
	}
	if packageKey != request.PackageKey || flowID != request.FlowID || instanceID != request.InstanceID || entityID != request.EntityID || runID != request.ResolvedRunID || generation != request.ExpectedGeneration || publicationSequence != request.ExpectedPublicationSequence {
		return fmt.Errorf("stale or conflicting inbound standing target")
	}
	if effectiveState != "active" || publicationState != "published" {
		return fmt.Errorf("standing service %s is %s/%s and cannot accept inbound publication", request.StableServiceID, effectiveState, publicationState)
	}
	if err := s.RunLifecyclePostgresOwner.RequireActiveTx(ctx, tx, request.ResolvedRunID); err != nil {
		return fmt.Errorf("admit inbound target run lifecycle: %w", err)
	}
	var generationRunID string
	if err := tx.QueryRowContext(ctx, `SELECT run_id::text FROM standing_service_generations WHERE service_id = $1::uuid AND generation = $2 AND retired_at IS NULL FOR UPDATE`, request.StableServiceID, generation).Scan(&generationRunID); err != nil {
		return fmt.Errorf("lock inbound standing generation: %w", err)
	}
	if generationRunID != request.ResolvedRunID {
		return fmt.Errorf("standing generation run changed during inbound admission")
	}
	return nil
}

func insertPostgresInboundPublicationPreparedTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO inbound_publications (
			publication_id, provider, entity_id, provider_event_id, request_fingerprint, request_projection_version,
			stable_service_id, package_key, flow_id, instance_id, target_alias, target_flow_instance,
			expected_generation, expected_publication_sequence, resolved_run_id, acknowledgement_mode,
			original_received_at, original_user_agent, original_transport_metadata, state, created_at
		) VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6, $7::uuid, $8, $9, $10, $11, $12, $13, $14, $15::uuid, $16, $17, $18, $19::jsonb, 'prepared', now())
	`, request.PublicationID, request.Provider, request.EntityID, request.ProviderEventID, request.RequestFingerprint, request.RequestProjectionVersion,
		request.StableServiceID, request.PackageKey, request.FlowID, request.InstanceID, request.TargetAlias, request.TargetFlowInstance,
		request.ExpectedGeneration, request.ExpectedPublicationSequence, request.ResolvedRunID, string(request.AcknowledgementMode), request.OriginalReceivedAt, request.OriginalUserAgent, string(request.OriginalTransportMetadata))
	if err != nil {
		return fmt.Errorf("insert prepared inbound publication: %w", err)
	}
	return nil
}

func (s *EventPostgresOwner) linkInboundPublicationEventTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request, child runtimeinbound.EventRecord) error {
	auth := child.Authorization
	_, err := tx.ExecContext(ctx, `
		INSERT INTO inbound_publication_events (
			publication_id, ordinal, event_id, event_name, output_kind,
			pack_id, pack_version, manifest_hash, observed_generation_id,
			event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count
		) VALUES ($1::uuid, $2, $3::uuid, $4, $5, NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), $10, $11, $12)
	`, request.PublicationID, child.Ordinal, child.EventID, child.EventName, string(child.Kind), auth.PackID(), auth.PackVersion(), auth.ManifestHash(), auth.Generation().Diagnostic(), child.EventIntegrityFingerprint, child.RecipientManifestFingerprint, child.RecipientCount)
	if err != nil {
		return fmt.Errorf("link inbound publication child ordinal %d: %w", child.Ordinal, err)
	}
	return nil
}

func (s *EventPostgresOwner) finalizeInboundPublicationTx(ctx context.Context, tx *sql.Tx, request runtimeinbound.Request, outputCount int) (runtimeinbound.Record, error) {
	var count, minOrdinal, maxOrdinal int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(ordinal), -1), COALESCE(MAX(ordinal), -1) FROM inbound_publication_events WHERE publication_id = $1::uuid`, request.PublicationID).Scan(&count, &minOrdinal, &maxOrdinal); err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("validate inbound publication child cardinality: %w", err)
	}
	if count != outputCount || minOrdinal != 0 || maxOrdinal != outputCount-1 {
		return runtimeinbound.Record{}, fmt.Errorf("inbound publication child ordinals are not contiguous: count=%d min=%d max=%d expected=%d", count, minOrdinal, maxOrdinal, outputCount)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE inbound_publications SET marker_event_id = $2::uuid, output_count = $3, state = 'committed', committed_at = now()
		WHERE publication_id = $1::uuid AND state = 'prepared'
	`, request.PublicationID, request.MarkerEventID, outputCount)
	if err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("finalize inbound publication: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		return runtimeinbound.Record{}, fmt.Errorf("prepared inbound publication %s was not finalized", request.PublicationID)
	}
	if err := s.RunLifecyclePostgresOwner.SyncCountersTx(ctx, tx, nil, request.ResolvedRunID); err != nil {
		return runtimeinbound.Record{}, fmt.Errorf("synchronize inbound publication event count: %w", err)
	}
	record, found, err := loadPostgresInboundPublicationTx(ctx, tx, request.Provider, request.EntityID, request.ProviderEventID, false)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	if !found {
		return runtimeinbound.Record{}, errInboundPublicationNotFound
	}
	if err := validatePostgresInboundPublicationIntegrityTx(ctx, tx, &record); err != nil {
		return runtimeinbound.Record{}, err
	}
	return record, nil
}

func validateInboundPublicationRetry(request runtimeinbound.Request, existing runtimeinbound.Record) error {
	if existing.State != "committed" {
		return fmt.Errorf("inbound publication %s is durably %s; store is corrupt and requires fresh-store remediation", existing.PublicationID, existing.State)
	}
	if existing.RequestProjectionVersion != request.RequestProjectionVersion || existing.RequestFingerprint != request.RequestFingerprint {
		return fmt.Errorf("%w: provider identity conflicts with the committed semantic request", runtimeinbound.ErrRequestIdentityConflict)
	}
	return nil
}

func validateInboundPublicationRecordShape(record *runtimeinbound.Record) error {
	if record == nil {
		return fmt.Errorf("inbound publication record is required")
	}
	if err := record.Request.Validate(); err != nil {
		return fmt.Errorf("inbound publication %s has invalid request authority: %w", record.PublicationID, err)
	}
	expectedPublicationID, expectedMarkerEventID := runtimeinbound.DeterministicIDs(record.Provider, record.EntityID, record.ProviderEventID)
	if record.PublicationID != expectedPublicationID || record.MarkerEventID != expectedMarkerEventID {
		return fmt.Errorf("inbound publication %s has invalid deterministic operation identity", record.PublicationID)
	}
	if record.State != "committed" || record.MarkerEventID == "" || record.CommittedAt.IsZero() || record.OutputCount < 1 || record.OutputCount > 2 {
		return fmt.Errorf("inbound publication %s has incomplete committed coupling", record.PublicationID)
	}
	if len(record.Events) != record.OutputCount {
		return fmt.Errorf("inbound publication %s child count %d does not match output_count %d", record.PublicationID, len(record.Events), record.OutputCount)
	}
	for index, child := range record.Events {
		if child.Ordinal != index {
			return fmt.Errorf("inbound publication %s child ordinals are not contiguous", record.PublicationID)
		}
		expectedID, err := runtimeinbound.DeterministicEventID(record.PublicationID, index)
		if err != nil || child.EventID != expectedID {
			return fmt.Errorf("inbound publication %s child ordinal %d has invalid deterministic event_id", record.PublicationID, index)
		}
		if child.Event.ID() != child.EventID || string(child.Event.Type()) != child.EventName || child.Event.RunID() != record.ResolvedRunID {
			return fmt.Errorf("inbound publication %s child ordinal %d event coupling is incoherent", record.PublicationID, index)
		}
		auth := child.Authorization
		if index == 0 {
			if child.Kind != runtimeprovideroutput.KindRaw || !auth.Empty() {
				return fmt.Errorf("inbound publication %s ordinal 0 is not an unauthorised raw output", record.PublicationID)
			}
		} else if child.Kind != runtimeprovideroutput.KindNormalized || !auth.Valid() || auth.Provider() != record.Provider || auth.Event() != child.EventName {
			return fmt.Errorf("inbound publication %s normalized child authorization is incoherent", record.PublicationID)
		}
		fingerprint, err := runtimeinbound.EventIntegrityFingerprint(child.Event, child.Kind, auth)
		if err != nil || fingerprint != child.EventIntegrityFingerprint {
			return fmt.Errorf("inbound publication %s child ordinal %d event integrity mismatch: stored=%s computed=%s", record.PublicationID, index, child.EventIntegrityFingerprint, fingerprint)
		}
	}
	return nil
}

func validatePostgresInboundPublicationIntegrityTx(ctx context.Context, db inboundPublicationQueryer, record *runtimeinbound.Record) error {
	if err := validateInboundPublicationRecordShape(record); err != nil {
		return err
	}
	var childCount, minOrdinal, maxOrdinal, markerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM inbound_publication_events WHERE publication_id = $1::uuid),
		       (SELECT COALESCE(MIN(ordinal), -1) FROM inbound_publication_events WHERE publication_id = $1::uuid),
		       (SELECT COALESCE(MAX(ordinal), -1) FROM inbound_publication_events WHERE publication_id = $1::uuid),
		       (SELECT COUNT(*) FROM events WHERE event_id = $2::uuid AND event_name = 'platform.inbound_recorded')
	`, record.PublicationID, record.MarkerEventID).Scan(&childCount, &minOrdinal, &maxOrdinal, &markerCount); err != nil {
		return fmt.Errorf("validate inbound publication cardinality: %w", err)
	}
	if childCount != record.OutputCount || minOrdinal != 0 || maxOrdinal != record.OutputCount-1 || markerCount != 1 {
		return fmt.Errorf("inbound publication %s is missing contiguous children or evidence", record.PublicationID)
	}
	marker, err := loadPostgresInboundPublicationEvent(ctx, db, record.MarkerEventID)
	if err != nil {
		return err
	}
	if err := runtimeinbound.ValidateEvidenceEvent(record.Request, marker, record.EventIDs(), record.EventNames()); err != nil {
		return fmt.Errorf("inbound publication %s evidence integrity mismatch: %w", record.PublicationID, err)
	}
	for index := range record.Events {
		child := &record.Events[index]
		if _, err := loadCommittedPipelineScope(ctx, db, child.EventID, true); err != nil {
			return fmt.Errorf("inbound publication %s child ordinal %d committed pipeline scope: %w", record.PublicationID, index, err)
		}
		routes, err := loadPostgresInboundPublicationRoutes(ctx, db, child.EventID)
		if err != nil {
			return err
		}
		_, fingerprint, count, err := runtimeinbound.CanonicalRecipientManifest(routes)
		if err != nil || fingerprint != child.RecipientManifestFingerprint || count != child.RecipientCount {
			return fmt.Errorf("inbound publication %s child ordinal %d recipient manifest mismatch", record.PublicationID, index)
		}
	}
	return nil
}

func loadPostgresInboundPublicationRoutes(ctx context.Context, db inboundPublicationQueryer, eventID string) ([]events.DeliveryRoute, error) {
	snapshots, err := postgresDeliveryAdapter.SnapshotsForEvent(ctx, db, eventID)
	if err != nil {
		return nil, fmt.Errorf("list inbound publication child routes: %w", err)
	}
	routes := make([]events.DeliveryRoute, 0, len(snapshots))
	for _, snapshot := range snapshots {
		routes = append(routes, snapshot.Route)
	}
	return events.NormalizeDeliveryRoutes(routes), nil
}

func loadPostgresInboundPublicationEvent(ctx context.Context, db inboundPublicationQueryer, eventID string) (events.Event, error) {
	row, found, err := loadPostgresEventIdentity(ctx, db, eventID)
	if err != nil {
		var event events.Event
		return event, fmt.Errorf("load inbound publication event: %w", err)
	}
	if !found {
		var event events.Event
		return event, fmt.Errorf("load inbound publication event: event %s not found", strings.TrimSpace(eventID))
	}
	admitted, err := decodeEventRecord(row)
	if err != nil {
		var event events.Event
		return event, err
	}
	return admitted.Event(), nil
}

func LoadPostgresInboundPublicationEvent(ctx context.Context, db interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string) (events.Event, error) {
	return loadPostgresInboundPublicationEvent(ctx, db, eventID)
}

func canonicalInboundRecipientManifest(raw json.RawMessage) (json.RawMessage, string, int, error) {
	var routes []events.DeliveryRoute
	if len(raw) == 0 {
		raw = json.RawMessage(`[]`)
	}
	if err := json.Unmarshal(raw, &routes); err != nil {
		return nil, "", 0, fmt.Errorf("decode inbound recipient manifest: %w", err)
	}
	return runtimeinbound.CanonicalRecipientManifest(routes)
}

func decodeInboundEventEnvelope(entityID, flowInstance, scope string, sourceRouteRaw, targetRouteRaw, targetSetRaw []byte) (events.EventEnvelope, error) {
	envelope := events.EventEnvelope{EntityID: strings.TrimSpace(entityID), FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"), Scope: events.EventScope(strings.TrimSpace(scope))}
	if len(sourceRouteRaw) > 0 {
		if err := json.Unmarshal(sourceRouteRaw, &envelope.Source); err != nil {
			return events.EventEnvelope{}, fmt.Errorf("decode inbound event source route: %w", err)
		}
	}
	if len(targetRouteRaw) > 0 {
		if err := json.Unmarshal(targetRouteRaw, &envelope.Target); err != nil {
			return events.EventEnvelope{}, fmt.Errorf("decode inbound event target route: %w", err)
		}
	}
	if len(targetSetRaw) > 0 {
		if err := json.Unmarshal(targetSetRaw, &envelope.TargetSet); err != nil {
			return events.EventEnvelope{}, fmt.Errorf("decode inbound event target set: %w", err)
		}
	}
	return envelope.Normalized(), nil
}

var _ runtimeinbound.Runner = (*EventPostgresOwner)(nil)

func inboundEventIdempotencyKey(providerEventID, entityID, provider string) string {
	return strings.Join([]string{"inbound-publication", strings.TrimSpace(strings.ToLower(provider)), strings.TrimSpace(entityID), strings.TrimSpace(providerEventID)}, ":")
}
