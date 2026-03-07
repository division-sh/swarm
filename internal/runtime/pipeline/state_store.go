package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"time"
)

type PipelineStateSnapshot struct {
	Scans       map[string]*scanAccumulator
	PendingDedup map[string]pendingCandidate
	Validations map[string]*validationPipelineState
	Processed   map[string]struct{}
}

type PipelineStateStore struct {
	db *sql.DB
	mu *sync.Mutex
}

func NewPipelineStateStore(db *sql.DB, mu *sync.Mutex) *PipelineStateStore {
	return &PipelineStateStore{db: db, mu: mu}
}

func (ps *PipelineStateStore) Enabled(ctx context.Context, enabled bool) bool {
	return ps != nil && ps.db != nil && enabled
}

func (ps *PipelineStateStore) Load(ctx context.Context) PipelineStateSnapshot {
	out := PipelineStateSnapshot{
		Scans:        make(map[string]*scanAccumulator),
		PendingDedup: make(map[string]pendingCandidate),
		Validations:  make(map[string]*validationPipelineState),
		Processed:    make(map[string]struct{}),
	}
	if ps == nil || ps.db == nil {
		return out
	}

	scanRows, err := dbQueryContext(ctx, ps.db, `
		SELECT scan_id, COALESCE(campaign_id,''), mode, geography,
		       expected, COALESCE(completed_by, '{}'::jsonb), reports,
		       discovered, skipped, COALESCE(started_at, now())
		FROM scan_accumulators
	`)
	if err == nil {
		for scanRows.Next() {
			var (
				scanID, campaignID, mode, geography    string
				expected, reports, discovered, skipped int
				completedRaw                           []byte
				createdAt                              time.Time
			)
			if scanErr := scanRows.Scan(&scanID, &campaignID, &mode, &geography, &expected, &completedRaw, &reports, &discovered, &skipped, &createdAt); scanErr != nil {
				continue
			}
			completedBy := map[string]struct{}{}
			var completedObj map[string]any
			if err := json.Unmarshal(completedRaw, &completedObj); err == nil && len(completedObj) > 0 {
				for key := range completedObj {
					key = strings.TrimSpace(key)
					if key != "" {
						completedBy[key] = struct{}{}
					}
				}
			} else {
				var completed []string
				_ = json.Unmarshal(completedRaw, &completed)
				for _, key := range completed {
					key = strings.TrimSpace(key)
					if key != "" {
						completedBy[key] = struct{}{}
					}
				}
			}
			out.Scans[scanID] = &scanAccumulator{
				ScanID:      scanID,
				CampaignID:  campaignID,
				Mode:        mode,
				Geography:   geography,
				Expected:    expected,
				CompletedBy: completedBy,
				ReportData:  make([]map[string]any, 0),
				Reports:     reports,
				Discovered:  discovered,
				Skipped:     skipped,
				CreatedAt:   createdAt,
			}
		}
		_ = scanRows.Close()
	}

	pendingRows, err := dbQueryContext(ctx, ps.db, `
		SELECT
			dedup_event_id,
			COALESCE(existing_id, ''),
			scan_id,
			COALESCE(campaign_id, ''),
			COALESCE(mode, ''),
			signal_strength,
			geography,
			discovery_mode,
			COALESCE(name, ''),
			COALESCE(payload, '{}'::jsonb)
		FROM pending_dedup_candidates
		WHERE status = 'pending'
	`)
	if err == nil {
		for pendingRows.Next() {
			var (
				dedupID, existingID, scanID, campaignID, mode, geography, discoveryMode, name string
				signalFloat                                                                   float64
				payloadRaw                                                                    []byte
			)
			if scanErr := pendingRows.Scan(&dedupID, &existingID, &scanID, &campaignID, &mode, &signalFloat, &geography, &discoveryMode, &name, &payloadRaw); scanErr != nil {
				continue
			}
			payload := parsePayloadMap(payloadRaw)
			candidateName := strings.TrimSpace(name)
			if candidateName == "" {
				candidateName = deriveDiscoveryCandidateName(payload)
			}
			resolvedCampaignID := strings.TrimSpace(campaignID)
			if resolvedCampaignID == "" {
				resolvedCampaignID = strings.TrimSpace(asString(payload["campaign_id"]))
			}
			resolvedMode := normalizeScanMode(firstNonEmpty(mode, discoveryMode))
			if resolvedMode == "" {
				resolvedMode = normalizeScanMode(asString(payload["mode"]))
			}
			out.PendingDedup[dedupID] = pendingCandidate{
				DedupEventID: dedupID,
				ExistingID:   strings.TrimSpace(existingID),
				ScanID:       scanID,
				CampaignID:   resolvedCampaignID,
				Mode:         resolvedMode,
				Geography:    geography,
				Name:         candidateName,
				Signal:       signalFloat,
				Payload:      payload,
			}
		}
		_ = pendingRows.Close()
	}

	validationRows, err := dbQueryContext(ctx, ps.db, `
		SELECT vertical_id::text, status, g1_research, g2_spec, g3_cto, g4_brand,
		       COALESCE(research_payload, '{}'::jsonb), COALESCE(spec_payload, '{}'::jsonb),
		       COALESCE(cto_payload, '{}'::jsonb), COALESCE(brand_payload, '{}'::jsonb),
		       COALESCE(scoring_payload, '{}'::jsonb),
		       revision_count, inner_revision_count, spec_version,
		       packaging_requested, packaging_requested_at, packaging_retries
		FROM validation_pipelines
	`)
	if err == nil {
		for validationRows.Next() {
			var (
				verticalID, status                                                     string
				g1, g2, g3, g4, packagingRequested                                     bool
				researchPayload, specPayload, ctoPayload, brandPayload, scoringPayload []byte
				revisionCount, innerRevisionCount, specVersion, packagingRetries       int
				packagingRequestedAt                                                   sql.NullTime
			)
			if scanErr := validationRows.Scan(
				&verticalID, &status, &g1, &g2, &g3, &g4,
				&researchPayload, &specPayload, &ctoPayload, &brandPayload,
				&scoringPayload,
				&revisionCount, &innerRevisionCount, &specVersion,
				&packagingRequested, &packagingRequestedAt, &packagingRetries,
			); scanErr != nil {
				continue
			}
			var packagingAt *time.Time
			if packagingRequestedAt.Valid {
				t := packagingRequestedAt.Time
				packagingAt = &t
			}
			out.Validations[verticalID] = &validationPipelineState{
				VerticalID:           verticalID,
				Status:               status,
				G1Research:           g1,
				G2Spec:               g2,
				G3CTO:                g3,
				G4Brand:              g4,
				ResearchPayload:      cloneRaw(researchPayload),
				SpecPayload:          cloneRaw(specPayload),
				CTOPayload:           cloneRaw(ctoPayload),
				BrandPayload:         cloneRaw(brandPayload),
				ScoringPayload:       cloneRaw(scoringPayload),
				RevisionCount:        revisionCount,
				InnerRevisionCount:   innerRevisionCount,
				SpecVersion:          specVersion,
				PackagingRequested:   packagingRequested || packagingAt != nil,
				PackagingRequestedAt: packagingAt,
				PackagingRetries:     packagingRetries,
			}
		}
		_ = validationRows.Close()
	}

	processedRows, err := dbQueryContext(ctx, ps.db, `
		SELECT event_id::text
		FROM pipeline_processed_events
		WHERE processed_at >= now() - interval '7 days'
	`)
	if err == nil {
		for processedRows.Next() {
			var eventID string
			if scanErr := processedRows.Scan(&eventID); scanErr != nil {
				continue
			}
			out.Processed[eventID] = struct{}{}
		}
		_ = processedRows.Close()
	}

	return out
}

func (ps *PipelineStateStore) MarkProcessed(ctx context.Context, processed map[string]struct{}, eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false
	}
	if _, ok := processed[eventID]; ok {
		return false
	}
	if ps != nil && ps.db != nil {
		res, err := dbExecContext(ctx, ps.db, `
			INSERT INTO pipeline_processed_events (event_id, processed_at)
			VALUES ($1, now())
			ON CONFLICT (event_id) DO NOTHING
		`, eventID)
		if err == nil {
			if n, _ := res.RowsAffected(); n == 0 {
				processed[eventID] = struct{}{}
				return false
			}
		}
	}
	processed[eventID] = struct{}{}
	return true
}

func (ps *PipelineStateStore) Persist(ctx context.Context, scans map[string]*scanAccumulator, pending map[string]pendingCandidate, validations map[string]*validationPipelineState) {
	if ps == nil || ps.db == nil {
		return
	}
	ctx = withoutSQLTxContext(ctx)
	_, _ = dbExecContext(ctx, ps.db, `DELETE FROM scan_accumulators`)
	_, _ = dbExecContext(ctx, ps.db, `DELETE FROM pending_dedup_candidates`)
	_, _ = dbExecContext(ctx, ps.db, `DELETE FROM validation_pipelines`)

	for _, acc := range scans {
		if acc == nil || strings.TrimSpace(acc.CampaignID) == "" {
			continue
		}
		completedByMap := make(map[string]any, len(acc.CompletedBy))
		for key := range acc.CompletedBy {
			key = strings.TrimSpace(key)
			if key != "" {
				completedByMap[key] = true
			}
		}
		startedAt := acc.CreatedAt
		if startedAt.IsZero() {
			startedAt = time.Now()
		}
		timeoutAt := startedAt.Add(scanTimeout)
		pendingCount := 0
		for _, cand := range pending {
			if cand.ScanID == acc.ScanID {
				pendingCount++
			}
		}
		_, _ = dbExecContext(ctx, ps.db, `
			INSERT INTO scan_accumulators (
				scan_id, campaign_id, mode, geography, expected, complete,
				completed_by, reports, discovered, skipped, pending_dedup,
				timeout_at, started_at, completed_at
			)
			VALUES (
				$1, $2, $3, $4, $5,
				$6, $7::jsonb, $8, $9, $10, $11, $12, $13, NULL
			)
			ON CONFLICT (scan_id) DO UPDATE SET
				campaign_id = EXCLUDED.campaign_id,
				mode = EXCLUDED.mode,
				geography = EXCLUDED.geography,
				expected = EXCLUDED.expected,
				complete = EXCLUDED.complete,
				completed_by = EXCLUDED.completed_by,
				reports = EXCLUDED.reports,
				discovered = EXCLUDED.discovered,
				skipped = EXCLUDED.skipped,
				pending_dedup = EXCLUDED.pending_dedup,
				timeout_at = EXCLUDED.timeout_at,
				started_at = EXCLUDED.started_at
		`, acc.ScanID, acc.CampaignID, acc.Mode, acc.Geography, acc.Expected, len(acc.CompletedBy), string(mustJSON(completedByMap)), maxInt(acc.Reports, len(acc.CompletedBy)), acc.Discovered, acc.Skipped, pendingCount, timeoutAt, startedAt)
	}

	for _, cand := range pending {
		dedupEventID := strings.TrimSpace(cand.DedupEventID)
		if dedupEventID == "" {
			dedupEventID = stableUUID(cand.ScanID + ":" + cand.Name + ":" + cand.Geography).String()
		}
		candidateName := strings.TrimSpace(cand.Name)
		if candidateName == "" {
			candidateName = deriveDiscoveryCandidateName(cand.Payload)
		}
		_, _ = dbExecContext(ctx, ps.db, `
			INSERT INTO pending_dedup_candidates (
				dedup_event_id, scan_id, campaign_id, mode, name, geography, discovery_mode, signal_strength, payload, existing_id, status, created_at, resolved_at
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NULLIF($10,''), 'pending', now(), NULL
			)
			ON CONFLICT (dedup_event_id) DO UPDATE SET
				campaign_id = EXCLUDED.campaign_id,
				mode = EXCLUDED.mode,
				name = EXCLUDED.name,
				geography = EXCLUDED.geography,
				discovery_mode = EXCLUDED.discovery_mode,
				signal_strength = EXCLUDED.signal_strength,
				payload = EXCLUDED.payload,
				existing_id = EXCLUDED.existing_id
		`, dedupEventID, cand.ScanID, strings.TrimSpace(cand.CampaignID), strings.TrimSpace(cand.Mode), candidateName, cand.Geography, cand.Mode, cand.Signal, string(mustJSON(cand.Payload)), strings.TrimSpace(cand.ExistingID))
	}

	for _, st := range validations {
		if st == nil {
			continue
		}
		var packagingAt any
		if st.PackagingRequestedAt != nil {
			packagingAt = *st.PackagingRequestedAt
		}
		_, _ = dbExecContext(ctx, ps.db, `
			INSERT INTO validation_pipelines (
				vertical_id, status, g1_research, g2_spec, g3_cto, g4_brand,
				research_payload, spec_payload, cto_payload, brand_payload,
				scoring_payload,
				revision_count, inner_revision_count, spec_version,
				packaging_requested, packaging_requested_at, packaging_retries, updated_at
			)
			VALUES (
				$1::uuid, $2, $3, $4, $5, $6,
				$7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb,
				$12, $13, $14, $15, $16, $17, now()
			)
			ON CONFLICT (vertical_id) DO UPDATE SET
				status = EXCLUDED.status,
				g1_research = EXCLUDED.g1_research,
				g2_spec = EXCLUDED.g2_spec,
				g3_cto = EXCLUDED.g3_cto,
				g4_brand = EXCLUDED.g4_brand,
				research_payload = EXCLUDED.research_payload,
				spec_payload = EXCLUDED.spec_payload,
				cto_payload = EXCLUDED.cto_payload,
				brand_payload = EXCLUDED.brand_payload,
				scoring_payload = EXCLUDED.scoring_payload,
				revision_count = EXCLUDED.revision_count,
				inner_revision_count = EXCLUDED.inner_revision_count,
				spec_version = EXCLUDED.spec_version,
				packaging_requested = EXCLUDED.packaging_requested,
				packaging_requested_at = EXCLUDED.packaging_requested_at,
				packaging_retries = EXCLUDED.packaging_retries,
				updated_at = now()
		`,
			st.VerticalID, st.Status, st.G1Research, st.G2Spec, st.G3CTO, st.G4Brand,
			string(mustJSON(parsePayloadMap(st.ResearchPayload))),
			string(mustJSON(parsePayloadMap(st.SpecPayload))),
			string(mustJSON(parsePayloadMap(st.CTOPayload))),
			string(mustJSON(parsePayloadMap(st.BrandPayload))),
			string(mustJSON(parsePayloadMap(st.ScoringPayload))),
			st.RevisionCount, st.InnerRevisionCount, st.SpecVersion,
			st.PackagingRequested, packagingAt, st.PackagingRetries,
		)
	}
}

func (ps *PipelineStateStore) Clear(ctx context.Context, clearScoringDigest bool) {
	if ps == nil || ps.db == nil {
		return
	}
	ctx = withoutSQLTxContext(ctx)
	if clearScoringDigest {
		_, _ = dbExecContext(ctx, ps.db, `DELETE FROM scoring_digest_buffer`)
	}
	_, _ = dbExecContext(ctx, ps.db, `DELETE FROM scan_accumulators`)
	_, _ = dbExecContext(ctx, ps.db, `DELETE FROM pending_dedup_candidates`)
	_, _ = dbExecContext(ctx, ps.db, `DELETE FROM validation_pipelines`)
	_, _ = dbExecContext(ctx, ps.db, `DELETE FROM pipeline_processed_events`)
}

func detectStatePersistenceTables(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}
	var (
		scansOK       bool
		pendingOK     bool
		validationsOK bool
		processedOK   bool
	)
	_ = db.QueryRowContext(ctx, `SELECT to_regclass('public.scan_accumulators') IS NOT NULL`).Scan(&scansOK)
	_ = db.QueryRowContext(ctx, `SELECT to_regclass('public.pending_dedup_candidates') IS NOT NULL`).Scan(&pendingOK)
	_ = db.QueryRowContext(ctx, `SELECT to_regclass('public.validation_pipelines') IS NOT NULL`).Scan(&validationsOK)
	_ = db.QueryRowContext(ctx, `SELECT to_regclass('public.pipeline_processed_events') IS NOT NULL`).Scan(&processedOK)
	return scansOK && pendingOK && validationsOK && processedOK
}
