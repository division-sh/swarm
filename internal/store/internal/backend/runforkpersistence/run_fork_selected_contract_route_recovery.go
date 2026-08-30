package runforkpersistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func (s *RunForkPostgresOwner) requireRunForkSelectedContractRouteRecoveryAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkSQLiteOwner) requireRunForkSelectedContractRouteRecoveryAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkPostgresOwner) RecordRunForkSelectedContractRouteRecovery(ctx context.Context, req runfork.RunForkSelectedContractRouteRecoveryRequest) (runfork.RunForkSelectedContractRouteRecovery, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryAccess(); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	record, err := normalizeRunForkSelectedContractRouteRecovery(req, time.Now().UTC())
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("begin selected-contract route recovery: %w", err)
	}
	defer tx.Rollback()
	if err := requirePostgresRunActive(ctx, tx, record.SourceRunID); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("admit selected-contract route recovery source: %w", err)
	}
	if err := requirePostgresRunActive(ctx, tx, record.ForkRunID); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("admit selected-contract route recovery fork: %w", err)
	}
	if err := insertRunForkSelectedContractRouteRecovery(ctx, tx, record); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	if err := tx.Commit(); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("commit selected-contract route recovery: %w", err)
	}
	return record, nil
}

func (s *RunForkSQLiteOwner) RecordRunForkSelectedContractRouteRecovery(ctx context.Context, req runfork.RunForkSelectedContractRouteRecoveryRequest) (runfork.RunForkSelectedContractRouteRecovery, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("sqlite store is required")
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryAccess(); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	record, err := normalizeRunForkSelectedContractRouteRecovery(req, s.now())
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	err = s.backend.RunTransaction(ctx, "record selected-contract route recovery", func(ctx context.Context, tx *sql.Tx) error {
		if err := requireSQLiteRunActive(ctx, tx, record.SourceRunID); err != nil {
			return fmt.Errorf("admit selected-contract route recovery source: %w", err)
		}
		if err := requireSQLiteRunActive(ctx, tx, record.ForkRunID); err != nil {
			return fmt.Errorf("admit selected-contract route recovery fork: %w", err)
		}
		return insertRunForkSelectedContractRouteRecovery(ctx, tx, record)
	})
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	return record, nil
}

type runForkSelectedContractRouteRecoveryExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertRunForkSelectedContractRouteRecovery(ctx context.Context, execer runForkSelectedContractRouteRecoveryExecer, record runfork.RunForkSelectedContractRouteRecovery) error {
	if _, err := execer.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_route_recoveries (
			fork_run_id, source_run_id, fork_event_id,
			owner, runtime_recovery_owner,
			mode, bundle_hash,
			route_topology_owner, dynamic_topology_owner, recipient_planning_owner,
			frontier_evidence_fingerprint, route_topology_fingerprint, recipient_planning_fingerprint,
			static_route_event_count, dynamic_topology_proof_count, recipient_plan_event_count,
			route_topology, recipient_planning, created_at
		)
		VALUES (
			$1, $2, $3,
			$4, $5,
			$6, NULLIF($7, ''),
			$8, NULLIF($9, ''), $10,
			$11, $12, $13,
			$14, $15, $16,
			$17, $18, $19
		)
		ON CONFLICT (fork_run_id) DO UPDATE
		SET owner = EXCLUDED.owner,
		    runtime_recovery_owner = EXCLUDED.runtime_recovery_owner,
		    mode = EXCLUDED.mode,
		    bundle_hash = EXCLUDED.bundle_hash,
		    route_topology_owner = EXCLUDED.route_topology_owner,
		    dynamic_topology_owner = EXCLUDED.dynamic_topology_owner,
		    recipient_planning_owner = EXCLUDED.recipient_planning_owner,
		    frontier_evidence_fingerprint = EXCLUDED.frontier_evidence_fingerprint,
		    route_topology_fingerprint = EXCLUDED.route_topology_fingerprint,
		    recipient_planning_fingerprint = EXCLUDED.recipient_planning_fingerprint,
		    static_route_event_count = EXCLUDED.static_route_event_count,
		    dynamic_topology_proof_count = EXCLUDED.dynamic_topology_proof_count,
		    recipient_plan_event_count = EXCLUDED.recipient_plan_event_count,
		    route_topology = EXCLUDED.route_topology,
		    recipient_planning = EXCLUDED.recipient_planning,
		    created_at = EXCLUDED.created_at
	`, record.ForkRunID, record.SourceRunID, record.ForkEventID,
		record.Owner, record.RuntimeRecoveryOwner,
		record.ContractSelection.Mode, record.ContractSelection.BundleHash,
		record.RouteTopologyOwner, record.DynamicTopologyOwner, record.RecipientPlanningOwner,
		record.FrontierEvidenceFingerprint, record.RouteTopologyFingerprint, record.RecipientPlanningFingerprint,
		record.StaticRouteEventCount, record.DynamicTopologyProofCount, record.RecipientPlanEventCount,
		string(record.RouteTopology), string(record.RecipientPlanning), record.CreatedAt); err != nil {
		return fmt.Errorf("record selected-contract route recovery: %w", err)
	}
	return nil
}

func validateRunForkSelectedContractRouteRecoveryAtActivation(ctx context.Context, tx *sql.Tx, expected runfork.RunForkSelectedContractRouteRecovery) error {
	actual, err := loadRunForkSelectedContractRouteRecovery(ctx, tx, `WHERE fork_run_id = $1`, expected.ForkRunID)
	if err == sql.ErrNoRows {
		return runForkReplayResumeError(
			runfork.RunForkBlockerFlowRouteHistoryUnproven,
			runfork.RunForkReplayResumeFactRouteHistory,
			"selected-contract activation requires persisted route recovery from materialization",
		)
	}
	if err != nil {
		return fmt.Errorf("load selected-contract route recovery for activation: %w", err)
	}
	if actual.Owner != expected.Owner ||
		actual.RuntimeRecoveryOwner != expected.RuntimeRecoveryOwner ||
		actual.SourceRunID != expected.SourceRunID ||
		actual.ForkEventID != expected.ForkEventID ||
		actual.RouteTopologyOwner != expected.RouteTopologyOwner ||
		actual.DynamicTopologyOwner != expected.DynamicTopologyOwner ||
		actual.RecipientPlanningOwner != expected.RecipientPlanningOwner ||
		actual.FrontierEvidenceFingerprint != expected.FrontierEvidenceFingerprint ||
		actual.RouteTopologyFingerprint != expected.RouteTopologyFingerprint ||
		actual.RecipientPlanningFingerprint != expected.RecipientPlanningFingerprint ||
		actual.StaticRouteEventCount != expected.StaticRouteEventCount ||
		actual.DynamicTopologyProofCount != expected.DynamicTopologyProofCount ||
		actual.RecipientPlanEventCount != expected.RecipientPlanEventCount {
		return runForkReplayResumeError(
			runfork.RunForkBlockerFlowRouteHistoryUnproven,
			runfork.RunForkReplayResumeFactRouteHistory,
			"selected-contract activation route recovery does not match current canonical topology proof",
		)
	}
	if err := validateRunForkSelectedContractRouteRecoverySelection("route recovery activation", actual.ContractSelection, expected.ContractSelection); err != nil {
		return err
	}
	return nil
}

func (s *RunForkPostgresOwner) LoadRunForkSelectedContractRouteRecovery(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractRouteRecovery, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("postgres store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryAccess(); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, err
	}
	record, err := loadRunForkSelectedContractRouteRecovery(ctx, s.backend, `WHERE fork_run_id = $1`, forkRunID)
	if err == sql.ErrNoRows {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, nil
	}
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, err
	}
	return record, true, nil
}

func (s *RunForkSQLiteOwner) LoadRunForkSelectedContractRouteRecovery(ctx context.Context, forkRunID string) (runfork.RunForkSelectedContractRouteRecovery, bool, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("sqlite store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryAccess(); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, err
	}
	record, err := loadRunForkSelectedContractRouteRecovery(ctx, s.backend, `WHERE fork_run_id = $1`, forkRunID)
	if err == sql.ErrNoRows {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, nil
	}
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, err
	}
	return record, true, nil
}

func (s *RunForkPostgresOwner) ListRunForkSelectedContractRouteRecoveries(ctx context.Context) ([]runfork.RunForkSelectedContractRouteRecovery, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryAccess(); err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, runForkSelectedContractRouteRecoverySelect()+`
		ORDER BY created_at ASC, fork_run_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list selected-contract route recoveries: %w", err)
	}
	defer rows.Close()
	out := []runfork.RunForkSelectedContractRouteRecovery{}
	for rows.Next() {
		record, err := scanRunForkSelectedContractRouteRecovery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read selected-contract route recoveries: %w", err)
	}
	return out, nil
}

func (s *RunForkSQLiteOwner) ListRunForkSelectedContractRouteRecoveries(ctx context.Context) ([]runfork.RunForkSelectedContractRouteRecovery, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite store is required")
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryAccess(); err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, runForkSelectedContractRouteRecoverySelect()+`
		ORDER BY created_at ASC, fork_run_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list selected-contract route recoveries: %w", err)
	}
	defer rows.Close()
	out := []runfork.RunForkSelectedContractRouteRecovery{}
	for rows.Next() {
		record, err := scanRunForkSelectedContractRouteRecovery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read selected-contract route recoveries: %w", err)
	}
	return out, nil
}

func (s *RunForkPostgresOwner) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]runtimemanager.SelectedContractRouteRecoveryRecord, error) {
	records, err := s.ListRunForkSelectedContractRouteRecoveries(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]runtimemanager.SelectedContractRouteRecoveryRecord, 0, len(records))
	for _, record := range records {
		out = append(out, runtimemanager.SelectedContractRouteRecoveryRecord{
			Owner:                        record.Owner,
			RuntimeRecoveryOwner:         record.RuntimeRecoveryOwner,
			ForkRunID:                    record.ForkRunID,
			SourceRunID:                  record.SourceRunID,
			ForkEventID:                  record.ForkEventID,
			RouteTopologyOwner:           record.RouteTopologyOwner,
			DynamicTopologyOwner:         record.DynamicTopologyOwner,
			RecipientPlanningOwner:       record.RecipientPlanningOwner,
			FrontierEvidenceFingerprint:  record.FrontierEvidenceFingerprint,
			RouteTopologyFingerprint:     record.RouteTopologyFingerprint,
			RecipientPlanningFingerprint: record.RecipientPlanningFingerprint,
			StaticRouteEventCount:        record.StaticRouteEventCount,
			DynamicTopologyProofCount:    record.DynamicTopologyProofCount,
			RecipientPlanEventCount:      record.RecipientPlanEventCount,
			RouteTopology:                append([]byte(nil), record.RouteTopology...),
			RecipientPlanning:            append([]byte(nil), record.RecipientPlanning...),
			CreatedAt:                    record.CreatedAt,
		})
	}
	return out, nil
}

func (s *RunForkSQLiteOwner) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]runtimemanager.SelectedContractRouteRecoveryRecord, error) {
	records, err := s.ListRunForkSelectedContractRouteRecoveries(ctx)
	if err != nil {
		return nil, err
	}
	return projectSelectedContractRouteRecoveryRecords(records), nil
}

func projectSelectedContractRouteRecoveryRecords(records []runfork.RunForkSelectedContractRouteRecovery) []runtimemanager.SelectedContractRouteRecoveryRecord {
	out := make([]runtimemanager.SelectedContractRouteRecoveryRecord, 0, len(records))
	for _, record := range records {
		out = append(out, runtimemanager.SelectedContractRouteRecoveryRecord{
			Owner: record.Owner, RuntimeRecoveryOwner: record.RuntimeRecoveryOwner,
			ForkRunID: record.ForkRunID, SourceRunID: record.SourceRunID, ForkEventID: record.ForkEventID,
			RouteTopologyOwner: record.RouteTopologyOwner, DynamicTopologyOwner: record.DynamicTopologyOwner,
			RecipientPlanningOwner:       record.RecipientPlanningOwner,
			FrontierEvidenceFingerprint:  record.FrontierEvidenceFingerprint,
			RouteTopologyFingerprint:     record.RouteTopologyFingerprint,
			RecipientPlanningFingerprint: record.RecipientPlanningFingerprint,
			StaticRouteEventCount:        record.StaticRouteEventCount,
			DynamicTopologyProofCount:    record.DynamicTopologyProofCount,
			RecipientPlanEventCount:      record.RecipientPlanEventCount,
			RouteTopology:                append([]byte(nil), record.RouteTopology...),
			RecipientPlanning:            append([]byte(nil), record.RecipientPlanning...), CreatedAt: record.CreatedAt,
		})
	}
	return out
}

func normalizeRunForkSelectedContractRouteRecovery(req runfork.RunForkSelectedContractRouteRecoveryRequest, createdAt time.Time) (runfork.RunForkSelectedContractRouteRecovery, error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires fork run_id")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery fork run_id must be a UUID: %w", err)
	}
	sourceRunID := strings.TrimSpace(req.SourceRunID)
	if sourceRunID == "" {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires source run_id")
	}
	if _, err := uuid.Parse(sourceRunID); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery source run_id must be a UUID: %w", err)
	}
	forkEventID := strings.TrimSpace(req.ForkEventID)
	if forkEventID == "" {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires fork event_id")
	}
	if _, err := uuid.Parse(forkEventID); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery fork event_id must be a UUID: %w", err)
	}
	selection, err := normalizeRunForkSelectedContractSelection(req.ContractSelection)
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	topology := req.RouteTopology
	if strings.TrimSpace(topology.Owner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires %s topology; got %q", runfork.RunForkSelectedContractRouteTopologyOwner, topology.Owner)
	}
	if !topology.NonMutating || topology.RoutePersistenceSupported || topology.ExecutableRecipientsSupported {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires non-mutating topology evidence without executable route persistence")
	}
	if strings.TrimSpace(topology.FrontierEvidenceFingerprint) == "" {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires topology frontier evidence fingerprint")
	}
	if err := validateRunForkSelectedContractRouteRecoverySelection("route recovery", selection, topology.ContractSelection); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	planning := req.RecipientPlanning
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires %s recipient planning; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	if !planning.NonMutating || planning.DeliveryWritesSupported {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires non-mutating recipient planning evidence")
	}
	if !planning.RecipientPlanningSupported {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires supported recipient planning")
	}
	if strings.TrimSpace(planning.RouteTopologyOwner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery recipient planning must consume %s; got %q", runfork.RunForkSelectedContractRouteTopologyOwner, planning.RouteTopologyOwner)
	}
	if strings.TrimSpace(planning.FrontierEvidenceFingerprint) != strings.TrimSpace(topology.FrontierEvidenceFingerprint) {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery topology and recipient planning frontier fingerprints differ")
	}
	if err := validateRunForkSelectedContractRouteRecoverySelection("route recovery recipient planning", selection, planning.ContractSelection); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	topologyJSON, topologyFingerprint, err := runForkSelectedContractRecoveryJSONFingerprint(topology)
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("fingerprint route topology: %w", err)
	}
	planningJSON, planningFingerprint, err := runForkSelectedContractRecoveryJSONFingerprint(planning)
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("fingerprint recipient planning: %w", err)
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	return runfork.RunForkSelectedContractRouteRecovery{
		Owner:                        runfork.RunForkSelectedContractRoutePersistenceOwner,
		RuntimeRecoveryOwner:         runfork.RunForkSelectedContractRouteRecoveryOwner,
		ForkRunID:                    forkRunID,
		SourceRunID:                  sourceRunID,
		ForkEventID:                  forkEventID,
		ContractSelection:            selection,
		RouteTopologyOwner:           topology.Owner,
		DynamicTopologyOwner:         topology.DynamicTopologyOwner,
		RecipientPlanningOwner:       planning.Owner,
		FrontierEvidenceFingerprint:  topology.FrontierEvidenceFingerprint,
		RouteTopologyFingerprint:     topologyFingerprint,
		RecipientPlanningFingerprint: planningFingerprint,
		StaticRouteEventCount:        len(topology.StaticRouteEvents),
		DynamicTopologyProofCount:    len(topology.DynamicTopologyProofs),
		RecipientPlanEventCount:      len(planning.RecipientPlanEvents),
		RouteTopology:                topologyJSON,
		RecipientPlanning:            planningJSON,
		CreatedAt:                    createdAt.UTC(),
	}, nil
}

func NormalizeRunForkSelectedContractRouteRecovery(req runfork.RunForkSelectedContractRouteRecoveryRequest, createdAt time.Time) (runfork.RunForkSelectedContractRouteRecovery, error) {
	return normalizeRunForkSelectedContractRouteRecovery(req, createdAt)
}

func validateRunForkSelectedContractRouteRecoverySelection(context string, left, right runfork.RunForkContractSelection) error {
	left, err := normalizeRunForkSelectedContractSelection(left)
	if err != nil {
		return err
	}
	right, err = normalizeRunForkSelectedContractSelection(right)
	if err != nil {
		return err
	}
	if left.Mode != right.Mode || left.BundleHash != right.BundleHash {
		return fmt.Errorf("%s selected contract selection mismatch", strings.TrimSpace(context))
	}
	return nil
}

func runForkSelectedContractRecoveryJSONFingerprint(value any) (json.RawMessage, string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	fingerprint, err := runForkSelectedContractRecoveryCanonicalJSONFingerprint(payload)
	if err != nil {
		return nil, "", err
	}
	return append(json.RawMessage(nil), payload...), fingerprint, nil
}

func runForkSelectedContractRecoveryCanonicalJSONFingerprint(raw json.RawMessage) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", fmt.Errorf("unexpected trailing JSON")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func runForkSelectedContractRouteRecoverySelect() string {
	return `
		SELECT
			owner,
			runtime_recovery_owner,
			fork_run_id,
			source_run_id,
			fork_event_id,
			mode,
			COALESCE(bundle_hash, ''),
			route_topology_owner,
			COALESCE(dynamic_topology_owner, ''),
			recipient_planning_owner,
			frontier_evidence_fingerprint,
			route_topology_fingerprint,
			recipient_planning_fingerprint,
			static_route_event_count,
			dynamic_topology_proof_count,
			recipient_plan_event_count,
			route_topology,
			recipient_planning,
			created_at
		FROM run_fork_selected_contract_route_recoveries
	`
}

func loadRunForkSelectedContractRouteRecovery(ctx context.Context, querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, where string, args ...any) (runfork.RunForkSelectedContractRouteRecovery, error) {
	row := querier.QueryRowContext(ctx, runForkSelectedContractRouteRecoverySelect()+" "+where, args...)
	return scanRunForkSelectedContractRouteRecovery(row)
}

type runForkSelectedContractRouteRecoveryScanner interface {
	Scan(dest ...any) error
}

func scanRunForkSelectedContractRouteRecovery(row runForkSelectedContractRouteRecoveryScanner) (runfork.RunForkSelectedContractRouteRecovery, error) {
	var record runfork.RunForkSelectedContractRouteRecovery
	var selection runfork.RunForkContractSelection
	var routeTopology, recipientPlanning []byte
	var createdAt any
	err := row.Scan(
		&record.Owner,
		&record.RuntimeRecoveryOwner,
		&record.ForkRunID,
		&record.SourceRunID,
		&record.ForkEventID,
		&selection.Mode,
		&selection.BundleHash,
		&record.RouteTopologyOwner,
		&record.DynamicTopologyOwner,
		&record.RecipientPlanningOwner,
		&record.FrontierEvidenceFingerprint,
		&record.RouteTopologyFingerprint,
		&record.RecipientPlanningFingerprint,
		&record.StaticRouteEventCount,
		&record.DynamicTopologyProofCount,
		&record.RecipientPlanEventCount,
		&routeTopology,
		&recipientPlanning,
		&createdAt,
	)
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, err
	}
	record.ContractSelection = selection
	record.RouteTopology = append(json.RawMessage(nil), routeTopology...)
	record.RecipientPlanning = append(json.RawMessage(nil), recipientPlanning...)
	parsedCreatedAt, ok, err := sqliteTimeValue(createdAt)
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("decode selected-contract route recovery created_at: %w", err)
	}
	if !ok {
		return runfork.RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery created_at is required")
	}
	record.CreatedAt = parsedCreatedAt
	return record, nil
}
