package store

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
	"github.com/google/uuid"
)

const (
	RunForkSelectedContractRoutePersistenceOwner = "store.run_fork.selected_contract_route_persistence"
	RunForkSelectedContractRouteRecoveryOwner    = "runtime.run_fork.selected_contract_route_recovery"

	runForkSelectedContractRouteRecoveryTable = "run_fork_selected_contract_route_recoveries"
)

type RunForkSelectedContractRouteRecoveryRequest struct {
	ForkRunID         string
	SourceRunID       string
	ForkEventID       string
	ContractSelection RunForkContractSelection
	RouteTopology     RunForkSelectedContractRouteTopology
	RecipientPlanning RunForkSelectedContractRecipientPlanning
}

type RunForkSelectedContractRouteRecovery struct {
	Owner                        string                   `json:"owner"`
	RuntimeRecoveryOwner         string                   `json:"runtime_recovery_owner"`
	ForkRunID                    string                   `json:"fork_run_id"`
	SourceRunID                  string                   `json:"source_run_id"`
	ForkEventID                  string                   `json:"fork_event_id"`
	ContractSelection            RunForkContractSelection `json:"contract_selection"`
	RouteTopologyOwner           string                   `json:"route_topology_owner"`
	DynamicTopologyOwner         string                   `json:"dynamic_topology_owner,omitempty"`
	RecipientPlanningOwner       string                   `json:"recipient_planning_owner"`
	FrontierEvidenceFingerprint  string                   `json:"frontier_evidence_fingerprint"`
	RouteTopologyFingerprint     string                   `json:"route_topology_fingerprint"`
	RecipientPlanningFingerprint string                   `json:"recipient_planning_fingerprint"`
	StaticRouteEventCount        int                      `json:"static_route_event_count"`
	DynamicTopologyProofCount    int                      `json:"dynamic_topology_proof_count"`
	RecipientPlanEventCount      int                      `json:"recipient_plan_event_count"`
	RouteTopology                json.RawMessage          `json:"route_topology"`
	RecipientPlanning            json.RawMessage          `json:"recipient_planning"`
	CreatedAt                    time.Time                `json:"created_at"`
}

func RequireRunForkSelectedContractRouteRecoveryCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	_ = caps
	required := []string{
		"recovery_id",
		"fork_run_id",
		"source_run_id",
		"fork_event_id",
		"owner",
		"runtime_recovery_owner",
		"mode",
		"contracts_root",
		"bundle_hash",
		"workflow_name",
		"workflow_version",
		"route_topology_owner",
		"dynamic_topology_owner",
		"recipient_planning_owner",
		"frontier_evidence_fingerprint",
		"route_topology_fingerprint",
		"recipient_planning_fingerprint",
		"static_route_event_count",
		"dynamic_topology_proof_count",
		"recipient_plan_event_count",
		"route_topology",
		"recipient_planning",
		"created_at",
	}
	if catalog.hasColumns(runForkSelectedContractRouteRecoveryTable, required...) {
		return nil
	}
	return fmt.Errorf("selected-contract route recovery requires %s columns %v", runForkSelectedContractRouteRecoveryTable, required)
}

func (s *PostgresStore) requireRunForkSelectedContractRouteRecoveryCapabilities(ctx context.Context) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	return RequireRunForkSelectedContractRouteRecoveryCapabilities(caps, catalog)
}

func (s *PostgresStore) RecordRunForkSelectedContractRouteRecovery(ctx context.Context, req RunForkSelectedContractRouteRecoveryRequest) (RunForkSelectedContractRouteRecovery, error) {
	if s == nil || s.DB == nil {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryCapabilities(ctx); err != nil {
		return RunForkSelectedContractRouteRecovery{}, err
	}
	record, err := normalizeRunForkSelectedContractRouteRecovery(req, time.Now().UTC())
	if err != nil {
		return RunForkSelectedContractRouteRecovery{}, err
	}
	if _, err := s.DB.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_route_recoveries (
			fork_run_id, source_run_id, fork_event_id,
			owner, runtime_recovery_owner,
			mode, contracts_root, bundle_hash, workflow_name, workflow_version,
			route_topology_owner, dynamic_topology_owner, recipient_planning_owner,
			frontier_evidence_fingerprint, route_topology_fingerprint, recipient_planning_fingerprint,
			static_route_event_count, dynamic_topology_proof_count, recipient_plan_event_count,
			route_topology, recipient_planning, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid,
			$4, $5,
			$6, NULLIF($7, ''), NULLIF($8, ''), $9, $10,
			$11, NULLIF($12, ''), $13,
			$14, $15, $16,
			$17, $18, $19,
			$20::jsonb, $21::jsonb, $22
		)
		ON CONFLICT (fork_run_id) DO UPDATE
		SET owner = EXCLUDED.owner,
		    runtime_recovery_owner = EXCLUDED.runtime_recovery_owner,
		    mode = EXCLUDED.mode,
		    contracts_root = EXCLUDED.contracts_root,
		    bundle_hash = EXCLUDED.bundle_hash,
		    workflow_name = EXCLUDED.workflow_name,
		    workflow_version = EXCLUDED.workflow_version,
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
		record.ContractSelection.Mode, record.ContractSelection.ContractsRoot, record.ContractSelection.BundleHash, record.ContractSelection.WorkflowName, record.ContractSelection.WorkflowVersion,
		record.RouteTopologyOwner, record.DynamicTopologyOwner, record.RecipientPlanningOwner,
		record.FrontierEvidenceFingerprint, record.RouteTopologyFingerprint, record.RecipientPlanningFingerprint,
		record.StaticRouteEventCount, record.DynamicTopologyProofCount, record.RecipientPlanEventCount,
		string(record.RouteTopology), string(record.RecipientPlanning), record.CreatedAt); err != nil {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("record selected-contract route recovery: %w", err)
	}
	return record, nil
}

func (s *PostgresStore) LoadRunForkSelectedContractRouteRecovery(ctx context.Context, forkRunID string) (RunForkSelectedContractRouteRecovery, bool, error) {
	if s == nil || s.DB == nil {
		return RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("postgres store is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("fork run_id is required")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("fork run_id must be a UUID: %w", err)
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryCapabilities(ctx); err != nil {
		return RunForkSelectedContractRouteRecovery{}, false, err
	}
	record, err := loadRunForkSelectedContractRouteRecovery(ctx, s.DB, `WHERE fork_run_id = $1::uuid`, forkRunID)
	if err == sql.ErrNoRows {
		return RunForkSelectedContractRouteRecovery{}, false, nil
	}
	if err != nil {
		return RunForkSelectedContractRouteRecovery{}, false, err
	}
	return record, true, nil
}

func (s *PostgresStore) ListRunForkSelectedContractRouteRecoveries(ctx context.Context) ([]RunForkSelectedContractRouteRecovery, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunForkSelectedContractRouteRecoveryCapabilities(ctx); err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, runForkSelectedContractRouteRecoverySelect()+`
		ORDER BY created_at ASC, fork_run_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list selected-contract route recoveries: %w", err)
	}
	defer rows.Close()
	out := []RunForkSelectedContractRouteRecovery{}
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

func (s *PostgresStore) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]runtimemanager.SelectedContractRouteRecoveryRecord, error) {
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

func normalizeRunForkSelectedContractRouteRecovery(req RunForkSelectedContractRouteRecoveryRequest, createdAt time.Time) (RunForkSelectedContractRouteRecovery, error) {
	forkRunID := strings.TrimSpace(req.ForkRunID)
	if forkRunID == "" {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires fork run_id")
	}
	if _, err := uuid.Parse(forkRunID); err != nil {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery fork run_id must be a UUID: %w", err)
	}
	sourceRunID := strings.TrimSpace(req.SourceRunID)
	if sourceRunID == "" {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires source run_id")
	}
	if _, err := uuid.Parse(sourceRunID); err != nil {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery source run_id must be a UUID: %w", err)
	}
	forkEventID := strings.TrimSpace(req.ForkEventID)
	if forkEventID == "" {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires fork event_id")
	}
	if _, err := uuid.Parse(forkEventID); err != nil {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery fork event_id must be a UUID: %w", err)
	}
	selection, err := normalizeRunForkSelectedContractSelection(req.ContractSelection)
	if err != nil {
		return RunForkSelectedContractRouteRecovery{}, err
	}
	topology := req.RouteTopology
	if strings.TrimSpace(topology.Owner) != RunForkSelectedContractRouteTopologyOwner {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires %s topology; got %q", RunForkSelectedContractRouteTopologyOwner, topology.Owner)
	}
	if !topology.NonMutating || topology.RoutePersistenceSupported || topology.ExecutableRecipientsSupported {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires non-mutating topology evidence without executable route persistence")
	}
	if strings.TrimSpace(topology.FrontierEvidenceFingerprint) == "" {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires topology frontier evidence fingerprint")
	}
	if err := validateRunForkSelectedContractRouteRecoverySelection("route recovery", selection, topology.ContractSelection); err != nil {
		return RunForkSelectedContractRouteRecovery{}, err
	}
	planning := req.RecipientPlanning
	if strings.TrimSpace(planning.Owner) != RunForkSelectedContractRecipientPlanningOwner {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires %s recipient planning; got %q", RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	if !planning.NonMutating || planning.DeliveryWritesSupported {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires non-mutating recipient planning evidence")
	}
	if !planning.RecipientPlanningSupported {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery requires supported recipient planning")
	}
	if strings.TrimSpace(planning.RouteTopologyOwner) != RunForkSelectedContractRouteTopologyOwner {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery recipient planning must consume %s; got %q", RunForkSelectedContractRouteTopologyOwner, planning.RouteTopologyOwner)
	}
	if strings.TrimSpace(planning.FrontierEvidenceFingerprint) != strings.TrimSpace(topology.FrontierEvidenceFingerprint) {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("selected-contract route recovery topology and recipient planning frontier fingerprints differ")
	}
	if err := validateRunForkSelectedContractRouteRecoverySelection("route recovery recipient planning", selection, planning.ContractSelection); err != nil {
		return RunForkSelectedContractRouteRecovery{}, err
	}
	topologyJSON, topologyFingerprint, err := runForkSelectedContractRecoveryJSONFingerprint(topology)
	if err != nil {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("fingerprint route topology: %w", err)
	}
	planningJSON, planningFingerprint, err := runForkSelectedContractRecoveryJSONFingerprint(planning)
	if err != nil {
		return RunForkSelectedContractRouteRecovery{}, fmt.Errorf("fingerprint recipient planning: %w", err)
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	return RunForkSelectedContractRouteRecovery{
		Owner:                        RunForkSelectedContractRoutePersistenceOwner,
		RuntimeRecoveryOwner:         RunForkSelectedContractRouteRecoveryOwner,
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

func validateRunForkSelectedContractRouteRecoverySelection(context string, left, right RunForkContractSelection) error {
	left, err := normalizeRunForkSelectedContractSelection(left)
	if err != nil {
		return err
	}
	right, err = normalizeRunForkSelectedContractSelection(right)
	if err != nil {
		return err
	}
	if left.Mode != right.Mode ||
		left.ContractsRoot != right.ContractsRoot ||
		left.BundleHash != right.BundleHash ||
		left.WorkflowName != right.WorkflowName ||
		left.WorkflowVersion != right.WorkflowVersion {
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
			fork_run_id::text,
			source_run_id::text,
			fork_event_id::text,
			mode,
			COALESCE(contracts_root, ''),
			COALESCE(bundle_hash, ''),
			workflow_name,
			workflow_version,
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
}, where string, args ...any) (RunForkSelectedContractRouteRecovery, error) {
	row := querier.QueryRowContext(ctx, runForkSelectedContractRouteRecoverySelect()+" "+where, args...)
	return scanRunForkSelectedContractRouteRecovery(row)
}

type runForkSelectedContractRouteRecoveryScanner interface {
	Scan(dest ...any) error
}

func scanRunForkSelectedContractRouteRecovery(row runForkSelectedContractRouteRecoveryScanner) (RunForkSelectedContractRouteRecovery, error) {
	var record RunForkSelectedContractRouteRecovery
	var selection RunForkContractSelection
	var routeTopology, recipientPlanning []byte
	err := row.Scan(
		&record.Owner,
		&record.RuntimeRecoveryOwner,
		&record.ForkRunID,
		&record.SourceRunID,
		&record.ForkEventID,
		&selection.Mode,
		&selection.ContractsRoot,
		&selection.BundleHash,
		&selection.WorkflowName,
		&selection.WorkflowVersion,
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
		&record.CreatedAt,
	)
	if err != nil {
		return RunForkSelectedContractRouteRecovery{}, err
	}
	record.ContractSelection = selection
	record.RouteTopology = append(json.RawMessage(nil), routeTopology...)
	record.RecipientPlanning = append(json.RawMessage(nil), recipientPlanning...)
	return record, nil
}
