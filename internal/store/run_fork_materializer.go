package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

const (
	RunForkMaterializedStatus = "paused"
)

type RunForkMaterializeRequest struct {
	SourceRunID       string
	At                string
	ContractSelection *RunForkContractSelection
	BundleHash        string
	BundleSource      string
}

type RunForkMaterialization struct {
	SourceRunID              string                          `json:"source_run_id"`
	ForkRunID                string                          `json:"fork_run_id"`
	ForkRunStatus            string                          `json:"fork_run_status"`
	ForkPoint                RunForkPoint                    `json:"fork_point"`
	MaterializedEntityCount  int                             `json:"materialized_entity_count"`
	ExecutionReady           bool                            `json:"execution_ready"`
	ReplayResumeAdmission    RunForkReplayResumeAdmission    `json:"replay_resume_admission"`
	SelectedContractBinding  *RunForkSelectedContractBinding `json:"selected_contract_binding,omitempty"`
	UnsupportedBlockers      []RunForkUnsupportedBlocker     `json:"unsupported_blockers,omitempty"`
	DeliveryResumeBlocked    bool                            `json:"delivery_resume_blocked"`
	SourceRunStatusUnchanged bool                            `json:"source_run_status_unchanged"`
}

type runForkEntityMetadata struct {
	FlowInstance string
	EntityType   string
	Slug         string
	Name         string
}

func RequireRunForkMaterializerCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	if err := RequireCanonicalRunForkPlannerCapabilities(caps, catalog); err != nil {
		return err
	}
	required := map[string][]string{
		"runs":         {"run_id", "status", "forked_from_run_id", "forked_from_event_id", "entity_count", "event_count", "started_at"},
		"entity_state": {"run_id", "entity_id", "flow_instance", "entity_type", "slug", "name", "current_state", "gates", "fields", "accumulator", "revision", "entered_state_at", "created_at", "updated_at"},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("run fork materializer requires %s columns %v", tableName, columns)
	}
	return nil
}

func (s *PostgresStore) requireRunForkMaterializerCapabilities(ctx context.Context) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	return RequireRunForkMaterializerCapabilities(caps, catalog)
}

func (s *PostgresStore) MaterializeRunFork(ctx context.Context, req RunForkMaterializeRequest) (RunForkMaterialization, error) {
	if s == nil || s.DB == nil {
		return RunForkMaterialization{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunForkMaterializerCapabilities(ctx); err != nil {
		return RunForkMaterialization{}, err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	var selection *RunForkContractSelection
	if req.ContractSelection != nil {
		normalized, err := normalizeRunForkSelectedContractSelection(*req.ContractSelection)
		if err != nil {
			return RunForkMaterialization{}, err
		}
		selection = &normalized
		if err := s.requireRunForkSelectedContractBindingCapabilities(ctx); err != nil {
			return RunForkMaterialization{}, err
		}
	}
	plan, err := s.PlanRunFork(ctx, RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(req.SourceRunID),
		At:          strings.TrimSpace(req.At),
	})
	if err != nil {
		return RunForkMaterialization{}, err
	}
	if !plan.ExecutionReady {
		return RunForkMaterialization{
			SourceRunID:           plan.SourceRunID,
			ForkPoint:             plan.ForkPoint,
			ExecutionReady:        false,
			ReplayResumeAdmission: plan.ReplayResumeAdmission,
			UnsupportedBlockers:   plan.UnsupportedBlockers,
			DeliveryResumeBlocked: true,
		}, fmt.Errorf("fork materialization requires execution-ready plan; blockers: %s", runForkBlockerCodes(plan.UnsupportedBlockers))
	}

	forkRunID := deterministicRunForkMaterializationID(plan.SourceRunID, plan.ForkPoint.EventID)
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return RunForkMaterialization{}, fmt.Errorf("begin fork materialization: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := ensureRunForkNotAlreadyMaterialized(ctx, tx, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID); err != nil {
		return RunForkMaterialization{}, err
	}
	metadata, err := loadRunForkEntityMetadata(plan)
	if err != nil {
		return RunForkMaterialization{}, err
	}
	now := time.Now().UTC()
	if err := insertRunForkRun(ctx, tx, catalog, forkRunID, plan.SourceRunID, plan.ForkPoint.EventID, len(plan.Entities), now, runForkBundleInsertIdentity{
		BundleHash:   req.BundleHash,
		BundleSource: req.BundleSource,
	}); err != nil {
		return RunForkMaterialization{}, fmt.Errorf("insert fork run: %w", err)
	}

	forkCtx := runtimecorrelation.WithRunID(ctx, forkRunID)
	for _, entity := range plan.Entities {
		if err := materializeRunForkEntityState(forkCtx, tx, forkRunID, plan, entity, metadata[entity.EntityID], now); err != nil {
			return RunForkMaterialization{}, err
		}
	}
	var selectedContractBinding *RunForkSelectedContractBinding
	if selection != nil {
		binding, err := insertRunForkSelectedContractBinding(ctx, tx, RunForkSelectedContractBindingRequest{
			ForkRunID:         forkRunID,
			SourceRunID:       plan.SourceRunID,
			ForkEventID:       plan.ForkPoint.EventID,
			ContractSelection: *selection,
		}, now)
		if err != nil {
			return RunForkMaterialization{}, err
		}
		selectedContractBinding = &binding
	}
	if err := tx.Commit(); err != nil {
		return RunForkMaterialization{}, fmt.Errorf("commit fork materialization: %w", err)
	}
	committed = true
	return RunForkMaterialization{
		SourceRunID:              plan.SourceRunID,
		ForkRunID:                forkRunID,
		ForkRunStatus:            RunForkMaterializedStatus,
		ForkPoint:                plan.ForkPoint,
		MaterializedEntityCount:  len(plan.Entities),
		ExecutionReady:           true,
		ReplayResumeAdmission:    plan.ReplayResumeAdmission,
		SelectedContractBinding:  selectedContractBinding,
		DeliveryResumeBlocked:    true,
		SourceRunStatusUnchanged: true,
	}, nil
}

func ensureRunForkNotAlreadyMaterialized(ctx context.Context, tx *sql.Tx, forkRunID, sourceRunID, forkEventID string) error {
	var existing string
	err := tx.QueryRowContext(ctx, `
		SELECT run_id::text
		FROM runs
		WHERE run_id = $1::uuid
		   OR (forked_from_run_id = $2::uuid AND forked_from_event_id = $3::uuid)
		ORDER BY started_at ASC
		LIMIT 1
	`, forkRunID, sourceRunID, forkEventID).Scan(&existing)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check existing fork materialization: %w", err)
	}
	return fmt.Errorf("fork materialization already exists for source run %s at event %s: %s", sourceRunID, forkEventID, existing)
}

type runForkBundleInsertIdentity struct {
	BundleHash   string
	BundleSource string
}

func insertRunForkRun(ctx context.Context, tx *sql.Tx, catalog schemaColumnCatalog, forkRunID, sourceRunID, forkEventID string, entityCount int, startedAt time.Time, identity runForkBundleInsertIdentity) error {
	bundleHash := strings.TrimSpace(identity.BundleHash)
	bundleSource, err := storerunlifecycle.CanonicalBundleSource(identity.BundleSource)
	if err != nil {
		return err
	}
	if bundleSource != storerunlifecycle.BundleSourceLegacy && bundleHash == "" {
		return fmt.Errorf("fork run canonical bundle identity requires bundle_hash for bundle_source=%s", bundleSource)
	}
	if bundleSource == storerunlifecycle.BundleSourceLegacy && bundleHash != "" {
		return fmt.Errorf("fork run legacy bundle identity cannot carry canonical bundle_hash")
	}
	opts := storerunlifecycle.InsertForkOptions{
		HasBundleHashCol:        catalog.hasColumns("runs", "bundle_hash"),
		HasBundleSourceCol:      catalog.hasColumns("runs", "bundle_source"),
		HasBundleFingerprintCol: catalog.hasColumns("runs", "bundle_fingerprint"),
		BundleHash:              bundleHash,
		BundleSource:            bundleSource,
	}
	if bundleSource == storerunlifecycle.BundleSourceLegacy && catalog.hasColumns("runs", "bundle_fingerprint") {
		fingerprint, err := runForkBundleFingerprintForInsert(ctx, tx, sourceRunID)
		if err != nil {
			return err
		}
		opts.BundleFingerprint = fingerprint
	}
	return storerunlifecycle.InsertFork(ctx, tx, forkRunID, RunForkMaterializedStatus, sourceRunID, forkEventID, entityCount, startedAt, opts)
}

func runForkBundleFingerprintForInsert(ctx context.Context, tx *sql.Tx, sourceRunID string) (string, error) {
	if fingerprint := runtimecorrelation.BundleFingerprintFromContext(ctx); fingerprint != "" {
		return fingerprint, nil
	}
	var source sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT bundle_fingerprint
		FROM runs
		WHERE run_id = $1::uuid
	`, sourceRunID).Scan(&source); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("source run %s not found", sourceRunID)
		}
		return "", fmt.Errorf("load source run bundle fingerprint: %w", err)
	}
	if !source.Valid {
		return "", nil
	}
	return strings.TrimSpace(source.String), nil
}

func loadRunForkEntityMetadata(plan RunForkPlan) (map[string]runForkEntityMetadata, error) {
	out := make(map[string]runForkEntityMetadata, len(plan.Entities))
	for _, entity := range plan.Entities {
		entityID := strings.TrimSpace(entity.EntityID)
		if entityID == "" {
			return nil, fmt.Errorf("fork entity_id is required")
		}
		if entity.MaterializationMetadata == nil {
			return nil, runForkReplayResumeError(RunForkBlockerEntitySnapshotMetadataUnproven, RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("fork materialization cannot prove source-at-T flow_instance/entity_type metadata for entity %s", entityID))
		}
		metadataOwner := strings.TrimSpace(entity.MaterializationMetadata.Owner)
		if metadataOwner != RunForkMaterializedEntitySnapshotMetadataOwner {
			return nil, runForkReplayResumeError(RunForkBlockerEntitySnapshotMetadataUnproven, RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("fork materialization metadata for entity %s must be owned by %s", entityID, RunForkMaterializedEntitySnapshotMetadataOwner))
		}
		meta := runForkEntityMetadata{
			FlowInstance: strings.TrimSpace(entity.MaterializationMetadata.FlowInstance),
			EntityType:   strings.TrimSpace(entity.MaterializationMetadata.EntityType),
			Slug:         stringFieldValue(entity.Fields, "slug"),
			Name:         stringFieldValue(entity.Fields, "name"),
		}
		if meta.FlowInstance == "" || meta.EntityType == "" {
			return nil, fmt.Errorf("source entity_state metadata for entity %s must include flow_instance and entity_type", entityID)
		}
		out[entityID] = meta
	}
	return out, nil
}

func stringFieldValue(fields map[string]any, key string) string {
	value, ok := fields[strings.TrimSpace(key)]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.RawMessage:
		var out string
		if err := json.Unmarshal(typed, &out); err == nil {
			return strings.TrimSpace(out)
		}
	case []byte:
		var out string
		if err := json.Unmarshal(typed, &out); err == nil {
			return strings.TrimSpace(out)
		}
	}
	return ""
}

func materializeRunForkEntityState(ctx context.Context, tx *sql.Tx, forkRunID string, plan RunForkPlan, entity RunForkEntityState, meta runForkEntityMetadata, now time.Time) error {
	entityID := strings.TrimSpace(entity.EntityID)
	currentState := strings.TrimSpace(entity.CurrentState)
	if currentState == "" {
		return fmt.Errorf("reconstructed current_state is required for entity %s", entityID)
	}
	if entity.EnteredStateAt == nil || entity.EnteredStateAt.IsZero() {
		return fmt.Errorf("reconstructed entered_state_at is required for entity %s", entityID)
	}
	fieldsJSON, err := jsonMapArg(entity.Fields)
	if err != nil {
		return fmt.Errorf("encode fork fields for entity %s: %w", entityID, err)
	}
	gatesJSON, err := jsonMapArg(entity.Gates)
	if err != nil {
		return fmt.Errorf("encode fork gates for entity %s: %w", entityID, err)
	}
	forkAccumulator, err := forkAttemptGenerationState(entity.Accumulator, forkRunID, entityID)
	if err != nil {
		return fmt.Errorf("fork loop state for entity %s: %w", entityID, err)
	}
	accJSON, err := jsonMapArg(forkAccumulator)
	if err != nil {
		return fmt.Errorf("encode fork accumulator for entity %s: %w", entityID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, NULLIF($5, ''), NULLIF($6, ''),
			$7, $8::jsonb, $9::jsonb, $10::jsonb, 1,
			$11, $12, $12
		)
	`, forkRunID, entityID, meta.FlowInstance, meta.EntityType, meta.Slug, meta.Name,
		currentState, gatesJSON, fieldsJSON, accJSON, entity.EnteredStateAt, now); err != nil {
		return fmt.Errorf("insert fork entity_state %s: %w", entityID, err)
	}
	return mutationlog.InsertEntityStateDiff(ctx, tx, entityID, mutationlog.EntityStateProjection{}, mutationlog.EntityStateProjection{
		CurrentState: currentState,
		Fields:       entity.Fields,
		Gates:        entity.Gates,
		Accumulator:  forkAccumulator,
	}, mutationlog.Writer{
		Type:        "platform",
		ID:          "run_fork_materializer",
		HandlerStep: "materialize_snapshot",
	})
}

func deterministicRunForkMaterializationID(sourceRunID, forkEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:run-fork-materialization:"+strings.TrimSpace(sourceRunID)+":"+strings.TrimSpace(forkEventID))).String()
}

func jsonMapArg(values map[string]any) (string, error) {
	if values == nil {
		values = map[string]any{}
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func runForkBlockerCodes(blockers []RunForkUnsupportedBlocker) string {
	if len(blockers) == 0 {
		return "none"
	}
	codes := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if code := strings.TrimSpace(blocker.Code); code != "" {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return "unnamed"
	}
	return strings.Join(codes, ", ")
}
