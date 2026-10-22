package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	managedcapabilitystore "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
)

// References canonical rows without copying their payloads or granting authority.
type selectedRetainedManifest struct {
	Tables     []selectedRetainedTable    `json:"tables"`
	Activities []selectedRetainedActivity `json:"activities"`
	Turns      []selectedRetainedTurn     `json:"turns"`
}

type selectedRetainedTable struct {
	Table   string                `json:"table"`
	Columns []string              `json:"columns"`
	Rows    []selectedRetainedRow `json:"rows"`
}

type selectedRetainedRow struct {
	Identity    string `json:"identity"`
	Fingerprint string `json:"fingerprint"`
	Detached    bool   `json:"detached"`
}

type selectedResetCohort struct {
	Permanent ClosedSelectedPermanentForkCohort
	Journals  ClosedSelectedInstalledJournalCohort
	Manifest  selectedRetainedManifest
}

func captureSelectedResetHistoryTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, sqlite bool) ([]selectedResetCohort, error) {
	runs, err := adminpersistence.ValidateDestructiveResetCleanupRequest(req, req.RequestedAt)
	if err != nil {
		return nil, err
	}
	prepared := make(map[string]bool, len(req.SelectedHistory))
	for _, item := range req.SelectedHistory {
		if prepared[item.Binding.ForkRunID] {
			return nil, errors.New("selected reset repeats a prepared child")
		}
		prepared[item.Binding.ForkRunID] = true
	}
	if err := validateSelectedResetPlanCoverageTx(ctx, tx, runs, prepared); err != nil {
		return nil, err
	}
	cohorts := make([]selectedResetCohort, 0, len(req.SelectedHistory))
	for _, item := range req.SelectedHistory {
		cohort, err := captureSelectedResetCohortTx(ctx, tx, item, sqlite)
		if err != nil {
			return nil, err
		}
		cohorts = append(cohorts, cohort)
	}
	return cohorts, ctx.Err()
}

func validateSelectedResetPlanCoverageTx(ctx context.Context, tx *sql.Tx, runs []string, prepared map[string]bool) error {
	for _, run := range runs {
		detached, err := readDetachedSelectedHistoryTx(ctx, tx, run)
		if err != nil {
			return err
		}
		if detached {
			if prepared[run] {
				return errors.New("detached selected history conflicts with live reset preparation")
			}
			continue
		}
		var attached bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1
			UNION ALL SELECT 1 FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1)`, run).Scan(&attached); err != nil {
			return err
		}
		if attached != prepared[run] {
			return errors.New("selected reset preparation does not cover the exact planned child set")
		}
	}
	return ctx.Err()
}

func captureSelectedResetCohortTx(ctx context.Context, tx *sql.Tx, item runfork.SelectedResetPreparation, sqlite bool) (selectedResetCohort, error) {
	var absent selectedResetCohort
	if err := ValidateSelectedResetPreparationTx(ctx, tx, item, sqlite); err != nil {
		return absent, err
	}
	permanent, err := ReadClosedSelectedPermanentForkCohortTx(ctx, tx, item.Binding.ForkRunID, sqlite)
	if err != nil {
		return absent, err
	}
	journals, err := ReadClosedSelectedInstalledJournalCohortTx(ctx, tx, item.Binding.ForkRunID, sqlite, item.Original)
	if err != nil {
		return absent, err
	}
	for _, generation := range permanent.Generations {
		if err := managedcapabilitystore.InspectSelectedPreparationHistoryTx(ctx, tx, generation.Preparation.SelectedForkPreparation); err != nil {
			return absent, err
		}
		if err := effectpersistence.RequireClosedSelectedEffectHistoryTx(ctx, tx, generation.Execution, generation.Preparation.Coordinates.BundleHash, sqlite); err != nil {
			return absent, err
		}
	}
	manifest, err := readSelectedRetainedManifestTx(ctx, tx, item.Binding.ForkRunID)
	if err != nil {
		return absent, err
	}
	cohort := selectedResetCohort{Permanent: permanent, Journals: journals, Manifest: manifest}
	if err := captureSelectedRetainedMembershipsTx(ctx, tx, &cohort); err != nil {
		return absent, err
	}
	return cohort, ctx.Err()
}

func captureSelectedRetainedMembershipsTx(ctx context.Context, tx *sql.Tx, cohort *selectedResetCohort) error {
	grants := make(map[string]string)
	for _, generation := range cohort.Permanent.Generations {
		for _, grant := range generation.Grants {
			grants[grant.FactID] = generation.Execution.ExecutionID
		}
	}
	for _, journal := range cohort.Journals.Journals {
		execution := grants[journal.GrantFactID]
		if execution == "" {
			return errors.New("selected retained journal has no captured grant generation")
		}
		lineage, err := canonicaljson.Bytes(map[string]string{"grant_fact_id": journal.GrantFactID})
		if err != nil {
			return err
		}
		cohort.Manifest.Activities = append(cohort.Manifest.Activities, selectedRetainedActivity{
			ExecutionID: execution, RequestID: journal.Record.RequestEventID, OriginKind: "installed_grant", Lineage: lineage})
	}
	for _, journal := range cohort.Journals.CopiedJournals {
		var cuts []map[string]any
		for _, cut := range journal.Origin.Cuts {
			cuts = append(cuts, map[string]any{"run_id": cut.RunID, "request_id": cut.RequestID, "revision": cut.Revision,
				"bundle_hash": cut.Source.BundleHash(), "parent_point_kind": cut.ParentPointKind,
				"parent_point_event_id": cut.ParentPointEventID, "parent_point_input": cut.ParentPointInput, "parent_point_revision": cut.ParentPointRevision})
		}
		lineage, err := canonicaljson.Bytes(map[string]any{"input_event_id": journal.InputEventID,
			"origin_request_id": journal.Origin.Request.ID(), "origin_run_id": journal.Origin.Request.RunID(), "cuts": cuts})
		if err != nil {
			return err
		}
		// Copied-terminal membership is in the complete cohort, not a fabricated
		// producing generation. The first cohort receipt anchors that distinction.
		cohort.Manifest.Activities = append(cohort.Manifest.Activities, selectedRetainedActivity{
			ExecutionID: cohort.Permanent.Generations[0].Execution.ExecutionID, RequestID: journal.Record.RequestEventID,
			OriginKind: "copied_terminal", Lineage: lineage})
	}
	slices.SortFunc(cohort.Manifest.Activities, func(a, b selectedRetainedActivity) int { return strings.Compare(a.RequestID, b.RequestID) })
	rows, err := tx.QueryContext(ctx, `SELECT CAST(o.selected_execution_id AS TEXT),CAST(t.turn_id AS TEXT),
		CAST(t.completion_attempt_id AS TEXT),CAST(t.capability_surface_id AS TEXT)
		FROM agent_turns t JOIN runtime_external_effect_attempts a ON a.attempt_id=t.completion_attempt_id
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE t.run_id=$1 ORDER BY t.turn_id`, cohort.Permanent.Binding.ForkRunID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var member selectedRetainedTurn
		if err := rows.Scan(&member.ExecutionID, &member.TurnID, &member.AttemptID, &member.SurfaceID); err != nil {
			return errors.Join(err, rows.Close())
		}
		cohort.Manifest.Turns = append(cohort.Manifest.Turns, member)
	}
	return errors.Join(rows.Err(), rows.Close())
}

func readSelectedRetainedManifestTx(ctx context.Context, tx *sql.Tx, child string) (selectedRetainedManifest, error) {
	var manifest selectedRetainedManifest
	generations, err := readClosedSelectedGenerationRows(ctx, tx, child)
	if err != nil {
		return manifest, err
	}
	probeIDs := make(map[string]bool)
	for _, generation := range generations {
		for _, id := range generation.Preparation.SurfaceIDs() {
			probeIDs[id] = true
		}
	}
	probes := make([]string, 0, len(probeIDs))
	for id := range probeIDs {
		probes = append(probes, id)
	}
	slices.Sort(probes)
	// A closed inventory, not caller-supplied tables or predicates.
	for _, entry := range []struct{ table, identity, predicate string }{
		{"run_fork_selected_contract_runtime_executions", "m.execution_id", "m.fork_run_id=$1"},
		{"run_fork_operations", "m.operation_id", "m.fork_run_id=$1"},
		{"runtime_generation_grants", "m.fact_id", "m.selected_fork_run_id=$1"},
		{"activity_attempts", "m.request_event_id", "m.run_id=$1"},
		{"managed_agent_capability_surfaces", "m.surface_id", "m.selected_fork_run_id=$1"},
		{"runtime_external_effect_operations", "m.operation_id", "m.selected_execution_id IN (SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1)"},
		{"runtime_external_effect_attempts", "m.attempt_id", "m.operation_id IN (SELECT o.operation_id FROM runtime_external_effect_operations o JOIN run_fork_selected_contract_runtime_executions e ON e.execution_id=o.selected_execution_id WHERE e.fork_run_id=$1)"},
		{"agent_turns", "m.turn_id", "m.run_id=$1"},
		{"runtime_agent_turn_lifetimes", "m.turn_id", "m.run_id=$1"},
		{"runtime_provider_attempt_drains", "m.drain_id", "m.run_id=$1"},
	} {
		args := []any{child}
		if entry.table == "managed_agent_capability_surfaces" {
			for _, id := range probes {
				args = append(args, id)
				entry.predicate += " OR m.surface_id=$" + strconv.Itoa(len(args))
			}
		}
		query := `SELECT CAST(` + entry.identity + ` AS TEXT),m.* FROM ` + entry.table + ` m WHERE ` + entry.predicate + ` ORDER BY ` + entry.identity
		table, err := readSelectedRetainedTableTx(ctx, tx, entry.table, query, args...)
		if err != nil {
			return selectedRetainedManifest{}, err
		}
		manifest.Tables = append(manifest.Tables, table)
	}
	return manifest, ctx.Err()
}

func readSelectedRetainedTableTx(ctx context.Context, tx *sql.Tx, table, query string, args ...any) (out selectedRetainedTable, err error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return out, fmt.Errorf("read selected retained %s: %w", table, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			out = selectedRetainedTable{}
		}
	}()
	columns, err := rows.Columns()
	if err != nil {
		return out, err
	}
	out = selectedRetainedTable{Table: table, Columns: columns[1:], Rows: []selectedRetainedRow{}}
	seen := make(map[string]bool)
	for rows.Next() {
		values, targets := make([]any, len(columns)), make([]any, len(columns))
		for i := range targets {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return out, err
		}
		id, ok := values[0].(string)
		if !ok {
			if raw, present := values[0].([]byte); present {
				id = string(raw)
			}
		}
		if id == "" || seen[id] {
			return out, fmt.Errorf("selected retained %s has absent or duplicate identity", table)
		}
		physical := make([]any, len(values)-1)
		for i, value := range values[1:] {
			physical[i], err = selectedRetainedSQLValue(value)
			if err != nil {
				return out, fmt.Errorf("selected retained %s.%s: %w", table, columns[i+1], err)
			}
		}
		fingerprint, err := canonicaljson.Hash(physical)
		if err != nil {
			return out, err
		}
		detached := true
		if table == "managed_agent_capability_surfaces" {
			// Runless preparation probes were already independent of mutable
			// parents. Preserve them without counting another detachment.
			index := slices.Index(columns, "selected_fork_run_id")
			if index < 0 {
				return out, errors.New("managed capability history lacks its physical run projection")
			}
			detached = values[index] != nil
		}
		out.Rows = append(out.Rows, selectedRetainedRow{Identity: id, Fingerprint: fingerprint, Detached: detached})
		seen[id] = true
	}
	return out, rows.Err()
}

// Preserve SQL NULL, bytes and text as distinct facts without retaining payloads.
func selectedRetainedSQLValue(value any) (any, error) {
	var kind string
	switch raw := value.(type) {
	case nil:
		kind = "null"
	case string:
		kind = "text"
	case []byte:
		kind, value = "bytes", base64.StdEncoding.EncodeToString(raw)
	case time.Time:
		kind, value = "timestamp", raw.UTC().Format(time.RFC3339Nano)
	case int64:
		kind = "integer"
	case float64:
		kind = "real"
	case bool:
		kind = "boolean"
	default:
		return nil, fmt.Errorf("unsupported SQL value %T", value)
	}
	return []any{kind, value}, nil
}
