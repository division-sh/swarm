package sourceadmission

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// ObserveLiveHandoff proves source applicability, not executable authority.
// Only the activation owner's immutable live ACK distinguishes the handoff
// from an arbitrarily closed selected executor or detached historical record.
func ObserveLiveHandoff(ctx context.Context, q RowQueryer, runID, bundleHash string) (bool, error) {
	return observeLiveHandoff(ctx, q, runID, bundleHash, false, false)
}

// A paused handoff stays applicable to controls and retained-source inspection,
// but is not positive evidence for an executable ordinary generation.
func ObserveRetainedLiveHandoff(ctx context.Context, q RowQueryer, runID, bundleHash string) (bool, error) {
	return observeLiveHandoff(ctx, q, runID, bundleHash, true, false)
}

// Historical diagnostic projection consumes the immutable handoff, including
// after later terminalization. It cannot be used as current execution admission.
func ObserveHistoricalLiveHandoff(ctx context.Context, q RowQueryer, runID, bundleHash string) (bool, error) {
	return observeLiveHandoff(ctx, q, runID, bundleHash, false, true)
}

func observeLiveHandoff(ctx context.Context, q RowQueryer, runID, bundleHash string, includePaused, historical bool) (bool, error) {
	if ctx == nil || q == nil {
		return false, errors.New("live handoff observation requires context and query owner")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var requestRaw, resultRaw []byte
	var sourceRunID, kind string
	var revision int64
	var eventID sql.NullString
	err := q.QueryRowContext(ctx, `SELECT operation.request_json,operation.result_json,
		operation.source_run_id,operation.fork_point_kind,operation.fork_revision,operation.fork_event_id
		FROM runs AS run
		JOIN run_fork_selected_contract_bindings AS binding ON binding.fork_run_id=run.run_id
		JOIN run_fork_operations AS operation ON operation.selected_binding_id=binding.binding_id
			AND operation.fork_run_id=run.run_id AND operation.source_run_id=binding.source_run_id
			AND operation.fork_point_kind=binding.fork_point_kind AND operation.fork_revision=binding.fork_revision
			AND (operation.fork_event_id=binding.fork_event_id OR (operation.fork_event_id IS NULL AND binding.fork_event_id IS NULL))
		JOIN run_fork_selected_contract_runtime_executions AS execution ON execution.binding_id=binding.binding_id
			AND execution.fork_run_id=run.run_id AND execution.source_run_id=operation.source_run_id
			AND execution.fork_point_kind=operation.fork_point_kind AND execution.fork_revision=operation.fork_revision
			AND (execution.fork_event_id=operation.fork_event_id OR (execution.fork_event_id IS NULL AND operation.fork_event_id IS NULL))
		WHERE run.run_id=$1 AND run.bundle_hash=$2 AND ($4 OR run.status='live' OR ($3 AND run.status='paused')) AND run.flow_path IS NOT NULL
			AND operation.state='activated' AND execution.state='closed' AND execution.failure IS NULL
			AND execution.lease_expires_at IS NULL AND execution.terminal_at IS NOT NULL
			AND execution.generation=(SELECT MAX(current.generation) FROM run_fork_selected_contract_runtime_executions current WHERE current.fork_run_id=run.run_id)
			AND NOT EXISTS (SELECT 1 FROM run_fork_selected_contract_runtime_executions current WHERE current.fork_run_id=run.run_id AND current.state<>'closed')`,
		runID, bundleHash, includePaused, historical).Scan(&requestRaw, &resultRaw, &sourceRunID, &kind, &revision, &eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read committed live handoff: %w", err)
	}
	var request runfork.ForkOperationRequest
	var result runfork.ForkOperationResult
	if err := canonicaljson.DecodeInto(requestRaw, &request); err != nil {
		return false, err
	}
	if err := canonicaljson.DecodeInto(resultRaw, &result); err != nil {
		return false, err
	}
	if _, _, err := request.Canonical(); err != nil {
		return false, err
	}
	if err := result.Validate(request, runID); err != nil {
		return false, err
	}
	if request.SourceRunID != sourceRunID || result.BundleHash != bundleHash ||
		string(result.ForkPoint.Kind) != kind || result.ForkPoint.Revision != revision || result.ForkPoint.EventID != eventID.String {
		return false, errors.New("committed live handoff contradicts its exact selected lineage")
	}
	return result.ForkRunStatus == runfork.RunForkLiveStatus, nil
}
