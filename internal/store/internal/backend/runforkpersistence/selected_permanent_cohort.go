package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

// ClosedSelectedPermanentForkCohort is partial evidence captured under the
// existing reset fence. It is not a completed manifest or reset permission;
// empty grant histories do not prove execution, retirement or drain completion.
type ClosedSelectedPermanentForkCohort struct {
	Generations                            []ClosedSelectedGenerationHistory
	Binding                                runfork.RunForkSelectedContractBinding
	Operation                              runfork.ForkOperationRecord
	RequestJSON, ResultJSON, FailureJSON   json.RawMessage
	OperationCreatedAt, OperationUpdatedAt time.Time
}

// The eventual reset owner must apply in this same fenced transaction or
// recapture. No live association or canonical evidence is changed here.
func ReadClosedSelectedPermanentForkCohortTx(ctx context.Context, tx *sql.Tx, forkRunID string, sqlite bool) (ClosedSelectedPermanentForkCohort, error) {
	var absent ClosedSelectedPermanentForkCohort
	generations, err := ReadClosedSelectedGenerationHistoryTx(ctx, tx, forkRunID, sqlite)
	if err != nil {
		return absent, err
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, forkRunID)
	if err != nil {
		return absent, fmt.Errorf("read permanent selected binding: %w", err)
	}
	for _, generation := range generations {
		if generation.BindingID != binding.BindingID || generation.Execution.ForkRunID != binding.ForkRunID ||
			generation.Execution.SourceRunID != binding.SourceRunID ||
			!sameSelectedForkPointIdentity(generation.Execution.ForkPoint, binding.ForkPoint) {
			return absent, errors.New("closed selected cohort conflicts with permanent binding/cut")
		}
		if generation.Preparation.Coordinates.BundleHash != generations[0].Preparation.Coordinates.BundleHash {
			return absent, errors.New("closed selected cohort has inconsistent permanent bundle coordinates")
		}
	}
	operation, err := selectedForkOperationForRecoveryTx(ctx, tx, binding, generations[0].Preparation.Coordinates.BundleHash, !sqlite)
	if err != nil {
		return absent, err
	}
	if operation.Status == runfork.ForkOperationMaterialized {
		return absent, errors.New("closed selected cohort permanent operation is not settled")
	}
	captured := ClosedSelectedPermanentForkCohort{Generations: generations, Binding: binding, Operation: operation}
	if err := readClosedSelectedOperationEvidenceTx(ctx, tx, &captured); err != nil {
		return absent, err
	}
	if err := ctx.Err(); err != nil {
		return absent, err
	}
	return captured, nil
}

func readClosedSelectedOperationEvidenceTx(ctx context.Context, tx *sql.Tx, captured *ClosedSelectedPermanentForkCohort) error {
	var sourceRun string
	var request, result, failure []byte
	var created, updated any
	err := tx.QueryRowContext(ctx, `SELECT CAST(source_run_id AS TEXT) AS source_run_id,request_json,result_json,failure_json,created_at,updated_at
		FROM run_fork_operations WHERE operation_id=$1`, captured.Operation.Request.OperationID).
		Scan(&sourceRun, &request, &result, &failure, &created, &updated)
	if err != nil {
		return fmt.Errorf("read permanent selected operation physical evidence: %w", err)
	}
	if sourceRun != captured.Binding.SourceRunID || sourceRun != captured.Operation.Request.SourceRunID {
		return errors.New("permanent selected operation physical source differs from its admitted cut")
	}
	for _, evidence := range []struct {
		raw   []byte
		value any
	}{{request, captured.Operation.Request}, {result, captured.Operation.Result}, {failure, captured.Operation.Failure}} {
		expected, err := canonicaljson.Bytes(evidence.value)
		if err != nil {
			return err
		}
		actual := evidence.raw
		if len(actual) == 0 {
			actual = []byte("null")
		}
		if !workflowCommitJSONEqual(actual, expected) {
			return errors.New("permanent selected operation physical request/result/failure differs from its decoded evidence")
		}
	}
	captured.RequestJSON, captured.ResultJSON, captured.FailureJSON = request, result, failure
	for _, clock := range []struct {
		raw    any
		target *time.Time
	}{{created, &captured.OperationCreatedAt}, {updated, &captured.OperationUpdatedAt}} {
		value, present, err := runstate.DecodeBindingTimestamp(clock.raw)
		if err != nil {
			return err
		}
		if !present || value.IsZero() {
			return errors.New("permanent selected operation lacks a physical clock")
		}
		*clock.target = value
	}
	if captured.OperationUpdatedAt.Before(captured.OperationCreatedAt) {
		return errors.New("permanent selected operation has inconsistent physical clocks")
	}
	return nil
}
