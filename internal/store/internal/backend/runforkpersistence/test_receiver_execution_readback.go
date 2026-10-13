package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type SelectedForkReceiverExecutionProofRequest struct {
	SourceRunID, ChildRunID, SourceEventID, ChildEventID string
	BindingID, ExecutionID, DeliveryID, AuthorityStamp   string
	Generation, ClaimVersion                             uint64
}

type SelectedForkReceiverExecutionProof struct {
	SourceRunID, SourceEventID, BindingID, CutKind, Mode string
	ExecutionState, RunStatus                            string
	CutRevision                                          int64
	ExecutionCount, PublicationCount, DeadLetterCount    int
	ConstructedRows                                      map[string]int
	DeliveryHandoff                                      bool
	PipelineOutcome, PipelineReason                      string
	PipelineFailure                                      []byte
	Failure                                              []byte
}

// This exact physical witness observes a selected receiver's committed facts.
// It grants no execution authority and never repairs missing rows.
func ReadSelectedForkReceiverExecutionProofTx(ctx context.Context, tx *sql.Tx, req SelectedForkReceiverExecutionProofRequest) (SelectedForkReceiverExecutionProof, error) {
	if err := ctx.Err(); err != nil {
		return SelectedForkReceiverExecutionProof{}, err
	}
	if tx == nil || req.SourceRunID == req.ChildRunID || req.Generation == 0 || req.ClaimVersion == 0 ||
		req.AuthorityStamp == "" || req.AuthorityStamp != strings.TrimSpace(req.AuthorityStamp) {
		return SelectedForkReceiverExecutionProof{}, fmt.Errorf("receiver execution proof requires exact selected identities and claim")
	}
	for _, id := range []string{req.SourceRunID, req.ChildRunID, req.SourceEventID, req.ChildEventID, req.BindingID, req.ExecutionID, req.DeliveryID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return SelectedForkReceiverExecutionProof{}, fmt.Errorf("receiver execution proof requires canonical identities")
		}
	}
	var out SelectedForkReceiverExecutionProof
	if err := tx.QueryRowContext(ctx, `SELECT CAST(b.source_run_id AS TEXT),CAST(b.fork_event_id AS TEXT),CAST(b.binding_id AS TEXT),b.fork_point_kind,b.fork_revision,b.mode
		FROM run_fork_selected_contract_bindings b JOIN runs r ON r.run_id=b.fork_run_id
		WHERE b.fork_run_id=$1 AND r.forked_from_run_id=b.source_run_id AND r.forked_from_event_id=b.fork_event_id
		AND r.forked_from_point_kind=b.fork_point_kind AND r.forked_from_revision=b.fork_revision`, req.ChildRunID).
		Scan(&out.SourceRunID, &out.SourceEventID, &out.BindingID, &out.CutKind, &out.CutRevision, &out.Mode); err != nil {
		return out, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions
		WHERE execution_id=$1 AND fork_run_id=$2 AND source_run_id=$3 AND binding_id=$4 AND generation=$5`,
		req.ExecutionID, req.ChildRunID, req.SourceRunID, req.BindingID, req.Generation).Scan(&out.ExecutionCount); err != nil {
		return out, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions x JOIN events e ON e.event_id=x.fork_event_id
		WHERE x.fork_run_id=$1 AND x.source_run_id=$2 AND x.source_event_id=$3 AND x.fork_event_id=$4
		AND x.event_name='producer/work.ready' AND x.selection_authority=$5 AND e.run_id=x.fork_run_id AND e.event_name=x.event_name`,
		req.ChildRunID, req.SourceRunID, req.SourceEventID, req.ChildEventID, req.AuthorityStamp).Scan(&out.PublicationCount); err != nil {
		return out, err
	}
	out.ConstructedRows = make(map[string]int)
	for _, table := range []string{"flow_instances", "flow_instance_runtime_readiness", "workflow_instance_initial_materializations", "entity_state"} {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE run_id=$1`, req.ChildRunID).Scan(&count); err != nil {
			return out, err
		}
		out.ConstructedRows[table] = count
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE delivery_id=$1 AND claim_version=$2 AND original_event_id=$3`,
		req.DeliveryID, req.ClaimVersion, req.ChildEventID).Scan(&out.DeadLetterCount); err != nil {
		return out, err
	}
	var pipelineFailure sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT d.continuation_handoff_at IS NOT NULL,COALESCE(r.outcome,''),COALESCE(r.reason_code,''),CAST(r.failure AS TEXT)
		FROM event_deliveries d LEFT JOIN event_receipts r ON r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'
		WHERE d.delivery_id=$1 AND d.run_id=$2 AND d.claim_version=$3`,
		req.DeliveryID, req.ChildRunID, req.ClaimVersion).Scan(&out.DeliveryHandoff, &out.PipelineOutcome, &out.PipelineReason, &pipelineFailure); err != nil {
		return out, err
	}
	if pipelineFailure.Valid {
		out.PipelineFailure = []byte(pipelineFailure.String)
	}
	var failure sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT e.state,r.status,CAST(e.failure AS TEXT)
		FROM run_fork_selected_contract_runtime_executions e JOIN runs r ON r.run_id=e.fork_run_id
		WHERE e.execution_id=$1 AND e.fork_run_id=$2 AND e.generation=$3`,
		req.ExecutionID, req.ChildRunID, req.Generation).Scan(&out.ExecutionState, &out.RunStatus, &failure); err != nil {
		return out, err
	}
	if failure.Valid {
		out.Failure = []byte(failure.String)
	}
	return out, nil
}
