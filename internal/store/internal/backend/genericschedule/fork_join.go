package genericschedule

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	"github.com/google/uuid"
)

// The fixed-cut adapter owns construction/generation correspondence. This
// native command owns persistence of that admitted pair, never source lookup.
type ForkJoinRequest struct {
	Source        runtimegenericschedule.Activation
	Child         runtimegenericschedule.AdmissionCommand
	PointKind     forkpoint.Kind
	PointRevision int64
	PointEventID  string
	BornAt        time.Time
}

func (r ForkJoinRequest) Validate() error {
	if err := r.Source.ValidateForkJoinRestorationSource(); err != nil {
		return err
	}
	if err := r.Child.Validate(); err != nil {
		return err
	}
	if err := forkpoint.ValidateIdentity(r.PointKind, r.PointRevision, r.PointEventID); err != nil {
		return err
	}
	for _, runID := range []string{r.Source.Command.RunID, r.Child.RunID} {
		id, err := uuid.Parse(runID)
		if err != nil || id == uuid.Nil || id.String() != runID {
			return fmt.Errorf("fork join requires canonical source and child runs")
		}
	}
	if r.Source.Command.RunID == r.Child.RunID || r.BornAt.IsZero() || r.BornAt != canonicalTime(r.BornAt) || r.BornAt.Before(r.Source.AdmittedAt) {
		return fmt.Errorf("fork join requires distinct runs and canonical child birth after source admission")
	}
	if r.Source.CurrentEventID != "" && r.BornAt.Before(r.Source.CurrentEventAdmittedAt) {
		return fmt.Errorf("fork join child birth precedes retained occurrence preparation")
	}
	if r.Source.Status == runtimegenericschedule.StatusCancelled && r.BornAt.Before(r.Source.CancelledAt) {
		return fmt.Errorf("fork join child birth precedes retained cancellation")
	}
	if r.Source.ClockSuspension != nil || r.Source.Command.ReplyContext != "" || r.Child.ReplyContext != "" ||
		r.Source.Command.Due.Recurring() || r.Child.Due.Kind != runtimegenericschedule.DueAbsolute ||
		!r.Child.Due.Absolute.Equal(r.Source.InitialDueAt) || r.Child.ExecutionMode != r.Source.Command.ExecutionMode {
		return fmt.Errorf("fork join must preserve absolute due and mode without reply or clock authority")
	}
	return r.validateHandles()
}

func (r ForkJoinRequest) validateHandles() error {
	sourceHandle, sourceRef, sourceOK := timeridentity.ParseJoinHandle(r.Source.Command.Payload.Interface().(map[string]any))
	childHandle, childRef, childOK := timeridentity.ParseJoinHandle(r.Child.Payload.Interface().(map[string]any))
	if !sourceOK || !childOK || sourceRef.Mode() != timeridentity.JoinRefModeArrival || childRef.Mode() != timeridentity.JoinRefModeArrival ||
		sourceHandle.Kind() != childHandle.Kind() || !sourceRef.Declaration().Equal(childRef.Declaration()) {
		return fmt.Errorf("fork join must preserve the exact arrival declaration and handle kind")
	}
	source, child := sourceRef.StageEntry(), childRef.StageEntry()
	origin := source.OriginRunID
	if origin == "" {
		origin = source.RunID
	}
	if source.RunID != r.Source.Command.RunID || source.EntityID != r.Source.Command.EntityID ||
		child.RunID != r.Child.RunID || child.EntityID != r.Child.EntityID || child.OriginRunID != origin ||
		source.Stage != child.Stage || source.Cause != child.Cause || source.EventID != child.EventID ||
		source.OccurrenceID != child.OccurrenceID || source.TransitionID != child.TransitionID {
		return fmt.Errorf("fork join command contradicts retained stage-entry provenance")
	}
	return nil
}

func (r ForkJoinRequest) Expected(childID string) (runtimegenericschedule.Activation, error) {
	if err := r.Validate(); err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	hash, err := r.Child.ImmutableHash()
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	child := runtimegenericschedule.Activation{
		ID: childID, Command: r.Child, ImmutableHash: hash, AdmittedAt: r.BornAt,
		InitialDueAt: r.Source.InitialDueAt, CurrentDueAt: r.Source.CurrentDueAt, Status: r.Source.Status,
		ForkJoinOrigin: &runtimegenericschedule.ForkJoinOrigin{
			SourceActivationID: r.Source.ID, SourceRunID: r.Source.Command.RunID,
			PointKind: r.PointKind, PointRevision: r.PointRevision, PointEventID: r.PointEventID,
			SourceAdmittedAt: r.Source.AdmittedAt, Owner: runtimegenericschedule.ForkJoinReconstructionOwner,
		},
	}
	if r.Source.Status == runtimegenericschedule.StatusCancelled {
		child.CancelCause, child.CancelledAt = r.Source.CancelCause, r.BornAt
	}
	return child, child.Validate()
}

// RestoreForkJoinTx composes immutable admission, retained cut provenance and
// terminal disposition in one existing mutation attempt. It installs no wakeup.
func RestoreForkJoinTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, request ForkJoinRequest) (runtimegenericschedule.Activation, error) {
	if err := requireForkJoinFrame(ctx, attempt, request.Child.RunID); err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	if err := request.Validate(); err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	var child runtimegenericschedule.Activation
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if postgres {
			_, err = sourceadmission.LoadPostgresTx(ctx, tx, request.Child.RunID, true, false)
		} else {
			_, err = sourceadmission.LoadSQLiteTx(ctx, tx, request.Child.RunID, true, false)
		}
		if err != nil {
			return err
		}
		admitted, err := admitTx(ctx, tx, attempt, postgres, request.Child, func() time.Time { return request.BornAt }, nil)
		if err != nil {
			return err
		}
		child = admitted.Activation
		if admitted.Outcome == runtimegenericschedule.AdmissionCreated {
			if err := stampForkJoinOrigin(ctx, tx, postgres, child.ID, request); err != nil {
				return err
			}
			if request.Source.Status == runtimegenericschedule.StatusCancelled {
				child, err = cancelLoadedTx(ctx, tx, dialectFor(postgres), child, request.Source.CancelCause, request.BornAt)
				if err != nil {
					return err
				}
			}
		}
		child, err = requireForkJoinTx(ctx, tx, postgres, request)
		return err
	})
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	return child, attempt.RequireExistingSQLFrame(ctx)
}

// ReadForkJoinInventoryTx includes every inherited generic row, including
// partial lineage. The shared decoder rejects corrupt or foreign variants.
func ReadForkJoinInventoryTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, childRunID string) ([]runtimegenericschedule.Activation, error) {
	if err := requireForkJoinFrame(ctx, attempt, childRunID); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(childRunID)
	if err != nil || id == uuid.Nil || id.String() != childRunID {
		return nil, fmt.Errorf("fork join inventory requires the canonical child run")
	}
	var inventory []runtimegenericschedule.Activation
	err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		query := activationSelectColumns + ` FROM timers WHERE run_id = ?
			AND task_type IN ('timer','scheduled_task','global_recurring')
			AND (source_timer_id IS NOT NULL OR forked_from_run_id IS NOT NULL
			 OR forked_from_point_kind IS NOT NULL OR forked_from_point_revision IS NOT NULL
			 OR forked_from_event_id IS NOT NULL OR source_armed_at IS NOT NULL OR reconstruction_owner IS NOT NULL)
			ORDER BY timer_id`
		if postgres {
			query = strings.Replace(query, "run_id = ?", "run_id = $1::uuid", 1) + " FOR UPDATE"
		}
		rows, err := tx.QueryContext(ctx, query, childRunID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			actual, err := scanActivationRow(rows, dialectFor(postgres))
			if err != nil {
				return err
			}
			if actual.Command.RunID != childRunID || actual.ForkJoinOrigin == nil {
				return fmt.Errorf("fork join inventory lacks exact inherited child ownership")
			}
			inventory = append(inventory, actual)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		return nil, err
	}
	return inventory, attempt.RequireExistingSQLFrame(ctx)
}

func requireForkJoinTx(ctx context.Context, tx *sql.Tx, postgres bool, request ForkJoinRequest) (runtimegenericschedule.Activation, error) {
	scope, err := request.Child.ScopeKey()
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	actual, found, err := loadByKeyTx(ctx, tx, dialectFor(postgres), scope, request.Child.ScheduleKey, postgres)
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	if !found {
		return runtimegenericschedule.Activation{}, fmt.Errorf("fork join lacks its exact child schedule")
	}
	expected, err := request.Expected(actual.ID)
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	want, err := expected.EvidenceDigest()
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	got, err := actual.EvidenceDigest()
	if err != nil || got != want {
		return runtimegenericschedule.Activation{}, fmt.Errorf("fork join readback differs from exact unactivated child projection")
	}
	return actual, nil
}

func requireForkJoinFrame(ctx context.Context, attempt *mutationprotocol.Attempt, childRunID string) error {
	if attempt == nil {
		return fmt.Errorf("fork join requires its native mutation attempt")
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return err
	}
	if childRunID == "" || correlation.RunIDFromContext(ctx) != childRunID {
		return fmt.Errorf("fork join mutation frame belongs to another child run")
	}
	return nil
}

func stampForkJoinOrigin(ctx context.Context, tx *sql.Tx, postgres bool, activationID string, r ForkJoinRequest) error {
	query := `UPDATE timers SET source_timer_id=$1, forked_from_run_id=$2,
		forked_from_point_kind=$3, forked_from_point_revision=$4, forked_from_event_id=NULLIF($5,''),
		source_armed_at=$6, reconstruction_owner=$7
		WHERE timer_id=$8 AND task_type='timer' AND source_timer_id IS NULL AND forked_from_run_id IS NULL
		AND forked_from_point_kind IS NULL AND forked_from_point_revision IS NULL AND forked_from_event_id IS NULL
		AND source_armed_at IS NULL AND reconstruction_owner IS NULL`
	if postgres {
		query = `UPDATE timers SET source_timer_id=$1::uuid, forked_from_run_id=$2::uuid,
			forked_from_point_kind=$3, forked_from_point_revision=$4, forked_from_event_id=NULLIF($5,'')::uuid,
			source_armed_at=$6, reconstruction_owner=$7
			WHERE timer_id=$8::uuid AND task_type='timer' AND source_timer_id IS NULL AND forked_from_run_id IS NULL
			AND forked_from_point_kind IS NULL AND forked_from_point_revision IS NULL AND forked_from_event_id IS NULL
			AND source_armed_at IS NULL AND reconstruction_owner IS NULL`
	}
	result, err := tx.ExecContext(ctx, query, r.Source.ID, r.Source.Command.RunID, r.PointKind, r.PointRevision, r.PointEventID,
		r.Source.AdmittedAt, runtimegenericschedule.ForkJoinReconstructionOwner, activationID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("fork join origin stamp lost its exact fresh activation")
	}
	return nil
}
