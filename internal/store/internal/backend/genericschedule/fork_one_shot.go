package genericschedule

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	"github.com/google/uuid"
)

// The parent authenticates the fixed-cut source and projects child identities
// and captured reply rows. This leaf neither reads source state nor adopts it.
type ForkOneShotRequest struct {
	Source        runtimegenericschedule.Activation
	Child         runtimegenericschedule.AdmissionCommand
	PointKind     forkpoint.Kind
	PointRevision int64
	PointEventID  string
	BornAt        time.Time
}

func (r ForkOneShotRequest) Validate() error {
	if err := r.Source.ValidateForkOneShotRestorationSource(); err != nil {
		return err
	}
	if err := r.Child.Validate(); err != nil {
		return err
	}
	if err := forkpoint.ValidateIdentity(r.PointKind, r.PointRevision, r.PointEventID); err != nil {
		return err
	}
	for _, value := range []string{r.Source.ID, r.Source.Command.RunID, r.Child.RunID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("fork one-shot requires canonical source activation, source run and child run UUIDs")
		}
	}
	if r.Source.Command.RunID == r.Child.RunID || r.BornAt.IsZero() || r.BornAt != canonicalTime(r.BornAt) || r.BornAt.Before(r.Source.AdmittedAt) {
		return fmt.Errorf("fork one-shot requires distinct runs and canonical child birth after source admission")
	}
	for _, at := range []time.Time{r.Source.CancelledAt, r.Source.FailedAt} {
		if !at.IsZero() && r.BornAt.Before(at) {
			return fmt.Errorf("fork one-shot child birth precedes retained terminal history")
		}
	}
	source, child := r.Source.Command.Canonical(), r.Child.Canonical()
	if child.OwnerKind != runtimegenericschedule.OwnerAgent || child.OwnerID != source.OwnerID ||
		child.AgentIdentity.Name != source.AgentIdentity.Name || child.AgentIdentity.Route != source.AgentIdentity.Route ||
		child.ScheduleKey != source.ScheduleKey || child.TaskID != source.TaskID || child.EventType != source.EventType ||
		child.Due != source.Due || child.ExecutionMode != source.ExecutionMode || !child.Payload.Equal(source.Payload) {
		return fmt.Errorf("fork one-shot must preserve exact agent, schedule command, due basis, payload and mode")
	}
	if (source.ReplyContext == "") != (child.ReplyContext == "") || source.ReplyContext != "" && source.ReplyContext == child.ReplyContext {
		return fmt.Errorf("fork one-shot requires the captured child reply projection without inventing or retaining source reply authority")
	}
	routing, err := runfork.ProjectProducerOwnership(source.RunID, child.RunID, source.RoutingSource)
	if err != nil {
		return err
	}
	entityID := source.EntityID
	if entityID == source.RunID {
		entityID = child.RunID
	}
	if child.RoutingSource != routing || child.EntityID != entityID || child.FlowInstance != source.FlowInstance {
		return fmt.Errorf("fork one-shot child routing or entity differs from its exact canonical projection")
	}
	return nil
}

func (r ForkOneShotRequest) Expected(childID string) (runtimegenericschedule.Activation, error) {
	if err := r.Validate(); err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	hash, err := r.Child.ImmutableHash()
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	basis := r.Source.AdmittedAt
	if r.Source.ForkOneShotOrigin != nil {
		basis = r.Source.ForkOneShotOrigin.DueBasisAt
	}
	child := runtimegenericschedule.Activation{
		ID: childID, Command: r.Child.Canonical(), ImmutableHash: hash, AdmittedAt: r.BornAt,
		InitialDueAt: r.Source.InitialDueAt, CurrentDueAt: r.Source.CurrentDueAt, Status: r.Source.Status,
		ForkOneShotOrigin: &runtimegenericschedule.ForkOneShotOrigin{
			SourceActivationID: r.Source.ID, SourceRunID: r.Source.Command.RunID,
			PointKind: r.PointKind, PointRevision: r.PointRevision, PointEventID: r.PointEventID,
			DueBasisAt: basis, Owner: runtimegenericschedule.ForkOneShotReconstructionOwner,
		},
	}
	if r.Source.Status == runtimegenericschedule.StatusCancelled {
		child.CancelCause, child.CancelledAt = r.Source.CancelCause, r.BornAt
	}
	if r.Source.Status == runtimegenericschedule.StatusFailed {
		child.Failure, child.FailedAt = r.Source.Failure, r.BornAt
	}
	return child, child.Validate()
}

// RestoreForkOneShotTx admits the complete inherited row in the existing
// mutation frame. In particular, delay intent is never resolved from BornAt.
func RestoreForkOneShotTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, request ForkOneShotRequest) (runtimegenericschedule.Activation, error) {
	if err := request.Validate(); err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	if attempt == nil {
		return runtimegenericschedule.Activation{}, fmt.Errorf("fork one-shot requires its native mutation attempt")
	}
	if err := attempt.RequireExistingSQLFrame(ctx); err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	if correlation.RunIDFromContext(ctx) != request.Child.RunID {
		return runtimegenericschedule.Activation{}, fmt.Errorf("fork one-shot mutation frame belongs to another child run")
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
		admitted, err := admitWithRestorationTx(ctx, tx, attempt, postgres, request.Child.Canonical(), nil, nil, &request)
		if err != nil {
			return err
		}
		actual, found, err := loadByIDTx(ctx, tx, dialectFor(postgres), admitted.Activation.ID, postgres)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("fork one-shot lacks its exact native child readback")
		}
		expected, err := request.Expected(actual.ID)
		if err != nil {
			return err
		}
		want, err := expected.EvidenceDigest()
		if err != nil {
			return err
		}
		got, err := actual.EvidenceDigest()
		if err != nil || got != want {
			return fmt.Errorf("fork one-shot readback differs from exact unactivated child projection")
		}
		child = actual
		return nil
	})
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	return child, attempt.RequireExistingSQLFrame(ctx)
}

// Only restoration extends the ordinary INSERT. All due, lineage and terminal
// facts are present before any native read or activation validation can occur.
func forkOneShotInsert(query string, d dialect, a runtimegenericschedule.Activation, args []any) (string, []any) {
	if a.ForkOneShotOrigin == nil {
		return query, args
	}
	o := a.ForkOneShotOrigin
	query = strings.Replace(query, "status, created_at", `status, created_at,
		cancel_cause, cancelled_at, failure_code, failure_message, failed_at,
		source_timer_id, forked_from_run_id, forked_from_point_kind, forked_from_point_revision,
		forked_from_event_id, source_armed_at, reconstruction_owner`, 1)
	if d == postgresDialect {
		query = strings.Replace(query, "$29, 'active', $30)", `$29, $30, $31,
			NULLIF($32,''), $33, NULLIF($34,''), NULLIF($35,''), $36,
			$37::uuid, $38::uuid, $39, $40, NULLIF($41,'')::uuid, $42, $43)`, 1)
	} else {
		query = strings.Replace(query, "?, 'active', ?)", `?, ?, ?,
			NULLIF(?,''), ?, NULLIF(?,''), NULLIF(?,''), ?, ?, ?, ?, ?, NULLIF(?,''), ?, ?)`, 1)
	}
	args = append(args[:29], a.Status, a.AdmittedAt, a.CancelCause, optionalTime(a.CancelledAt), a.Failure.Code, a.Failure.Message, optionalTime(a.FailedAt),
		o.SourceActivationID, o.SourceRunID, o.PointKind, o.PointRevision, o.PointEventID, o.DueBasisAt, o.Owner)
	return query, args
}

func optionalTime(at time.Time) any {
	if at.IsZero() {
		return nil
	}
	return at
}

// DecodeSnapshot projects historical native storage through the live decoder;
// it additionally checks storage identity/family columns absent from that SELECT.
func DecodeSnapshot(s runforkrevision.TimerSnapshot) (runtimegenericschedule.Activation, error) {
	switch s.TaskType {
	case "timer", "scheduled_task", "global_recurring":
	default:
		return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule snapshot requires its native task family")
	}
	timeColumn := func(at *time.Time) any {
		if at == nil {
			return nil
		}
		return *at
	}
	var suspension any
	if raw := strings.TrimSpace(string(s.ClockSuspension)); raw != "" && raw != "null" {
		suspension = s.ClockSuspension
	}
	row := snapshotActivationRow{
		s.TimerID, s.ScheduleKey, s.ImmutableHash, s.RunID, s.EntityID, s.FlowInstance, s.OwnerKind, s.OwnerAgent,
		s.AgentNameOwner, s.AgentNameSource, s.AgentRoutePresence, s.AgentFlowScopeKey, s.AgentFlowInstanceID,
		s.FireEvent, s.FirePayload, s.RoutingSource, s.ExecutionMode, s.ReplyContextID, s.DueBasisKind,
		timeColumn(s.DueBasisAbsolute), s.DueBasisDuration, s.DueBasisCron, s.TaskID, s.ImmutableHash,
		s.CreatedAt, timeColumn(s.InitialFireAt), s.FireAt, s.OccurrenceEventID, timeColumn(s.OccurrenceAdmittedAt), s.Status,
		s.CancelCause, timeColumn(s.CancelledAt), timeColumn(s.FiredAt), timeColumn(s.AcceptedAt), timeColumn(s.FailedAt),
		s.FailureCode, s.FailureMessage, suspension, s.SourceTimerID, s.ForkedFromRunID, s.ForkedFromPointKind,
		s.ForkedFromPointRevision, s.ForkedFromEventID, timeColumn(s.SourceArmedAt), s.ReconstructionOwner,
	}
	a, err := scanActivationRow(row, sqliteDialect)
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	scope, err := a.Command.ScopeKey()
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	c := a.Command
	if a.ID != s.TimerID || a.ImmutableHash != s.ImmutableHash || c.RunID != s.RunID || c.EntityID != s.EntityID ||
		c.FlowInstance != s.FlowInstance || string(c.OwnerKind) != s.OwnerKind || c.OwnerID != s.OwnerAgent ||
		c.EventType != s.FireEvent || string(c.ExecutionMode) != s.ExecutionMode || c.ReplyContext != s.ReplyContextID || c.TaskID != s.TaskID ||
		string(a.Status) != s.Status || a.CancelCause != s.CancelCause || a.Failure.Code != s.FailureCode || a.Failure.Message != s.FailureMessage {
		return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule snapshot requires exact canonical command and disposition storage")
	}
	if !c.AgentIdentity.IsZero() {
		identity, err := c.AgentIdentity.StorageFields()
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		if identity.NameOwner != s.AgentNameOwner || identity.NameSource != s.AgentNameSource || identity.RoutePresence != s.AgentRoutePresence ||
			identity.FlowScopeKey != s.AgentFlowScopeKey || identity.FlowInstanceID != s.AgentFlowInstanceID {
			return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule snapshot requires exact canonical actor storage")
		}
	} else if s.AgentNameOwner != "" || s.AgentNameSource != "" || s.AgentRoutePresence != "" || s.AgentFlowScopeKey != "" || s.AgentFlowInstanceID != "" {
		return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule snapshot carries foreign actor storage")
	}
	if s.TimerName != a.Command.ScheduleKey || s.ScheduleKey != a.Command.ScheduleKey || s.ScheduleScope != scope ||
		s.Recurring != a.Command.Due.Recurring() || s.RecurrenceInterval != "" || s.OwnerNode != "" || s.FlowScopeKey != "" || s.FlowInstanceID != "" ||
		s.TaskType == "timer" && s.Recurring || s.TaskType == "scheduled_task" && s.RunID == "" ||
		s.TaskType == "global_recurring" && (!s.Recurring || s.RunID != "") {
		return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule snapshot contradicts its exact native key, scope or task family")
	}
	dueAbsolute, duration, cron := encodeDueBasis(a.Command.Due)
	if s.DueBasisKind != string(a.Command.Due.Kind) || s.DueBasisDuration != duration || s.DueBasisCron != cron ||
		(dueAbsolute == nil) != (s.DueBasisAbsolute == nil) {
		return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule snapshot contradicts its immutable due storage")
	}
	for _, at := range []any{s.CreatedAt, s.FireAt, timeColumn(s.InitialFireAt), timeColumn(s.DueBasisAbsolute),
		timeColumn(s.OccurrenceAdmittedAt), timeColumn(s.CancelledAt), timeColumn(s.FiredAt), timeColumn(s.AcceptedAt),
		timeColumn(s.FailedAt), timeColumn(s.SourceArmedAt)} {
		if at, ok := at.(time.Time); ok && (at.IsZero() || at != canonicalTime(at)) {
			return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule snapshot requires exact canonical storage coordinates")
		}
	}
	return a, nil
}

type snapshotActivationRow []any

func (row snapshotActivationRow) Scan(destinations ...any) error {
	if len(row) != len(destinations) {
		return fmt.Errorf("generic schedule snapshot projection has %d columns, want %d", len(row), len(destinations))
	}
	for i, destination := range destinations {
		switch destination := destination.(type) {
		case *string:
			*destination = row[i].(string)
		case *forkpoint.Kind:
			*destination = row[i].(forkpoint.Kind)
		case *int64:
			*destination = row[i].(int64)
		case *any:
			*destination = row[i]
		default:
			return fmt.Errorf("generic schedule snapshot has unsupported scan column %d (%T)", i, destination)
		}
	}
	return nil
}
