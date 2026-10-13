package genericschedule

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

const ForkOneShotReconstructionOwner = "store.run_fork.one_shot_schedule"

// ForkOneShotOrigin retains the immediate source and the original due basis.
// DueBasisAt uses source_armed_at, not the child row's execution admission time.
type ForkOneShotOrigin struct {
	SourceActivationID string
	SourceRunID        string
	PointKind          forkpoint.Kind
	PointRevision      int64
	PointEventID       string
	DueBasisAt         time.Time
	Owner              string
}

func DecodeForkOneShotOrigin(record ForkOneShotOrigin) (*ForkOneShotOrigin, error) {
	if record == (ForkOneShotOrigin{}) {
		return nil, nil
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return &record, nil
}

func (o ForkOneShotOrigin) Validate() error {
	for _, value := range []string{o.SourceActivationID, o.SourceRunID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("inherited one-shot origin requires exact source activation and run UUIDs")
		}
	}
	if err := forkpoint.ValidateIdentity(o.PointKind, o.PointRevision, o.PointEventID); err != nil {
		return err
	}
	if o.Owner != ForkOneShotReconstructionOwner || o.DueBasisAt.IsZero() || o.DueBasisAt != canonicalTime(o.DueBasisAt) {
		return fmt.Errorf("inherited one-shot origin requires its canonical owner and original due basis coordinate")
	}
	return nil
}

func validateForkOneShotCommand(c AdmissionCommand) error {
	if c.OwnerKind != OwnerAgent || c.Due.Recurring() || c.EventType == "platform.join_timeout" || c.EventType == "platform.join_complete" {
		return fmt.Errorf("one-shot restoration cannot replace recurrence or join ownership")
	}
	if payload, ok := c.Payload.Interface().(map[string]any); ok {
		if _, _, join := timeridentity.ParseJoinHandle(payload); join {
			return fmt.Errorf("one-shot restoration cannot carry a join handle")
		}
	}
	return nil
}

func validateForkOneShotOrigin(a Activation) error {
	if err := a.ForkOneShotOrigin.Validate(); err != nil {
		return err
	}
	if a.ForkOneShotOrigin.SourceActivationID == a.ID || a.ForkOneShotOrigin.SourceRunID == a.Command.RunID ||
		a.ForkOneShotOrigin.DueBasisAt.After(a.AdmittedAt) || a.ClockSuspension != nil || a.ForkJoinOrigin != nil ||
		a.Status == StatusParked || a.Status == StatusFired && a.CurrentEventID == "" {
		return fmt.Errorf("inherited one-shot activation contradicts its source origin")
	}
	for _, value := range []string{a.ID, a.Command.RunID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("inherited one-shot activation requires its exact child activation and run")
		}
	}
	for _, at := range []time.Time{a.CurrentEventAdmittedAt, a.CancelledAt, a.FiredAt, a.AcceptedAt, a.FailedAt} {
		if !at.IsZero() && at.Before(a.AdmittedAt) {
			return fmt.Errorf("inherited one-shot lifecycle precedes child admission")
		}
	}
	if !a.CurrentEventAdmittedAt.IsZero() && a.CurrentEventAdmittedAt.Before(a.CurrentDueAt) {
		return fmt.Errorf("inherited one-shot occurrence precedes its retained due coordinate")
	}
	return validateForkOneShotCommand(a.Command)
}

// This validates captured shape only; the fixed-cut owner authenticates source
// evidence and publication absence. No source execution authority is adopted.
func (a Activation) ValidateForkOneShotRestorationSource() error {
	if err := a.Validate(); err != nil {
		return err
	}
	if a.Status != StatusActive && a.Status != StatusCancelled && a.Status != StatusFailed ||
		a.CurrentEventID != "" || !a.CurrentEventAdmittedAt.IsZero() || a.ClockSuspension != nil || a.ForkJoinOrigin != nil {
		return fmt.Errorf("one-shot restoration requires exact pending active, cancelled, or failed nonpublished history without clock authority")
	}
	for _, at := range []time.Time{a.CancelledAt, a.FailedAt} {
		if !at.IsZero() && at.Before(a.AdmittedAt) {
			return fmt.Errorf("one-shot source terminal history precedes admission")
		}
	}
	return validateForkOneShotCommand(a.Command)
}

// ValidateForkOneShotReplay permits canonical lifecycle progress while keeping
// the exact command, original due basis, birth, cut and retained disposition.
func (a Activation) ValidateForkOneShotReplay(expected Activation) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if err := expected.ValidateForkOneShotRestorationSource(); err != nil {
		return err
	}
	a, expected = a.Canonical(), expected.Canonical()
	if expected.ForkOneShotOrigin == nil {
		return fmt.Errorf("fork one-shot replay requires an unprogressed inherited projection")
	}
	if a.Status == StatusParked || a.Status == StatusFired && a.CurrentEventID == "" {
		return fmt.Errorf("fork one-shot progress requires exact accepted occurrence history")
	}
	for _, at := range []time.Time{a.CurrentEventAdmittedAt, a.CancelledAt, a.FiredAt, a.AcceptedAt, a.FailedAt} {
		if !at.IsZero() && at.Before(a.AdmittedAt) {
			return fmt.Errorf("fork one-shot lifecycle progress precedes child admission")
		}
	}
	if !a.CurrentEventAdmittedAt.IsZero() && a.CurrentEventAdmittedAt.Before(a.CurrentDueAt) {
		return fmt.Errorf("fork one-shot occurrence precedes its retained due coordinate")
	}
	want, err := expected.EvidenceDigest()
	if err != nil {
		return err
	}
	if expected.Status == StatusActive {
		a.CurrentEventID, a.CurrentEventAdmittedAt = expected.CurrentEventID, expected.CurrentEventAdmittedAt
		a.Status, a.CancelCause, a.CancelledAt = expected.Status, expected.CancelCause, expected.CancelledAt
		a.FiredAt, a.AcceptedAt, a.FailedAt, a.Failure = expected.FiredAt, expected.AcceptedAt, expected.FailedAt, expected.Failure
	}
	got, err := a.EvidenceDigest()
	if err != nil || got != want {
		return fmt.Errorf("fork one-shot replay changes its immutable child cause or retained disposition")
	}
	return nil
}
