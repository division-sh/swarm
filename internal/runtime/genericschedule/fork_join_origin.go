package genericschedule

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

const ForkJoinReconstructionOwner = "store.run_fork.arrival_join_schedule"

type ForkJoinDisposition string

const (
	ForkJoinRetained    ForkJoinDisposition = "retained"
	ForkJoinRuleRemoved ForkJoinDisposition = "rule_removed"
)

func (d ForkJoinDisposition) Validate(source Activation) error {
	switch d {
	case ForkJoinRetained:
		return nil
	case ForkJoinRuleRemoved:
		if source.Status == StatusActive {
			return nil
		}
	}
	return fmt.Errorf("inherited join disposition requires retained history or an unfulfilled removed rule")
}

// This validates source shape, not fixed-cut publication absence or authority.
// Published occurrences require exact event/delivery continuation, not rearming.
func (a Activation) ValidateForkJoinRestorationSource() error {
	if err := a.Validate(); err != nil {
		return err
	}
	if a.Status != StatusActive && a.Status != StatusCancelled ||
		a.CurrentEventID != "" && a.CurrentEventAdmittedAt.Before(a.CurrentDueAt) {
		return fmt.Errorf("published or otherwise terminal join requires historical occurrence continuation, not schedule restoration")
	}
	return nil
}

// ForkJoinOrigin is retained provenance, not a permission to publish or resume.
// It consumes the existing timer lineage columns on live and historical reads.
type ForkJoinOrigin struct {
	SourceActivationID string
	SourceRunID        string
	PointKind          forkpoint.Kind
	PointRevision      int64
	PointEventID       string
	SourceAdmittedAt   time.Time
	Owner              string
}

func DecodeForkJoinOrigin(record ForkJoinOrigin) (*ForkJoinOrigin, error) {
	if record == (ForkJoinOrigin{}) {
		return nil, nil
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	record.SourceAdmittedAt = canonicalTime(record.SourceAdmittedAt)
	return &record, nil
}

func (o ForkJoinOrigin) Validate() error {
	for _, value := range []string{o.SourceActivationID, o.SourceRunID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("inherited join origin requires exact source activation and run UUIDs")
		}
	}
	if err := forkpoint.ValidateIdentity(o.PointKind, o.PointRevision, o.PointEventID); err != nil {
		return err
	}
	if o.Owner != ForkJoinReconstructionOwner || o.SourceAdmittedAt.IsZero() || o.SourceAdmittedAt != canonicalTime(o.SourceAdmittedAt) {
		return fmt.Errorf("inherited join origin requires its canonical owner and source admission coordinate")
	}
	return nil
}

func validateForkJoinOrigin(a Activation) error {
	if a.ForkJoinOrigin == nil {
		return nil
	}
	if err := a.ForkJoinOrigin.Validate(); err != nil {
		return err
	}
	if a.ForkJoinOrigin.SourceActivationID == a.ID || a.ForkJoinOrigin.SourceRunID == a.Command.RunID ||
		a.ForkJoinOrigin.SourceAdmittedAt.After(a.AdmittedAt) || a.ClockSuspension != nil || a.Command.ReplyContext != "" {
		return fmt.Errorf("inherited join activation contradicts its source origin")
	}
	payload, ok := a.Command.Payload.Interface().(map[string]any)
	if !ok {
		return fmt.Errorf("inherited join activation lacks its typed handle payload")
	}
	_, ref, ok := timeridentity.ParseJoinHandle(payload)
	if !ok || ref.Mode() != timeridentity.JoinRefModeArrival || ref.StageEntry().RunID != a.Command.RunID ||
		ref.StageEntry().EntityID != a.Command.EntityID || ref.StageEntry().OriginRunID == "" {
		return fmt.Errorf("inherited join activation lacks exact child stage-entry ownership")
	}
	return nil
}

// ValidateForkJoinReplay preserves the materialized cause while permitting the
// canonical child lifecycle to progress. It does not authorize that progress.
func (a Activation) ValidateForkJoinReplay(expected Activation) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if err := expected.Validate(); err != nil {
		return err
	}
	a, expected = a.Canonical(), expected.Canonical()
	if expected.ForkJoinOrigin == nil || expected.CurrentEventID != "" ||
		expected.Status != StatusActive && expected.Status != StatusCancelled {
		return fmt.Errorf("fork join replay requires an unprogressed inherited projection")
	}
	if a.Status == StatusParked || a.Status == StatusFired && a.CurrentEventID == "" {
		return fmt.Errorf("fork join progress requires a one-shot lifecycle and exact accepted occurrence")
	}
	for _, at := range []time.Time{a.CurrentEventAdmittedAt, a.CancelledAt, a.FiredAt, a.AcceptedAt, a.FailedAt} {
		if !at.IsZero() && at.Before(a.AdmittedAt) {
			return fmt.Errorf("fork join lifecycle progress precedes child admission")
		}
	}
	if !a.CurrentEventAdmittedAt.IsZero() && a.CurrentEventAdmittedAt.Before(a.CurrentDueAt) {
		return fmt.Errorf("fork join occurrence precedes its retained due coordinate")
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
		return fmt.Errorf("fork join replay changes its immutable child cause or retained disposition")
	}
	return nil
}
