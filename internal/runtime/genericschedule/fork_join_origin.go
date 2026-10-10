package genericschedule

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

const ForkJoinReconstructionOwner = "store.run_fork.arrival_join_schedule"

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
