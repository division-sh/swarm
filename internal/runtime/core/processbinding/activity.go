package processbinding

import (
	"errors"
	"math"

	"github.com/google/uuid"
)

const ActivityExecutionBindingVersion = "activity-execution.v1"

// ActivityExecutionBinding names one immutable generation-grant snapshot.
// It is attribution, not execution or recovery authority. Native writers must
// validate the original snapshot and hold its currentness fences through commit.
type ActivityExecutionBinding struct {
	Version      string `json:"version"`
	GrantID      string `json:"grant_id"`
	StateVersion uint64 `json:"state_version"`
}

func (b ActivityExecutionBinding) IsZero() bool {
	return b == (ActivityExecutionBinding{})
}

func (b ActivityExecutionBinding) Validate() error {
	if b.Version != ActivityExecutionBindingVersion {
		return errors.New("activity execution binding requires version activity-execution.v1")
	}
	id, err := uuid.Parse(b.GrantID)
	if err != nil || id == uuid.Nil || id.String() != b.GrantID {
		return errors.New("activity execution binding requires an exact canonical nonzero grant UUID")
	}
	if b.StateVersion == 0 || b.StateVersion > math.MaxInt64 {
		return errors.New("activity execution binding state_version must be a positive signed 64-bit value")
	}
	return nil
}
