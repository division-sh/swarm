package genericschedule

import (
	"encoding/json"
	"errors"
	"time"
)

// ClockSuspension is mutable deployment-binding evidence, not authored intent
// or a new activation identity. The last completed interval survives recurrence.
type ClockSuspension struct {
	ParkedAt           time.Time `json:"parked_at,omitempty"`
	SuspendedFrom      time.Time `json:"suspended_from,omitempty"`
	ResumedAt          time.Time `json:"resumed_at,omitempty"`
	SkippedOccurrences int64     `json:"skipped_occurrences"`
}

func (s ClockSuspension) MarshalJSON() ([]byte, error) {
	timestamp := func(at time.Time) *time.Time {
		if at.IsZero() {
			return nil
		}
		return &at
	}
	return json.Marshal(struct {
		ParkedAt           *time.Time `json:"parked_at,omitempty"`
		SuspendedFrom      *time.Time `json:"suspended_from,omitempty"`
		ResumedAt          *time.Time `json:"resumed_at,omitempty"`
		SkippedOccurrences int64      `json:"skipped_occurrences"`
	}{timestamp(s.ParkedAt), timestamp(s.SuspendedFrom), timestamp(s.ResumedAt), s.SkippedOccurrences})
}

func (s *ClockSuspension) Canonical() *ClockSuspension {
	if s == nil {
		return nil
	}
	copy := *s
	copy.ParkedAt = canonicalTime(copy.ParkedAt)
	copy.SuspendedFrom = canonicalTime(copy.SuspendedFrom)
	copy.ResumedAt = canonicalTime(copy.ResumedAt)
	return &copy
}

func validateClockSuspension(a Activation) error {
	s := a.ClockSuspension
	if s == nil {
		if a.Status == StatusParked {
			return errors.New("parked clock requires suspension evidence")
		}
		return nil
	}
	if a.Command.OwnerKind != OwnerInstance || !a.Command.Due.Recurring() {
		return errors.New("only a declared clock can carry suspension evidence")
	}
	if (a.Status == StatusParked) != !s.ParkedAt.IsZero() {
		return errors.New("clock parking state and timestamp disagree")
	}
	if !s.ParkedAt.IsZero() && (s.ParkedAt.Before(a.AdmittedAt) || s.ParkedAt.Before(s.ResumedAt)) {
		return errors.New("clock parking precedes its admitted lifetime")
	}
	if s.SuspendedFrom.IsZero() != s.ResumedAt.IsZero() || s.SkippedOccurrences < 0 {
		return errors.New("clock suspension interval is incomplete")
	}
	if s.ResumedAt.IsZero() {
		if s.SkippedOccurrences != 0 || s.ParkedAt.IsZero() {
			return errors.New("clock suspension lacks an interval")
		}
		return nil
	}
	if s.SuspendedFrom.Before(a.AdmittedAt) || s.ResumedAt.Before(s.SuspendedFrom) {
		return errors.New("clock suspension interval is not chronological")
	}
	return nil
}

func (a Activation) cadenceBase() (time.Time, error) {
	if a.ClockSuspension != nil && !a.ClockSuspension.ResumedAt.IsZero() {
		return a.Command.Due.FirstDue(a.ClockSuspension.ResumedAt)
	}
	return a.InitialDueAt, nil
}
