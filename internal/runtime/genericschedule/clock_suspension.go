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

func ParkClock(activation Activation, at time.Time) (Activation, error) {
	activation = activation.Canonical()
	if err := activation.Validate(); err != nil {
		return Activation{}, err
	}
	at = canonicalTime(at)
	if activation.Command.OwnerKind != OwnerInstance || activation.Status != StatusActive || at.Before(activation.AdmittedAt) {
		return Activation{}, errors.New("parking requires an active declared clock and admitted time")
	}
	if activation.ClockSuspension == nil {
		activation.ClockSuspension = &ClockSuspension{}
	}
	activation.ClockSuspension.ParkedAt = at
	activation.Status = StatusParked
	return activation, activation.Validate()
}

func ResumeClock(activation Activation, at time.Time) (Activation, error) {
	activation = activation.Canonical()
	if err := activation.Validate(); err != nil {
		return Activation{}, err
	}
	at = canonicalTime(at)
	if activation.Command.OwnerKind != OwnerInstance || activation.Status != StatusParked || at.Before(activation.ClockSuspension.ParkedAt) {
		return Activation{}, errors.New("resumption requires a parked declared clock and chronological time")
	}
	skipped, err := skippedClockOccurrences(activation.Command.Due, activation.CurrentDueAt, activation.ClockSuspension.ParkedAt, at)
	if err != nil {
		return Activation{}, err
	}
	next, err := activation.Command.Due.FirstDue(at)
	if err != nil {
		return Activation{}, err
	}
	activation.ClockSuspension.SuspendedFrom = activation.ClockSuspension.ParkedAt
	activation.ClockSuspension.ResumedAt = at
	activation.ClockSuspension.SkippedOccurrences = skipped
	activation.ClockSuspension.ParkedAt = time.Time{}
	activation.Status = StatusActive
	activation.CurrentDueAt = next
	activation.CurrentEventID, activation.CurrentEventAdmittedAt = "", time.Time{}
	return activation, activation.Validate()
}

func skippedClockOccurrences(due DueBasis, next, from, through time.Time) (int64, error) {
	if next.After(through) {
		return 0, nil
	}
	if due.Kind == DueEvery {
		if next.Before(from) {
			distance := from.UnixMicro() - next.UnixMicro()
			steps := (distance-1)/due.Every.Microseconds() + 1
			next = time.UnixMicro(next.UnixMicro() + steps*due.Every.Microseconds())
		}
		if next.After(through) {
			return 0, nil
		}
		return (through.UnixMicro()-next.UnixMicro())/due.Every.Microseconds() + 1, nil
	}
	if next.Before(from) {
		var err error
		next, err = due.FirstDue(from.Add(-time.Microsecond))
		if err != nil {
			return 0, err
		}
	}
	var skipped int64
	for !next.After(through) {
		skipped++
		var err error
		next, err = due.Next(next)
		if err != nil {
			return 0, err
		}
	}
	return skipped, nil
}
