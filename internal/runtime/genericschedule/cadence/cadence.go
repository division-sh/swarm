// Package cadence owns recurrence admission and persisted-time progression.
// It is shared by source compilation and the generic schedule lifecycle.
package cadence

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

func ParseCron(spec string) (cron.Schedule, error) {
	if len(strings.Fields(spec)) != 5 || strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ=") {
		return nil, errors.New("cron requires exactly five UTC fields; descriptors and timezone prefixes are not supported")
	}
	parsed, err := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("invalid UTC cron expression: %w", err)
	}
	// Fixed UTC time makes declaration admission deterministic. Arming and
	// restoration must also check progression from their actual persisted time.
	probe := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	if _, err := Advance(probe, parsed.Next(probe)); err != nil {
		return nil, fmt.Errorf("cron has no executable future occurrence: %w", err)
	}
	return parsed, nil
}

func ValidateEvery(interval time.Duration) error {
	if interval <= 0 {
		return errors.New("every schedule interval must be positive")
	}
	if interval%time.Microsecond != 0 {
		return errors.New("every schedule interval must be exactly representable in persisted microseconds")
	}
	return nil
}

func Advance(previous, next time.Time) (time.Time, error) {
	next = next.UTC().Truncate(time.Microsecond)
	if next.IsZero() || !next.After(previous) {
		return time.Time{}, errors.New("schedule recurrence must advance persisted time")
	}
	return next, nil
}
