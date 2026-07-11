package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type cliReadWindowBoundInput struct {
	Value string
	Set   bool
}

type cliReadWindowInput struct {
	Since        cliReadWindowBoundInput
	Until        cliReadWindowBoundInput
	ReferenceUTC time.Time
}

type cliReadWindow struct {
	Since *time.Time
	Until *time.Time
}

func captureCLIReadWindowReference(opts rootCommandOptions) time.Time {
	now := time.Now
	if opts.now != nil {
		now = opts.now
	}
	return now().UTC()
}

func resolveCLIReadWindow(input cliReadWindowInput) (cliReadWindow, error) {
	if input.ReferenceUTC.IsZero() {
		return cliReadWindow{}, fmt.Errorf("read-window reference time is required")
	}
	reference := input.ReferenceUTC.UTC()
	window := cliReadWindow{}
	if input.Since.Set {
		since, err := resolveCLIReadWindowBound("--since", input.Since.Value, reference)
		if err != nil {
			return cliReadWindow{}, err
		}
		window.Since = &since
	}
	if input.Until.Set {
		until, err := resolveCLIReadWindowBound("--until", input.Until.Value, reference)
		if err != nil {
			return cliReadWindow{}, err
		}
		window.Until = &until
	}
	if window.Since != nil && window.Until != nil && window.Since.After(*window.Until) {
		return cliReadWindow{}, fmt.Errorf("--until must be greater than or equal to --since")
	}
	return window, nil
}

func resolveCLIReadWindowBound(name, raw string, reference time.Time) (time.Time, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return time.Time{}, fmt.Errorf("%s must not be empty or contain surrounding whitespace", name)
	}
	if absolute, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return absolute.UTC(), nil
	}
	duration, ok := parseCLIReadWindowDuration(raw)
	if !ok {
		return time.Time{}, fmt.Errorf("%s must be an RFC3339 timestamp or a positive relative duration using lowercase h, m, s, or ms", name)
	}
	return reference.Add(-duration).UTC(), nil
}

func parseCLIReadWindowDuration(raw string) (time.Duration, bool) {
	if raw == "" {
		return 0, false
	}
	var total time.Duration
	for offset := 0; offset < len(raw); {
		digitStart := offset
		for offset < len(raw) && raw[offset] >= '0' && raw[offset] <= '9' {
			offset++
		}
		if digitStart == offset {
			return 0, false
		}
		amount, err := strconv.ParseUint(raw[digitStart:offset], 10, 64)
		if err != nil || amount == 0 {
			return 0, false
		}

		unit := time.Duration(0)
		switch {
		case strings.HasPrefix(raw[offset:], "ms"):
			unit = time.Millisecond
			offset += 2
		case offset < len(raw) && raw[offset] == 's':
			unit = time.Second
			offset++
		case offset < len(raw) && raw[offset] == 'm':
			unit = time.Minute
			offset++
		case offset < len(raw) && raw[offset] == 'h':
			unit = time.Hour
			offset++
		default:
			return 0, false
		}

		if amount > uint64(math.MaxInt64)/uint64(unit) {
			return 0, false
		}
		component := time.Duration(amount) * unit
		if total > time.Duration(math.MaxInt64)-component {
			return 0, false
		}
		total += component
	}
	return total, total > 0
}

func (window cliReadWindow) addParams(params map[string]any) {
	if window.Since != nil {
		params["since"] = window.Since.UTC().Format(time.RFC3339Nano)
	}
	if window.Until != nil {
		params["until"] = window.Until.UTC().Format(time.RFC3339Nano)
	}
}
