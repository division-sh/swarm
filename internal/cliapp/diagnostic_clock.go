package cliapp

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func diagnosticClockRows(clocks []genericschedule.ClockReadback) []cliLabeledDetailRow {
	rows := make([]cliLabeledDetailRow, 0, len(clocks))
	for _, clock := range clocks {
		value := fmt.Sprintf("%s/%s: %s", clock.FlowID, clock.Name, clock.Status)
		if clock.NextDueAt != nil {
			value += "; next due " + clock.NextDueAt.UTC().Format(time.RFC3339Nano)
		}
		if clock.RetainsRun {
			value += "; retaining this run"
		}
		if clock.CancelCause != "" {
			value += "; " + clock.CancelCause
		}
		if clock.Failure != nil {
			value += "; " + clock.Failure.Message
		}
		rows = append(rows, cliLabeledDetailRow{Label: "clock", Value: value})
	}
	return rows
}
