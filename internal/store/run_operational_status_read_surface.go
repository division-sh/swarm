package store

import "strings"

type RunOperationalStatus struct {
	State          string
	BlockingLayer  string
	BlockingReason string
	Heuristics     []string
}

// ProjectRunOperationalStatus owns supported run-status semantics above the
// canonical persisted run-debug evidence surface.
func ProjectRunOperationalStatus(report RunDebugReport) RunOperationalStatus {
	out := RunOperationalStatus{
		Heuristics: runOperationalStatusHeuristics(report),
	}

	status := strings.ToLower(strings.TrimSpace(report.RunTableStatus))
	if status == "" {
		return out
	}
	if status != "running" {
		out.State = status
		return out
	}

	eventCounts := map[string]int{}
	for _, item := range report.EventCounts {
		eventCounts[strings.TrimSpace(item.EventName)] = item.Count
	}

	activeDeliveries := 0
	for _, item := range report.Deliveries {
		switch strings.ToLower(strings.TrimSpace(item.Status)) {
		case "pending", "in_progress":
			activeDeliveries += item.Count
		}
	}

	terminalScoring := eventCounts["scoring/vertical.marginal"] +
		eventCounts["scoring/vertical.rejected"] +
		eventCounts["scoring/vertical.shortlisted"]
	if activeDeliveries == 0 && eventCounts["scoring/scoring.requested"] > 0 && terminalScoring == 0 {
		out.State = "stalled"
		out.BlockingLayer = "scoring_terminal_outcome"
		out.BlockingReason = "terminal_scoring_outcome_missing"
		return out
	}
	if activeDeliveries == 0 && !report.LastEventAt.IsZero() {
		out.State = "stalled"
		out.BlockingLayer = "delivery_lifecycle"
		out.BlockingReason = "no_active_deliveries"
		return out
	}

	out.State = "running"
	return out
}

func runOperationalStatusHeuristics(report RunDebugReport) []string {
	if len(report.DeadLetters) == 0 {
		return nil
	}
	return []string{"dead letters exist for this run"}
}
