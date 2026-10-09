package mutationlog

import "fmt"

// HistoryError distinguishes unusable evidence from a successfully observed
// disagreement. Neither result authorizes repairing the history or state.
type HistoryError struct {
	RunID      string `json:"run_id"`
	EntityID   string `json:"entity_id,omitempty"`
	MutationID string `json:"mutation_id,omitempty"`
	Code       string `json:"code"`
	Reason     string `json:"reason"`
}

func (e *HistoryError) Error() string {
	return fmt.Sprintf("%s: run_id=%s entity_id=%s mutation_id=%s: %s", e.Code, e.RunID, e.EntityID, e.MutationID, e.Reason)
}
