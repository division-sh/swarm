package sessions

import (
	"fmt"
	"strings"
	"time"
)

type TerminationReason string

const (
	TerminationReasonNormal       TerminationReason = "normal"
	TerminationReasonCancelled    TerminationReason = "cancelled"
	TerminationReasonFailed       TerminationReason = "failed"
	TerminationReasonOrphaned     TerminationReason = "orphaned"
	TerminationReasonContaminated TerminationReason = "contaminated"
)

func (r TerminationReason) String() string { return string(r) }

func normalizeTerminationReason(raw string) TerminationReason {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case TerminationReasonNormal.String():
		return TerminationReasonNormal
	case TerminationReasonCancelled.String():
		return TerminationReasonCancelled
	case TerminationReasonFailed.String():
		return TerminationReasonFailed
	case TerminationReasonOrphaned.String():
		return TerminationReasonOrphaned
	case TerminationReasonContaminated.String():
		return TerminationReasonContaminated
	default:
		return ""
	}
}

func validateRuntimeTerminationReason(reason TerminationReason) error {
	reason = normalizeTerminationReason(reason.String())
	if reason == "" {
		return fmt.Errorf("termination reason is required")
	}
	return nil
}

type TerminationMetadata struct {
	Reason             TerminationReason
	Detail             string
	SuccessorSessionID string
	TerminatedAt       time.Time
}
