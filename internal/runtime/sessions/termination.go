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
	TerminationReasonLegacy       TerminationReason = "legacy"
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
	case TerminationReasonLegacy.String():
		return TerminationReasonLegacy
	default:
		return ""
	}
}

func ParseTerminationReason(raw string) (TerminationReason, error) {
	reason := normalizeTerminationReason(raw)
	if reason == "" {
		return "", fmt.Errorf("invalid termination reason %q", strings.TrimSpace(raw))
	}
	return reason, nil
}

func validateRuntimeTerminationReason(reason TerminationReason) error {
	reason = normalizeTerminationReason(reason.String())
	if reason == "" {
		return fmt.Errorf("termination reason is required")
	}
	if reason == TerminationReasonLegacy {
		return fmt.Errorf("termination reason legacy is reserved for migration backfill")
	}
	return nil
}

func rotationTermination(reason string) (TerminationReason, string, error) {
	detail := strings.TrimSpace(reason)
	switch detail {
	case "":
		return TerminationReasonNormal, "", nil
	case "session in use", "session not found":
		return TerminationReasonContaminated, detail, nil
	default:
		return TerminationReasonFailed, detail, nil
	}
}

type TerminationMetadata struct {
	Reason             TerminationReason
	Detail             string
	SuccessorSessionID string
	TerminatedAt       time.Time
}

func (m TerminationMetadata) ValidateForWrite() error {
	if err := validateRuntimeTerminationReason(m.Reason); err != nil {
		return err
	}
	if m.TerminatedAt.IsZero() {
		return fmt.Errorf("terminated_at is required")
	}
	return nil
}
