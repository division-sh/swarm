package pipeline

import (
	"errors"
	"fmt"
)

var ErrFlowAttachmentStale = errors.New("flow activation attempt is no longer current")

type FlowAttachmentPhase string

const (
	FlowAttachmentPlanned          FlowAttachmentPhase = "planned"
	FlowAttachmentAgentsRegistered FlowAttachmentPhase = "agents_registered"
	FlowAttachmentRouteInstalled   FlowAttachmentPhase = "route_installed"
	FlowAttachmentTimersArmed      FlowAttachmentPhase = "timers_armed"
	FlowAttachmentReady            FlowAttachmentPhase = "ready"
)

func (p FlowAttachmentPhase) Next() (FlowAttachmentPhase, error) {
	switch p {
	case FlowAttachmentPlanned:
		return FlowAttachmentAgentsRegistered, nil
	case FlowAttachmentAgentsRegistered:
		return FlowAttachmentRouteInstalled, nil
	case FlowAttachmentRouteInstalled:
		return FlowAttachmentTimersArmed, nil
	case FlowAttachmentTimersArmed:
		return FlowAttachmentReady, nil
	default:
		return "", fmt.Errorf("attachment phase %q has no successor", p)
	}
}

func (p FlowAttachmentPhase) Valid() bool {
	if p == FlowAttachmentReady {
		return true
	}
	_, err := p.Next()
	return err == nil
}

func (p FlowAttachmentPhase) Includes(previous FlowAttachmentPhase) bool {
	for cursor := previous; cursor.Valid(); {
		if cursor == p {
			return true
		}
		next, err := cursor.Next()
		if err != nil {
			break
		}
		cursor = next
	}
	return false
}

type FlowAttachmentProgress string

const (
	FlowAttachmentAdvanced        FlowAttachmentProgress = "advanced"
	FlowAttachmentAlreadyAdvanced FlowAttachmentProgress = "already_advanced"
	FlowAttachmentStale           FlowAttachmentProgress = "stale"
	FlowAttachmentIneligible      FlowAttachmentProgress = "ineligible"
)

type FlowAttachmentAdvanceResult struct {
	Progress     FlowAttachmentProgress
	Phase        FlowAttachmentPhase
	Acknowledged bool
	Terminal     bool
	cancellation *flowAttachmentCancellation
}

type flowAttachmentCancellation struct {
	cause         error
	beforeAttempt bool
}

func (e *flowAttachmentCancellation) Error() string { return e.cause.Error() }
func (e *flowAttachmentCancellation) Unwrap() error { return e.cause }

// WithCleanCancellation carries the native owner's completed non-COMMIT
// disposition. Only the attachment persistence owner supplies this result.
func (r FlowAttachmentAdvanceResult) WithCleanCancellation(cause error, beforeAttempt bool) (FlowAttachmentAdvanceResult, error) {
	if cause == nil || r.Acknowledged {
		return r, cause
	}
	r.cancellation = &flowAttachmentCancellation{cause: cause, beforeAttempt: beforeAttempt}
	return r, r.cancellation
}

// CleanCancellation accepts only the exact returned error; a joined independent
// failure cannot inherit the native cancellation disposition.
func (r FlowAttachmentAdvanceResult) CleanCancellation(err error) (beforeAttempt, ok bool) {
	matched, ok := err.(*flowAttachmentCancellation)
	if !ok || r.Acknowledged || matched == nil || matched != r.cancellation {
		return false, false
	}
	return matched.beforeAttempt, true
}

func (r FlowAttachmentAdvanceResult) Admitted() bool {
	return r.Acknowledged && (r.Progress == FlowAttachmentAdvanced || r.Progress == FlowAttachmentAlreadyAdvanced)
}
