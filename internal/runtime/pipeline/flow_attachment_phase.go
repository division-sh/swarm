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
}

func (r FlowAttachmentAdvanceResult) Admitted() bool {
	return r.Acknowledged && (r.Progress == FlowAttachmentAdvanced || r.Progress == FlowAttachmentAlreadyAdvanced)
}
