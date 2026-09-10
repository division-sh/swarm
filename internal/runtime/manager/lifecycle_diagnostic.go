package manager

import (
	"context"
	"errors"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type LifecycleDiagnosticOwner string
type LifecycleDiagnosticCausality string

const (
	LifecycleDiagnosticNormal        LifecycleDiagnosticOwner     = "normal"
	LifecycleDiagnosticSelectedFork  LifecycleDiagnosticOwner     = "selected_contract_fork"
	LifecycleDiagnosticObservation   LifecycleDiagnosticCausality = "observation"
	LifecycleDiagnosticAcceptedEvent LifecycleDiagnosticCausality = "accepted_event"
)

// LifecycleDiagnosticOrigin is supplied by the lifecycle entrance, then checked
// against durable owners in the transition transaction. It is never caller log context.
type LifecycleDiagnosticOrigin struct {
	Owner         LifecycleDiagnosticOwner                     `json:"owner"`
	Causality     LifecycleDiagnosticCausality                 `json:"causality"`
	ParentEventID string                                       `json:"parent_event_id,omitempty"`
	SelectedFork  runtimeeffects.SelectedContractForkAuthority `json:"selected_fork"`
	SourceRunID   string                                       `json:"source_run_id,omitempty"`
	ForkEventID   string                                       `json:"fork_event_id,omitempty"`
}

func (o LifecycleDiagnosticOrigin) Validate() error {
	switch o.Owner {
	case LifecycleDiagnosticNormal:
		if o.SelectedFork != (runtimeeffects.SelectedContractForkAuthority{}) || o.SourceRunID != "" || o.ForkEventID != "" {
			return errors.New("normal lifecycle diagnostic cannot carry selected-fork authority")
		}
	case LifecycleDiagnosticSelectedFork:
		for _, id := range []string{o.SelectedFork.ExecutionID, o.SelectedFork.ForkRunID, o.SourceRunID, o.ForkEventID} {
			if _, err := uuid.Parse(id); err != nil {
				return fmt.Errorf("selected lifecycle diagnostic identity: %w", err)
			}
		}
		if o.SelectedFork.Generation == 0 || o.SelectedFork.AdmissionFingerprint == "" || o.SelectedFork.ContainerPlanFingerprint == "" || o.SelectedFork.ActorCensusFingerprint == "" || o.SelectedFork.EffectiveConfigFingerprint == "" {
			return errors.New("selected lifecycle diagnostic requires exact execution fingerprints")
		}
	default:
		return errors.New("lifecycle diagnostic execution owner is required")
	}
	switch o.Causality {
	case LifecycleDiagnosticObservation:
		if o.ParentEventID != "" {
			return errors.New("lifecycle observation cannot carry a causal parent")
		}
	case LifecycleDiagnosticAcceptedEvent:
		if _, err := uuid.Parse(o.ParentEventID); err != nil {
			return fmt.Errorf("lifecycle diagnostic parent: %w", err)
		}
	default:
		return errors.New("lifecycle diagnostic causality is required")
	}
	return nil
}

type LifecycleDiagnosticProvenance struct {
	Origin    LifecycleDiagnosticOrigin `json:"origin"`
	ActorMode executionmode.Mode        `json:"actor_mode"`
	EventMode executionmode.Mode        `json:"event_mode"`
	BindingID string                    `json:"binding_id,omitempty"`
}

func (p LifecycleDiagnosticProvenance) Validate() error {
	if err := p.Origin.Validate(); err != nil {
		return err
	}
	if !p.ActorMode.Valid() || !p.EventMode.Valid() {
		return errors.New("lifecycle diagnostic execution modes are required")
	}
	if p.Origin.Causality == LifecycleDiagnosticObservation && p.EventMode != p.ActorMode {
		return errors.New("lifecycle observation mode differs from actor snapshot")
	}
	if p.Origin.Owner == LifecycleDiagnosticSelectedFork {
		if _, err := uuid.Parse(p.BindingID); err != nil {
			return fmt.Errorf("lifecycle diagnostic binding: %w", err)
		}
	} else if p.BindingID != "" {
		return errors.New("normal lifecycle diagnostic cannot carry a fork binding")
	}
	return nil
}

func (c *agentLifecycleCoordinator) commitLifecycleTransition(ctx context.Context, store AgentLifecyclePersistence, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	origin := c.diagnosticOrigin
	if event, ok := runtimebus.InboundEventFromContext(ctx); ok {
		origin.Causality, origin.ParentEventID = LifecycleDiagnosticAcceptedEvent, event.ID()
	}
	if err := origin.Validate(); err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	req.DiagnosticOrigin = origin
	return store.CommitAgentLifecycleTransition(ctx, req)
}
