package runcontrol

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type SelectedForkRecoveryRequest struct {
	Entry   runfork.SelectedForkRecoveryEntry
	Process startupownership.Authority
	Effects effects.RecoveryRequest
}

func (r SelectedForkRecoveryRequest) Validate() error {
	if err := r.Process.Validate(); err != nil {
		return err
	}
	if r.Process.State != startupownership.StateActive {
		return fmt.Errorf("selected recovery requires active process possession")
	}
	if err := r.Effects.Validate(); err != nil {
		return err
	}
	if err := bundleidentity.ValidateCanonicalHash(r.Entry.BundleHash); err != nil {
		return err
	}
	b := r.Entry.Binding
	for _, value := range []string{b.BindingID, b.ForkRunID, b.SourceRunID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("selected recovery requires exact canonical binding")
		}
	}
	if b.Owner != runfork.RunForkSelectedContractBindingOwner || b.CreatedAt.IsZero() || b.ForkRunID == b.SourceRunID {
		return fmt.Errorf("selected recovery binding is invalid")
	}
	if err := b.ForkPoint.Validate(); err != nil {
		return fmt.Errorf("selected recovery fork point: %w", err)
	}
	if b.ForkEventID != b.ForkPoint.EventID {
		return fmt.Errorf("selected recovery binding event differs from its fork point")
	}
	if b.ForkPoint.Kind == runfork.RunForkPointEvent {
		id, err := uuid.Parse(b.ForkEventID)
		if err != nil || id == uuid.Nil || id.String() != b.ForkEventID {
			return fmt.Errorf("selected recovery requires exact canonical fork event")
		}
	}
	switch b.ContractSelection.Mode {
	case runfork.RunForkContractSelectionModeSelectedContracts:
		if b.ContractSelection.BundleHash != "" {
			return fmt.Errorf("selected recovery source selection carries replacement hash")
		}
	case runfork.RunForkContractSelectionModeBundleHash:
		if b.ContractSelection.BundleHash != r.Entry.BundleHash {
			return fmt.Errorf("selected recovery replacement differs from run source")
		}
	default:
		return fmt.Errorf("selected recovery selection is invalid")
	}
	return nil
}
