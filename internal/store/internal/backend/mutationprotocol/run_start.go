package mutationprotocol

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	privatefork "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (a *Attempt) DeclareRunStart(ctx context.Context, runID, bundleHash, firstTurnEventID string) error {
	if err := a.requireRunStartAttempt(ctx); err != nil {
		return err
	}
	key, valid := physicalRunKey(a.dialect, runID)
	if !valid {
		return fmt.Errorf("run start requires an exact physical run identity")
	}
	return a.effects.DeclareStart(key, bundleHash, firstTurnEventID)
}

// BeginInitialRunConstruction admits only the canonical root in the very
// attempt that inserted its run. Its recursive constructor writes contribute
// exact initial facts; later routing constructs outside this named boundary.
func (a *Attempt) BeginInitialRunConstruction(ctx context.Context, runID string, instance flowidentity.Instance, creatingEventID string) (bool, error) {
	if err := a.requireRunStartAttempt(ctx); err != nil {
		return false, err
	}
	key, valid := physicalRunKey(a.dialect, runID)
	if !valid {
		return false, fmt.Errorf("initial construction requires a physical run identity")
	}
	projection, declared := a.effects.DeclaredStart(key)
	if !declared {
		return false, nil
	}
	if instance.EntityID != key || instance.InstancePath != key || instance.InstanceID != key ||
		!instance.HasStoredPath || !instance.Route().Valid() || instance.ParentEntityID != "" || !instance.ParentRoute.Empty() {
		return false, nil
	}
	if projection.FirstTurnEventID != "" && projection.FirstTurnEventID != creatingEventID {
		return false, fmt.Errorf("initial root constructor differs from original creating input")
	}
	if err := a.BeginInitialRunProjection(ctx, key); err != nil {
		return false, err
	}
	return true, nil
}

// BeginInitialRunProjection bounds the semantic writes of an admitted creation
// plan. It cannot capture an existing run, borrow another attempt, or publish
// operational families. Constructor and fork materializers share this owner.
func (a *Attempt) BeginInitialRunProjection(ctx context.Context, runID string) error {
	if err := a.requireRunStartAttempt(ctx); err != nil {
		return err
	}
	key, valid := physicalRunKey(a.dialect, runID)
	_, declared := a.effects.DeclaredStart(key)
	if !valid || !declared || a.initialRunConstruction != "" {
		return fmt.Errorf("initial projection requires one exact new run in this creation attempt")
	}
	a.initialRunConstruction = key
	return nil
}

func (a *Attempt) EndInitialRunProjection(ctx context.Context, runID string) error {
	if err := a.requireRunStartAttempt(ctx); err != nil {
		return err
	}
	key, valid := physicalRunKey(a.dialect, runID)
	if !valid || a.initialRunConstruction != key {
		return fmt.Errorf("initial construction completion differs from its exact attempt/run")
	}
	a.initialRunConstruction = ""
	return nil
}

func (a *Attempt) AddRunStartFacts(ctx context.Context, runID string, refs ...privatefork.FactRef) error {
	if err := a.requireRunStartAttempt(ctx); err != nil {
		return err
	}
	key, valid := physicalRunKey(a.dialect, runID)
	if !valid {
		return fmt.Errorf("run start requires an exact physical run identity")
	}
	return a.effects.AddStartFacts(key, refs...)
}

func (a *Attempt) requireRunStartAttempt(ctx context.Context) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if a.kind != Ordinary || a.cleanup {
		return fmt.Errorf("run start requires an ordinary creation attempt before finalization")
	}
	current, err := runAdmissionAttempt(ctx, a.tx)
	if err != nil {
		return err
	}
	if current != a {
		return fmt.Errorf("run start belongs to another mutation attempt")
	}
	return nil
}
