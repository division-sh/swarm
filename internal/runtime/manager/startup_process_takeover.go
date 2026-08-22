package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

type processExecutionBindingProvider interface {
	ProcessExecutionBinding() (ProcessExecutionBinding, error)
}

// RebindLifecycleExecutionForStartup advances every lifecycle cell belonging
// to this bundle coordinate onto the exact current process generation grant.
func (am *AgentManager) RebindLifecycleExecutionForStartup(ctx context.Context) error {
	if am == nil || am.lifecycle == nil {
		return nil
	}
	store := am.lifecycle.persistence()
	provider, ok := store.(processExecutionBindingProvider)
	if !ok {
		return errors.New("startup lifecycle persistence does not expose process execution binding")
	}
	target, err := provider.ProcessExecutionBinding()
	if err != nil {
		return fmt.Errorf("load startup process execution binding: %w", err)
	}
	if err := target.Validate(); err != nil {
		return err
	}
	if am.roles.LifecycleCensus == nil {
		return errors.New("startup lifecycle persistence does not expose the durable lifecycle cell census")
	}
	states, err := am.roles.LifecycleCensus.ListDurableAgentLifecycleStates(ctx)
	if err != nil {
		return fmt.Errorf("list durable lifecycle cells for process takeover: %w", err)
	}
	sort.Slice(states, func(i, j int) bool {
		leftKey, _ := states[i].Identity.Fingerprint()
		rightKey, _ := states[j].Identity.Fingerprint()
		return leftKey < rightKey
	})
	for _, state := range states {
		identity := state.Identity.Normalize()
		if err := identity.Validate(); err != nil {
			return fmt.Errorf("durable lifecycle census identity is invalid: %w", err)
		}
		current := state.ProcessBinding
		if err := current.Validate(); err != nil {
			return fmt.Errorf("persisted lifecycle process binding for %s is invalid: %w", identity.Description(), err)
		}
		if strings.TrimSpace(current.BundleHash) != strings.TrimSpace(target.BundleHash) ||
			strings.TrimSpace(current.BundleSource) != strings.TrimSpace(target.BundleSource) {
			continue
		}
		if current.Equal(target) {
			continue
		}
		if err := commitProcessTakeover(ctx, store, identity, state, target); err != nil {
			return err
		}
	}
	return nil
}

func commitProcessTakeover(
	ctx context.Context,
	store AgentLifecyclePersistence,
	identity runtimeagentidentity.Identity,
	state AgentLifecycleState,
	target ProcessExecutionBinding,
) error {
	if state.RuntimeEpoch == int64(^uint64(0)>>1) || state.Generation == ^uint64(0) {
		return fmt.Errorf("lifecycle takeover generation exhausted for %s", identity.Description())
	}
	subordinate, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		return err
	}
	identityKey, err := identity.Fingerprint()
	if err != nil {
		return err
	}
	operationID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"agent-process-takeover-v1", target.GenerationGrantID, identityKey,
		state.ProcessBinding.GenerationGrantID, fmt.Sprint(state.RuntimeEpoch),
		fmt.Sprint(state.Generation), string(state.Phase), state.ConfigRevision,
	}, "\x00"))).String()
	requestHash := lifecycleRequestHashForIdentity(identity, state.Topology,
		"process_takeover", state.ConfigRevision, planHash,
		state.ProcessBinding.ProcessAuthorityID, state.ProcessBinding.ProcessBootID,
		target.ProcessAuthorityID, target.ProcessBootID, target.GenerationGrantID,
	)
	result, err := store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
		OperationID: operationID, OperationKind: "process_takeover", RequestHash: requestHash,
		Identity: identity, AgentID: identity.AgentID(), Trigger: "process_takeover",
		ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
		TargetEpoch: state.RuntimeEpoch + 1, TargetGeneration: state.Generation + 1, TargetPhase: state.Phase,
		ConfigRevision: state.ConfigRevision, RunMode: state.RunMode, Subordinate: subordinate,
		Topology: state.Topology, Now: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("take over lifecycle execution for %s: %w", identity.Description(), err)
	}
	if result.Identity.Normalize() != identity.Normalize() || result.PreviousEpoch != state.RuntimeEpoch ||
		result.PreviousGeneration != state.Generation || result.PreviousPhase != state.Phase ||
		result.RuntimeEpoch != state.RuntimeEpoch+1 || result.Generation != state.Generation+1 ||
		result.Phase != state.Phase || result.ConfigRevision != state.ConfigRevision ||
		result.RunMode != state.RunMode || !result.Topology.Equal(state.Topology) ||
		!result.ProcessBinding.Equal(target) {
		return fmt.Errorf("lifecycle takeover returned conflicting evidence for %s", identity.Description())
	}
	return nil
}
