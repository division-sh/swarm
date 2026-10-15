package startuprecovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

type AgentRecoveryReader interface {
	deliverylifecycle.ActiveSnapshotReader
	agenttopology.SourceSetHeadReader
	manager.AgentLifecycleStateReader
	bus.RunLifecycleReadPersistence
	runlifecycle.StandingRestartDispositionReader
	InspectSnapshot(context.Context, func(context.Context) error) error
	InspectAuthority(context.Context) (startupownership.AuthorityInspection, error)
	ObserveOrdinaryRunSource(context.Context, correlation.SourceArtifactFact, string) (bool, error)
	ReadFirstAgentClaimProfile(context.Context, deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error)
	ReadSelectedAgentPreparation(context.Context, deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error)
}

type AgentRecoveryCaptureKind string

const (
	AgentRecoveryUnclaimed           AgentRecoveryCaptureKind = "unclaimed"
	AgentRecoveryOrdinaryFirstClaim  AgentRecoveryCaptureKind = "ordinary_first_claim"
	AgentRecoverySelectedPreparation AgentRecoveryCaptureKind = "selected_preparation"
)

type AgentRecoveryMember struct {
	Delivery    deliverylifecycle.Snapshot
	Run         bus.RunLifecycleSnapshot
	Lifecycle   manager.AgentLifecycleState
	Restart     runlifecycle.StandingRestartDisposition
	CaptureKind AgentRecoveryCaptureKind
	FirstClaim  *deliverylifecycle.AgentClaimProfile
	Selected    *runfork.SelectedForkPreparedActor
}

// AgentRecoveryImage is a coherent durable observation, not possession,
// occupied-slot accounting, claim eligibility or permission to launch work.
type AgentRecoveryImage struct {
	SourceHead agenttopology.SourceSetPlan
	Process    startupownership.AuthorityInspection
	Members    []AgentRecoveryMember
}

// InspectAgentRecoveryImage publishes nothing unless every required read and
// the original inspection transaction finish successfully.
func InspectAgentRecoveryImage(ctx context.Context, reader AgentRecoveryReader) (AgentRecoveryImage, error) {
	if ctx == nil || reader == nil {
		return AgentRecoveryImage{}, errors.New("agent recovery image requires context and selected readers")
	}
	if err := ctx.Err(); err != nil {
		return AgentRecoveryImage{}, err
	}
	var image AgentRecoveryImage
	err := reader.InspectSnapshot(ctx, func(snapshot context.Context) error {
		var err error
		image, err = readAgentRecoveryImage(snapshot, reader)
		return err
	})
	if err != nil {
		return AgentRecoveryImage{}, err
	}
	if err := ctx.Err(); err != nil {
		return AgentRecoveryImage{}, err
	}
	return image, nil
}

func readAgentRecoveryImage(ctx context.Context, reader AgentRecoveryReader) (AgentRecoveryImage, error) {
	head, found, err := reader.InspectSourceSetHead(ctx)
	if err != nil {
		return AgentRecoveryImage{}, fmt.Errorf("inspect agent recovery source head: %w", err)
	}
	if !found || head.Validate() != nil {
		return AgentRecoveryImage{}, errors.New("agent recovery requires a complete valid source head")
	}
	stamp, err := reader.InspectAuthority(ctx)
	if err != nil {
		return AgentRecoveryImage{}, fmt.Errorf("inspect agent recovery process stamp: %w", err)
	}
	if stamp.Validate() != nil || stamp.Authority == nil || stamp.Possession != nil {
		return AgentRecoveryImage{}, errors.New("agent recovery requires a durable process stamp without inferred possession")
	}
	active, err := reader.ReadActiveDeliverySnapshots(ctx)
	if err != nil {
		return AgentRecoveryImage{}, fmt.Errorf("inspect complete agent recovery membership: %w", err)
	}
	if active == nil {
		return AgentRecoveryImage{}, errors.New("agent recovery membership is incomplete")
	}
	image := AgentRecoveryImage{SourceHead: head, Process: stamp, Members: make([]AgentRecoveryMember, 0)}
	seen := make(map[string]bool, len(active))
	for _, snapshot := range active {
		if err := ctx.Err(); err != nil {
			return AgentRecoveryImage{}, err
		}
		if err := validateAgentRecoveryMembership(snapshot); err != nil {
			return AgentRecoveryImage{}, err
		}
		if seen[snapshot.DeliveryID] {
			return AgentRecoveryImage{}, fmt.Errorf("agent recovery membership duplicates delivery %s", snapshot.DeliveryID)
		}
		seen[snapshot.DeliveryID] = true
		if snapshot.SubscriberClass == deliverylifecycle.SubscriberNode {
			continue
		}
		member, err := readAgentRecoveryMember(ctx, reader, head, snapshot)
		if err != nil {
			return AgentRecoveryImage{}, fmt.Errorf("inspect agent recovery delivery %s: %w", snapshot.DeliveryID, err)
		}
		image.Members = append(image.Members, member)
	}
	return image, nil
}

func validateAgentRecoveryMembership(snapshot deliverylifecycle.Snapshot) error {
	obligation, err := deliverylifecycle.NewObligation(snapshot.EventID, snapshot.RunID, snapshot.Route, snapshot.Authority)
	if err != nil {
		return fmt.Errorf("agent recovery membership identity: %w", err)
	}
	if snapshot.DeliveryID != obligation.DeliveryID() || snapshot.RouteIdentity != obligation.RouteIdentity() ||
		snapshot.SubscriberID != obligation.SubscriberID() || snapshot.SubscriberClass != obligation.SubscriberClass() {
		return errors.New("agent recovery membership differs from exact delivery identity")
	}
	switch snapshot.Status {
	case deliverylifecycle.StatusPending:
		if snapshot.ClaimVersion != 0 {
			return errors.New("agent recovery pending delivery has claimed history")
		}
	case deliverylifecycle.StatusInProgress, deliverylifecycle.StatusFailed:
		if snapshot.ClaimVersion < 1 {
			return errors.New("agent recovery claimed delivery lacks its claim generation")
		}
	default:
		return errors.New("agent recovery active membership contains terminal or unknown state")
	}
	return nil
}

func readAgentRecoveryMember(ctx context.Context, reader AgentRecoveryReader, head agenttopology.SourceSetPlan, snapshot deliverylifecycle.Snapshot) (AgentRecoveryMember, error) {
	member := AgentRecoveryMember{Delivery: snapshot, CaptureKind: AgentRecoveryUnclaimed}
	var err error
	member.Run, err = reader.LoadRunLifecycleSnapshot(ctx, snapshot.RunID)
	if err != nil {
		return AgentRecoveryMember{}, err
	}
	if _, err := runlifecycle.ValidatePersistedState(member.Run.Status, member.Run.Failure); err != nil || member.Run.RunID != snapshot.RunID {
		return AgentRecoveryMember{}, errors.New("agent recovery run differs from its exact delivery")
	}
	member.Lifecycle, err = readAgentRecoveryLifecycle(ctx, reader, snapshot)
	if err != nil {
		return AgentRecoveryMember{}, err
	}
	if snapshot.Authority.Kind() == deliverylifecycle.ExecutionAuthoritySelectedContractFork {
		if err := readAgentRecoverySelected(ctx, reader, &member); err != nil {
			return AgentRecoveryMember{}, err
		}
		return member, nil
	}
	if err := readAgentRecoveryOrdinary(ctx, reader, head, &member); err != nil {
		return AgentRecoveryMember{}, err
	}
	return member, nil
}

func readAgentRecoveryLifecycle(ctx context.Context, reader AgentRecoveryReader, snapshot deliverylifecycle.Snapshot) (manager.AgentLifecycleState, error) {
	state, found, err := reader.LoadAgentLifecycleState(ctx, snapshot.Route.AgentIdentity)
	if err != nil {
		return manager.AgentLifecycleState{}, err
	}
	if !found || state.Identity != snapshot.Route.AgentIdentity || state.AgentID != snapshot.SubscriberID ||
		state.RuntimeEpoch < 1 || state.Generation == 0 || state.ConfigRevision == "" || state.Topology.Validate() != nil ||
		state.ProcessBinding.Validate() != nil || state.ProcessBinding.BundleHash != snapshot.Authority.SourceArtifact().BundleHash() {
		return manager.AgentLifecycleState{}, errors.New("agent recovery lifecycle/source generation is missing, corrupt or foreign")
	}
	return state, nil
}

func readAgentRecoverySelected(ctx context.Context, reader AgentRecoveryReader, member *AgentRecoveryMember) error {
	snapshot := member.Delivery
	declared := member.Lifecycle.Topology.Authority.Selected
	if member.Lifecycle.Topology.Authority.Kind != agenttopology.AuthoritySelectedForkDeclarationPlan || declared == nil ||
		declared.RunID != snapshot.RunID || declared.BundleHash != snapshot.Authority.SourceArtifact().BundleHash() {
		return errors.New("agent recovery selected declaration differs from exact fork/source")
	}
	if snapshot.ClaimVersion == 0 {
		return nil
	}
	prepared, err := reader.ReadSelectedAgentPreparation(ctx, snapshot)
	if err != nil {
		return err
	}
	identity, err := snapshot.Route.AgentIdentity.Plan()
	if err != nil || prepared.Plan != identity || prepared.ConfigurationRevision != member.Lifecycle.ConfigRevision || prepared.Backend == "" {
		return errors.New("agent recovery selected preparation is missing, corrupt or foreign")
	}
	member.CaptureKind, member.Selected = AgentRecoverySelectedPreparation, &prepared
	return nil
}

func readAgentRecoveryOrdinary(ctx context.Context, reader AgentRecoveryReader, head agenttopology.SourceSetPlan, member *AgentRecoveryMember) error {
	snapshot := member.Delivery
	source := snapshot.Authority.SourceArtifact()
	owned, err := reader.ObserveOrdinaryRunSource(ctx, source, snapshot.RunID)
	if err != nil {
		return err
	}
	if !owned || !agentRecoveryHeadContainsSource(head, source.BundleHash()) {
		return errors.New("agent recovery ordinary source/run is missing or foreign")
	}
	member.Restart, err = reader.StandingRunRestartDisposition(ctx, snapshot.RunID)
	if err != nil {
		return err
	}
	if member.Restart.Validate() != nil || member.Restart.ExactCurrent() &&
		(member.Restart.RunID != snapshot.RunID || member.Restart.RunState != member.Run.Status) {
		return errors.New("agent recovery standing generation differs from exact run")
	}
	if err := validateAgentRecoveryOrdinaryTopology(head, *member); err != nil {
		return err
	}
	if snapshot.ClaimVersion == 0 {
		return nil
	}
	profile, err := reader.ReadFirstAgentClaimProfile(ctx, snapshot)
	if err != nil {
		return err
	}
	if !profile.MatchesDelivery(snapshot) || profile.AgentRuntimeEpoch > member.Lifecycle.RuntimeEpoch || profile.AgentGeneration > member.Lifecycle.Generation {
		return errors.New("agent recovery first capture differs from exact delivery/generation history")
	}
	if profile.AgentRuntimeEpoch == member.Lifecycle.RuntimeEpoch && profile.AgentGeneration == member.Lifecycle.Generation &&
		profile.ConfigurationRevision != member.Lifecycle.ConfigRevision {
		return errors.New("agent recovery unchanged agent generation has contradictory captured configuration")
	}
	member.CaptureKind, member.FirstClaim = AgentRecoveryOrdinaryFirstClaim, &profile
	return nil
}

func agentRecoveryHeadContainsSource(head agenttopology.SourceSetPlan, bundleHash string) bool {
	for _, source := range head.Sources {
		if source.BundleHash == bundleHash {
			return true
		}
	}
	return false
}

func validateAgentRecoveryOrdinaryTopology(head agenttopology.SourceSetPlan, member AgentRecoveryMember) error {
	authority := member.Lifecycle.Topology.Authority
	switch authority.Kind {
	case agenttopology.AuthorityStaticDeclarationPlan:
		if authority.Static.BundleHash != member.Delivery.Authority.SourceArtifact().BundleHash() {
			return errors.New("agent recovery static source differs from exact delivery")
		}
		identity, err := member.Lifecycle.Identity.Plan()
		if err != nil {
			return err
		}
		for _, desired := range head.Agents {
			if desired.Identity == identity && desired.ConfigRevision == member.Lifecycle.ConfigRevision && desired.Source.BundleHash == authority.Static.BundleHash {
				return nil
			}
		}
		return errors.New("agent recovery static configuration is missing from complete source head")
	case agenttopology.AuthorityFlowReadinessPlan:
		if authority.Readiness.RunID == member.Delivery.RunID && authority.Readiness.InstancePath == member.Lifecycle.Identity.FlowInstance() {
			return nil
		}
		return errors.New("agent recovery readiness differs from exact run/instance")
	default:
		return errors.New("agent recovery ordinary delivery has a foreign topology owner")
	}
}
