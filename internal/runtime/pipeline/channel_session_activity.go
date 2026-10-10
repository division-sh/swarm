package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	channelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	correlation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func (pc *PipelineCoordinator) prepareSessionActivityIntents(ctx context.Context, intents []engine.ActivityIntent) ([]engine.ActivityIntent, func(), error) {
	prepared := append([]engine.ActivityIntent(nil), intents...)
	var leases []*channelactivation.Lease
	release := func() {
		for _, lease := range leases {
			lease.Release()
		}
	}
	if pc == nil || pc.SemanticSource() == nil {
		return prepared, release, nil
	}
	for index, intent := range prepared {
		tool, declared := pc.SemanticSource().ToolEntries()[intent.Tool]
		_, native := tool.InProcess()
		if intent.NativeSessionTarget != "" && (!declared || !native || intent.ExecutionMode != executionmode.Live) {
			release()
			return nil, nil, fmt.Errorf("frozen session activity selection has no exact live native declaration")
		}
		if !declared || !native || intent.ExecutionMode != executionmode.Live {
			continue
		}
		if pc.channelActivations == nil {
			release()
			return nil, nil, fmt.Errorf("authored session activity requires its original activation owner")
		}
		fact, pinned := correlation.SourceArtifactFactFromContext(ctx)
		version := pc.SemanticSource().WorkflowVersion()
		if !pinned || version == "" || (intent.BundleHash != "" && intent.BundleHash != fact.BundleHash()) || (intent.WorkflowVersion != "" && intent.WorkflowVersion != version) {
			release()
			return nil, nil, fmt.Errorf("authored session activity has no exact source pin")
		}
		if intent.NativeSessionTarget == "" && (intent.PlanGeneration.Valid() || intent.ChannelActivationGeneration.Valid()) {
			release()
			return nil, nil, fmt.Errorf("session activity cannot repair incomplete frozen selection by choosing again")
		}
		lease, err := pc.channelActivations.AcquirePresentationContext(ctx)
		if err != nil {
			release()
			return nil, nil, err
		}
		leases = append(leases, lease)
		intent.BundleHash, intent.WorkflowVersion = fact.BundleHash(), version
		projection, err := pc.compileAuthoredSessionActivity(ctx, intent, lease)
		if err != nil {
			release()
			return nil, nil, err
		}
		private := projection.PrivateTarget()
		if intent.NativeSessionTarget != "" && (intent.NativeSessionTarget != private.ToolID() || !intent.PlanGeneration.Equal(private.Generation()) || !intent.ChannelActivationGeneration.Equal(lease.Generation())) {
			release()
			return nil, nil, fmt.Errorf("authored session activity cannot adopt a replacement target")
		}
		intent.NativeSessionTarget = private.ToolID()
		intent.PlanGeneration = private.Generation()
		intent.ChannelActivationGeneration = lease.Generation()
		prepared[index] = intent
	}
	return prepared, release, nil
}

func (pc *PipelineCoordinator) compileAuthoredSessionActivity(ctx context.Context, intent engine.ActivityIntent,
	lease *channelactivation.Lease,
) (channelonboarding.CompiledSessionActivityTarget, error) {
	var absent channelonboarding.CompiledSessionActivityTarget
	node, declared := intent.Owner.Node()
	if pc == nil || pc.SemanticSource() == nil || lease == nil || !declared {
		return absent, fmt.Errorf("session activity requires its exact declaration and held original publication")
	}
	if err := lease.ValidateAdmission(ctx); err != nil {
		return absent, err
	}
	var sites []contracts.ActivitySite
	for _, site := range contracts.ActivitySitesForNode(node, pc.SemanticSource().ExecutableNodeEventHandlers(node)) {
		if site.HandlerEventKey == intent.HandlerEventKey && site.Spec.Tool == intent.Tool && contracts.ActivityResultEventsForSite(site).ActivityID == intent.ActivityID {
			sites = append(sites, site)
		}
	}
	if len(sites) != 1 {
		return absent, fmt.Errorf("session activity requires exactly one authored site, found %d", len(sites))
	}
	publication, err := channelonboarding.NewChannelActivationPublication(lease.Activations())
	if err != nil || !publication.Generation().Equal(lease.Generation()) {
		return absent, errors.Join(err, fmt.Errorf("session activity lost its held publication identity"))
	}
	return channelonboarding.CompileSessionActivityTarget(pc.SemanticSource(), intent.BundleHash, sites[0], publication)
}

func (pc *PipelineCoordinator) validateAuthoredSessionActivity(ctx context.Context, intent engine.ActivityIntent,
	lease *channelactivation.Lease,
) (channelonboarding.CompiledSessionActivityTarget, error) {
	projection, err := pc.compileAuthoredSessionActivity(ctx, intent, lease)
	if err != nil {
		return channelonboarding.CompiledSessionActivityTarget{}, err
	}
	private := projection.PrivateTarget()
	if intent.NativeSessionTarget != private.ToolID() || !intent.PlanGeneration.Equal(private.Generation()) || !intent.ChannelActivationGeneration.Equal(projection.PublicationGeneration()) ||
		lease.Operation().Binding.BindingID() != projection.Activation().Plan.BindingID() || lease.Operation().Name != projection.Operation() {
		return channelonboarding.CompiledSessionActivityTarget{}, fmt.Errorf("session activity differs from its frozen admitted target")
	}
	return projection, nil
}

func validateSessionActivityRequestSelection(intent engine.ActivityIntent) error {
	if intent.NativeSessionTarget == "" {
		return nil
	}
	if !strings.HasPrefix(intent.NativeSessionTarget, contracts.PrivateChannelActivityPrefix) || !intent.PlanGeneration.Valid() ||
		!intent.ChannelActivationGeneration.Valid() || intent.BundleHash == "" || intent.WorkflowVersion == "" ||
		intent.ExecutionMode != executionmode.Live || !intent.Owner.IsNode() {
		return fmt.Errorf("authored session activity requires its complete frozen native selection and source identity")
	}
	return nil
}
