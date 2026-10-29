package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// accountDeclarations consumes S1's immutable held declarations. It never
// promotes inherited visibility into an account holder.
func (rt *Runtime) accountDeclarations() (packadmission.ProviderSource, *contracts.WorkflowContractBundle, error) {
	if rt == nil || rt.Options.WorkflowModule == nil {
		return packadmission.ProviderSource{}, nil, fmt.Errorf("account binding requires its admitted runtime source")
	}
	bundle, ok := semanticview.Bundle(rt.Options.WorkflowModule.SemanticSource())
	if !ok || bundle == nil {
		return packadmission.ProviderSource{}, nil, fmt.Errorf("account binding requires its admitted contract")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return packadmission.ProviderSource{}, nil, err
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	return providers, bundle, err
}

// AccountHolders observes ordinary state through the canonical instance reader.
// Registration/link availability is deliberately not a construction condition.
func (rt *Runtime) AccountHolders(ctx context.Context, runID string, ref channelonboarding.AccountReference) ([]channelonboarding.AccountHolder, error) {
	if ref.Validate() != nil || rt == nil || rt.Pipeline == nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	providers, bundle, err := rt.accountDeclarations()
	if err != nil {
		return nil, err
	}
	instances, err := rt.Pipeline.ListWorkflowInstances(ctx, runID)
	if err != nil {
		return nil, err
	}
	holders := []channelonboarding.AccountHolder{}
	for _, flow := range providers.FlowPaths() {
		scope, err := providers.Scope(flow)
		if err != nil {
			return nil, err
		}
		provider, declaration, err := scope.ConnectionAccount(bundle)
		if err != nil {
			return nil, err
		}
		plan, held := provider.ChannelPlan()
		if !held || declaration.FlowID == "" || plan.Provider() != ref.Provider {
			continue
		}
		for _, instance := range instances {
			if instance.WorkflowName != flow || !instance.TerminatedAt.IsZero() {
				continue
			}
			value, present := declaration.Value(instance.Fields)
			if !present || value == "" {
				continue // Dynamic keyless holders may be populated by a later event.
			}
			account, err := channelonboarding.CanonicalAccount(ref.Provider, value)
			if err != nil {
				return nil, fmt.Errorf("flow %s operator_account: %w", flow, err)
			}
			if account != ref {
				continue
			}
			holders = append(holders, channelonboarding.AccountHolder{BundleHash: rt.Options.SourceArtifactFact.BundleHash(),
				Flow: flow, RunID: runID, FlowInstance: instance.StorageRef})
		}
	}
	return holders, nil
}

// AccountHolders reads only currently admitted runs, acquiring their original
// runtime use. A stale loaded bundle is not a live account holder.
func (m *RuntimeContextManager) AccountHolders(ctx context.Context, ref channelonboarding.AccountReference) ([]channelonboarding.AccountHolder, error) {
	holders := []channelonboarding.AccountHolder{}
	seen := map[string]bool{}
	for _, loaded := range m.LoadedContexts() {
		for _, activation := range loaded.StandingActivations {
			if seen[activation.RunID] {
				continue
			}
			seen[activation.RunID] = true
			rows, err := m.activationAccountHolders(ctx, activation, ref)
			if err != nil {
				return nil, err
			}
			// W1 constructs the eager root tree in each ingress run. Copies in
			// another flow's run are not that flow's receiving-account owner.
			for _, row := range rows {
				if row.Flow == activation.FlowPath {
					holders = append(holders, row)
				}
			}
		}
	}
	slices.SortFunc(holders, func(a, b channelonboarding.AccountHolder) int {
		if a.BundleHash != b.BundleHash {
			return strings.Compare(a.BundleHash, b.BundleHash)
		}
		return strings.Compare(a.RunID+"/"+a.FlowInstance, b.RunID+"/"+b.FlowInstance)
	})
	if len(holders) > 1 {
		return holders, fmt.Errorf("%w: account %s has competing flow holders; assign it to exactly one flow", channelonboarding.ErrConflict, ref.Account)
	}
	return holders, nil
}

func (m *RuntimeContextManager) activationAccountHolders(ctx context.Context, activation StandingActivation, ref channelonboarding.AccountReference) ([]channelonboarding.AccountHolder, error) {
	if m.landingRuns == nil {
		return nil, fmt.Errorf("account binding requires the deployment landing-run reader")
	}
	landing, found, err := m.landingRuns.LoadLandingRun(ctx, runlifecycle.LiveRunScope{FlowPath: activation.FlowPath})
	if err != nil {
		return nil, err
	}
	if !found || landing.RunID != activation.RunID || landing.Source.BundleHash() != activation.BundleHash || landing.State != runlifecycle.StateLive {
		return nil, nil
	}
	use, _, availability, err := m.AcquireRun(ctx, activation.RunID)
	if err != nil || use == nil {
		return nil, err
	}
	if !availability.Available() || availability.Status != string(runlifecycle.StateLive) {
		return nil, use.Done()
	}
	rows, err := use.Runtime().AccountHolders(use.WorkContext(), activation.RunID, ref)
	return rows, errors.Join(err, use.Done())
}
