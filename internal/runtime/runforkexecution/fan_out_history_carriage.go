package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func retainOriginalFanOutHistory(ctx context.Context, loader SelectedContractSourceLoader, original semanticview.OriginalLoopCarriage, originalRoot sourceartifact.RootAdmission, plan runfork.RunForkPlan) (semanticview.OriginalLoopCarriage, error) {
	if err := ctx.Err(); err != nil {
		return semanticview.OriginalLoopCarriage{}, err
	}
	if err := originalRoot.Validate(); err != nil {
		return semanticview.OriginalLoopCarriage{}, err
	}
	requirements, err := runfork.FanOutContinuationRequirements(plan)
	if err != nil {
		return semanticview.OriginalLoopCarriage{}, err
	}
	seen := map[sourceartifact.RootAdmissionReference]struct{}{originalRoot.Reference(): {}}
	for _, obligation := range plan.FanOutObligations {
		if err := ctx.Err(); err != nil {
			return semanticview.OriginalLoopCarriage{}, err
		}
		if obligation.Intent.Request.Deployment != nil {
			continue
		}
		if requirements[obligation.Intent.Request.Key] {
			continue
		}
		hash := obligation.Intent.Request.PlanRef.BundleHash
		rooted := obligation.ExecutionSource
		if err := rooted.Validate(); err != nil {
			return semanticview.OriginalLoopCarriage{}, fmt.Errorf("fan-out history requires its proven execution source: %w", err)
		}
		if rooted.Admission().BundleHash() != hash {
			return semanticview.OriginalLoopCarriage{}, fmt.Errorf("fan-out history source contradicts its exact execution reference")
		}
		if _, retained := seen[rooted.Admission().Reference()]; retained {
			continue
		}
		var loaded LoadedSelectedContractSource
		_, err := loadRunForkSelectedContractSource(ctx, loader, SelectedContractSourceLoadRequest{
			Responsibility: retainedFanOutExecutionSource(rooted),
			BundleHash:     hash, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: hash},
		}, &loaded)
		if err == nil && !loaded.RootedSource.Admission().Equal(rooted.Admission()) {
			err = fmt.Errorf("retained fan-out loader crossed its proven execution root")
		}
		if err == nil {
			original, err = original.WithFanOutHistory(loaded.Source)
		}
		if err = errors.Join(err, cleanupLoadedSelectedContractSource(loaded)); err != nil {
			return semanticview.OriginalLoopCarriage{}, fmt.Errorf("retain original fan-out artifact %s: %w", hash, err)
		}
		seen[rooted.Admission().Reference()] = struct{}{}
	}
	return original, nil
}
