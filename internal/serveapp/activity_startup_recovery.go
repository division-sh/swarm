package serveapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// This startup caller owns only sequencing and exact source routing. Native
// recovery owns disposition; the original pipeline owns result publication.
func recoverServeDisplacedHTTPActivities(ctx context.Context, capability startupownership.ProcessCapability, contexts []serveRuntimeBundleContext) error {
	if ctx == nil {
		return fmt.Errorf("activity startup recovery requires its process context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	targets, err := serveActivityRecoveryTargets(contexts)
	if err != nil {
		return err
	}
	for _, candidate := range contexts {
		if err := candidate.runtime.PrepareAuthorActivityCatalog(); err != nil {
			return err
		}
	}
	summary, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, capability)
	if err != nil {
		return err
	}
	// Validate the complete returned routing set before publishing any result.
	for _, recovered := range summary.Results {
		if targets[recovered.BundleHash] == nil {
			return fmt.Errorf("recovered activity %s has no exact admitted source context", recovered.RequestEventID)
		}
	}
	for _, recovered := range summary.Results {
		rt := targets[recovered.BundleHash]
		publicationCtx := worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
		publicationCtx = correlation.WithRuntimeInstanceID(publicationCtx, rt.Options.RuntimeInstanceID)
		publicationCtx = correlation.WithSourceArtifactFact(publicationCtx, rt.Options.SourceArtifactFact)
		publicationCtx = authoractivity.WithScope(publicationCtx, authoractivity.BundleScope(rt.Options.RuntimeInstanceID, recovered.BundleHash))
		if err := rt.Pipeline.PublishRecoveredHTTPActivity(publicationCtx, recovered); err != nil {
			return fmt.Errorf("publish recovered activity %s: %w", recovered.RequestEventID, err)
		}
	}
	return nil
}

func serveActivityRecoveryTargets(contexts []serveRuntimeBundleContext) (map[string]*runtime.Runtime, error) {
	targets := make(map[string]*runtime.Runtime, len(contexts))
	var process *worklifetime.Process
	var instance string
	for _, candidate := range contexts {
		rt := candidate.runtime
		if candidate.sourceArtifactFact.Validate() != nil || rt == nil || rt.Pipeline == nil ||
			!candidate.sourceArtifactFact.Matches(rt.Options.SourceArtifactFact) ||
			rt.Options.ProcessWorkOwner == nil || strings.TrimSpace(rt.Options.RuntimeInstanceID) == "" {
			return nil, fmt.Errorf("activity recovery source requires its exact constructed runtime context")
		}
		if process != nil && (process != rt.Options.ProcessWorkOwner || instance != rt.Options.RuntimeInstanceID) {
			return nil, fmt.Errorf("activity recovery sources belong to different process runtimes")
		}
		process, instance = rt.Options.ProcessWorkOwner, rt.Options.RuntimeInstanceID
		bundle := candidate.sourceArtifactFact.BundleHash()
		if targets[bundle] != nil {
			return nil, fmt.Errorf("activity recovery source has ambiguous runtime contexts")
		}
		targets[bundle] = rt
	}
	return targets, nil
}
