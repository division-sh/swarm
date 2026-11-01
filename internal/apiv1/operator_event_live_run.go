package apiv1

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func selectEventPublicationLiveRun(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams, requestedHash string) (eventPublicationParams, error) {
	var scope runlifecycle.LiveRunScope
	if scenarioexecution.HasPrivateRunScope(ctx) {
		selected, ok := scenarioexecution.PrivateRunScopeFromContext(ctx)
		if !ok {
			return params, fmt.Errorf("private scenario publication requires its exact run scope")
		}
		scope.FlowPath = selected.FlowPath
	} else {
		sources, err := eventPublicationLiveSources(ctx, opts, requestedHash)
		if err != nil {
			return params, err
		}
		scope, err = eventPublicationFlowScope(sources, params.EventName)
		if err != nil {
			return params, err
		}
	}
	if opts.LiveRuns == nil || opts.Runs == nil {
		return params, fmt.Errorf("event publication requires the live-run and run-history readers")
	}
	live, found, err := opts.LiveRuns.LoadLiveRun(ctx, scope)
	if err != nil {
		return params, err
	}
	if found {
		if params.RunID != "" && params.RunID != live.RunID {
			return params, NewApplicationError(EventPublishRejectedCode, false, map[string]any{
				"event_name": params.EventName, "run_id": params.RunID, "live_run_id": live.RunID,
				"reason": "live_run_mismatch", "remediation": "omit run_id to publish into the flow's live run",
			})
		}
		params.RunID, params.LiveRun = live.RunID, &live
		return params, nil
	}
	if scenarioexecution.HasPrivateRunScope(ctx) {
		return params, NewApplicationError(EventPublishRejectedCode, false, map[string]any{
			"run_id": params.RunID, "reason": "scenario_run_not_live",
		})
	}
	return params, eventPublicationNoLiveRun(ctx, opts.Runs, scope, params.EventName)
}

func eventPublicationLiveSources(ctx context.Context, opts EventPublicationOptions, requestedHash string) ([]semanticview.Source, error) {
	sources := []semanticview.Source{opts.Source}
	if opts.RuntimeContexts == nil && requestedHash != "" {
		_, fact, present, err := eventPublicationRuntimeSourceContext(ctx, opts.SourceArtifact)
		if err != nil {
			return nil, err
		}
		if !present || fact.BundleHash() != requestedHash {
			return nil, NewApplicationError(BundleUnavailableCode, false, map[string]any{
				"bundle_hash": requestedHash, "cause": "single_active_runtime_unavailable",
			})
		}
	}
	if opts.RuntimeContexts != nil {
		if requestedHash != "" {
			lookup := opts.RuntimeContexts.LookupBundleHashStatus(requestedHash)
			if !lookup.Loaded() {
				return nil, NewApplicationError(BundleUnavailableCode, false, map[string]any{
					"bundle_hash": requestedHash, "cause": runtimeContextLookupCause(lookup),
				})
			}
		}
		sources = nil
		for _, loaded := range opts.RuntimeContexts.LoadedContexts() {
			if requestedHash == "" || loaded.SourceArtifactFact.BundleHash() == requestedHash {
				sources = append(sources, loaded.Source)
			}
		}
	}
	return sources, nil
}

func eventPublicationNoLiveRun(ctx context.Context, runs RunReadStore, scope runlifecycle.LiveRunScope, eventName string) error {
	details := map[string]any{"event_name": eventName, "flow_path": scope.FlowPath, "reason": "no_run_yet"}
	private, privateScope := scenarioexecution.PrivateRunScopeFromContext(ctx)
	for cursor := ""; ; {
		headers, next, err := runs.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 100, Cursor: cursor})
		if err != nil {
			return err
		}
		for _, header := range headers {
			if privateScope && header.RunID != private.RunID {
				continue
			}
			if !privateScope && header.FlowPath != scope.FlowPath {
				continue
			}
			if header.Status == "live" || (privateScope && header.Status == "running") {
				return NewApplicationError(EventPublishRejectedCode, false, map[string]any{
					"event_name": eventName, "reason": "live_run_changed", "remediation": "select the live run again",
				})
			}
			if header.Status == "paused" {
				details["reason"], details["run_id"] = "all_runs_paused", header.RunID
				details["remediation"] = "swarm run continue " + header.RunID
			} else if details["reason"] == "no_run_yet" {
				details["reason"] = "all_runs_ended"
			}
		}
		if strings.TrimSpace(next) == "" {
			break
		}
		cursor = next
	}
	return NewApplicationError(EventPublishRejectedCode, false, details)
}

func eventPublicationFlowScope(sources []semanticview.Source, eventName string) (runlifecycle.LiveRunScope, error) {
	flows := map[string]bool{}
	for _, source := range sources {
		if _, public := semanticview.SelectedRootInputPin(source, eventName); public {
			flows[flowidentity.ScopeKey(source, ".")] = true
		}
	}
	var matches []string
	for flow := range flows {
		matches = append(matches, flow)
	}
	sort.Strings(matches)
	if len(matches) == 0 {
		return runlifecycle.LiveRunScope{}, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name": eventName, "reason": "selected_root_input_required",
		})
	}
	if len(matches) > 1 {
		return runlifecycle.LiveRunScope{}, NewApplicationError(EventPublishRejectedCode, false, map[string]any{
			"event_name": eventName, "reason": "ambiguous_flow", "matches": matches,
		})
	}
	return runlifecycle.LiveRunScope{FlowPath: matches[0]}, nil
}
