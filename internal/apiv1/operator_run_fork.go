package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimerunforkexecution "swarm/internal/runtime/runforkexecution"
	"swarm/internal/store"
	"swarm/internal/store/runbundle"
)

const runForkIdempotencyTTL = 24 * time.Hour

type RunForkAvailabilityStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type RunForkExecutor interface {
	ExecuteRunFork(context.Context, RunForkExecutionRequest) (RunForkExecutionResult, error)
}

type RunForkExecutionRequest struct {
	SourceRunID string
	ForkEventID string
	BundleHash  string
}

type RunForkExecutionResult struct {
	Owner              string `json:"owner"`
	SourceRunID        string `json:"source_run_id"`
	ForkRunID          string `json:"fork_run_id"`
	ForkEventID        string `json:"fork_event_id"`
	ForkRunStatus      string `json:"fork_run_status"`
	BundleHash         string `json:"bundle_hash"`
	ExecutedEventCount int    `json:"executed_event_count"`
}

type SelectedContractRunForkExecutor struct {
	Store             *store.PostgresStore
	SourceLoader      runtimerunforkexecution.SelectedContractSourceLoader
	ContractSelection store.RunForkContractSelection
	AgentRuntime      runtimerunforkexecution.SelectedContractAgentRuntimeOptions
}

func (e SelectedContractRunForkExecutor) ExecuteRunFork(ctx context.Context, req RunForkExecutionRequest) (RunForkExecutionResult, error) {
	if e.Store == nil {
		return RunForkExecutionResult{}, fmt.Errorf("run.fork requires selected-contract store")
	}
	result, err := runtimerunforkexecution.ExecuteSelectedContractRunFork(ctx, runtimerunforkexecution.SelectedContractExecutionRequest{
		SourceRunID:       strings.TrimSpace(req.SourceRunID),
		At:                strings.TrimSpace(req.ForkEventID),
		Store:             e.Store,
		SourceLoader:      e.SourceLoader,
		ContractSelection: e.ContractSelection,
		AgentRuntime:      e.AgentRuntime,
	})
	if err != nil {
		return RunForkExecutionResult{}, err
	}
	status := strings.TrimSpace(result.Activation.ForkRunStatus)
	if status == "" {
		status = strings.TrimSpace(result.Materialization.ForkRunStatus)
	}
	return RunForkExecutionResult{
		Owner:              strings.TrimSpace(result.Owner),
		SourceRunID:        strings.TrimSpace(result.Materialization.SourceRunID),
		ForkRunID:          strings.TrimSpace(result.Materialization.ForkRunID),
		ForkEventID:        strings.TrimSpace(result.Materialization.ForkPoint.EventID),
		ForkRunStatus:      status,
		BundleHash:         strings.TrimSpace(req.BundleHash),
		ExecutedEventCount: result.ExecutedEventCount,
	}, nil
}

func OperatorRunForkHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.RunForkAvailability == nil || opts.RunFork == nil || opts.Idempotency == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"run.fork": func(ctx context.Context, req Request) (any, error) {
			return executeRunFork(ctx, req, opts, now().UTC())
		},
	}
}

func executeRunFork(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	params, err := runForkParamsFromRequest(req.Params)
	if err != nil {
		return nil, err
	}
	availability, err := opts.RunForkAvailability.LoadRunBundleAvailability(ctx, params.SourceRunID)
	if err != nil {
		return nil, runForkError(params.SourceRunID, params.ForkEventID, err)
	}
	if availability.DataIntegrityError() {
		return nil, NewApplicationError(BundleDataIntegrityErrorCode, false, runForkAvailabilityDetails(availability))
	}
	if !availability.Available() {
		return nil, NewApplicationError(BundleUnavailableCode, false, runForkAvailabilityDetails(availability))
	}
	if params.BundleHash != "" && params.BundleHash != availability.BundleHash {
		return nil, NewApplicationError(UnsupportedBundleHashForkCode, false, map[string]any{
			"source_run_id":      availability.RunID,
			"source_bundle_hash": availability.BundleHash,
			"requested_hash":     params.BundleHash,
			"tracked_follow_up":  "#976",
			"unsupported_reason": "cross-bundle run.fork is split to #976",
			"supported_selector": "same source bundle_hash only",
		})
	}
	params.BundleHash = availability.BundleHash

	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: params.IdempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     params.SourceRunID,
		TTL:            runForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		result, err := opts.RunFork.ExecuteRunFork(ctx, RunForkExecutionRequest{
			SourceRunID: params.SourceRunID,
			ForkEventID: params.ForkEventID,
			BundleHash:  params.BundleHash,
		})
		if err != nil {
			return store.APIIdempotencyCompletion{}, runForkError(params.SourceRunID, params.ForkEventID, err)
		}
		if result.BundleHash == "" {
			result.BundleHash = params.BundleHash
		}
		response, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: result.ForkRunID, Response: response}, nil
	})
	if err != nil {
		return nil, runForkError(params.SourceRunID, params.ForkEventID, err)
	}
	var stored RunForkExecutionResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode run.fork idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode run.fork response: %w", err)
	}
	return stored, nil
}

type runForkParams struct {
	SourceRunID    string
	ForkEventID    string
	BundleHash     string
	IdempotencyKey string
}

func runForkParamsFromRequest(params map[string]any) (runForkParams, error) {
	sourceRunID, err := requiredUUIDParam(params, "source_run_id")
	if err != nil {
		return runForkParams{}, err
	}
	forkEventID, _, err := optionalUUIDParam(params, "fork_event_id")
	if err != nil {
		return runForkParams{}, err
	}
	bundleHash, _, err := optionalStringParam(params, "bundle_hash")
	if err != nil {
		return runForkParams{}, err
	}
	if bundleHash != "" && !bundleHashPattern.MatchString(bundleHash) {
		return runForkParams{}, NewInvalidParamsError(map[string]any{"field": "bundle_hash", "reason": "must be bundle-v1:sha256:<64 lowercase hex>"})
	}
	idempotencyKey, _, err := optionalStringParam(params, "idempotency_key")
	if err != nil {
		return runForkParams{}, err
	}
	return runForkParams{
		SourceRunID:    sourceRunID,
		ForkEventID:    forkEventID,
		BundleHash:     bundleHash,
		IdempotencyKey: idempotencyKey,
	}, nil
}

func requiredUUIDParam(params map[string]any, name string) (string, error) {
	value, err := requiredStringParam(params, name)
	if err != nil {
		return "", err
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", NewInvalidParamsError(map[string]any{"field": name, "reason": "must be a UUID"})
	}
	return parsed.String(), nil
}

func optionalUUIDParam(params map[string]any, name string) (string, bool, error) {
	value, present, err := optionalStringParam(params, name)
	if err != nil {
		return "", present, err
	}
	if !present || value == "" {
		return "", present, nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", present, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be a UUID"})
	}
	return parsed.String(), present, nil
}

func runForkAvailabilityDetails(availability runbundle.Availability) map[string]any {
	details := map[string]any{"run_id": strings.TrimSpace(availability.RunID)}
	for key, value := range map[string]string{
		"status":                    availability.Status,
		"bundle_hash":               availability.BundleHash,
		"bundle_source":             availability.BundleSource,
		"legacy_bundle_fingerprint": availability.BundleFingerprint,
		"cause":                     availability.Cause,
	} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			details[key] = trimmed
		}
	}
	return details
}

func runForkError(sourceRunID, forkEventID string, err error) error {
	if err == nil {
		return nil
	}
	var conflict *store.APIIdempotencyConflictError
	if errors.As(err, &conflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    conflict.OriginalRequestHash,
			"conflicting_request_hash": conflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{
				"method":      conflict.Method,
				"resource_id": conflict.ResourceID,
			},
		})
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "fork point event"):
		eventID := strings.TrimSpace(forkEventID)
		if eventID == "" {
			return NewInvalidParamsError(map[string]any{"field": "fork_event_id", "reason": msg})
		}
		return NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": eventID})
	case strings.Contains(msg, "no source-run event"):
		return NewInvalidParamsError(map[string]any{"field": "fork_event_id", "reason": msg, "source_run_id": strings.TrimSpace(sourceRunID)})
	case strings.Contains(msg, "not found") && strings.Contains(msg, "run"):
		return NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": strings.TrimSpace(sourceRunID)})
	case strings.Contains(msg, "fork point --at"):
		return NewInvalidParamsError(map[string]any{"field": "fork_event_id", "reason": msg})
	default:
		return err
	}
}
