package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunforkadmission "github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

const runForkIdempotencyTTL = 24 * time.Hour

func activeRunStatus(raw string) bool {
	state, err := runtimerunlifecycle.ParseState(raw)
	return err == nil && state.Active()
}

type RunForkAvailabilityStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type RunForkExecutor interface {
	ExecuteRunFork(context.Context, RunForkExecutionRequest) (RunForkExecutionResult, error)
}

type RunForkExecutorSelector interface {
	SelectRunForkExecutor(*swruntime.BundleContext, *swruntime.Runtime) (RunForkExecutor, error)
}

type RunForkExecutionRequest struct {
	SourceRunID         string
	ForkEventID         string
	BundleHash          string
	ConfirmSourceFreeze bool
	DataPinOverrides    []durabledata.ExplicitPin
	ContractSelection   runfork.RunForkContractSelection
}

type RunForkExecutionResult struct {
	Owner              string            `json:"owner"`
	SourceRunID        string            `json:"source_run_id"`
	SourceRunStatus    string            `json:"source_run_status"`
	SourceFrozen       bool              `json:"source_frozen"`
	ForkRunID          string            `json:"fork_run_id"`
	ForkEventID        string            `json:"fork_event_id"`
	ForkRunStatus      string            `json:"fork_run_status"`
	BundleHash         string            `json:"bundle_hash"`
	ExecutedEventCount int               `json:"executed_event_count"`
	DataPins           []durabledata.Pin `json:"data_pins"`
}

type SelectedContractRunForkExecutionFunc func(context.Context, runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error)

type SelectedContractRunForkExecutor struct {
	ExecuteSelectedContractRunFork SelectedContractRunForkExecutionFunc
	SourceLoader                   runtimerunforkexecution.SelectedContractSourceLoader
	ContractSelection              runfork.RunForkContractSelection
	AgentRuntime                   runtimerunforkexecution.SelectedContractAgentRuntimeOptions
	EffectiveSourceIdentity        scenarioexecution.EffectiveSourceIdentity
}

func (e SelectedContractRunForkExecutor) SelectRunForkExecutor(contextDef *swruntime.BundleContext, selectedRuntime *swruntime.Runtime) (RunForkExecutor, error) {
	if contextDef == nil || selectedRuntime == nil {
		return nil, fmt.Errorf("selected run fork runtime context is required")
	}
	e.AgentRuntime.Config = selectedRuntime.Config
	e.AgentRuntime.Workspace = selectedRuntime.Workspace
	e.AgentRuntime.Credentials = selectedRuntime.Credentials
	e.EffectiveSourceIdentity = contextDef.EffectiveSourceIdentity
	e.ContractSelection = runtimerunforkadmission.SelectedContractSelection(contextDef.Source)
	loader, err := runtimerunforkexecution.NewAdmittedSelectedContractSourceLoader(
		e.ContractSelection,
		selectedRuntime.Options.WorkflowModule,
		contextDef.SourceArtifactFact,
		contextDef.EffectiveSourceIdentity,
	)
	if err != nil {
		return nil, fmt.Errorf("bind selected run fork to admitted runtime source: %w", err)
	}
	e.SourceLoader = loader
	return e, nil
}

func (e SelectedContractRunForkExecutor) ExecuteRunFork(ctx context.Context, req RunForkExecutionRequest) (RunForkExecutionResult, error) {
	if e.ExecuteSelectedContractRunFork == nil {
		return RunForkExecutionResult{}, fmt.Errorf("run.fork requires selected-contract executor")
	}
	selection := req.ContractSelection
	if strings.TrimSpace(selection.Mode) == "" {
		selection = e.ContractSelection
	}
	result, err := e.ExecuteSelectedContractRunFork(ctx, runtimerunforkexecution.SelectedContractExecutionRequest{
		SourceRunID:             strings.TrimSpace(req.SourceRunID),
		At:                      strings.TrimSpace(req.ForkEventID),
		ExpectedBundleHash:      strings.TrimSpace(req.BundleHash),
		ConfirmSourceFreeze:     req.ConfirmSourceFreeze,
		DataPinOverrides:        req.DataPinOverrides,
		SourceLoader:            e.SourceLoader,
		ContractSelection:       selection,
		AgentRuntime:            e.AgentRuntime,
		EffectiveSourceIdentity: e.EffectiveSourceIdentity,
	})
	if err != nil {
		return RunForkExecutionResult{}, err
	}
	status := strings.TrimSpace(result.Activation.ForkRunStatus)
	if status == "" {
		status = strings.TrimSpace(result.Materialization.ForkRunStatus)
	}
	pins := make([]durabledata.Pin, len(result.Materialization.DataPins))
	copy(pins, result.Materialization.DataPins)
	for index := range pins {
		pins[index].RunState = status
	}
	return RunForkExecutionResult{
		Owner:              strings.TrimSpace(result.Owner),
		SourceRunID:        strings.TrimSpace(result.Materialization.SourceRunID),
		SourceRunStatus:    strings.TrimSpace(result.Activation.SourceRunStatus),
		SourceFrozen:       result.Activation.SourceFrozen,
		ForkRunID:          strings.TrimSpace(result.Materialization.ForkRunID),
		ForkEventID:        strings.TrimSpace(result.Materialization.ForkPoint.EventID),
		ForkRunStatus:      status,
		BundleHash:         strings.TrimSpace(req.BundleHash),
		ExecutedEventCount: result.ExecutedEventCount,
		DataPins:           pins,
	}, nil
}

func OperatorRunForkHandlers(opts RunForkHandlerOptions) map[string]MethodHandler {
	if opts.Availability == nil || opts.Executor == nil || opts.Idempotency == nil {
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

func executeRunFork(ctx context.Context, req Request, opts RunForkHandlerOptions, now time.Time) (any, error) {
	params, err := runForkParamsFromRequest(req.Params)
	if err != nil {
		return nil, err
	}
	availability, err := opts.Availability.LoadRunBundleAvailability(ctx, params.SourceRunID)
	if err != nil {
		return nil, runForkError(params.SourceRunID, params.ForkEventID, err)
	}
	if availability.DataIntegrityError() {
		return nil, NewApplicationError(BundleDataIntegrityErrorCode, false, runForkAvailabilityDetails(availability))
	}
	if !availability.Available() {
		return nil, NewApplicationError(BundleUnavailableCode, false, runForkAvailabilityDetails(availability))
	}
	if activeRunStatus(availability.Status) && !params.ConfirmSourceFreeze {
		return nil, NewInvalidParamsError(map[string]any{
			"field":  "confirm_source_freeze",
			"reason": "must be true when forking a running or paused source because the operation may permanently freeze it unless source advancement selects branch divergence",
		})
	}
	sourceBundleHash := strings.TrimSpace(availability.BundleHash)
	targetBundleHash := strings.TrimSpace(params.BundleHash)
	if targetBundleHash == "" {
		targetBundleHash = sourceBundleHash
	}
	if targetBundleHash == "" {
		return nil, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
			"source_run_id": availability.RunID,
			"reason":        "source run has no canonical bundle_hash",
		})
	}
	executor := opts.Executor
	selectedCtx := ctx
	var contractSelection runfork.RunForkContractSelection
	if runtimeContextManager(opts.RuntimeContexts) != nil {
		var contextErr error
		var selected selectedRuntimeContext
		selectedCtx, selected, contextErr = runtimeBundleContextByHash(ctx, opts.RuntimeContexts, targetBundleHash, params.SourceRunID)
		if contextErr != nil {
			return nil, contextErr
		}
		if opts.Selector == nil {
			return nil, fmt.Errorf("run fork executor selector is required for loaded runtime contexts")
		}
		executor, contextErr = opts.Selector.SelectRunForkExecutor(selected.BundleContext, selected.Runtime)
		if contextErr != nil {
			return nil, contextErr
		}
	} else if targetBundleHash != sourceBundleHash {
		return nil, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"source_run_id":      availability.RunID,
			"source_bundle_hash": sourceBundleHash,
			"bundle_hash":        targetBundleHash,
			"cause":              "runtime_context_not_loaded",
			"supported_selector": "same source bundle_hash in disk/serial mode, or loaded target BundleContext in DB-loaded RuntimeContextManager mode",
		})
	}
	if targetBundleHash != sourceBundleHash {
		contractSelection = runfork.RunForkContractSelection{
			Mode:       runfork.RunForkContractSelectionModeBundleHash,
			BundleHash: targetBundleHash,
		}
	}
	params.BundleHash = targetBundleHash

	completion, replay, err := opts.Idempotency.WithAPIIdempotency(selectedCtx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: params.IdempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     params.SourceRunID,
		TTL:            runForkIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		result, err := executor.ExecuteRunFork(ctx, RunForkExecutionRequest{
			SourceRunID:         params.SourceRunID,
			ForkEventID:         params.ForkEventID,
			BundleHash:          params.BundleHash,
			ConfirmSourceFreeze: params.ConfirmSourceFreeze,
			DataPinOverrides:    params.DataPinOverrides,
			ContractSelection:   contractSelection,
		})
		if err != nil {
			return apiidempotency.Completion{}, runForkError(params.SourceRunID, params.ForkEventID, err)
		}
		if result.BundleHash == "" {
			result.BundleHash = params.BundleHash
		}
		if result.DataPins == nil {
			result.DataPins = []durabledata.Pin{}
		}
		if err := validateRunForkExecutionResult(result); err != nil {
			return apiidempotency.Completion{}, err
		}
		response, err := json.Marshal(result)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{ResourceID: result.ForkRunID, Response: response}, nil
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
	if err := validateRunForkExecutionResult(stored); err != nil {
		return nil, err
	}
	return stored, nil
}

func validateRunForkExecutionResult(result RunForkExecutionResult) error {
	status, err := runtimerunlifecycle.ParseState(result.SourceRunStatus)
	if err != nil {
		return fmt.Errorf("run.fork result has invalid source_run_status %q", result.SourceRunStatus)
	}
	if result.SourceFrozen != (status == runtimerunlifecycle.StateForked) {
		return fmt.Errorf("run.fork result source_frozen=%t contradicts source_run_status %q", result.SourceFrozen, result.SourceRunStatus)
	}
	return nil
}

type runForkParams struct {
	SourceRunID         string
	ForkEventID         string
	BundleHash          string
	ConfirmSourceFreeze bool
	DataPinOverrides    []durabledata.ExplicitPin
	IdempotencyKey      string
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
	if bundleHash != "" {
		if err := runtimecontracts.ValidateBundleHash(bundleHash); err != nil {
			return runForkParams{}, NewInvalidParamsError(map[string]any{"field": "bundle_hash", "reason": "must be bundle-v2:sha256:<64 lowercase hex>"})
		}
	}
	confirmSourceFreeze, err := optionalBoolParam(params, "confirm_source_freeze", false)
	if err != nil {
		return runForkParams{}, err
	}
	idempotencyKey, _, err := optionalStringParam(params, "idempotency_key")
	if err != nil {
		return runForkParams{}, err
	}
	dataPinOverrides, err := runForkDataPinOverrides(params)
	if err != nil {
		return runForkParams{}, err
	}
	return runForkParams{
		SourceRunID:         sourceRunID,
		ForkEventID:         forkEventID,
		BundleHash:          bundleHash,
		ConfirmSourceFreeze: confirmSourceFreeze,
		DataPinOverrides:    dataPinOverrides,
		IdempotencyKey:      idempotencyKey,
	}, nil
}

func runForkDataPinOverrides(params map[string]any) ([]durabledata.ExplicitPin, error) {
	raw, present := params["data_pin_overrides"]
	if !present {
		return []durabledata.ExplicitPin{}, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_pin_overrides", "reason": "must be an array"})
	}
	if len(items) > durabledata.MaxDataDeclarationsPerBundle {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_pin_overrides", "reason": fmt.Sprintf("must contain at most %d items", durabledata.MaxDataDeclarationsPerBundle)})
	}
	overrides := make([]durabledata.ExplicitPin, 0, len(items))
	for index, rawItem := range items {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return nil, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("data_pin_overrides[%d]", index), "reason": "must be an object"})
		}
		if err := exactDataParams(item, "declaration", "version_id"); err != nil {
			return nil, err
		}
		declaration, err := dataDeclarationRef(item["declaration"])
		if err != nil {
			return nil, err
		}
		versionID, err := dataVersionID(item["version_id"], fmt.Sprintf("data_pin_overrides[%d].version_id", index))
		if err != nil {
			return nil, err
		}
		overrides = append(overrides, durabledata.ExplicitPin{Declaration: declaration, VersionID: versionID})
	}
	canonical, err := durabledata.CanonicalExplicitPins(overrides)
	if err != nil {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_pin_overrides", "reason": err.Error()})
	}
	return canonical, nil
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
		"status":      availability.Status,
		"bundle_hash": availability.BundleHash,
		"cause":       availability.Cause,
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
	var applicationErr *ApplicationError
	if errors.As(err, &applicationErr) {
		return applicationErr
	}
	var dataErr *durabledata.DomainError
	if errors.As(err, &dataErr) {
		return dataApplicationError(dataErr)
	}
	var conflict *apiidempotency.ConflictError
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
	case errors.Is(err, runfork.ErrRunForkSourceFreezeConfirmationRequired):
		return NewInvalidParamsError(map[string]any{
			"field":  "confirm_source_freeze",
			"reason": "must be true when the selected fork freezes a running or paused source",
		})
	case errors.Is(err, runtimerunlifecycle.ErrRunNotActive):
		details := map[string]any{"run_id": strings.TrimSpace(sourceRunID)}
		var inactive *runtimerunlifecycle.RunNotActiveError
		if errors.As(err, &inactive) {
			details["current_status"] = string(inactive.State)
		}
		return NewApplicationError(RunAlreadyTerminalCode, false, details)
	case errors.Is(err, runbundle.ErrRunNotFound):
		return NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": strings.TrimSpace(sourceRunID)})
	case strings.Contains(msg, UnsupportedBundleHashForkCode):
		return NewApplicationError(UnsupportedBundleHashForkCode, false, map[string]any{
			"run_id":             strings.TrimSpace(sourceRunID),
			"event_id":           strings.TrimSpace(forkEventID),
			"reason":             msg,
			"unsupported_reason": "run.fork target bundle selection is unsupported for the requested mode",
		})
	case strings.Contains(msg, runbundle.CodeBundleDataIntegrityError):
		return NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
			"run_id":   strings.TrimSpace(sourceRunID),
			"event_id": strings.TrimSpace(forkEventID),
			"reason":   msg,
		})
	case strings.Contains(msg, runbundle.CodeBundleUnavailable):
		return NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"run_id":   strings.TrimSpace(sourceRunID),
			"event_id": strings.TrimSpace(forkEventID),
			"reason":   msg,
		})
	case strings.Contains(msg, "fork point event"):
		eventID := strings.TrimSpace(forkEventID)
		if eventID == "" {
			return NewInvalidParamsError(map[string]any{"field": "fork_event_id", "reason": msg})
		}
		return NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": eventID})
	case strings.Contains(msg, "no source-run event"):
		return NewInvalidParamsError(map[string]any{"field": "fork_event_id", "reason": msg, "source_run_id": strings.TrimSpace(sourceRunID)})
	case strings.Contains(msg, "fork point --at"):
		return NewInvalidParamsError(map[string]any{"field": "fork_event_id", "reason": msg})
	default:
		return err
	}
}
