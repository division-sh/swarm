package apiv1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func activeRunStatus(raw string) bool {
	state, err := runtimerunlifecycle.ParseState(raw)
	return err == nil && state.Active()
}

type RunForkAvailabilityStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type RunForkOperationReader interface {
	LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
	LoadForkOperationByID(context.Context, string) (runfork.ForkOperationRecord, bool, error)
}

type RunForkExecutor interface {
	ExecuteRunFork(context.Context, RunForkExecutionRequest) (RunForkExecutionResult, error)
}

type RunForkExecutionRequest struct {
	ForkOperation     *runfork.ForkOperationRequest
	SourceRunID       string
	ForkEventID       string
	BundleHash        string
	AllowSourceFreeze bool
	DataPinOverrides  []durabledata.ExplicitPin
	ContractSelection runfork.RunForkContractSelection
}

type RunForkExecutionResult struct {
	Owner                  string            `json:"owner"`
	SourceRunID            string            `json:"source_run_id"`
	SourceRunStatus        string            `json:"source_run_status"`
	SourceFrozen           bool              `json:"source_frozen"`
	ForkRunID              string            `json:"fork_run_id"`
	ForkPointKind          string            `json:"fork_point_kind"`
	ForkRevision           int64             `json:"fork_revision"`
	ForkEventID            string            `json:"fork_event_id,omitempty"`
	ForkRunStatus          string            `json:"fork_run_status"`
	BundleHash             string            `json:"bundle_hash"`
	ExecutedEventCount     int               `json:"executed_event_count"`
	DataPins               []durabledata.Pin `json:"data_pins"`
	activationAcknowledged bool
}

type SelectedContractRunForkExecutionFunc func(context.Context, runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error)

type SelectedContractRunForkExecutor struct {
	ExecuteSelectedContractRunFork SelectedContractRunForkExecutionFunc
	SourceLoader                   runtimerunforkexecution.SelectedContractSourceLoader
	AgentRuntime                   runtimerunforkexecution.SelectedContractAgentRuntimeOptions
}

func (e SelectedContractRunForkExecutor) ExecuteRunFork(ctx context.Context, req RunForkExecutionRequest) (RunForkExecutionResult, error) {
	if e.ExecuteSelectedContractRunFork == nil {
		return RunForkExecutionResult{}, fmt.Errorf("run.fork requires selected-contract executor")
	}
	result, err := e.ExecuteSelectedContractRunFork(ctx, runtimerunforkexecution.SelectedContractExecutionRequest{
		ForkOperation:      req.ForkOperation,
		SourceRunID:        strings.TrimSpace(req.SourceRunID),
		At:                 strings.TrimSpace(req.ForkEventID),
		ExpectedBundleHash: strings.TrimSpace(req.BundleHash),
		AllowSourceFreeze:  req.AllowSourceFreeze,
		DataPinOverrides:   req.DataPinOverrides,
		SourceLoader:       e.SourceLoader,
		ContractSelection:  req.ContractSelection,
		AgentRuntime:       e.AgentRuntime,
	})
	acknowledged := exactSelectedForkActivation(req, result)
	if result.Activation.Activated && !acknowledged {
		return RunForkExecutionResult{}, errors.Join(err, fmt.Errorf("selected fork activation has no exact acknowledged identity"))
	}
	if err == nil && !acknowledged {
		return RunForkExecutionResult{}, fmt.Errorf("selected fork execution returned no acknowledged activation")
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
		Owner:                  strings.TrimSpace(result.Owner),
		SourceRunID:            strings.TrimSpace(result.Materialization.SourceRunID),
		SourceRunStatus:        strings.TrimSpace(result.Activation.SourceRunStatus),
		SourceFrozen:           result.Activation.SourceFrozen,
		ForkRunID:              strings.TrimSpace(result.Materialization.ForkRunID),
		ForkPointKind:          string(result.Materialization.ForkPoint.Kind),
		ForkRevision:           result.Materialization.ForkPoint.Revision,
		ForkEventID:            strings.TrimSpace(result.Materialization.ForkPoint.EventID),
		ForkRunStatus:          status,
		BundleHash:             strings.TrimSpace(req.BundleHash),
		ExecutedEventCount:     result.ExecutedEventCount,
		DataPins:               pins,
		activationAcknowledged: acknowledged,
	}, err
}

func exactSelectedForkActivation(req RunForkExecutionRequest, result runtimerunforkexecution.SelectedContractExecutionResult) bool {
	materialization, activation := result.Materialization, result.Activation
	if err := materialization.ForkPoint.Validate(); err != nil {
		return false
	}
	if err := activation.ForkPoint.Validate(); err != nil {
		return false
	}
	return result.Owner == runfork.RunForkSelectedContractExecutionOwner && activation.Activated &&
		strings.TrimSpace(materialization.SourceRunID) != "" && strings.TrimSpace(materialization.ForkRunID) != "" &&
		activation.SourceRunID == materialization.SourceRunID && activation.ForkRunID == materialization.ForkRunID &&
		activation.ForkPoint.Kind == materialization.ForkPoint.Kind &&
		activation.ForkPoint.Revision == materialization.ForkPoint.Revision &&
		activation.ForkPoint.EventID == materialization.ForkPoint.EventID &&
		activation.ForkRunStatus == runfork.RunForkActivatedStatus &&
		(req.SourceRunID == "" || materialization.SourceRunID == req.SourceRunID) &&
		(req.ForkEventID == "" || materialization.ForkPoint.EventID == req.ForkEventID)
}

func OperatorRunForkHandlers(opts RunForkHandlerOptions) map[string]MethodHandler {
	if opts.Availability == nil || opts.Operations == nil || opts.Executor == nil {
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
	_ = now
	if opts.Availability == nil || opts.Operations == nil || opts.Executor == nil {
		return nil, fmt.Errorf("run.fork requires availability, durable operation and selected executor")
	}
	params, err := runForkParamsFromRequest(req.Params)
	if err != nil {
		return nil, err
	}
	actor := apiidempotency.BearerActor(req.ActorTokenID)
	if err := actor.ValidateMethod("run.fork"); err != nil {
		return nil, err
	}
	actorKey := string(actor.Kind) + ":" + actor.ID
	var operation runfork.ForkOperationRequest
	if params.IdempotencyKey != "" {
		stored, found, err := opts.Operations.LoadForkOperation(ctx, actorKey, params.IdempotencyKey, req.RequestHash)
		if err != nil {
			return nil, runForkError(params.SourceRunID, params.ForkEventID, err)
		}
		if found {
			if stored.Request.SourceRunID != params.SourceRunID || stored.Request.ForkEventID != params.ForkEventID {
				return nil, runForkError(params.SourceRunID, params.ForkEventID, fmt.Errorf("durable fork operation disagrees with request coordinates"))
			}
			if stored.Status == runfork.ForkOperationActivated {
				return projectForkOperationResult(stored)
			}
			if stored.Status != runfork.ForkOperationMaterialized {
				return nil, runForkError(params.SourceRunID, params.ForkEventID, fmt.Errorf("durable fork operation ended with %s", stored.Status))
			}
			operation = stored.Request
		}
	}
	if operation.OperationID == "" {
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
		if activeRunStatus(availability.Status) && !params.AllowSourceFreeze {
			return nil, NewInvalidParamsError(map[string]any{
				"field":  "allow_source_freeze",
				"reason": "must be true to allow permanent source freeze if it has not advanced beyond the fork point. A frozen source cannot resume; an advanced source stays independently live. Without this permission, no fork is started",
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
		selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
		if targetBundleHash != sourceBundleHash {
			selection = runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: targetBundleHash}
		}
		operation = runfork.ForkOperationRequest{
			OperationID: uuid.NewString(), Actor: actorKey, IdempotencyKey: params.IdempotencyKey,
			TransportHash: req.RequestHash, SourceRunID: params.SourceRunID, ForkEventID: params.ForkEventID,
			TargetBundleHash: targetBundleHash, AllowSourceFreeze: params.AllowSourceFreeze,
			ContractSelection: selection, DataPinOverrides: params.DataPinOverrides,
		}
	}
	canonical, _, err := operation.Canonical()
	if err != nil {
		return nil, runForkError(params.SourceRunID, params.ForkEventID, err)
	}
	_, executionErr := opts.Executor.ExecuteRunFork(ctx, RunForkExecutionRequest{
		ForkOperation: &canonical, SourceRunID: canonical.SourceRunID, ForkEventID: canonical.ForkEventID,
		BundleHash: canonical.TargetBundleHash, AllowSourceFreeze: canonical.AllowSourceFreeze,
		DataPinOverrides: canonical.DataPinOverrides, ContractSelection: canonical.ContractSelection,
	})
	stored, found, lookupErr := opts.Operations.LoadForkOperationByID(ctx, canonical.OperationID)
	if lookupErr != nil {
		return nil, runForkError(params.SourceRunID, params.ForkEventID, errors.Join(executionErr, lookupErr))
	}
	if found && stored.Status == runfork.ForkOperationActivated {
		if executionErr != nil {
			diaglog.ProcessLog(diaglog.LevelWarn, "api", "acknowledged run.fork cleanup failed",
				"source_run_id", canonical.SourceRunID, "fork_run_id", stored.ForkRunID, "error", executionErr.Error())
		}
		return projectForkOperationResult(stored)
	}
	if executionErr != nil {
		return nil, runForkError(params.SourceRunID, params.ForkEventID, executionErr)
	}
	return nil, runForkError(params.SourceRunID, params.ForkEventID, fmt.Errorf("run.fork returned without a durable activation result"))
}

func projectForkOperationResult(record runfork.ForkOperationRecord) (RunForkExecutionResult, error) {
	if err := record.Validate(); err != nil {
		return RunForkExecutionResult{}, err
	}
	if record.Status != runfork.ForkOperationActivated || record.Result == nil {
		return RunForkExecutionResult{}, fmt.Errorf("fork operation has no activated result")
	}
	r := record.Result
	pins := append([]durabledata.Pin(nil), r.DataPins...)
	if pins == nil {
		pins = []durabledata.Pin{}
	}
	result := RunForkExecutionResult{
		Owner:       runfork.RunForkSelectedContractExecutionOwner,
		SourceRunID: r.SourceRunID, SourceRunStatus: r.SourceRunStatus, SourceFrozen: r.SourceFrozen,
		ForkRunID: r.ForkRunID, ForkPointKind: string(r.ForkPoint.Kind), ForkRevision: r.ForkPoint.Revision,
		ForkEventID: r.ForkPoint.EventID, ForkRunStatus: r.ForkRunStatus,
		BundleHash: r.BundleHash, ExecutedEventCount: r.ExecutedEventCount, DataPins: pins,
		activationAcknowledged: true,
	}
	if err := validateRunForkExecutionResult(result); err != nil {
		return RunForkExecutionResult{}, err
	}
	return result, nil
}

func validateRunForkExecutionResult(result RunForkExecutionResult) error {
	if result.ForkRevision <= 0 {
		return fmt.Errorf("run.fork result has no selected revision")
	}
	switch result.ForkPointKind {
	case string(runfork.RunForkPointEvent):
		if result.ForkEventID == "" {
			return fmt.Errorf("event run.fork result has no exact event identity")
		}
	case string(runfork.RunForkPointDeploymentRevision):
		if result.ForkEventID != "" {
			return fmt.Errorf("deployment revision run.fork result invented an event identity")
		}
	default:
		return fmt.Errorf("run.fork result has invalid point kind %q", result.ForkPointKind)
	}
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
	SourceRunID       string
	ForkEventID       string
	BundleHash        string
	AllowSourceFreeze bool
	DataPinOverrides  []durabledata.ExplicitPin
	IdempotencyKey    string
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
	allowSourceFreeze, err := optionalBoolParam(params, "allow_source_freeze", false)
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
		SourceRunID:       sourceRunID,
		ForkEventID:       forkEventID,
		BundleHash:        bundleHash,
		AllowSourceFreeze: allowSourceFreeze,
		DataPinOverrides:  dataPinOverrides,
		IdempotencyKey:    idempotencyKey,
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
	var operationConflict *runfork.ForkOperationKeyConflictError
	if errors.As(err, &operationConflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    operationConflict.OriginalRequestHash,
			"conflicting_request_hash": operationConflict.ConflictingRequestHash,
		})
	}
	msg := err.Error()
	switch {
	case errors.Is(err, runfork.ErrRunForkSourceFreezeConfirmationRequired):
		return NewInvalidParamsError(map[string]any{
			"field":  "allow_source_freeze",
			"reason": "must be true to allow permanently freezing a source that has not advanced beyond the fork point. A frozen source cannot resume; an advanced source stays independently live. Withhold permission to decline source freeze",
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
