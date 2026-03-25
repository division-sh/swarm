package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func (h *Handler) handleBuilderRPC(w http.ResponseWriter, r *http.Request) {
	var req builderRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, builderRPCResponse{
			JSONRPC: "2.0",
			Error: &builderRPCError{
				Code:    -32700,
				Message: err.Error(),
			},
		})
		return
	}
	resp := builderRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}
	result, rpcErr := h.dispatchBuilderRPC(r.Context(), strings.TrimSpace(req.Method), req.Params)
	if rpcErr != nil {
		resp.Error = rpcErr
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Result = result
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) dispatchBuilderRPC(
	ctx context.Context,
	method string,
	params map[string]any,
) (any, *builderRPCError) {
	switch strings.TrimSpace(method) {
	case "engine.ping":
		return map[string]any{
			"status":  "ok",
			"version": h.builderVersion(),
		}, nil
	case "project.open":
		if h.projectControl == nil {
			return nil, builderMethodUnavailable("project controller is not configured")
		}
		projectDir := strings.TrimSpace(asString(params["project_dir"]))
		if projectDir == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "project_dir is required",
			}
		}
		status, err := h.projectControl.OpenProject(ctx, projectDir)
		if err != nil {
			return nil, builderInternalError(err)
		}
		return status, nil
	case "project.reload":
		if h.projectControl == nil {
			return nil, builderMethodUnavailable("project controller is not configured")
		}
		projectDir := strings.TrimSpace(asString(params["project_dir"]))
		status, err := h.projectControl.ReloadProject(ctx, projectDir)
		if err != nil {
			return nil, builderInternalError(err)
		}
		return status, nil
	case "project.close":
		if h.projectControl == nil {
			return nil, builderMethodUnavailable("project controller is not configured")
		}
		status, err := h.projectControl.CloseProject(ctx)
		if err != nil {
			return nil, builderInternalError(err)
		}
		return status, nil
	case "run.start":
		if h.runHub == nil {
			return nil, builderMethodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "run_id is required",
			}
		}
		inputs, _ := params["inputs"].(map[string]any)
		breakpoints := asStringSlice(params["breakpoints"])
		if err := h.runHub.startRun(ctx, runID, inputs, breakpoints); err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{
			"run_id": runID,
			"status": "started",
		}, nil
	case "run.stop":
		if h.runHub == nil {
			return nil, builderMethodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "run_id is required",
			}
		}
		if err := h.runHub.stopRun(runID); err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{
			"run_id": runID,
			"status": "stopped",
		}, nil
	case "run.pause":
		if h.runHub == nil {
			return nil, builderMethodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "run_id is required",
			}
		}
		if err := h.runHub.pauseRun(runID); err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{
			"run_id": runID,
			"status": "paused",
		}, nil
	case "run.continue":
		if h.runHub == nil {
			return nil, builderMethodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "run_id is required",
			}
		}
		instanceIDs := asStringSlice(params["instance_ids"])
		decision := strings.TrimSpace(asString(params["decision"]))
		if err := h.runHub.continueRun(runID, instanceIDs, decision); err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{
			"run_id": runID,
			"status": "running",
		}, nil
	case "run.step":
		if h.runHub == nil {
			return nil, builderMethodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "run_id is required",
			}
		}
		nodeID := strings.TrimSpace(asString(params["node_id"]))
		instanceID := strings.TrimSpace(asString(params["instance_id"]))
		if err := h.runHub.stepRun(runID, nodeID, instanceID); err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{
			"run_id": runID,
			"status": "running",
		}, nil
	case "run.retry":
		if h.runHub == nil {
			return nil, builderMethodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "run_id is required",
			}
		}
		nodeID := strings.TrimSpace(asString(params["node_id"]))
		instanceID := strings.TrimSpace(asString(params["instance_id"]))
		if err := h.runHub.retryRun(runID, nodeID, instanceID); err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{
			"run_id": runID,
			"status": "running",
		}, nil
	case "run.skip":
		if h.runHub == nil {
			return nil, builderMethodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "run_id is required",
			}
		}
		nodeID := strings.TrimSpace(asString(params["node_id"]))
		instanceID := strings.TrimSpace(asString(params["instance_id"]))
		if err := h.runHub.skipRun(runID, nodeID, instanceID); err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{
			"run_id": runID,
			"status": "running",
		}, nil
	case "state.list_instances", "state.get_instances":
		if h.instances == nil {
			return nil, builderMethodUnavailable("instance reader is not configured")
		}
		rows, err := h.instances.List(ctx)
		if err != nil {
			return nil, builderInternalError(err)
		}
		return map[string]any{"instances": rows}, nil
	case "state.get_entity":
		if h.instances == nil {
			return nil, builderMethodUnavailable("instance reader is not configured")
		}
		instanceID := strings.TrimSpace(asString(params["instance_id"]))
		if instanceID == "" {
			return nil, &builderRPCError{
				Code:    -32602,
				Message: "instance_id is required",
			}
		}
		instance, ok, err := h.instances.Load(ctx, instanceID)
		if err != nil {
			return nil, builderInternalError(err)
		}
		if !ok {
			return nil, &builderRPCError{
				Code:    -32004,
				Message: "instance not found",
				Data:    map[string]any{"instance_id": instanceID},
			}
		}
		return map[string]any{
			"entity":      builderEntityPayload(instance),
			"gates":       builderEntityGates(instance),
			"accumulated": builderEntityAccumulated(instance),
		}, nil
	case "validate.full":
		return h.runFullValidation(ctx), nil
	default:
		return nil, &builderRPCError{
			Code:    -32601,
			Message: "method not found",
			Data:    map[string]any{"method": method},
		}
	}
}

func builderEntityPayload(instance runtimepipeline.WorkflowInstance) map[string]any {
	entity := map[string]any{
		"state": strings.TrimSpace(instance.CurrentState),
	}
	for key, value := range instance.Metadata {
		key = strings.TrimSpace(key)
		if key == "" || key == "gates" {
			continue
		}
		switch key {
		case "slug",
			"name",
			"entity_type",
			"storage_ref",
			"instance_id",
			"flow_path",
			"instance_kind",
			"template_version",
			"last_source_event",
			"status",
			"transition_history":
			continue
		}
		entity[key] = value
	}
	return entity
}

func builderEntityGates(instance runtimepipeline.WorkflowInstance) map[string]any {
	gates, _ := instance.Metadata["gates"].(map[string]any)
	if len(gates) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(gates))
	for key, value := range gates {
		out[key] = value
	}
	return out
}

func builderEntityAccumulated(instance runtimepipeline.WorkflowInstance) map[string]any {
	if len(instance.StateBuckets) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(instance.StateBuckets))
	for key, value := range instance.StateBuckets {
		out[key] = value
	}
	return out
}

func (h *Handler) builderHealthSnapshot(ctx context.Context) builderEngineHealth {
	snapshot := builderEngineHealth{
		Status:    "ok",
		Version:   h.builderVersion(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	if h.health == nil {
		return snapshot
	}
	checks, err := h.health(ctx)
	if err != nil {
		snapshot.Status = "degraded"
		snapshot.DatabaseErr = err.Error()
		return snapshot
	}
	if runtimeCheck, ok := checks["runtime"].(map[string]any); ok {
		snapshot.Runtime = runtimeCheck
		if ready, ok := runtimeCheck["ready"].(bool); ok {
			snapshot.Ready = ready
		}
	}
	if dbCheck, ok := checks["database"].(map[string]any); ok {
		snapshot.Database = dbCheck
	}
	if dbErr, ok := checks["database_error"].(string); ok {
		snapshot.DatabaseErr = strings.TrimSpace(dbErr)
		if snapshot.DatabaseErr != "" {
			snapshot.Status = "degraded"
		}
	}
	snapshot.Project = h.currentProjectStatus()
	return snapshot
}

func builderMethodUnavailable(message string) *builderRPCError {
	return &builderRPCError{
		Code:    -32004,
		Message: strings.TrimSpace(message),
	}
}

func builderInternalError(err error) *builderRPCError {
	if err == nil {
		err = errors.New("internal error")
	}
	return &builderRPCError{
		Code:    -32000,
		Message: strings.TrimSpace(err.Error()),
	}
}
