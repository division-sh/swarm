package builder

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimerunstart "github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/store"
)

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (h *handler) handleRPC(w http.ResponseWriter, r *http.Request) {
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RPCResponse{
			JSONRPC: "2.0",
			Error: &RPCError{
				Code:    -32700,
				Message: err.Error(),
			},
		})
		return
	}
	resp := RPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}
	result, rpcErr := h.dispatchRPC(r.Context(), strings.TrimSpace(req.Method), req.Params)
	if rpcErr != nil {
		resp.Error = rpcErr
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Result = result
	writeJSON(w, http.StatusOK, resp)
}

func (h *handler) dispatchRPC(ctx context.Context, method string, params map[string]any) (any, *RPCError) {
	switch strings.TrimSpace(method) {
	case "engine.ping":
		return map[string]any{
			"status":  "ok",
			"version": h.builderVersion(),
		}, nil
	case "project.open":
		if h.projectControl == nil {
			return nil, methodUnavailable("project controller is not configured")
		}
		projectDir := strings.TrimSpace(asString(params["project_dir"]))
		if projectDir == "" {
			return nil, &RPCError{Code: -32602, Message: "project_dir is required"}
		}
		status, err := h.projectControl.OpenProject(ctx, projectDir)
		if err != nil {
			return nil, internalError(err)
		}
		return status, nil
	case "project.reload":
		if h.projectControl == nil {
			return nil, methodUnavailable("project controller is not configured")
		}
		status, err := h.projectControl.ReloadProject(ctx, strings.TrimSpace(asString(params["project_dir"])))
		if err != nil {
			return nil, internalError(err)
		}
		return status, nil
	case "project.close":
		if h.projectControl == nil {
			return nil, methodUnavailable("project controller is not configured")
		}
		status, err := h.projectControl.CloseProject(ctx)
		if err != nil {
			return nil, internalError(err)
		}
		return status, nil
	case "run.start":
		if h.runHub == nil {
			return nil, methodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &RPCError{Code: -32602, Message: "run_id is required"}
		}
		inputs, _ := params["inputs"].(map[string]any)
		if source := h.currentSemanticSource(); source != nil {
			inputEvents := make([]string, 0, len(inputs))
			for eventName := range inputs {
				if strings.TrimSpace(eventName) != "" {
					inputEvents = append(inputEvents, eventName)
				}
			}
			if _, err := runtimerunstart.ValidateInputEvents(source, inputEvents); err != nil {
				return nil, &RPCError{Code: -32602, Message: err.Error()}
			}
		}
		breakpoints := asStringSlice(params["breakpoints"])
		if err := h.runHub.startRun(ctx, runID, inputs, breakpoints); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"run_id": runID, "status": "started"}, nil
	case "run.stop":
		if h.runHub == nil {
			return nil, methodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &RPCError{Code: -32602, Message: "run_id is required"}
		}
		if err := h.runHub.stopRun(ctx, runID); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"run_id": runID, "status": "stopped"}, nil
	case "run.pause":
		if h.runHub == nil {
			return nil, methodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &RPCError{Code: -32602, Message: "run_id is required"}
		}
		if err := h.runHub.pauseRun(ctx, runID); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"run_id": runID, "status": "paused"}, nil
	case "run.continue":
		if h.runHub == nil {
			return nil, methodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &RPCError{Code: -32602, Message: "run_id is required"}
		}
		instanceIDs := asStringSlice(params["instance_ids"])
		decision := strings.TrimSpace(asString(params["decision"]))
		if len(instanceIDs) > 0 || decision != "" {
			return nil, &RPCError{Code: -32602, Message: "run.continue no longer accepts human-decision parameters; use mailbox decision methods"}
		}
		if err := h.runHub.continueRun(ctx, runID); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"run_id": runID, "status": "running"}, nil
	case "run.step":
		if h.runHub == nil {
			return nil, methodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &RPCError{Code: -32602, Message: "run_id is required"}
		}
		if err := h.runHub.stepRun(runID, strings.TrimSpace(asString(params["node_id"])), strings.TrimSpace(asString(params["instance_id"]))); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"run_id": runID, "status": "running"}, nil
	case "run.retry":
		if h.runHub == nil {
			return nil, methodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &RPCError{Code: -32602, Message: "run_id is required"}
		}
		if err := h.runHub.retryRun(runID, strings.TrimSpace(asString(params["node_id"])), strings.TrimSpace(asString(params["instance_id"]))); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"run_id": runID, "status": "running"}, nil
	case "run.skip":
		if h.runHub == nil {
			return nil, methodUnavailable("run hub is not configured")
		}
		runID := strings.TrimSpace(asString(params["run_id"]))
		if runID == "" {
			return nil, &RPCError{Code: -32602, Message: "run_id is required"}
		}
		if err := h.runHub.skipRun(runID, strings.TrimSpace(asString(params["node_id"])), strings.TrimSpace(asString(params["instance_id"]))); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"run_id": runID, "status": "running"}, nil
	case "state.list_instances", "state.get_instances":
		if h.entities == nil {
			return nil, methodUnavailable("entity reader is not configured")
		}
		rows, err := h.legacyBuilderListEntities(ctx)
		if err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"instances": rows}, nil
	case "state.get_entity":
		if h.entities == nil {
			return nil, methodUnavailable("entity reader is not configured")
		}
		instanceID := strings.TrimSpace(asString(params["instance_id"]))
		if instanceID == "" {
			return nil, &RPCError{Code: -32602, Message: "instance_id is required"}
		}
		entity, err := h.entities.LoadOperatorEntity(ctx, runtimeflowidentity.EntityID(instanceID), strings.TrimSpace(asString(params["run_id"])))
		if errors.Is(err, store.ErrEntityNotFound) {
			return nil, &RPCError{Code: -32004, Message: "instance not found", Data: map[string]any{"instance_id": instanceID}}
		}
		if errors.Is(err, store.ErrAmbiguousEntityRunID) || errors.Is(err, store.ErrInvalidEntityReadParam) {
			return nil, &RPCError{Code: -32602, Message: err.Error()}
		}
		if err != nil {
			return nil, internalError(err)
		}
		return entity, nil
	case "credentials.list":
		if h.credentials == nil {
			return nil, methodUnavailable("credential store is not configured")
		}
		items, err := runtimecredentials.ListDescriptors(ctx, h.credentials, h.currentSemanticSource())
		if err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"credentials": credentialRecords(items)}, nil
	case "credentials.set":
		if h.credentials == nil {
			return nil, methodUnavailable("credential store is not configured")
		}
		key := strings.TrimSpace(asString(params["key"]))
		if key == "" {
			return nil, &RPCError{Code: -32602, Message: "key is required"}
		}
		rawValue, ok := params["value"]
		if !ok {
			return nil, &RPCError{Code: -32602, Message: "value is required"}
		}
		if err := h.credentials.Set(ctx, key, asString(rawValue)); err != nil {
			return nil, internalError(err)
		}
		record, err := runtimecredentials.Describe(ctx, h.credentials, h.currentSemanticSource(), key)
		if err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"credential": credentialRecord(record)}, nil
	case "credentials.delete":
		if h.credentials == nil {
			return nil, methodUnavailable("credential store is not configured")
		}
		key := strings.TrimSpace(asString(params["key"]))
		if key == "" {
			return nil, &RPCError{Code: -32602, Message: "key is required"}
		}
		if err := h.credentials.Delete(ctx, key); err != nil {
			return nil, internalError(err)
		}
		record, err := runtimecredentials.Describe(ctx, h.credentials, h.currentSemanticSource(), key)
		if err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"credential": credentialRecord(record)}, nil
	case "validate.full":
		return h.runFullValidation(ctx), nil
	default:
		return nil, &RPCError{
			Code:    -32601,
			Message: "method not found",
			Data:    map[string]any{"method": method},
		}
	}
}
