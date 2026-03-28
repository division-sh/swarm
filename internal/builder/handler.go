package builder

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
)

type handler struct {
	health         HealthChecker
	instances      InstanceReader
	runtime        RuntimeController
	credentials    runtimecredentials.Store
	version        string
	semanticSource semanticview.Source
	currentSource  SourceProvider
	currentRuntime RuntimeProvider
	projectControl ProjectController
	runHub         *runHub
	mux            *http.ServeMux
}

var healthHeartbeatInterval = 5 * time.Second

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		http.NotFound(w, r)
		return
	}
	if h.mux == nil {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /rpc", h.handleRPC)
		mux.HandleFunc("POST /api/rpc", h.handleRPC)
		mux.HandleFunc("GET /ws", h.handleWS)
		mux.HandleFunc("GET /api/ws", h.handleWS)
		h.mux = mux
	}
	h.mux.ServeHTTP(w, r)
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
		if err := h.runHub.stopRun(runID); err != nil {
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
		if err := h.runHub.pauseRun(runID); err != nil {
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
		if err := h.runHub.continueRun(runID, instanceIDs, decision); err != nil {
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
		if h.instances == nil {
			return nil, methodUnavailable("instance reader is not configured")
		}
		rows, err := h.instances.List(ctx)
		if err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"instances": rows}, nil
	case "state.get_entity":
		if h.instances == nil {
			return nil, methodUnavailable("instance reader is not configured")
		}
		instanceID := strings.TrimSpace(asString(params["instance_id"]))
		if instanceID == "" {
			return nil, &RPCError{Code: -32602, Message: "instance_id is required"}
		}
		instance, ok, err := h.instances.Load(ctx, instanceID)
		if err != nil {
			return nil, internalError(err)
		}
		if !ok {
			return nil, &RPCError{Code: -32004, Message: "instance not found", Data: map[string]any{"instance_id": instanceID}}
		}
		return map[string]any{
			"entity":      entityPayload(instance),
			"gates":       entityGates(instance),
			"accumulated": entityAccumulated(instance),
		}, nil
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

func (h *handler) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &wsClient{
		handler:    h,
		conn:       conn,
		subscribed: map[string]context.CancelFunc{},
	}
	client.run(r.Context())
}

func (h *handler) builderVersion() string {
	if version := strings.TrimSpace(h.version); version != "" {
		return version
	}
	return "dev"
}

func (h *handler) currentSemanticSource() semanticview.Source {
	if h.currentSource != nil {
		if source := h.currentSource(); source != nil {
			return source
		}
	}
	return h.semanticSource
}

func (h *handler) currentProjectStatus() ProjectStatus {
	if h.projectControl == nil {
		return ProjectStatus{}
	}
	return h.projectControl.CurrentProject()
}

func (h *handler) runFullValidation(_ context.Context) ValidationResult {
	startedAt := time.Now()
	flowCount := 0
	source := h.currentSemanticSource()
	if source != nil {
		flowCount = len(source.FlowSchemaEntries())
	}
	result := ValidationResult{
		Status:   "pass",
		Errors:   []ValidationIssue{},
		Warnings: []ValidationIssue{},
		Summary: ValidationSummary{
			FlowsChecked: flowCount,
		},
	}
	if source == nil {
		result.Status = "fail"
		result.Errors = append(result.Errors, ValidationIssue{
			CheckID:  "engine_source_unavailable",
			Severity: "error",
			Message:  "semantic source is not configured",
		})
		result.Summary.Errors = len(result.Errors)
		result.Summary.DurationMS = time.Since(startedAt).Milliseconds()
		return result
	}
	warnings, err := runtimepipeline.ValidateWorkflowContractsDetailed(source)
	if err != nil {
		for _, line := range strings.Split(strings.TrimSpace(err.Error()), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				result.Errors = append(result.Errors, ValidationIssue{
					CheckID:  normalizeValidationErrorCheckID(line),
					Severity: "error",
					Message:  line,
				})
			}
		}
	}
	_, permissionErrors := runtimetools.ValidateAgentPermissions(source)
	for _, permissionErr := range permissionErrors {
		result.Errors = append(result.Errors, ValidationIssue{
			CheckID:  "agent_permission_validation",
			Severity: "error",
			Message:  strings.TrimSpace(permissionErr.Error()),
		})
	}
	for _, warning := range warnings {
		result.Warnings = append(result.Warnings, ValidationIssue{
			CheckID:  normalizeCheckID(warning.Category),
			Severity: "warning",
			Message:  strings.TrimSpace(warning.Message),
		})
	}
	if h.credentials != nil {
		missing, err := runtimecredentials.MissingRequired(context.Background(), h.credentials, source)
		if err != nil {
			result.Errors = append(result.Errors, ValidationIssue{
				CheckID:  "credential_validation",
				Severity: "error",
				Message:  strings.TrimSpace(err.Error()),
			})
		} else {
			for _, item := range missing {
				requiredBy := make([]string, 0, len(item.RequiredBy))
				for _, ref := range item.RequiredBy {
					requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+" "+strings.TrimSpace(ref.Name))
				}
				result.Warnings = append(result.Warnings, ValidationIssue{
					CheckID:    "credential_key_exists",
					Severity:   "warning",
					Message:    fmtCredentialWarning(item.Key, requiredBy),
					Suggestion: "set the credential with credentials.set before executing dependent tools",
				})
			}
		}
	}
	if len(result.Errors) > 0 {
		result.Status = "fail"
	}
	result.Summary.Errors = len(result.Errors)
	result.Summary.Warnings = len(result.Warnings)
	result.Summary.DurationMS = time.Since(startedAt).Milliseconds()
	return result
}

func entityPayload(instance runtimepipeline.WorkflowInstance) map[string]any {
	entity := map[string]any{"state": strings.TrimSpace(instance.CurrentState)}
	for key, value := range instance.Metadata {
		key = strings.TrimSpace(key)
		if key == "" || key == "gates" {
			continue
		}
		switch key {
		case "slug", "name", "entity_type", "storage_ref", "instance_id", "flow_path", "instance_kind", "template_version", "last_source_event", "status", "transition_history":
			continue
		}
		entity[key] = value
	}
	return entity
}

func entityGates(instance runtimepipeline.WorkflowInstance) map[string]any {
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

func entityAccumulated(instance runtimepipeline.WorkflowInstance) map[string]any {
	if len(instance.StateBuckets) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(instance.StateBuckets))
	for key, value := range instance.StateBuckets {
		out[key] = value
	}
	return out
}

func (h *handler) healthSnapshot(ctx context.Context) EngineHealth {
	snapshot := EngineHealth{
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

type wsClient struct {
	handler    *handler
	conn       *websocket.Conn
	subscribed map[string]context.CancelFunc
	mu         sync.Mutex
}

func (c *wsClient) run(ctx context.Context) {
	defer c.close()
	for {
		var frame map[string]any
		if err := c.conn.ReadJSON(&frame); err != nil {
			return
		}
		switch strings.TrimSpace(asString(frame["type"])) {
		case "rpc":
			c.handleRPC(ctx, frame)
		case "subscribe":
			c.handleSubscribe(ctx, strings.TrimSpace(asString(frame["channel"])))
		case "unsubscribe":
			c.handleUnsubscribe(strings.TrimSpace(asString(frame["channel"])))
		}
	}
}

func (c *wsClient) handleRPC(ctx context.Context, frame map[string]any) {
	params, _ := frame["params"].(map[string]any)
	result, rpcErr := c.handler.dispatchRPC(ctx, strings.TrimSpace(asString(frame["method"])), params)
	_ = c.writeJSON(RPCResponse{JSONRPC: "2.0", ID: frame["id"], Result: result, Error: rpcErr})
}

func (c *wsClient) handleSubscribe(ctx context.Context, channel string) {
	if channel == "" {
		return
	}
	c.mu.Lock()
	if _, exists := c.subscribed[channel]; exists {
		c.mu.Unlock()
		return
	}
	subCtx, cancel := context.WithCancel(ctx)
	c.subscribed[channel] = cancel
	c.mu.Unlock()
	switch channel {
	case "engine:health":
		go c.runEngineHealth(subCtx, channel)
	default:
		if strings.HasPrefix(channel, "run:events:") && c.handler.runHub != nil {
			runID := strings.TrimSpace(strings.TrimPrefix(channel, "run:events:"))
			cancel = c.handler.runHub.subscribe(runID, func(data RunEventEnvelope) {
				_ = c.writeEvent(channel, data)
			})
			c.mu.Lock()
			c.subscribed[channel] = cancel
			c.mu.Unlock()
			return
		}
		c.handleUnsubscribe(channel)
	}
}

func (c *wsClient) handleUnsubscribe(channel string) {
	if channel == "" {
		return
	}
	c.mu.Lock()
	cancel := c.subscribed[channel]
	delete(c.subscribed, channel)
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *wsClient) runEngineHealth(ctx context.Context, channel string) {
	defer c.handleUnsubscribe(channel)
	ticker := time.NewTicker(healthHeartbeatInterval)
	defer ticker.Stop()
	_ = c.writeEvent(channel, c.handler.healthSnapshot(ctx))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.writeEvent(channel, c.handler.healthSnapshot(ctx)); err != nil {
				return
			}
		}
	}
}

func (c *wsClient) writeEvent(channel string, data any) error {
	return c.writeJSON(WSEventFrame{Type: "event", Channel: channel, Data: data})
}

func (c *wsClient) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

func (c *wsClient) close() {
	c.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(c.subscribed))
	for _, cancel := range c.subscribed {
		cancels = append(cancels, cancel)
	}
	c.subscribed = map[string]context.CancelFunc{}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	_ = c.conn.Close()
}

func normalizeCheckID(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "validation"
	}
	switch raw {
	case "condition-payload":
		return "condition_payload_alignment"
	case "tool-missing":
		return "tool_resolution"
	case "event-no-consumer":
		return "event_consumer_exists"
	case "event-no-producer":
		return "event_producer_exists"
	case "event-cycle":
		return "event_cycle_detection"
	case "prompt-missing", "prompt-stub":
		return "prompt_exists"
	case "policy-conflict":
		return "policy_conflict_detection"
	case "deprecated":
		return "deprecated_contract_alias"
	case "permission-mismatch":
		return "agent_permission_validation"
	}
	replacer := strings.NewReplacer(" ", "_", "-", "_", ".", "_", "/", "_")
	return replacer.Replace(raw)
}

func normalizeValidationErrorCheckID(message string) string {
	msg := strings.ToLower(strings.TrimSpace(message))
	switch {
	case strings.Contains(msg, "reserved platform.* namespace"):
		return "platform_namespace_violation"
	case strings.Contains(msg, "native_tools."):
		return "native_tools_valid"
	case strings.Contains(msg, "required agent role"):
		return "required_agents_match"
	case strings.Contains(msg, "payload.") && strings.Contains(msg, "condition"):
		return "condition_payload_alignment"
	case strings.Contains(msg, "payload.") && strings.Contains(msg, "schema"):
		return "payload_field_coverage"
	case strings.Contains(msg, "cycle"):
		return "event_cycle_detection"
	case strings.Contains(msg, "on_complete") || strings.Contains(msg, "deprecated handler-level condition") || strings.Contains(msg, "deprecated logic field"):
		return "dialect_compliance"
	case strings.Contains(msg, "advances_to") || strings.Contains(msg, "initial_state") || strings.Contains(msg, "unreachable"):
		return "state_machine_coherence"
	case strings.Contains(msg, "workspace_class"):
		return "workspace_class_exists"
	default:
		return "workflow_contract_validation"
	}
}

func methodUnavailable(message string) *RPCError {
	return &RPCError{Code: -32004, Message: strings.TrimSpace(message)}
}

func internalError(err error) *RPCError {
	if err == nil {
		err = errors.New("internal error")
	}
	return &RPCError{Code: -32000, Message: strings.TrimSpace(err.Error())}
}

func errUnavailable(message string) error { return errors.New(strings.TrimSpace(message)) }

func fmtCredentialWarning(key string, requiredBy []string) string {
	if len(requiredBy) == 0 {
		return "missing credential " + strconvQuote(key)
	}
	return "missing credential " + strconvQuote(key) + " required by " + strings.Join(requiredBy, ", ")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func asStringSlice(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		if stringsSlice, ok := v.([]string); ok {
			out := make([]string, 0, len(stringsSlice))
			for _, item := range stringsSlice {
				if item = strings.TrimSpace(item); item != "" {
					out = append(out, item)
				}
			}
			return out
		}
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if value := strings.TrimSpace(asString(item)); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func credentialRecords(items []runtimecredentials.Descriptor) []CredentialRecord {
	out := make([]CredentialRecord, 0, len(items))
	for _, item := range items {
		out = append(out, credentialRecord(item))
	}
	return out
}

func credentialRecord(item runtimecredentials.Descriptor) CredentialRecord {
	record := CredentialRecord{
		Key:      item.Key,
		Present:  item.Present,
		Source:   item.Source,
		Writable: item.Writable,
	}
	if item.UpdatedAt != nil && !item.UpdatedAt.IsZero() {
		record.UpdatedAt = item.UpdatedAt.UTC().Format(time.RFC3339)
	}
	for _, ref := range item.RequiredBy {
		record.RequiredBy = append(record.RequiredBy, CredentialRequirement{
			Kind: ref.Kind,
			Name: ref.Name,
		})
	}
	return record
}

func strconvQuote(value string) string {
	raw, err := json.Marshal(strings.TrimSpace(value))
	if err != nil {
		return `""`
	}
	return string(raw)
}
