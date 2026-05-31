package apiv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"swarm/internal/apispec"
)

const (
	jsonRPCVersion = "2.0"

	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603

	transportHTTP      = "http"
	transportWebSocket = "websocket"
)

const (
	DefaultLoopbackAPIToken        = "swarm-dev-loopback-api-token"
	DefaultLoopbackAPITokenWarning = "using built-in dev API token on loopback; set SWARM_API_TOKEN before exposing"
)

type AuthTokenSource string

const (
	AuthTokenSourceEnvironment          AuthTokenSource = "SWARM_API_TOKEN"
	AuthTokenSourceBuiltInLoopbackToken AuthTokenSource = "built-in-loopback-default"
)

type AuthTokenResolution struct {
	Tokens   []string
	Source   AuthTokenSource
	Explicit bool
}

func (r AuthTokenResolution) UsesDefaultLoopbackToken() bool {
	return !r.Explicit && r.Source == AuthTokenSourceBuiltInLoopbackToken
}

type Handler struct {
	registry *Registry
	tokens   map[string]struct{}
	handlers map[string]MethodHandler
	upgrader websocket.Upgrader
	subs     *SubscriptionRuntime
}

type MethodHandler func(context.Context, Request) (any, error)

type Options struct {
	PlatformSpecPath string
	Registry         *Registry
	AuthTokens       []string
	Handlers         map[string]MethodHandler
	Subscriptions    *SubscriptionRuntime
}

type Request struct {
	ID            any
	Method        string
	Params        map[string]any
	CorrelationID string
	Transport     string
	ActorTokenID  string
	RequestHash   string
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type standardErrorData struct {
	CorrelationID string `json:"correlation_id"`
	Details       any    `json:"details,omitempty"`
}

type applicationErrorData struct {
	Code          string `json:"code"`
	Details       any    `json:"details"`
	Retryable     bool   `json:"retryable"`
	CorrelationID string `json:"correlation_id"`
}

func NewHandler(opts Options) (*Handler, error) {
	registry := opts.Registry
	if registry == nil {
		var err error
		registry, err = LoadRegistry(opts.PlatformSpecPath)
		if err != nil {
			return nil, err
		}
	}
	handlers := map[string]MethodHandler{
		"rpc.unsubscribe": func(context.Context, Request) (any, error) {
			return map[string]any{"ok": true}, nil
		},
	}
	for name, handler := range opts.Handlers {
		clean := strings.TrimSpace(name)
		if clean == "" {
			return nil, errors.New("api handler method name is empty")
		}
		if _, ok := registry.Method(clean); !ok {
			return nil, fmt.Errorf("api handler %s is not in the canonical method catalog", clean)
		}
		if handler == nil {
			return nil, fmt.Errorf("api handler %s is nil", clean)
		}
		handlers[clean] = handler
	}
	return &Handler{
		registry: registry,
		tokens:   tokenSet(opts.AuthTokens),
		handlers: handlers,
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
		subs:     opts.Subscriptions.withDefaults(),
	}, nil
}

func AuthTokensFromEnvironment() []string {
	return ResolveAuthTokensFromEnvironment().Tokens
}

func ResolveAuthTokensFromEnvironment() AuthTokenResolution {
	value := strings.TrimSpace(os.Getenv("SWARM_API_TOKEN"))
	if value != "" {
		return AuthTokenResolution{
			Tokens:   []string{value},
			Source:   AuthTokenSourceEnvironment,
			Explicit: true,
		}
	}
	return AuthTokenResolution{
		Tokens: []string{DefaultLoopbackAPIToken},
		Source: AuthTokenSourceBuiltInLoopbackToken,
	}
}

func DefaultLoopbackAPITokenAllowedHost(host string) bool {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if host == "" {
		return false
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.IsLoopback()
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		http.NotFound(w, r)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL != nil && r.URL.Path == "/v1/rpc":
		h.handleRPC(w, r)
	case r.Method == http.MethodGet && r.URL != nil && r.URL.Path == "/v1/ws":
		h.handleWS(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleRPC(w http.ResponseWriter, r *http.Request) {
	correlationID := requestCorrelationID(r, nil)
	w.Header().Set("X-Correlation-ID", correlationID)
	token, failure := h.authorize(r)
	if failure != nil {
		writeAuthFailure(w, failure)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeRPC(w, rpcResponse{JSONRPC: jsonRPCVersion, ID: nil, Error: h.standardError(codeInvalidRequest, "invalid request", correlationID, map[string]any{"reason": "read body failed"})})
		return
	}
	resp := h.dispatch(r.Context(), raw, transportHTTP, correlationID, actorTokenID(token))
	writeRPC(w, resp)
}

func (h *Handler) handleWS(w http.ResponseWriter, r *http.Request) {
	token, failure := h.authorize(r)
	if failure != nil {
		writeAuthFailure(w, failure)
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	session := newWebSocketSession(h, conn, r.Context(), requestCorrelationID(r, nil), actorTokenID(token))
	session.run()
}

func (h *Handler) dispatch(ctx context.Context, raw []byte, transport, fallbackCorrelationID, actorTokenID string) rpcResponse {
	req, failure := h.prepareRequest(raw, transport, fallbackCorrelationID, actorTokenID)
	if failure != nil {
		return *failure
	}
	return h.dispatchPrepared(ctx, req)
}

func (h *Handler) prepareRequest(raw []byte, transport, fallbackCorrelationID, actorTokenID string) (Request, *rpcResponse) {
	req, id, correlationID, rpcErr := h.parseRequest(raw, fallbackCorrelationID)
	if rpcErr != nil {
		return Request{}, &rpcResponse{JSONRPC: jsonRPCVersion, ID: id, Error: rpcErr}
	}
	req.Transport = transport
	req.ActorTokenID = strings.TrimSpace(actorTokenID)
	req.RequestHash = requestBodyHash(req.Method, req.Params)
	if method, ok := h.registry.Method(req.Method); ok {
		if rpcErr := h.admitMethodTransport(req.Method, method, transport, correlationID); rpcErr != nil {
			return Request{}, &rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Error: rpcErr}
		}
		if rpcErr := validateParams(method, req.Params, req.CorrelationID); rpcErr != nil {
			return Request{}, &rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Error: rpcErr}
		}
	} else {
		return Request{}, &rpcResponse{
			JSONRPC: jsonRPCVersion,
			ID:      req.ID,
			Error:   h.standardError(codeMethodNotFound, "method not found", correlationID, map[string]any{"method": req.Method}),
		}
	}
	return req, nil
}

func (h *Handler) admitMethodTransport(methodName string, method apispec.Method, transport, correlationID string) *rpcError {
	expected := runtimeMethodTransport(methodName, method)
	if strings.TrimSpace(transport) == expected {
		return nil
	}
	return h.standardError(codeMethodNotFound, "method not found", correlationID, map[string]any{
		"method":    strings.TrimSpace(methodName),
		"transport": strings.TrimSpace(transport),
	})
}

func runtimeMethodTransport(methodName string, method apispec.Method) string {
	if strings.TrimSpace(methodName) == "rpc.unsubscribe" || method.NotificationSchema != nil {
		return transportWebSocket
	}
	return transportHTTP
}

func (h *Handler) dispatchPrepared(ctx context.Context, req Request) rpcResponse {
	handler := h.handlers[req.Method]
	if handler == nil {
		return rpcResponse{
			JSONRPC: jsonRPCVersion,
			ID:      req.ID,
			Error: h.applicationError(MethodUnavailableCode, req.CorrelationID, false, map[string]any{
				"method": req.Method,
			}),
		}
	}
	result, err := handler(ctx, req)
	return h.responseFromResult(req, result, err)
}

func (h *Handler) responseFromResult(req Request, result any, err error) rpcResponse {
	if err != nil {
		var appErr *ApplicationError
		if errors.As(err, &appErr) && appErr != nil {
			return rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Error: h.applicationError(appErr.Code, req.CorrelationID, appErr.Retryable, appErr.Details)}
		}
		var paramsErr *InvalidParamsError
		if errors.As(err, &paramsErr) && paramsErr != nil {
			return rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Error: invalidParams(req.CorrelationID, paramsErr.Details)}
		}
		return rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Error: h.standardError(codeInternalError, "internal error", req.CorrelationID, map[string]any{"error": err.Error()})}
	}
	return rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Result: result}
}

type webSocketSession struct {
	handler               *Handler
	conn                  *websocket.Conn
	ctx                   context.Context
	cancel                context.CancelFunc
	out                   chan any
	fallbackCorrelationID string
	actorTokenID          string
	mu                    sync.Mutex
	subs                  map[string]context.CancelFunc
}

func newWebSocketSession(handler *Handler, conn *websocket.Conn, parent context.Context, fallbackCorrelationID, actorTokenID string) *webSocketSession {
	ctx, cancel := context.WithCancel(parent)
	queueSize := defaultSubscriptionQueueSize
	if handler != nil && handler.subs != nil && handler.subs.queueSize > 0 {
		queueSize = handler.subs.queueSize
	}
	return &webSocketSession{
		handler:               handler,
		conn:                  conn,
		ctx:                   ctx,
		cancel:                cancel,
		out:                   make(chan any, queueSize),
		fallbackCorrelationID: strings.TrimSpace(fallbackCorrelationID),
		actorTokenID:          strings.TrimSpace(actorTokenID),
		subs:                  map[string]context.CancelFunc{},
	}
}

func (s *webSocketSession) run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.writeLoop()
	}()
	defer func() {
		s.cancel()
		s.cancelAllSubscriptions()
		_ = s.conn.Close()
		wg.Wait()
	}()
	for {
		_, raw, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		if !s.handleRaw(raw) {
			return
		}
	}
}

func (s *webSocketSession) writeLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.out:
			if err := s.conn.WriteJSON(msg); err != nil {
				s.close()
				return
			}
		}
	}
}

func (s *webSocketSession) handleRaw(raw []byte) bool {
	req, failure := s.handler.prepareRequest(raw, transportWebSocket, s.fallbackCorrelationID, s.actorTokenID)
	if failure != nil {
		return s.enqueue(*failure)
	}
	switch req.Method {
	case "health.subscribe", "event.subscribe", "run.subscribe_trace", "runtime.subscribe_logs":
		if s.handler.subs == nil {
			return s.enqueue(s.handler.dispatchPrepared(s.ctx, req))
		}
		plan, err := s.handler.subs.prepare(s, req)
		resp := s.handler.responseFromResult(req, plan.Result, err)
		if !s.enqueue(resp) {
			if plan.Cancel != nil {
				plan.Cancel()
			}
			return false
		}
		if err == nil && plan.Start != nil {
			plan.Start()
		}
		return true
	case "rpc.unsubscribe":
		subscriptionID := stringParam(req.Params, "subscription_id")
		s.cancelSubscription(subscriptionID)
		return s.enqueue(rpcResponse{JSONRPC: jsonRPCVersion, ID: req.ID, Result: map[string]any{"ok": true}})
	default:
		return s.enqueue(s.handler.dispatchPrepared(s.ctx, req))
	}
}

func (s *webSocketSession) enqueue(msg any) bool {
	select {
	case <-s.ctx.Done():
		return false
	case s.out <- msg:
		return true
	default:
		s.close()
		return false
	}
}

func (s *webSocketSession) close() {
	if s == nil {
		return
	}
	s.cancel()
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (s *webSocketSession) notify(subscriptionID string, result any) bool {
	return s.enqueue(rpcSubscriptionNotification{
		JSONRPC: jsonRPCVersion,
		Method:  "rpc.subscription",
		Params: rpcSubscriptionParams{
			Subscription: subscriptionID,
			Result:       result,
		},
	})
}

func (s *webSocketSession) registerSubscription(subscriptionID string, cancel context.CancelFunc) {
	if s == nil || cancel == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[subscriptionID] = cancel
}

func (s *webSocketSession) cancelSubscription(subscriptionID string) {
	subscriptionID = strings.TrimSpace(subscriptionID)
	s.mu.Lock()
	cancel := s.subs[subscriptionID]
	delete(s.subs, subscriptionID)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *webSocketSession) cancelAllSubscriptions() {
	s.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.subs))
	for subscriptionID, cancel := range s.subs {
		delete(s.subs, subscriptionID)
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		if cancel != nil {
			cancel()
		}
	}
}

func (h *Handler) parseRequest(raw []byte, fallbackCorrelationID string) (Request, any, string, *rpcError) {
	correlationID := strings.TrimSpace(fallbackCorrelationID)
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return Request{}, nil, correlationID, h.standardError(codeParseError, "parse error", correlationID, map[string]any{"reason": "empty request body"})
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return Request{}, nil, correlationID, h.standardError(codeParseError, "parse error", correlationID, map[string]any{"error": err.Error()})
	}
	if object == nil {
		return Request{}, nil, correlationID, h.standardError(codeInvalidRequest, "invalid request", correlationID, map[string]any{"reason": "request must be an object"})
	}
	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return Request{}, nil, correlationID, h.standardError(codeInvalidRequest, "invalid request", correlationID, map[string]any{"error": err.Error()})
	}
	idRaw, hasID := object["id"]
	if !hasID {
		return Request{}, nil, correlationID, h.standardError(codeInvalidRequest, "invalid request", correlationID, map[string]any{"reason": "id is required"})
	}
	if strings.TrimSpace(req.JSONRPC) != jsonRPCVersion {
		return Request{}, req.ID, requestCorrelationID(nil, req.ID, correlationID), h.standardError(codeInvalidRequest, "invalid request", requestCorrelationID(nil, req.ID, correlationID), map[string]any{"reason": "jsonrpc must be 2.0"})
	}
	if len(bytes.TrimSpace(idRaw)) == 0 {
		return Request{}, nil, correlationID, h.standardError(codeInvalidRequest, "invalid request", correlationID, map[string]any{"reason": "id is invalid"})
	}
	if !validJSONRPCID(req.ID) {
		correlationID = requestCorrelationID(nil, req.ID, correlationID)
		return Request{}, req.ID, correlationID, h.standardError(codeInvalidRequest, "invalid request", correlationID, map[string]any{"reason": "id must be a string, number, or null"})
	}
	correlationID = requestCorrelationID(nil, req.ID, correlationID)
	method := strings.TrimSpace(req.Method)
	if method == "" {
		return Request{}, req.ID, correlationID, h.standardError(codeInvalidRequest, "invalid request", correlationID, map[string]any{"reason": "method is required"})
	}
	params, rpcErr := decodeParams(req.Params, correlationID)
	if rpcErr != nil {
		return Request{}, req.ID, correlationID, rpcErr
	}
	return Request{
		ID:            req.ID,
		Method:        method,
		Params:        params,
		CorrelationID: correlationID,
	}, req.ID, correlationID, nil
}

func decodeParams(raw json.RawMessage, correlationID string) (map[string]any, *rpcError) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return map[string]any{}, nil
	}
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, &rpcError{
			Code:    codeInvalidParams,
			Message: "invalid params",
			Data:    standardErrorData{CorrelationID: correlationID, Details: map[string]any{"reason": "params must be an object"}},
		}
	}
	if params == nil {
		params = map[string]any{}
	}
	return params, nil
}

func validJSONRPCID(id any) bool {
	switch id.(type) {
	case nil, string, float64, json.Number:
		return true
	default:
		return false
	}
}

var opaqueIDPattern = regexp.MustCompile(`^[A-Za-z0-9_:.-]+$`)

func validateParams(method apispec.Method, params map[string]any, correlationID string) *rpcError {
	if params == nil {
		params = map[string]any{}
	}
	declared := make(map[string]apispec.ContentDescriptor, len(method.Params))
	for _, descriptor := range method.Params {
		declared[descriptor.Name] = descriptor
		value, exists := params[descriptor.Name]
		if descriptor.Required && (!exists || isEmptyParam(value)) {
			return invalidParams(correlationID, map[string]any{
				"field":  descriptor.Name,
				"reason": "required parameter is missing",
			})
		}
		if exists && !isEmptyParam(value) {
			if reason := validateParamValue(descriptor, value); reason != "" {
				return invalidParams(correlationID, map[string]any{
					"field":  descriptor.Name,
					"reason": reason,
				})
			}
		}
	}
	for name := range params {
		if _, ok := declared[name]; !ok {
			return invalidParams(correlationID, map[string]any{
				"field":  name,
				"reason": "unknown parameter",
			})
		}
	}
	return nil
}

func validateParamValue(descriptor apispec.ContentDescriptor, value any) string {
	schema := asStringMap(descriptor.Schema)
	ref, _ := schema["$ref"].(string)
	switch ref {
	case "#/components/schemas/OpaqueId":
		text, ok := value.(string)
		if !ok {
			return "must be a string"
		}
		if strings.TrimSpace(text) == "" {
			return "must not be empty"
		}
		if len(text) > 256 {
			return "must be at most 256 characters"
		}
		if !opaqueIDPattern.MatchString(text) {
			return "must match OpaqueId pattern"
		}
		return ""
	}
	if typ, _ := schema["type"].(string); typ != "" {
		switch typ {
		case "string":
			if _, ok := value.(string); !ok {
				return "must be a string"
			}
		case "object":
			if _, ok := value.(map[string]any); !ok {
				return "must be an object"
			}
		case "array":
			if _, ok := value.([]any); !ok {
				return "must be an array"
			}
		case "boolean":
			if _, ok := value.(bool); !ok {
				return "must be a boolean"
			}
		case "number", "integer":
			if !isJSONNumber(value) {
				return "must be a number"
			}
			if typ == "integer" && !isJSONInteger(value) {
				return "must be an integer"
			}
		}
	}
	return ""
}

func isJSONNumber(value any) bool {
	switch typed := value.(type) {
	case float64:
		return true
	case json.Number:
		_, err := typed.Float64()
		return err == nil
	default:
		return false
	}
}

func isJSONInteger(value any) bool {
	switch typed := value.(type) {
	case float64:
		return typed == float64(int64(typed))
	case json.Number:
		if _, err := typed.Int64(); err == nil {
			return true
		}
		return false
	default:
		return false
	}
}

func invalidParams(correlationID string, details any) *rpcError {
	return &rpcError{
		Code:    codeInvalidParams,
		Message: "invalid params",
		Data:    standardErrorData{CorrelationID: correlationID, Details: details},
	}
}

func isEmptyParam(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(typed) == ""
	default:
		return false
	}
}

func asStringMap(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	case map[interface{}]interface{}:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[fmt.Sprint(key)] = value
		}
		return out
	default:
		return nil
	}
}

func (h *Handler) standardError(code int, message, correlationID string, details any) *rpcError {
	return &rpcError{
		Code:    code,
		Message: message,
		Data: standardErrorData{
			CorrelationID: strings.TrimSpace(correlationID),
			Details:       details,
		},
	}
}

func (h *Handler) applicationError(code, correlationID string, retryable bool, details any) *rpcError {
	numeric, ok := h.registry.ApplicationErrorCode(code)
	if !ok {
		return h.standardError(codeInternalError, "internal error", correlationID, map[string]any{"missing_application_error": code})
	}
	return &rpcError{
		Code:    numeric,
		Message: "Application error: " + code,
		Data: applicationErrorData{
			Code:          code,
			Details:       details,
			Retryable:     retryable,
			CorrelationID: correlationID,
		},
	}
}

type authFailure struct {
	status  int
	message string
}

func (h *Handler) authorize(r *http.Request) (string, *authFailure) {
	if len(h.tokens) == 0 {
		return "", &authFailure{status: http.StatusServiceUnavailable, message: "v1 API auth is not configured"}
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if authz == "" {
		return "", &authFailure{status: http.StatusUnauthorized, message: "missing authorization bearer token"}
	}
	const prefix = "bearer "
	if !strings.HasPrefix(strings.ToLower(authz), prefix) {
		return "", &authFailure{status: http.StatusUnauthorized, message: "invalid authorization header"}
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if _, ok := h.tokens[token]; !ok {
		return "", &authFailure{status: http.StatusUnauthorized, message: "invalid bearer token"}
	}
	return token, nil
}

func writeAuthFailure(w http.ResponseWriter, failure *authFailure) {
	if failure == nil {
		return
	}
	if failure.status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="swarm-api"`)
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(failure.status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": failure.message})
}

func writeRPC(w http.ResponseWriter, resp rpcResponse) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func requestCorrelationID(r *http.Request, id any, fallback ...string) string {
	if r != nil {
		for _, header := range []string{"X-Correlation-ID", "X-Request-ID"} {
			if value := strings.TrimSpace(r.Header.Get(header)); value != "" {
				return value
			}
		}
	}
	switch typed := id.(type) {
	case string:
		if strings.TrimSpace(typed) != "" {
			return strings.TrimSpace(typed)
		}
	case json.Number:
		if strings.TrimSpace(typed.String()) != "" {
			return strings.TrimSpace(typed.String())
		}
	case float64:
		return fmt.Sprintf("%v", typed)
	}
	for _, value := range fallback {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return uuid.NewString()
}

func tokenSet(tokens []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, token := range tokens {
		if token = strings.TrimSpace(token); token != "" {
			out[token] = struct{}{}
		}
	}
	return out
}

func actorTokenID(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func requestBodyHash(method string, params map[string]any) string {
	body := map[string]any{
		"method": strings.TrimSpace(method),
		"params": params,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		raw = []byte(strings.TrimSpace(method))
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
