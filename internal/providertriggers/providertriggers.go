package providertriggers

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"gopkg.in/yaml.v3"
)

//go:embed manifests/*.yaml
var builtinManifestFS embed.FS

type Target struct {
	EntityID      string
	EntitySlug    string
	WebhookSecret string
}

func (t Target) EffectiveEntityID() string {
	return firstNonEmpty(t.EntityID, t.EntitySlug)
}

type Request struct {
	Provider  string
	Target    Target
	Body      []byte
	Headers   http.Header
	Payload   any
	Received  time.Time
	UserAgent string
}

type Delivery struct {
	ProviderEventID           string
	ProviderEventType         string
	EventName                 events.EventType
	Payload                   map[string]any
	Response                  *Response
	AcknowledgeBeforeDispatch bool
}

type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

type Error struct {
	Status  int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

type Registry struct {
	manifests map[string]Manifest
}

func DefaultRegistry() *Registry {
	return defaultRegistry
}

var defaultRegistry = mustDefaultRegistry()

func mustDefaultRegistry() *Registry {
	entries, err := builtinManifestFS.ReadDir("manifests")
	if err != nil {
		panic(err)
	}
	manifests := make([]Manifest, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		body, err := builtinManifestFS.ReadFile("manifests/" + entry.Name())
		if err != nil {
			panic(err)
		}
		manifest, err := ParseManifest(body)
		if err != nil {
			panic(fmt.Sprintf("%s: %v", entry.Name(), err))
		}
		manifests = append(manifests, manifest)
	}
	registry, err := NewRegistry(manifests...)
	if err != nil {
		panic(err)
	}
	return registry
}

func NewRegistry(manifests ...Manifest) (*Registry, error) {
	r := &Registry{manifests: make(map[string]Manifest, len(manifests))}
	for _, manifest := range manifests {
		if err := manifest.Validate(); err != nil {
			return nil, err
		}
		provider := NormalizeProviderName(manifest.Provider)
		if _, exists := r.manifests[provider]; exists {
			return nil, fmt.Errorf("duplicate provider trigger manifest for %q", provider)
		}
		manifest.Provider = provider
		r.manifests[provider] = manifest
	}
	return r, nil
}

func (r *Registry) Accept(req Request) (Delivery, error) {
	provider := NormalizeProviderName(req.Provider)
	if provider == "" {
		return Delivery{}, badRequest("provider is required")
	}
	if r != nil {
		if manifest, ok := r.manifests[provider]; ok {
			return manifest.Accept(req.withProvider(provider))
		}
	}
	return acceptRaw(req.withProvider(provider))
}

func (req Request) withProvider(provider string) Request {
	req.Provider = provider
	return req
}

type Manifest struct {
	Provider              string             `yaml:"provider"`
	PayloadObjectRequired bool               `yaml:"payload_object_required"`
	PayloadObjectError    string             `yaml:"payload_object_error"`
	Secret                SecretManifest     `yaml:"secret"`
	Signature             SignatureManifest  `yaml:"signature"`
	Challenge             *ChallengeManifest `yaml:"challenge"`
	DeliveryCondition     *ConditionManifest `yaml:"delivery_condition"`
	DeliveryID            ValueSource        `yaml:"delivery_id"`
	EventType             ValueSource        `yaml:"event_type"`
	EventName             EventNameManifest  `yaml:"event_name"`
	Ack                   AckManifest        `yaml:"ack"`
	RedactKeys            []string           `yaml:"redact_keys"`
	Metadata              map[string]string  `yaml:"metadata"`
}

type SecretManifest struct {
	Required bool `yaml:"required"`
}

type SignatureManifest struct {
	Type           string             `yaml:"type"`
	Header         string             `yaml:"header"`
	Prefix         string             `yaml:"prefix"`
	SignedPayload  string             `yaml:"signed_payload"`
	SignatureParam string             `yaml:"signature_param"`
	MissingError   string             `yaml:"missing_error"`
	InvalidError   string             `yaml:"invalid_error"`
	Timestamp      *TimestampManifest `yaml:"timestamp"`
}

type TimestampManifest struct {
	Header       string `yaml:"header"`
	Param        string `yaml:"param"`
	Tolerance    string `yaml:"tolerance"`
	MissingError string `yaml:"missing_error"`
	InvalidError string `yaml:"invalid_error"`
	StaleError   string `yaml:"stale_error"`
}

type ChallengeManifest struct {
	When     ConditionManifest `yaml:"when"`
	Response ResponseManifest  `yaml:"response"`
}

type ResponseManifest struct {
	JSONPath    string `yaml:"json_path"`
	MissingErr  string `yaml:"missing_error"`
	ContentType string `yaml:"content_type"`
	Status      int    `yaml:"status"`
}

type ConditionManifest struct {
	JSONPath     string `yaml:"json_path"`
	Equals       string `yaml:"equals"`
	Normalize    bool   `yaml:"normalize"`
	MissingError string `yaml:"missing_error"`
	MismatchErr  string `yaml:"mismatch_error"`
}

type ValueSource struct {
	Header       string `yaml:"header"`
	JSONPath     string `yaml:"json_path"`
	Required     bool   `yaml:"required"`
	MissingError string `yaml:"missing_error"`
}

type EventNameManifest struct {
	Literal  string `yaml:"literal"`
	Template string `yaml:"template"`
}

type AckManifest struct {
	Mode string `yaml:"mode"`
}

func ParseManifest(body []byte) (Manifest, error) {
	var manifest Manifest
	if err := yaml.Unmarshal(body, &manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (m Manifest) Validate() error {
	provider := NormalizeProviderName(m.Provider)
	if provider == "" {
		return fmt.Errorf("provider is required")
	}
	if m.Secret.Required && m.Signature.Type == "" {
		return fmt.Errorf("%s manifest requires signature when secret is required", provider)
	}
	if m.Signature.Type != "" && strings.TrimSpace(m.Signature.Type) != "hmac_sha256" {
		return fmt.Errorf("%s manifest has unsupported signature type %q", provider, m.Signature.Type)
	}
	if m.Signature.Type != "" {
		if strings.TrimSpace(m.Signature.Header) == "" {
			return fmt.Errorf("%s manifest signature header is required", provider)
		}
		switch strings.TrimSpace(m.Signature.SignedPayload) {
		case "raw_body", "slack_v0", "timestamp_dot_raw_body":
		default:
			return fmt.Errorf("%s manifest has unsupported signed_payload %q", provider, m.Signature.SignedPayload)
		}
		if m.Signature.SignedPayload != "raw_body" && m.Signature.Timestamp == nil {
			return fmt.Errorf("%s manifest timestamp is required for %s", provider, m.Signature.SignedPayload)
		}
	}
	if m.DeliveryID.Required && m.DeliveryID.Header == "" && m.DeliveryID.JSONPath == "" {
		return fmt.Errorf("%s manifest delivery_id source is required", provider)
	}
	if m.EventType.Required && m.EventType.Header == "" && m.EventType.JSONPath == "" {
		return fmt.Errorf("%s manifest event_type source is required", provider)
	}
	if strings.TrimSpace(m.EventName.Literal) == "" && strings.TrimSpace(m.EventName.Template) == "" {
		return fmt.Errorf("%s manifest event_name is required", provider)
	}
	switch strings.TrimSpace(m.Ack.Mode) {
	case "", "after_publish", "durable_before_dispatch":
	default:
		return fmt.Errorf("%s manifest has unsupported ack mode %q", provider, m.Ack.Mode)
	}
	return nil
}

func (m Manifest) Accept(req Request) (Delivery, error) {
	provider := NormalizeProviderName(m.Provider)
	secret := strings.TrimSpace(req.Target.WebhookSecret)
	if m.Secret.Required && secret == "" {
		return Delivery{}, unauthorized(provider + " webhook signing secret is required")
	}
	if m.Signature.Type != "" {
		if err := m.verifySignature(secret, req); err != nil {
			return Delivery{}, err
		}
	}
	if m.PayloadObjectRequired {
		if _, ok := req.Payload.(map[string]any); !ok {
			return Delivery{}, badRequest(firstNonEmpty(m.PayloadObjectError, provider+" payload object is required"))
		}
	}
	if m.Challenge != nil {
		matched, err := m.Challenge.When.Evaluate(req.Payload)
		if err != nil {
			return Delivery{}, err
		}
		if matched {
			value, ok := stringFromJSONPath(req.Payload, m.Challenge.Response.JSONPath)
			if !ok || strings.TrimSpace(value) == "" {
				return Delivery{}, badRequest(firstNonEmpty(m.Challenge.Response.MissingErr, provider+" challenge is required"))
			}
			status := m.Challenge.Response.Status
			if status == 0 {
				status = http.StatusOK
			}
			contentType := firstNonEmpty(m.Challenge.Response.ContentType, "text/plain; charset=utf-8")
			return Delivery{Response: &Response{Status: status, ContentType: contentType, Body: []byte(value)}}, nil
		}
	}
	if m.DeliveryCondition != nil {
		matched, err := m.DeliveryCondition.Evaluate(req.Payload)
		if err != nil {
			return Delivery{}, err
		}
		if !matched {
			return Delivery{}, badRequest(firstNonEmpty(m.DeliveryCondition.MismatchErr, "unsupported "+provider+" payload type"))
		}
	}
	deliveryID, ok := m.DeliveryID.Resolve(req)
	if !ok || strings.TrimSpace(deliveryID) == "" {
		return Delivery{}, badRequest(firstNonEmpty(m.DeliveryID.MissingError, provider+" delivery id is required"))
	}
	eventType, ok := m.EventType.Resolve(req)
	eventType = NormalizeEventToken(eventType)
	if !ok || eventType == "event" {
		return Delivery{}, badRequest(firstNonEmpty(m.EventType.MissingError, provider+" event type is required"))
	}
	entityID := req.Target.EffectiveEntityID()
	payload := m.buildPublishPayload(provider, entityID, deliveryID, eventType, req)
	return Delivery{
		ProviderEventID:           deliveryID,
		ProviderEventType:         eventType,
		EventName:                 events.EventType(m.resolveEventName(eventType)),
		Payload:                   payload,
		AcknowledgeBeforeDispatch: strings.TrimSpace(m.Ack.Mode) == "durable_before_dispatch",
	}, nil
}

func (m Manifest) verifySignature(secret string, req Request) error {
	sigHeader := strings.TrimSpace(req.Headers.Get(m.Signature.Header))
	if sigHeader == "" {
		return unauthorized(firstNonEmpty(m.Signature.MissingError, "signature is required"))
	}
	var (
		timestamp  string
		candidates []string
	)
	if strings.TrimSpace(m.Signature.SignatureParam) != "" {
		params, err := parseHeaderParams(sigHeader)
		if err != nil {
			return unauthorized(firstNonEmpty(m.Signature.InvalidError, "invalid signature"))
		}
		timestampValues := params.Values(firstNonEmpty(m.Signature.TimestampParam(), "t"))
		if len(timestampValues) > 0 {
			timestamp = timestampValues[0]
		}
		candidates = params.Values(strings.TrimSpace(m.Signature.SignatureParam))
	} else {
		if m.Signature.Prefix != "" {
			lower := strings.ToLower(sigHeader)
			prefix := strings.ToLower(m.Signature.Prefix)
			if !strings.HasPrefix(lower, prefix) {
				return unauthorized(firstNonEmpty(m.Signature.MissingError, "signature is required"))
			}
			candidates = []string{strings.TrimSpace(sigHeader[len(m.Signature.Prefix):])}
		} else {
			candidates = []string{sigHeader}
		}
	}
	if m.Signature.Timestamp != nil {
		var err error
		timestamp, err = m.Signature.Timestamp.Resolve(timestamp, req)
		if err != nil {
			return err
		}
	}
	if len(candidates) == 0 {
		return unauthorized(firstNonEmpty(m.Signature.MissingError, "signature is required"))
	}
	signedPayload, err := m.Signature.signedPayload(timestamp, req.Body)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(strings.TrimSpace(secret)))
	_, _ = mac.Write(signedPayload)
	expected := hex.EncodeToString(mac.Sum(nil))
	for _, candidate := range candidates {
		if hmac.Equal([]byte(strings.ToLower(strings.TrimSpace(candidate))), []byte(strings.ToLower(expected))) {
			return nil
		}
	}
	return unauthorized(firstNonEmpty(m.Signature.InvalidError, "invalid signature"))
}

func (s SignatureManifest) TimestampParam() string {
	if s.Timestamp == nil {
		return ""
	}
	return strings.TrimSpace(s.Timestamp.Param)
}

func (s SignatureManifest) signedPayload(timestamp string, body []byte) ([]byte, error) {
	switch strings.TrimSpace(s.SignedPayload) {
	case "raw_body":
		return body, nil
	case "slack_v0":
		return []byte("v0:" + timestamp + ":" + string(body)), nil
	case "timestamp_dot_raw_body":
		return []byte(timestamp + "." + string(body)), nil
	default:
		return nil, unauthorized(firstNonEmpty(s.InvalidError, "invalid signature"))
	}
}

func (t TimestampManifest) Resolve(paramTimestamp string, req Request) (string, error) {
	raw := strings.TrimSpace(paramTimestamp)
	if strings.TrimSpace(t.Header) != "" {
		raw = strings.TrimSpace(req.Headers.Get(t.Header))
	}
	if raw == "" {
		return "", unauthorized(firstNonEmpty(t.MissingError, "signature timestamp is required"))
	}
	secs, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return "", unauthorized(firstNonEmpty(t.InvalidError, "invalid signature timestamp"))
	}
	if strings.TrimSpace(t.Tolerance) != "" {
		tolerance, err := time.ParseDuration(strings.TrimSpace(t.Tolerance))
		if err != nil {
			return "", unauthorized(firstNonEmpty(t.InvalidError, "invalid signature timestamp"))
		}
		requestTime := time.Unix(secs, 0).UTC()
		now := req.Received.UTC()
		if requestTime.After(now.Add(tolerance)) || requestTime.Before(now.Add(-tolerance)) {
			return "", unauthorized(firstNonEmpty(t.StaleError, "stale signature timestamp"))
		}
	}
	return raw, nil
}

func (c ConditionManifest) Evaluate(payload any) (bool, error) {
	value, ok := stringFromJSONPath(payload, c.JSONPath)
	if !ok || strings.TrimSpace(value) == "" {
		if c.MissingError != "" {
			return false, badRequest(c.MissingError)
		}
		return false, nil
	}
	if c.Normalize {
		value = NormalizeEventToken(value)
	}
	expected := c.Equals
	if c.Normalize {
		expected = NormalizeEventToken(expected)
	}
	return value == expected, nil
}

func (s ValueSource) Resolve(req Request) (string, bool) {
	if strings.TrimSpace(s.Header) != "" {
		value := strings.TrimSpace(req.Headers.Get(s.Header))
		return value, value != ""
	}
	if strings.TrimSpace(s.JSONPath) != "" {
		return stringFromJSONPath(req.Payload, s.JSONPath)
	}
	return "", false
}

func (m Manifest) resolveEventName(eventType string) string {
	if name := strings.TrimSpace(m.EventName.Literal); name != "" {
		return name
	}
	name := strings.TrimSpace(m.EventName.Template)
	name = strings.ReplaceAll(name, "{event_type}", eventType)
	return name
}

func (m Manifest) buildPublishPayload(provider, entityID, deliveryID, eventType string, req Request) map[string]any {
	rawPayload := redactPayload(req.Payload, m.RedactKeys)
	headers := make(map[string]any, len(m.Metadata))
	for key, source := range m.Metadata {
		switch source {
		case "user_agent":
			headers[key] = req.UserAgent
		case "delivery_id":
			headers[key] = deliveryID
		case "event_type":
			headers[key] = eventType
		}
	}
	return map[string]any{
		"entity_id":            strings.TrimSpace(entityID),
		"provider":             strings.TrimSpace(provider),
		"event_type":           strings.TrimSpace(eventType),
		"provider_event_type":  strings.TrimSpace(eventType),
		"provider_event_id":    strings.TrimSpace(deliveryID),
		"provider_delivery_id": strings.TrimSpace(deliveryID),
		"payload":              rawPayload,
		"headers":              headers,
		"received_at":          req.Received.UTC().Format(time.RFC3339),
	}
}

func acceptRaw(req Request) (Delivery, error) {
	provider := NormalizeProviderName(req.Provider)
	if provider == "" {
		return Delivery{}, badRequest("provider is required")
	}
	if !verifyRawWebhookSignature(req.Target.WebhookSecret, req.Body, req.Headers) {
		return Delivery{}, unauthorized("invalid signature")
	}
	entityID := req.Target.EffectiveEntityID()
	deliveryID := firstNonEmpty(
		req.Headers.Get("X-Provider-Event-ID"),
		req.Headers.Get("X-Request-ID"),
		extractProviderEventID(req.Payload),
		fingerprintInbound(entityID, provider, req.Body),
	)
	eventType := resolveProviderEventType(req.Payload)
	payload := map[string]any{
		"entity_id":            strings.TrimSpace(entityID),
		"provider":             provider,
		"event_type":           eventType,
		"provider_event_type":  eventType,
		"provider_event_id":    deliveryID,
		"provider_delivery_id": deliveryID,
		"payload":              req.Payload,
		"headers":              map[string]any{"user_agent": req.UserAgent},
		"received_at":          req.Received.UTC().Format(time.RFC3339),
	}
	return Delivery{
		ProviderEventID:   deliveryID,
		ProviderEventType: eventType,
		EventName:         events.EventType("inbound." + provider),
		Payload:           payload,
	}, nil
}

func verifyRawWebhookSignature(secret string, body []byte, headers http.Header) bool {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return true
	}
	if sig := strings.TrimSpace(headers.Get("X-Hub-Signature-256")); strings.HasPrefix(strings.ToLower(sig), "sha256=") {
		given := strings.TrimSpace(sig[len("sha256="):])
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(body)
		expected := hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(strings.ToLower(given)), []byte(strings.ToLower(expected)))
	}
	token := strings.TrimSpace(headers.Get("X-Webhook-Token"))
	if token == "" {
		auth := strings.TrimSpace(headers.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			token = strings.TrimSpace(auth[7:])
		}
	}
	return hmac.Equal([]byte(token), []byte(secret))
}

func extractProviderEventID(payload any) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"id", "event_id", "message_id"} {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func fingerprintInbound(entityID, provider string, body []byte) string {
	h := sha1.New()
	_, _ = h.Write([]byte(entityID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(provider))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write(body)
	return "fp:" + hex.EncodeToString(h.Sum(nil))
}

func resolveProviderEventType(payload any) string {
	m, _ := payload.(map[string]any)
	for _, key := range []string{"event_type", "type", "status", "kind", "action"} {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return NormalizeEventToken(v)
		}
	}
	return "event"
}

func stringFromJSONPath(payload any, path string) (string, bool) {
	value, ok := valueFromJSONPath(payload, path)
	if !ok {
		return "", false
	}
	switch t := value.(type) {
	case string:
		return strings.TrimSpace(t), strings.TrimSpace(t) != ""
	case json.Number:
		return strings.TrimSpace(t.String()), strings.TrimSpace(t.String()) != ""
	case float64:
		return strings.TrimSpace(strconv.FormatFloat(t, 'f', -1, 64)), true
	default:
		return "", false
	}
}

func valueFromJSONPath(payload any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "$" {
		return payload, true
	}
	if !strings.HasPrefix(path, "$.") {
		return nil, false
	}
	current := payload
	for _, part := range strings.Split(strings.TrimPrefix(path, "$."), ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func redactPayload(payload any, keys []string) any {
	if len(keys) == 0 {
		return payload
	}
	keySet := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keySet[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	return redactValue(payload, keySet)
}

func redactValue(payload any, keys map[string]struct{}) any {
	switch v := payload.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(v))
		for key, value := range v {
			if _, ok := keys[strings.ToLower(key)]; ok {
				redacted[key] = "[redacted]"
				continue
			}
			redacted[key] = redactValue(value, keys)
		}
		return redacted
	case []any:
		redacted := make([]any, len(v))
		for i, value := range v {
			redacted[i] = redactValue(value, keys)
		}
		return redacted
	default:
		return v
	}
}

type headerParams map[string][]string

func parseHeaderParams(header string) (headerParams, error) {
	out := make(headerParams)
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty signature parameter")
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("malformed signature parameter")
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key == "" || value == "" {
			return nil, fmt.Errorf("empty signature parameter")
		}
		out[key] = append(out[key], value)
	}
	return out, nil
}

func (p headerParams) Values(key string) []string {
	values := p[strings.TrimSpace(key)]
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	return out
}

func NormalizeProviderName(raw string) string {
	token := strings.TrimSpace(strings.ToLower(raw))
	token = strings.ReplaceAll(token, ".", "_")
	token = strings.ReplaceAll(token, "-", "_")
	token = strings.ReplaceAll(token, " ", "_")
	return token
}

func NormalizeEventToken(raw string) string {
	token := strings.TrimSpace(strings.ToLower(raw))
	token = strings.ReplaceAll(token, ".", "_")
	token = strings.ReplaceAll(token, "-", "_")
	token = strings.ReplaceAll(token, " ", "_")
	if token == "" {
		return "event"
	}
	return token
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func badRequest(message string) Error {
	return Error{Status: http.StatusBadRequest, Message: message}
}

func unauthorized(message string) Error {
	return Error{Status: http.StatusUnauthorized, Message: message}
}
