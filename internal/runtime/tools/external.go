package tools

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
)

func NormalizeExternalContractPayload(payload map[string]any, method string) {
	if payload == nil {
		return
	}
	if strings.TrimSpace(AsString(payload["method"])) == "" {
		payload["method"] = method
	}
	if payload["body"] != nil {
		return
	}
	body := map[string]any{}
	for key, value := range payload {
		switch key {
		case "method", "url", "path", "query", "headers", "body", "timeout_seconds":
			continue
		default:
			body[key] = value
		}
	}
	if len(body) > 0 {
		payload["body"] = body
	}
}

func DefaultExternalMethod(toolName string) string {
	switch toolName {
	case "domain_availability_check", "whatsapp_name_check":
		return http.MethodGet
	default:
		return http.MethodPost
	}
}

func ApplyExternalHeaders(req *http.Request, headers map[string]any) {
	for k, v := range headers {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(AsString(v))
		if key == "" || val == "" {
			continue
		}
		req.Header.Set(key, val)
	}
}

func ApplyExternalCredentialHeaders(req *http.Request, creds map[string]any, toolName string) {
	defaults := DefaultExternalCredentialEnv(toolName)
	for k, v := range defaults {
		if strings.TrimSpace(v) == "" {
			continue
		}
		if _, exists := creds[k]; !exists {
			creds[k] = v
		}
	}
	if hdrs := AsMap(creds["headers"]); len(hdrs) > 0 {
		for k, v := range hdrs {
			req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(AsString(v)))
		}
	}
	headerName := strings.TrimSpace(AsString(creds["auth_header"]))
	if headerName == "" {
		headerName = "Authorization"
	}
	token := strings.TrimSpace(AsString(creds["bearer_token"]))
	if token == "" {
		token = strings.TrimSpace(AsString(creds["token"]))
	}
	if token == "" {
		token = strings.TrimSpace(AsString(creds["api_key"]))
	}
	if token == "" {
		return
	}
	if strings.EqualFold(headerName, "Authorization") && !strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = "Bearer " + token
	}
	req.Header.Set(headerName, token)
}

func DefaultExternalCredentialEnv(toolName string) map[string]string {
	switch toolName {
	case "domain_purchase", "domain_availability_check":
		return map[string]string{
			"endpoint": os.Getenv("REGISTRAR_API_ENDPOINT"),
			"api_key":  os.Getenv("REGISTRAR_API_KEY"),
		}
	case "dns_configure":
		endpoint := strings.TrimSpace(os.Getenv("CLOUDFLARE_API_ENDPOINT"))
		if endpoint == "" {
			endpoint = "https://api.cloudflare.com/client/v4"
		}
		return map[string]string{
			"endpoint": endpoint,
			"api_key":  os.Getenv("CLOUDFLARE_API_TOKEN"),
		}
	case "whatsapp_name_check":
		return map[string]string{
			"endpoint": os.Getenv("WHATSAPP_NAME_CHECK_API_ENDPOINT"),
			"api_key":  os.Getenv("WHATSAPP_NAME_CHECK_API_KEY"),
		}
	case "whatsapp_business_api":
		return map[string]string{
			"endpoint": os.Getenv("WHATSAPP_API_ENDPOINT"),
			"api_key":  os.Getenv("WHATSAPP_API_KEY"),
		}
	case "instagram_api":
		return map[string]string{
			"endpoint": os.Getenv("INSTAGRAM_API_ENDPOINT"),
			"api_key":  os.Getenv("INSTAGRAM_API_KEY"),
		}
	default:
		return map[string]string{}
	}
}

func ParseExternalResponseBody(raw []byte) any {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return map[string]any{}
	}
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err == nil {
		return parsed
	}
	return trimmed
}

func MergeCredMap(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

func AsMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func AsString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

func SafeTelemetryText(v any) string {
	redacted := RedactTelemetryValue(v)
	raw, err := json.Marshal(redacted)
	if err != nil {
		return TruncateTelemetry(fmt.Sprintf("%v", redacted), maxToolTelemetryChars)
	}
	return TruncateTelemetry(string(raw), maxToolTelemetryChars)
}

var telemetryPaymentRefRegex = regexp.MustCompile(`\b(?:pi|pm|ch|cs|txn|tx|tr|pay)_[a-zA-Z0-9]{6,}\b`)

const maxToolTelemetryChars = 1000

func RedactTelemetryValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			if isSensitiveKey(k) {
				out[k] = "[REDACTED]"
				continue
			}
			out[k] = RedactTelemetryValue(vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = RedactTelemetryValue(t[i])
		}
		return out
	case string:
		t = telemetryPaymentRefRegex.ReplaceAllString(t, "[PAYMENT_REF]")
		return TruncateTelemetry(t, 220)
	default:
		return t
	}
}

func TruncateTelemetry(v string, max int) string {
	if max <= 0 || len(v) <= max {
		return v
	}
	return v[:max] + "..."
}

func isSensitiveKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	for _, needle := range []string{
		"secret", "token", "password", "api_key", "apikey", "authorization", "auth",
		"payment_ref", "payment_reference", "transaction_id", "charge_id",
	} {
		if strings.Contains(k, needle) {
			return true
		}
	}
	return false
}
