package tools

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var telemetryPaymentRefRegex = regexp.MustCompile(`\b(?:pi|pm|ch|cs|txn|tx|tr|pay)_[a-zA-Z0-9]{6,}\b`)

const maxToolTelemetryChars = 1000

func SafeTelemetryText(v any) string {
	redacted := RedactTelemetryValue(v)
	raw, err := json.Marshal(redacted)
	if err != nil {
		return TruncateTelemetry(fmt.Sprintf("%v", redacted), maxToolTelemetryChars)
	}
	return TruncateTelemetry(string(raw), maxToolTelemetryChars)
}

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
