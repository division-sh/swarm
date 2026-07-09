package httpresponsesuccess

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
)

const (
	KindHTTPStatus2xx   = "http_status_2xx"
	KindJSONFieldEquals = "json_field_equals"
)

var responsePathPattern = regexp.MustCompile(`^response(?:\.[A-Za-z0-9_-]+|\[[0-9]+\])+$`)

func NormalizeKind(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func Validate(check runtimecontracts.HTTPResponseSuccess) error {
	switch kind := NormalizeKind(check.Kind); kind {
	case KindHTTPStatus2xx:
		if strings.TrimSpace(check.Path) != "" {
			return fmt.Errorf("response_success.path is forbidden for kind %s", kind)
		}
		if check.Equals != nil {
			return fmt.Errorf("response_success.equals is forbidden for kind %s", kind)
		}
		return nil
	case KindJSONFieldEquals:
		path := strings.TrimSpace(check.Path)
		if path == "" {
			return fmt.Errorf("response_success.path is required for kind %s", kind)
		}
		if !strings.HasPrefix(path, "response.") {
			return fmt.Errorf("response_success.path must start with response.")
		}
		if !responsePathPattern.MatchString(path) {
			return fmt.Errorf("response_success.path %q is invalid", path)
		}
		if check.Equals == nil {
			return fmt.Errorf("response_success.equals is required for kind %s", kind)
		}
		if !scalar(check.Equals) {
			return fmt.Errorf("response_success.equals must be a scalar value")
		}
		return nil
	case "":
		return fmt.Errorf("response_success.kind is required")
	default:
		return fmt.Errorf("response_success.kind %q is unsupported", strings.TrimSpace(check.Kind))
	}
}

func Equivalent(a, b runtimecontracts.HTTPResponseSuccess) bool {
	if NormalizeKind(a.Kind) != NormalizeKind(b.Kind) || strings.TrimSpace(a.Path) != strings.TrimSpace(b.Path) {
		return false
	}
	if a.Equals == nil || b.Equals == nil {
		return a.Equals == nil && b.Equals == nil
	}
	return valuesEqual(a.Equals, b.Equals) && valuesEqual(b.Equals, a.Equals)
}

func Evaluate(subject string, check *runtimecontracts.HTTPResponseSuccess, responseEnv map[string]any, secrets []string) error {
	if check == nil {
		return nil
	}
	subject = strings.TrimSpace(subject)
	if err := Validate(*check); err != nil {
		return fmt.Errorf("%s %s", subject, err)
	}
	switch NormalizeKind(check.Kind) {
	case KindHTTPStatus2xx:
		status, err := lookupPath(responseEnv, "response.status")
		if err != nil {
			return fmt.Errorf("%s response_success path %q did not resolve", subject, "response.status")
		}
		value, ok := number(status)
		if ok && value >= 200 && value < 300 {
			return nil
		}
		return redactedFailure(subject, "response.status", status, "HTTP 2xx", secrets)
	case KindJSONFieldEquals:
		path := strings.TrimSpace(check.Path)
		got, err := lookupPath(responseEnv, path)
		if err != nil {
			return fmt.Errorf("%s response_success path %q did not resolve", subject, path)
		}
		if valuesEqual(got, check.Equals) {
			return nil
		}
		return redactedFailure(subject, path, got, check.Equals, secrets)
	default:
		panic("validated response_success kind was not handled")
	}
}

func redactedFailure(subject, path string, got, want any, secrets []string) error {
	message := fmt.Sprintf("%s response_success failed: %s = %s, want %s", subject, path, formatValue(got), formatValue(want))
	return fmt.Errorf("%s", runtimemanagedcredentials.RedactString(message, secrets...))
}

func lookupPath(root map[string]any, raw string) (any, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return nil, fmt.Errorf("empty response path")
	}
	var current any = root
	for _, part := range splitPath(path) {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, fmt.Errorf("response path %q is not available", path)
			}
			current = next
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("response path %q is not available", path)
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("response path %q is not available", path)
		}
	}
	return current, nil
}

func splitPath(path string) []string {
	replacer := strings.NewReplacer("[", ".", "]", "")
	parts := strings.Split(replacer.Replace(path), ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func scalar(value any) bool {
	switch value.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return true
	default:
		return false
	}
}

func valuesEqual(got, want any) bool {
	switch wantTyped := want.(type) {
	case bool:
		gotTyped, ok := got.(bool)
		return ok && gotTyped == wantTyped
	case string:
		gotTyped, ok := got.(string)
		return ok && gotTyped == wantTyped
	default:
		wantNumber, wantOK := number(want)
		gotNumber, gotOK := number(got)
		return wantOK && gotOK && gotNumber == wantNumber
	}
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func formatValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}
