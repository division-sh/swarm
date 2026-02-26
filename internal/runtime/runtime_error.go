package runtime

import (
	"errors"
	"fmt"
	"strings"
)

// RuntimeError is a structured runtime error envelope for operator-facing diagnostics.
type RuntimeError struct {
	Code      string
	Component string
	Operation string
	Retryable bool
	Message   string
	Cause     error
}

func (e *RuntimeError) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{"runtime_error"}
	if code := strings.TrimSpace(e.Code); code != "" {
		parts = append(parts, "code="+code)
	}
	if component := strings.TrimSpace(e.Component); component != "" {
		parts = append(parts, "component="+component)
	}
	if operation := strings.TrimSpace(e.Operation); operation != "" {
		parts = append(parts, "operation="+operation)
	}
	parts = append(parts, fmt.Sprintf("retryable=%t", e.Retryable))

	msg := strings.TrimSpace(e.Message)
	if msg == "" && e.Cause != nil {
		msg = strings.TrimSpace(e.Cause.Error())
	}
	base := strings.Join(parts, " ")
	if msg != "" {
		base += ": " + msg
	}
	if e.Cause != nil {
		causeText := strings.TrimSpace(e.Cause.Error())
		if causeText != "" && !strings.Contains(msg, causeText) {
			base += "; cause=" + causeText
		}
	}
	return strings.TrimSpace(base)
}

func (e *RuntimeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func WrapRuntimeError(code, component, operation string, retryable bool, cause error, format string, args ...any) error {
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	if msg == "" && cause == nil {
		return nil
	}
	return &RuntimeError{
		Code:      strings.TrimSpace(code),
		Component: strings.TrimSpace(component),
		Operation: strings.TrimSpace(operation),
		Retryable: retryable,
		Message:   msg,
		Cause:     cause,
	}
}

func NewRuntimeError(code, component, operation string, retryable bool, format string, args ...any) error {
	return WrapRuntimeError(code, component, operation, retryable, nil, format, args...)
}

func AsRuntimeError(err error) (*RuntimeError, bool) {
	if err == nil {
		return nil, false
	}
	var out *RuntimeError
	if errors.As(err, &out) {
		return out, true
	}
	return nil, false
}

func FormatRuntimeError(err error) string {
	if err == nil {
		return ""
	}
	if re, ok := AsRuntimeError(err); ok {
		return re.Error()
	}
	return strings.TrimSpace(err.Error())
}
