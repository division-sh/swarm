package apiv1

import (
	"fmt"
	"strings"
)

type ApplicationError struct {
	Code      string
	Retryable bool
	Details   any
}

func NewApplicationError(code string, retryable bool, details any) *ApplicationError {
	return &ApplicationError{
		Code:      strings.TrimSpace(code),
		Retryable: retryable,
		Details:   details,
	}
}

func (e *ApplicationError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("application error: %s", strings.TrimSpace(e.Code))
}

type InvalidParamsError struct {
	Details any
}

func NewInvalidParamsError(details any) *InvalidParamsError {
	return &InvalidParamsError{Details: details}
}

func (e *InvalidParamsError) Error() string {
	return "invalid params"
}
