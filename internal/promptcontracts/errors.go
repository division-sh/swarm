package promptcontracts

import (
	"errors"
	"strings"
)

var ErrUnresolvedPromptVariables = errors.New("promptcontracts: unresolved prompt variables")

type UnresolvedPromptVariablesError struct {
	Variables []string
}

func (e *UnresolvedPromptVariablesError) Error() string {
	if e == nil {
		return ErrUnresolvedPromptVariables.Error()
	}
	if len(e.Variables) == 0 {
		return ErrUnresolvedPromptVariables.Error()
	}
	return ErrUnresolvedPromptVariables.Error() + ": " + strings.Join(e.Variables, ", ")
}

func (e *UnresolvedPromptVariablesError) Unwrap() error {
	return ErrUnresolvedPromptVariables
}
