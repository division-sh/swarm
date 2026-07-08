package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

const (
	cliExitOK          = 0
	cliExitValidation  = 2
	cliExitRuntime     = 3
	cliExitAuth        = 4
	cliExitNotFound    = 5
	cliExitConflict    = 6
	cliExitRunTerminal = 7
	cliExitInterrupted = 130
)

var errCLIAPITokenRequired = errors.New("API token source is required for non-loopback API targets; use --api-token-file, context descriptor auth, or config connection.api_token_file")

type cliAPIErrorClassifier struct {
	runtimeExit   int
	authExit      int
	notFoundExit  int
	conflictExit  int
	notFoundCodes []string
	conflictCodes []string
}

func cliAPIErrorExitCode(err error, classifier cliAPIErrorClassifier) int {
	if err == nil {
		return cliExitOK
	}
	runtimeExit := classifier.runtimeExit
	if runtimeExit == 0 {
		runtimeExit = cliExitRuntime
	}
	authExit := classifier.authExit
	if authExit == 0 {
		authExit = cliExitAuth
	}
	notFoundExit := classifier.notFoundExit
	if notFoundExit == 0 {
		notFoundExit = cliExitNotFound
	}
	conflictExit := classifier.conflictExit
	if conflictExit == 0 {
		conflictExit = cliExitConflict
	}
	if errors.Is(err, errCLIAPITokenRequired) {
		return authExit
	}
	var authConfigErr *cliAPIAuthConfigError
	if errors.As(err, &authConfigErr) {
		return authExit
	}
	var validationErr *cliAPIValidationError
	if errors.As(err, &validationErr) {
		return cliExitValidation
	}
	var configErr unifiedConfigError
	if errors.As(err, &configErr) {
		return cliExitValidation
	}
	var httpErr *cliAPIHTTPError
	if errors.As(err, &httpErr) {
		if httpErr.statusCode == http.StatusUnauthorized || httpErr.statusCode == http.StatusForbidden {
			return authExit
		}
		return runtimeExit
	}
	var rpcErr *jsonRPCError
	if errors.As(err, &rpcErr) {
		switch code := applicationErrorCode(rpcErr.Data); {
		case code == "UNAUTHORIZED":
			return authExit
		case cliCodeInSet(code, classifier.notFoundCodes):
			return notFoundExit
		case cliCodeInSet(code, classifier.conflictCodes):
			return conflictExit
		default:
			return runtimeExit
		}
	}
	return runtimeExit
}

func returnCLIAPIError(errOut io.Writer, err error, classifier cliAPIErrorClassifier) error {
	if errOut != nil {
		fmt.Fprintln(errOut, err)
	}
	return commandExitError{code: cliAPIErrorExitCode(err, classifier)}
}

func returnCLIValidationError(errOut io.Writer, err error) error {
	if errOut != nil {
		fmt.Fprintln(errOut, err)
	}
	return commandExitError{code: cliExitValidation}
}

func cliCodeInSet(code string, codes []string) bool {
	for _, candidate := range codes {
		if code == candidate {
			return true
		}
	}
	return false
}
