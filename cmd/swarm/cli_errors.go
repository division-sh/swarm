package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
	writeCLIAPIError(errOut, err)
	return commandExitError{code: cliAPIErrorExitCode(err, classifier)}
}

func returnCLIValidationError(errOut io.Writer, err error) error {
	if errOut != nil {
		fmt.Fprintln(errOut, formatCLIAPIError(err))
	}
	return commandExitError{code: cliExitValidation}
}

func writeCLIAPIError(errOut io.Writer, err error) {
	if errOut == nil || err == nil {
		return
	}
	fmt.Fprintln(errOut, formatCLIAPIError(err))
}

func formatCLIAPIError(err error) string {
	if err == nil {
		return ""
	}
	if diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
		return formatCLILoaderDiagnostic(diagnostic)
	}
	if diagnostic, ok := cliAPITransportDiagnosticParts(err); ok {
		lines := []string{"ERROR: " + cliAPIProblemWithWrapper(err, diagnostic.leaf, diagnostic.problem)}
		lines = append(lines, diagnostic.evidence...)
		lines = append(lines, diagnostic.remediation...)
		return strings.Join(lines, "\n")
	}
	return err.Error()
}

func formatCLILoaderDiagnostic(diagnostic *runtimecontracts.LoaderDiagnostic) string {
	if diagnostic == nil {
		return ""
	}
	problem := strings.TrimSpace(diagnostic.Problem)
	if problem == "" {
		problem = strings.TrimSpace(diagnostic.RawCause)
	}
	if problem == "" {
		problem = "contract loader validation failed."
	}
	lines := []string{"ERROR: " + problem}
	if location := diagnostic.Location.String(); location != "" {
		lines = append(lines, "  Location: "+location)
	}
	if len(diagnostic.ValidOptions) > 0 {
		lines = append(lines, "  Valid options: "+strings.Join(diagnostic.ValidOptions, ", "))
	}
	if remediation := strings.TrimSpace(diagnostic.Remediation); remediation != "" {
		lines = append(lines, "  Remediation: "+remediation)
	}
	return strings.Join(lines, "\n")
}

func formatCLIAPITransportDetail(err error) string {
	if err == nil {
		return ""
	}
	if diagnostic, ok := cliAPITransportDiagnosticParts(err); ok {
		return cliAPIProblemWithWrapper(err, diagnostic.leaf, diagnostic.problem)
	}
	return err.Error()
}

type cliAPITransportDiagnostic struct {
	leaf        error
	problem     string
	evidence    []string
	remediation []string
}

func cliAPITransportDiagnosticParts(err error) (cliAPITransportDiagnostic, bool) {
	var transportErr *cliAPITransportError
	if errors.As(err, &transportErr) {
		target := cliAPIDiagnosticTarget(transportErr.endpoint)
		problem := fmt.Sprintf("cannot reach the Swarm runtime at %s.", target)
		return cliAPITransportDiagnostic{
			leaf:    transportErr,
			problem: problem,
			remediation: []string{
				"  Is the runtime running? Start one with `swarm serve` (or `swarm serve --dev` for local development).",
				"  Check the selected target with `swarm context current`; override with --api-server.",
			},
		}, true
	}
	var protocolErr *cliAPIProtocolError
	if errors.As(err, &protocolErr) {
		target := cliAPIDiagnosticTarget(protocolErr.endpoint)
		problem := fmt.Sprintf("the Swarm runtime at %s returned an invalid API response.", target)
		var evidence []string
		if protocolErr.err != nil {
			if text := strings.TrimSpace(protocolErr.err.Error()); text != "" {
				evidence = []string{"  Evidence: " + text}
			}
		}
		return cliAPITransportDiagnostic{
			leaf:     protocolErr,
			problem:  problem,
			evidence: evidence,
			remediation: []string{
				"  Check the selected target with `swarm context current`; override with --api-server.",
				"  If this is a local runtime, restart it with `swarm serve`.",
			},
		}, true
	}
	var httpErr *cliAPIHTTPError
	if errors.As(err, &httpErr) {
		target := cliAPIDiagnosticTarget(httpErr.endpoint)
		status := httpErr.statusCode
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			problem := fmt.Sprintf("the Swarm runtime at %s rejected the request with status %d.", target, status)
			return cliAPITransportDiagnostic{
				leaf:    httpErr,
				problem: problem,
				remediation: []string{
					"  Check API credentials with --api-token-file or the selected context.",
					"  Check the selected target with `swarm context current`; override with --api-server.",
				},
			}, true
		}
		problem := fmt.Sprintf("the Swarm runtime at %s returned status %d.", target, status)
		return cliAPITransportDiagnostic{
			leaf:    httpErr,
			problem: problem,
			remediation: []string{
				"  Check the runtime with `swarm health` or restart it with `swarm serve`.",
				"  Check the selected target with `swarm context current`; override with --api-server.",
			},
		}, true
	}
	return cliAPITransportDiagnostic{}, false
}

func cliAPIProblemWithWrapper(err, leaf error, problem string) string {
	full := strings.TrimSpace(err.Error())
	leafText := strings.TrimSpace(leaf.Error())
	if err != leaf && full != "" && leafText != "" && full != leafText && strings.HasSuffix(full, leafText) {
		prefix := strings.TrimSpace(strings.TrimSuffix(full, leafText))
		if prefix != "" {
			return prefix + " " + problem
		}
	}
	return problem
}

func cliAPIDiagnosticTarget(endpoint string) string {
	trimmed := strings.TrimSpace(endpoint)
	if trimmed == "" {
		return "the selected target"
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	host := strings.TrimSpace(parsed.Host)
	if host == "" {
		return trimmed
	}
	path := strings.TrimRight(parsed.Path, "/")
	for _, suffix := range []string{cliAPIRPCPath, cliAPIWSPath} {
		if path == suffix {
			path = ""
			break
		}
		if strings.HasSuffix(path, suffix) {
			path = strings.TrimRight(strings.TrimSuffix(path, suffix), "/")
			break
		}
	}
	if path == "" || path == "/" {
		return host
	}
	return host + path
}

func cliAPIIsTransportFailure(err error) bool {
	var transportErr *cliAPITransportError
	return errors.As(err, &transportErr)
}

func cliCodeInSet(code string, codes []string) bool {
	for _, candidate := range codes {
		if code == candidate {
			return true
		}
	}
	return false
}
