package runtime

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

func writeInboundPublicationError(w http.ResponseWriter, logger *RuntimeLogger, ctx context.Context, provider, serviceID, providerEventID string, err error) {
	logInboundPublicationFailure(logger, ctx, provider, serviceID, providerEventID, err)
	status, message := inboundPublicationErrorResponse(err)
	http.Error(w, message, status)
}

func inboundPublicationErrorResponse(err error) (int, string) {
	if path, permanent := runtimebus.SubmittedInboundValidationPath(err); permanent {
		message := "invalid webhook payload"
		if path != "" {
			message += " at " + inboundValidationDisplayPath(path)
		}
		return http.StatusBadRequest, message
	}
	if runtimebus.IsRootConstructorKeyConflict(err) {
		return http.StatusServiceUnavailable, "root input contradicts its immutable constructor key"
	}
	if providerErr, onlyProvider := inboundProviderError(err); onlyProvider {
		return providerErr.Status, providerErr.Message
	}
	return http.StatusServiceUnavailable, "publish inbound failed"
}

func inboundValidationDisplayPath(path string) string {
	var out strings.Builder
	for _, char := range path {
		quoted := strconv.QuoteToASCII(string(char))
		escaped := quoted[1 : len(quoted)-1]
		if out.Len()+len(escaped) > 128 {
			out.WriteString("...")
			break
		}
		out.WriteString(escaped)
	}
	return out.String()
}

// A provider's existing public contract survives wrapping, but not a join with
// an independent internal failure or a contradictory public disposition.
func inboundProviderError(err error) (providertriggers.Error, bool) {
	switch err := err.(type) {
	case providertriggers.Error:
		return err, true
	case *providertriggers.Error:
		if err != nil {
			return *err, true
		}
	case interface{ Unwrap() []error }:
		var first providertriggers.Error
		causes := err.Unwrap()
		for i, cause := range causes {
			current, valid := inboundProviderError(cause)
			if !valid || i > 0 && current != first {
				return providertriggers.Error{}, false
			}
			first = current
		}
		return first, len(causes) > 0
	case interface{ Unwrap() error }:
		return inboundProviderError(err.Unwrap())
	}
	return providertriggers.Error{}, false
}
