package builder

import (
	"net/http"
	"strings"
)

const builderRPCAuthErrorCode = -32001

type builderAuthFailure struct {
	status  int
	message string
}

func (h *handler) authorizeControlPlane(r *http.Request) *builderAuthFailure {
	if h == nil {
		return &builderAuthFailure{status: http.StatusNotFound, message: "builder handler is unavailable"}
	}
	if strings.TrimSpace(h.authToken) == "" {
		return &builderAuthFailure{status: http.StatusServiceUnavailable, message: "builder control-plane auth is not configured"}
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if authz == "" {
		return &builderAuthFailure{status: http.StatusUnauthorized, message: "missing authorization bearer token"}
	}
	const prefix = "bearer "
	if !strings.HasPrefix(strings.ToLower(authz), prefix) {
		return &builderAuthFailure{status: http.StatusUnauthorized, message: "invalid authorization header"}
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if token != h.authToken {
		return &builderAuthFailure{status: http.StatusUnauthorized, message: "invalid bearer token"}
	}
	return nil
}

func (h *handler) writeAuthFailure(w http.ResponseWriter, r *http.Request, failure *builderAuthFailure) {
	if failure == nil {
		return
	}
	if failure.status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="swarm-builder"`)
	}
	if r != nil && r.Method == http.MethodPost {
		writeJSON(w, failure.status, RPCResponse{
			JSONRPC: "2.0",
			Error: &RPCError{
				Code:    builderRPCAuthErrorCode,
				Message: failure.message,
			},
		})
		return
	}
	http.Error(w, failure.message, failure.status)
}
