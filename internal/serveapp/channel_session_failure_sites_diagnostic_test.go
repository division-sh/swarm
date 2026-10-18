//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

// Disposable, failure-only observation under reviewer-g 6252394166. Never log
// response bodies, identifiers, error prose, or infer a repair from these tags.
type nativeJourneyDiagnostic struct{ phase atomic.Value }

var nativeJourneyDiagnosticScopes sync.Map

func nativeJourneyDiagnosticScope(t *testing.T, phase string) (*nativeJourneyDiagnostic, func()) {
	scope := &nativeJourneyDiagnostic{}
	scope.phase.Store(phase)
	prior, present := nativeJourneyDiagnosticScopes.Load(t)
	nativeJourneyDiagnosticScopes.Store(t, scope)
	return scope, func() {
		if present {
			nativeJourneyDiagnosticScopes.Store(t, prior)
		} else {
			nativeJourneyDiagnosticScopes.Delete(t)
		}
	}
}

func (scope *nativeJourneyDiagnostic) mark(phase string) { scope.phase.Store(phase) }

func nativeJourneyErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	if err == context.Canceled {
		return "context_canceled"
	}
	if err == context.DeadlineExceeded {
		return "context_deadline"
	}
	switch err.(type) {
	case *json.SyntaxError:
		return "json_syntax"
	case *json.UnmarshalTypeError:
		return "json_type"
	case *url.Error:
		return "url"
	case *os.PathError:
		return "path"
	default:
		return "unlisted"
	}
}

func nativeJourneyFailureJSON(leaf, kind, phase, site, method string, flags map[string]bool, err error, priorFailure bool) string {
	switch method {
	case "", "channel.onboarding_start", "channel.onboarding_retry", "channel.onboarding_get", "channel.confirm", "channel.logout", "mailbox.list":
	default:
		method = "other"
	}
	raw, _ := json.Marshal(struct {
		Leaf         string          `json:"leaf"`
		Kind         string          `json:"kind"`
		Phase        string          `json:"phase"`
		Site         string          `json:"site,omitempty"`
		Method       string          `json:"method,omitempty"`
		Flags        map[string]bool `json:"flags,omitempty"`
		ErrorClass   string          `json:"error_class"`
		PriorFailure bool            `json:"prior_failure"`
	}{leaf, kind, phase, site, method, flags, nativeJourneyErrorClass(err), priorFailure})
	return string(raw)
}

func logNativeJourneyFailure(t *testing.T, phase, site string, flags map[string]bool, err error) {
	t.Helper()
	if os.Getenv("SWARM_TEST_2614_ORIGINAL_CAUSE") == "1" {
		t.Logf("NATIVE_JOURNEY_FAILURE %s", nativeJourneyFailureJSON(t.Name(), "site", phase, site, "", flags, err, t.Failed()))
	}
}

func logNativeJourneyRPCFailure(t *testing.T, method, site string, flags map[string]bool, err error) {
	t.Helper()
	if scope, ok := nativeJourneyDiagnosticScopes.Load(t); ok && os.Getenv("SWARM_TEST_2614_ORIGINAL_CAUSE") == "1" {
		phase := scope.(*nativeJourneyDiagnostic).phase.Load().(string)
		t.Logf("NATIVE_JOURNEY_FAILURE %s", nativeJourneyFailureJSON(t.Name(), "site", phase, site, method, flags, err, t.Failed()))
	}
}

func (scope *nativeJourneyDiagnostic) failed(t *testing.T) {
	t.Helper()
	if os.Getenv("SWARM_TEST_2614_ORIGINAL_CAUSE") == "1" {
		t.Logf("NATIVE_JOURNEY_FAILURE %s", nativeJourneyFailureJSON(t.Name(), "phase", scope.phase.Load().(string), "", "", nil, nil, true))
	}
}
