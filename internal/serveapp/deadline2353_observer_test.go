package serveapp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil/sqlite2353"
)

var deadline2353Clients sync.Map

// One client observation per exact endpoint/test; it does not resend anything.
func deadline2353ObserveHTTP(t *testing.T, req *http.Request, endpoint, method string, timeout time.Duration, params map[string]any) (*http.Request, func()) {
	row := strings.TrimSpace(os.Getenv("TEST_2353_ROW"))
	if (row == "S01" && method != "event.publish") || (row == "S02" && method != "run.fork") || row == "S03" || row == "S04" || !sqlite2353.Enabled() {
		return req, func() {}
	}
	key := t.Name() + "|" + endpoint + "|" + method
	if _, loaded := deadline2353Clients.LoadOrStore(key, true); loaded {
		return req, func() {}
	}
	started := time.Now()
	record := func(phase string, err error) {
		sqlite2353.Record("client."+method+"."+phase, map[string]any{"test": t.Name(), "elapsed_ns": time.Since(started).Nanoseconds(), "timeout_ns": timeout.Nanoseconds(), "error": fmt.Sprint(err)})
	}
	record("begin", nil)
	t.Cleanup(func() {
		if t.Failed() {
			if row == "S01" {
				r7P27ReadCommittedOutcome(t, endpoint, params["bundle_hash"])
			} else if row == "S02" {
				issue642ObserveCommittedFork(t, endpoint, params["source_run_id"])
			}
		}
	})
	trace := &httptrace.ClientTrace{
		GetConn:              func(string) { record("get_connection", nil) },
		GotConn:              func(httptrace.GotConnInfo) { record("got_connection", nil) },
		WroteRequest:         func(info httptrace.WroteRequestInfo) { record("wrote_request", info.Err) },
		GotFirstResponseByte: func() { record("first_response_byte", nil) },
	}
	return req.WithContext(httptrace.WithClientTrace(req.Context(), trace)), func() { record("helper_return", context.Cause(req.Context())) }
}
