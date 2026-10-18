package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/sessionstate"
)

type originalCauseSQLFixture struct {
	state  string
	secret string
	calls  *int
}

func (e *originalCauseSQLFixture) Error() string    { *e.calls++; return e.secret }
func (e *originalCauseSQLFixture) SQLState() string { return e.state }

func exerciseOnboardingOriginalCauseDiagnostic(t *testing.T, handler *Handler, lifecycle *recordingChannelOnboardingLifecycle, operationID string) {
	t.Helper()
	const secret = "PRIVATE_DIAGNOSTIC_PROSE_2614"
	called := 0
	sql := &originalCauseSQLFixture{state: "40P01", secret: secret, calls: &called}
	deep := error(sql)
	for i := 0; i < 12; i++ {
		deep = fmt.Errorf("%s: %w", secret, deep)
	}
	wide := make([]error, 40)
	for i := range wide {
		wide[i] = sql
	}
	for _, tc := range []struct {
		name      string
		err       error
		want      string
		truncated bool
	}{
		{"wrapped", fmt.Errorf("%s: %w", secret, context.DeadlineExceeded), "context_deadline", false},
		{"joined", errors.Join(fmt.Errorf("%s: %w", secret, sessionstate.ErrSessionAccount), context.Canceled), "session_account", false},
		{"sqlstate", sql, "40P01", false},
		{"unknown_secret_prose", errors.New(secret), "*errors.errorString", false},
		{"typed", runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "unclassified_runtime_error", "api", "channel.onboarding_retry", nil, errors.New(secret)), "platform.internal_failure", false},
		{"invalid_sqlstate", &originalCauseSQLFixture{state: "40p01\n" + secret, secret: secret, calls: &called}, "sqlstate_invalid", false},
		{"deep", deep, "truncated", true},
		{"wide", errors.Join(wide...), "truncated", true},
	} {
		t.Run("original_cause_"+tc.name, func(t *testing.T) {
			before := called
			metadata := onboardingOriginalCauseMetadata(tc.err, 17)
			if called != before {
				t.Fatal("capture called an error's Error method")
			}
			raw, err := json.Marshal(metadata)
			if err != nil || len(metadata.Nodes) > 32 || metadata.Truncated != tc.truncated || !strings.Contains(string(raw), tc.want) || strings.Contains(string(raw), secret) {
				t.Fatal("capture lost bounded metadata or disclosed error prose")
			}
			for _, node := range metadata.Nodes {
				if node.Depth > 8 {
					t.Fatal("capture exceeded depth bound")
				}
			}
			lifecycle.retryErr = tc.err
			request := `{"jsonrpc":"2.0","id":"capture-control","method":"channel.onboarding_retry","params":{"operation_id":"` + operationID + `"}}`
			t.Setenv("SWARM_TEST_2614_ORIGINAL_CAUSE", "")
			baseline := rpcCall(t, handler, request)
			t.Setenv("SWARM_TEST_2614_ORIGINAL_CAUSE", "1")
			var captured rpcResponse
			output := captureProcessLog(t, func() { captured = rpcCall(t, handler, request) })
			if !reflect.DeepEqual(captured, baseline) {
				t.Fatal("capture changed the public response")
			}
			if !strings.Contains(output, onboardingOriginalCausePrefix) || strings.Contains(output, secret) {
				t.Fatal("method boundary lost capture or disclosed private prose")
			}
		})
	}
	lifecycle.retryErr = nil
	// Independently joined failures remain visible; capture does not consume or
	// replace the original error passed to the existing projection.
	joined := errors.Join(channelonboarding.ErrRevisionConflict, sql)
	metadata := onboardingOriginalCauseMetadata(joined, 18)
	if len(metadata.Nodes) != 3 || len(metadata.Nodes[1].Sentinels) != 1 || metadata.Nodes[2].SQLState != "40P01" {
		t.Fatal("joined cause branch was lost")
	}
}
