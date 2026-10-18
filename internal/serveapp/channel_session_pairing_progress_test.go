//go:build linux || darwin

package serveapp

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

// An explicit resume is permitted only for canonical pending preparation.
// Every request shares the original pairing deadline, including readback.
func servedSessionPairingProgressRequest(original, current channelonboarding.Result, deadline time.Time) (string, time.Duration, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return "", 0, errors.New("original pairing window expired")
	}
	o, c := original.Operation, current.Operation
	if o.OperationID == "" || c.OperationID != o.OperationID || c.Interface != o.Interface ||
		c.Provider != o.Provider || c.TargetSelector != o.TargetSelector || c.Posture != o.Posture ||
		!c.Coordinate.MatchesDeclaration(o.Coordinate) || c.Phase.Terminal() || !c.Phase.Valid() ||
		current.Readiness != nil && current.Readiness.Ready {
		return "", 0, errors.New("pairing progress contradicts the original non-executable responsibility")
	}
	pending, err := current.SessionPreparationPending()
	if err != nil {
		return "", 0, err
	}
	if pending {
		return "channel.onboarding_retry", remaining, nil
	}
	if current.Pairing != nil && (current.Pairing.Status == "failed" || current.Pairing.Status == "stopped" || current.Pairing.Status == "recovery_required") {
		return "", 0, errors.New("failed or closed pairing cannot continue automatically")
	}
	return "channel.onboarding_get", remaining, nil
}

func requireServedNativePairingProgress(t *testing.T, endpoint string, result channelonboarding.Result, deadline time.Time) channelonboarding.Result {
	t.Helper()
	original := result
	continuations := 0
	if _, _, err := servedSessionPairingProgressRequest(original, result, deadline); err != nil {
		t.Fatal("SDK pairing start has invalid responsibility or progress", err)
	}
	for result.Pairing == nil || result.Pairing.Code == "" {
		time.Sleep(10 * time.Millisecond)
		method, remaining, err := servedSessionPairingProgressRequest(original, result, deadline)
		if err != nil {
			t.Fatal("SDK pairing progress refused", result.Operation.Phase, err)
		}
		if method == "channel.onboarding_retry" {
			continuations++
		}
		response := requestTimedServedSessionPreparation(t, endpoint, method, original.Operation.OperationID, remaining, continuations)
		if response.Error != nil {
			t.Fatal("SDK pairing progress request failed", method, response.Error.Code)
		}
		if err := json.Unmarshal(response.Result, &result); err != nil {
			t.Fatal("SDK pairing progress has malformed evidence", err)
		}
		if _, _, err := servedSessionPairingProgressRequest(original, result, deadline); err != nil {
			t.Fatal("SDK pairing progress lost its original window or responsibility", result.Operation.Phase, err)
		}
	}
	if time.Now().After(deadline) {
		t.Fatal("authorized QR arrived after the original pairing deadline")
	}
	return result
}

func requestTimedServedSessionPreparation(t *testing.T, endpoint, method, operationID string, remaining time.Duration, continuation int) servedJSONRPCEnvelope {
	t.Helper()
	if method == "channel.onboarding_retry" {
		began := time.Now()
		t.Logf("onboarding_retry stage=preparation continuation=%d begin=%s remaining=%s", continuation, began.UTC().Format(time.RFC3339Nano), remaining)
		defer func() {
			t.Logf("onboarding_retry stage=preparation continuation=%d elapsed=%s", continuation, time.Since(began))
		}()
	}
	return requestServedJSONRPCWithTimeout(t, endpoint, method, map[string]any{"operation_id": operationID}, remaining)
}

func requireTimedServedNativeOnboardingRetry(t *testing.T, endpoint, operationID, stage string, result *channelonboarding.Result) {
	t.Helper()
	began := time.Now()
	t.Logf("onboarding_retry stage=%s begin=%s limit=5s", stage, began.UTC().Format(time.RFC3339Nano))
	defer func() { t.Logf("onboarding_retry stage=%s elapsed=%s", stage, time.Since(began)) }()
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": operationID}, result)
}

func TestServedSessionPairingContinuationRequiresCanonicalPending(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			service, adapter, op, release := startPendingSessionFixture(t, backend, "pairing-consumer-controls")
			defer release()
			original, err := service.Get(t.Context(), op.OperationID)
			requireSessionPendingResult(t, original, op, err)
			requireServedPairingConsumerCases(t, original)
			release()
			if err := adapter.RetireInactiveSessions(t.Context()); err != nil {
				t.Fatal(err)
			}
			adapter.mu.Lock()
			attempt := adapter.connections[op.OperationID]
			adapter.mu.Unlock()
			select {
			case <-attempt.done:
			case <-time.After(30 * time.Second):
				t.Fatal("pairing fixture did not join the original preparation")
			}
			if attempt.err != nil || attempt.connection == nil {
				t.Fatal("pairing fixture lost original preparation", attempt.err)
			}
			if err := attempt.connection.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func requireServedPairingConsumerCases(t *testing.T, original channelonboarding.Result) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for _, cell := range []string{"pending", "no_qr", "failed", "closed", "uncertain", "foreign_operation", "foreign_source", "malformed_pending", "expired"} {
		t.Run(cell, func(t *testing.T) {
			current := original
			window := deadline
			switch cell {
			case "no_qr":
				current.Pairing = nil
			case "failed":
				current.Operation.Phase = channelonboarding.PhaseFailed
			case "closed":
				current.Pairing = &channelonboarding.PairingReadback{Status: "stopped"}
			case "uncertain":
				current.Pairing = &channelonboarding.PairingReadback{Status: "recovery_required"}
			case "foreign_operation":
				current.Operation.OperationID = "foreign"
			case "foreign_source":
				current.Operation.Coordinate.BundleIdentity = "foreign"
			case "malformed_pending":
				current.Pairing = &channelonboarding.PairingReadback{Status: channelonboarding.PairingStatusPreparing, Connected: true}
			case "expired":
				window = time.Now().Add(-time.Second)
			}
			method, remaining, err := servedSessionPairingProgressRequest(original, current, window)
			if cell == "pending" || cell == "no_qr" {
				want := "channel.onboarding_get"
				if cell == "pending" {
					want = "channel.onboarding_retry"
				}
				if err != nil || method != want || remaining <= 0 || remaining > time.Until(deadline)+time.Millisecond {
					t.Fatal("pairing continuation changed the operation, deadline or read-only disposition", method, remaining, err)
				}
			} else if err == nil || method != "" || remaining != 0 {
				t.Fatal("invalid pairing gained automatic continuation or a fresh window", method, remaining, err)
			}
		})
	}
}
