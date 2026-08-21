package registration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

type registrationRoundTripFunc func(*http.Request) (*http.Response, error)

func (f registrationRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func serveRegistrationTestContext(harness *effecttest.Harness, identity string) context.Context {
	intentID := uuid.NewString()
	startupID := uuid.NewString()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityServeRegistration, ID: intentID,
		ExecutionOwner: "serve-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 3,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ServeRegistration: runtimeeffects.ServeRegistrationAuthority{
			IntentID: intentID, StartupAuthorityID: startupID, StartupStateVersion: 2,
		},
	}
	ctx := runtimeeffects.WithController(context.Background(), runtimeeffects.NewController(harness).WithExecutionPosture(executionposture.Live))
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	ctx = runtimeeffects.WithAuthority(ctx, authority)
	return runtimeeffects.WithLogicalOperationIdentity(ctx, identity)
}

func TestProviderRegistrationApplyEffectOutcomes(t *testing.T) {
	tool := packfixture.ConnectorTool(t, "telegram", "telegram.apply_webhook").Tool
	input := map[string]any{"callback_url": "https://hooks.example.test/webhooks/support/telegram?swarm_callback_generation=current"}
	credentials := map[string]any{"telegram_bot_token": "bot-secret", "webhook_signing_secret": "signing-secret"}
	lineage := map[string]string{"binding_id": "hitl", "target": "ingress:support:telegram:telegram", "intent_id": uuid.NewString(), "slot_id": "telegram:bot_webhook:42"}

	t.Run("known success", func(t *testing.T) {
		harness := effecttest.New()
		executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if err := harness.RequireState("provider_registration", runtimeeffects.StateLaunched); err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(request.URL.String(), "bot-secret") || !strings.Contains(string(body), "signing-secret") {
				t.Fatalf("provider request did not consume admitted credential snapshots: url=%s body=%s", request.URL, body)
			}
			return registrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
		})}}
		result, err := executor.Apply(serveRegistrationTestContext(harness, "known-success"), "telegram.apply_webhook", tool, input, credentials, lineage)
		if err != nil || !result.Acknowledged || result.Pending == nil {
			t.Fatalf("Apply = %#v, %v", result, err)
		}
		if err := harness.RequireState("provider_registration", runtimeeffects.StateResponseObserved); err != nil {
			t.Fatal(err)
		}
		if err := result.Pending.SettleReadback(context.Background(), true, nil); err != nil {
			t.Fatalf("SettleReadback exact: %v", err)
		}
		if err := harness.RequireState("provider_registration", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("acknowledgment loss exact readback", func(t *testing.T) {
		harness := effecttest.New()
		executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			if err := harness.RequireState("provider_registration", runtimeeffects.StateLaunched); err != nil {
				t.Fatal(err)
			}
			return nil, errors.New("transport lost bot-secret signing-secret")
		})}}
		result, err := executor.Apply(serveRegistrationTestContext(harness, "ack-loss-exact"), "telegram.apply_webhook", tool, input, credentials, lineage)
		if err == nil || result.Pending == nil || strings.Contains(err.Error(), "bot-secret") || strings.Contains(err.Error(), "signing-secret") {
			t.Fatalf("Apply acknowledgment loss = %#v, %v", result, err)
		}
		if err := result.Pending.SettleReadback(context.Background(), true, nil); err != nil {
			t.Fatalf("SettleReadback exact: %v", err)
		}
		if err := harness.RequireState("provider_registration", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("acknowledgment loss mismatch", func(t *testing.T) {
		harness := effecttest.New()
		executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			if err := harness.RequireState("provider_registration", runtimeeffects.StateLaunched); err != nil {
				t.Fatal(err)
			}
			return nil, errors.New("transport lost")
		})}}
		result, err := executor.Apply(serveRegistrationTestContext(harness, "ack-loss-mismatch"), "telegram.apply_webhook", tool, input, credentials, lineage)
		if err == nil || result.Pending == nil {
			t.Fatalf("Apply acknowledgment loss = %#v, %v", result, err)
		}
		if err := result.Pending.SettleReadback(context.Background(), false, errors.New("callback mismatch")); err != nil {
			t.Fatalf("terminalize mismatched readback: %v", err)
		}
		if err := harness.RequireState("provider_registration", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("settlement acknowledgment loss reconciles by readback", func(t *testing.T) {
		harness := effecttest.New()
		harness.SettleErr = errors.New("injected settlement acknowledgment loss")
		executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return registrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
		})}}
		result, err := executor.Apply(serveRegistrationTestContext(harness, "settlement-ack-loss"), "telegram.apply_webhook", tool, input, credentials, lineage)
		if err != nil || result.Pending == nil || !result.Acknowledged {
			t.Fatalf("Apply settlement acknowledgment loss = %#v, %v", result, err)
		}
		if err := result.Pending.SettleReadback(context.Background(), true, nil); err == nil {
			t.Fatal("first readback settlement unexpectedly succeeded")
		}
		harness.SettleErr = nil
		if err := result.Pending.SettleReadback(context.Background(), true, nil); err != nil {
			t.Fatalf("SettleReadback after settlement acknowledgment loss: %v", err)
		}
		if err := harness.RequireState("provider_registration", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("malformed success response reconciles by readback", func(t *testing.T) {
		harness := effecttest.New()
		executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return registrationResponse(http.StatusOK, `{"ok":true,"result":"not-a-boolean"}`), nil
		})}}
		result, err := executor.Apply(serveRegistrationTestContext(harness, "malformed-success"), "telegram.apply_webhook", tool, input, credentials, lineage)
		if err == nil || result.Pending == nil {
			t.Fatalf("Apply malformed success = %#v, %v", result, err)
		}
		if err := result.Pending.SettleReadback(context.Background(), true, nil); err != nil {
			t.Fatalf("SettleReadback malformed success: %v", err)
		}
		if err := harness.RequireState("provider_registration", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("stale authority stops before launch", func(t *testing.T) {
		harness := effecttest.New()
		harness.AuthorizeErr = errors.New("superseded startup authority")
		executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("stale authority reached provider transport")
			return nil, nil
		})}}
		if _, err := executor.Apply(serveRegistrationTestContext(harness, "stale"), "telegram.apply_webhook", tool, input, credentials, lineage); err == nil {
			t.Fatal("stale authority Apply returned nil")
		}
		if _, exists := harness.StateForAdapter("provider_registration"); exists {
			t.Fatal("stale authority created an effect attempt")
		}
	})

	t.Run("launch persistence failure stops before provider transport", func(t *testing.T) {
		harness := effecttest.New()
		harness.MarkErr = errors.New("injected launch persistence failure")
		executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("launch persistence failure reached provider transport")
			return nil, nil
		})}}
		result, err := executor.Apply(serveRegistrationTestContext(harness, "launch-persistence-failure"), "telegram.apply_webhook", tool, input, credentials, lineage)
		if err == nil || result.Pending != nil || result.Acknowledged {
			t.Fatalf("Apply launch persistence failure = %#v, %v", result, err)
		}
		if strings.Contains(err.Error(), "acknowledgment_lost") {
			t.Fatalf("prelaunch persistence failure was mislabeled as provider acknowledgment loss: %v", err)
		}
		if err := harness.RequireState("provider_registration", runtimeeffects.StateTerminalFailure); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProviderRegistrationTransportFailsClosedBeforeCredentialsLeaveProcess(t *testing.T) {
	tool := packfixture.ConnectorTool(t, "telegram", "telegram.apply_webhook").Tool
	httpSpec, ok := tool.HTTP()
	if !ok {
		t.Fatal("Telegram registration HTTP contract is missing")
	}
	httpSpec.URL = strings.Replace(httpSpec.URL, "https://", "http://", 1)
	insecure, err := tool.WithHTTP(httpSpec)
	if err != nil {
		t.Fatalf("construct insecure registration tool: %v", err)
	}
	harness := effecttest.New()
	executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("insecure registration request reached transport")
		return nil, nil
	})}}
	_, err = executor.Apply(
		serveRegistrationTestContext(harness, "insecure"),
		"telegram.apply_webhook",
		insecure,
		map[string]any{"callback_url": "https://hooks.example.test/webhooks/support/telegram?swarm_callback_generation=current"},
		map[string]any{"telegram_bot_token": "bot-secret", "webhook_signing_secret": "signing-secret"},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "HTTPS URL") {
		t.Fatalf("insecure registration error = %v", err)
	}
	if _, exists := harness.StateForAdapter("provider_registration"); exists {
		t.Fatal("insecure registration created an effect attempt")
	}

	if _, err := readProviderBody(bytes.NewReader(make([]byte, (4<<20)+1))); err == nil {
		t.Fatal("oversized provider response was accepted")
	}
}

func registrationResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
