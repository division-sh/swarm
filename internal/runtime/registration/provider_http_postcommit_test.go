package registration

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

type postCommitRegistrationStore struct {
	runtimeeffects.Store
	launchErr        error
	observationErr   error
	settlementErr    error
	observationCalls int
	cancel           context.CancelFunc
	stale            bool
	staleAfterLaunch bool
}

func (s *postCommitRegistrationStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, at time.Time) error {
	if err := s.Store.MarkExternalAttemptLaunched(ctx, attempt, at); err != nil {
		return err
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.staleAfterLaunch {
		s.stale = true
	}
	return runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationLaunch, attempt, s.launchErr)
}

func (s *postCommitRegistrationStore) IsExternalEffectAuthorityCurrent(ctx context.Context, authority runtimeeffects.Authority) (bool, error) {
	if s.stale {
		return false, nil
	}
	if authority.Kind == runtimeeffects.AuthorityServeRegistration || authority.Kind == runtimeeffects.AuthorityChannelConfirmation {
		return authority.Valid(), nil
	}
	return s.Store.IsExternalEffectAuthorityCurrent(ctx, authority)
}

func (s *postCommitRegistrationStore) MarkExternalAttemptResponseObserved(ctx context.Context, attempt runtimeeffects.Attempt, evidence map[string]any, at time.Time) error {
	s.observationCalls++
	if err := s.Store.MarkExternalAttemptResponseObserved(ctx, attempt, evidence, at); err != nil {
		return err
	}
	return runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationObservation, attempt, s.observationErr)
}

func (s *postCommitRegistrationStore) SettleExternalAttempt(ctx context.Context, settlement runtimeeffects.Settlement) error {
	if err := s.Store.SettleExternalAttempt(ctx, settlement); err != nil {
		return err
	}
	return runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationSettlement,
		runtimeeffects.Attempt{OperationID: settlement.OperationID, AttemptID: settlement.AttemptID}, s.settlementErr)
}

func TestProviderRegistrationAcknowledgedMutationCleanupKeepsOneDispatch(t *testing.T) {
	tool := packfixture.ConnectorTool(t, "telegram", "telegram.apply_webhook").Tool
	input := map[string]any{"callback_url": "https://hooks.example.test/webhooks/support/telegram?swarm_callback_generation=current"}
	credentials := map[string]any{"telegram_bot_token": "bot-secret", "webhook_signing_secret": "signing-secret"}
	for _, phase := range []runtimeeffects.MutationPhase{runtimeeffects.MutationLaunch, runtimeeffects.MutationObservation} {
		t.Run(string(phase), func(t *testing.T) {
			harness := effecttest.New()
			cleanup := errors.New("injected acknowledged cleanup failure")
			store := &postCommitRegistrationStore{Store: harness}
			if phase == runtimeeffects.MutationLaunch {
				store.launchErr = cleanup
			} else {
				store.observationErr = cleanup
			}
			ctx := serveRegistrationTestContext(harness, "registration-"+string(phase))
			ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(store).WithExecutionPosture(executionposture.Live))
			calls := 0
			executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if err := harness.RequireState("provider_registration", runtimeeffects.StateLaunched); err != nil {
					t.Fatal(err)
				}
				return registrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
			})}}
			result, err := executor.Apply(ctx, "telegram.apply_webhook", tool, input, credentials, map[string]string{"binding_id": "hitl", "intent_id": uuid.NewString()})
			if result.Pending == nil || !errors.Is(err, cleanup) || !registrationPhaseDiagnostic(err, phase, result.Pending.Attempt()) || !result.Acknowledged || result.Output == nil {
				t.Fatalf("acknowledged %s result = %+v, %v", phase, result, err)
			}
			if calls != 1 || store.observationCalls != 1 {
				t.Fatalf("HTTP calls=%d observation calls=%d, want 1/1", calls, store.observationCalls)
			}
			if err := harness.RequireState("provider_registration", runtimeeffects.StateResponseObserved); err != nil {
				t.Fatal(err)
			}
			if err := result.Pending.SettleReadback(context.Background(), true, nil); err != nil {
				t.Fatalf("settle exact readback: %v", err)
			}
			if calls != 1 || store.observationCalls != 1 {
				t.Fatalf("readback duplicated effect: HTTP=%d observation=%d", calls, store.observationCalls)
			}
		})
	}
}

func TestChannelConfirmationAcknowledgedMutationCleanupKeepsOneDispatch(t *testing.T) {
	tool := packfixture.ConnectorTool(t, "telegram", "telegram.send_interactive").Tool
	input := map[string]any{"chat_id": "42", "text": "Swarm channel connected.", "reply_markup": map[string]any{"inline_keyboard": []any{}}}
	credentials := map[string]any{"telegram_bot_token": "bot-secret"}
	for _, phase := range []runtimeeffects.MutationPhase{runtimeeffects.MutationLaunch, runtimeeffects.MutationObservation} {
		t.Run(string(phase), func(t *testing.T) {
			harness := &channelConfirmationHarness{Harness: effecttest.New()}
			cleanup := errors.New("injected acknowledged cleanup failure")
			store := &postCommitRegistrationStore{Store: harness}
			if phase == runtimeeffects.MutationLaunch {
				store.launchErr = cleanup
			} else {
				store.observationErr = cleanup
			}
			operationID := uuid.NewString()
			ctx := channelConfirmationTestContext(harness, operationID)
			ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(store).WithExecutionPosture(executionposture.Live))
			calls := 0
			executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return registrationResponse(http.StatusOK, `{"ok":true,"result":{"message_id":1}}`), nil
			})}}
			result, err := executor.DeliverChannelConfirmation(ctx, "telegram.send_interactive", tool, input, credentials, nil)
			attempt := singleRegistrationAttempt(t, harness.Harness)
			if !errors.Is(err, cleanup) || !registrationPhaseDiagnostic(err, phase, attempt) || result.OperationID != operationID || result.Output == nil {
				t.Fatalf("acknowledged %s result = %+v, %v", phase, result, err)
			}
			if calls != 1 || store.observationCalls != 1 {
				t.Fatalf("HTTP calls=%d observation calls=%d, want 1/1", calls, store.observationCalls)
			}
			if err := harness.RequireState("channel_confirmation", runtimeeffects.StateSettled); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func singleRegistrationAttempt(t *testing.T, harness *effecttest.Harness) runtimeeffects.Attempt {
	t.Helper()
	if len(harness.Attempts) != 1 {
		t.Fatalf("effect attempts=%d, want one", len(harness.Attempts))
	}
	for _, attempt := range harness.Attempts {
		return attempt
	}
	return runtimeeffects.Attempt{}
}

func registrationPhaseDiagnostic(err error, phase runtimeeffects.MutationPhase, attempt runtimeeffects.Attempt) bool {
	var committed *runtimeeffects.PostCommitMutationError
	return errors.As(err, &committed) && committed.Phase == phase &&
		committed.OperationID == attempt.OperationID && committed.AttemptID == attempt.AttemptID
}

func TestRegistrationAcknowledgedLaunchGuardPreventsDispatch(t *testing.T) {
	for _, kind := range []string{"provider_registration", "channel_confirmation"} {
		for _, reason := range []string{"canceled", "stale_authority"} {
			t.Run(kind+"/"+reason, func(t *testing.T) {
				harness := effecttest.New()
				cleanup := errors.New("injected launch cleanup failure")
				store := &postCommitRegistrationStore{Store: harness, launchErr: cleanup, staleAfterLaunch: reason == "stale_authority"}
				var ctx context.Context
				var toolID string
				var input, credentials map[string]any
				if kind == "provider_registration" {
					ctx = serveRegistrationTestContext(harness, "blocked-"+reason)
					toolID = "telegram.apply_webhook"
					input = map[string]any{"callback_url": "https://hooks.example.test/webhooks/support/telegram?swarm_callback_generation=current"}
					credentials = map[string]any{"telegram_bot_token": "bot-secret", "webhook_signing_secret": "signing-secret"}
				} else {
					channelHarness := &channelConfirmationHarness{Harness: harness}
					store.Store = channelHarness
					ctx = channelConfirmationTestContext(channelHarness, uuid.NewString())
					toolID = "telegram.send_interactive"
					input = map[string]any{"chat_id": "42", "text": "Swarm channel connected.", "reply_markup": map[string]any{"inline_keyboard": []any{}}}
					credentials = map[string]any{"telegram_bot_token": "bot-secret"}
				}
				if reason == "canceled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					store.cancel = cancel
				}
				ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(store).WithExecutionPosture(executionposture.Live))
				calls := 0
				executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
					calls++
					return registrationResponse(http.StatusOK, `{}`), nil
				})}}
				tool := packfixture.ConnectorTool(t, "telegram", toolID).Tool
				var err error
				if kind == "provider_registration" {
					result, applyErr := executor.Apply(ctx, toolID, tool, input, credentials, map[string]string{"binding_id": "hitl", "intent_id": uuid.NewString()})
					if result.Pending != nil || result.Acknowledged || result.Output != nil {
						t.Fatalf("no-dispatch result=%+v", result)
					}
					err = applyErr
				} else {
					result, deliveryErr := executor.DeliverChannelConfirmation(ctx, toolID, tool, input, credentials, nil)
					if result.OperationID == "" || result.Output != nil {
						t.Fatalf("no-dispatch result=%+v", result)
					}
					err = deliveryErr
				}
				if calls != 0 || !registrationPhaseDiagnostic(err, runtimeeffects.MutationLaunch, singleRegistrationAttempt(t, harness)) {
					t.Fatalf("guard = calls:%d err:%v", calls, err)
				}
				if err := harness.RequireState(kind, runtimeeffects.StateTerminalFailure); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestProviderRegistrationReadbackAcknowledgedCleanupLeavesNoPendingAttempt(t *testing.T) {
	for _, phase := range []runtimeeffects.MutationPhase{runtimeeffects.MutationObservation, runtimeeffects.MutationSettlement} {
		t.Run(string(phase), func(t *testing.T) {
			harness := effecttest.New()
			cleanup := errors.New("injected readback cleanup failure")
			store := &postCommitRegistrationStore{Store: harness}
			if phase == runtimeeffects.MutationObservation {
				store.observationErr = cleanup
			} else {
				store.settlementErr = cleanup
			}
			ctx := serveRegistrationTestContext(harness, "readback-"+string(phase))
			ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(store).WithExecutionPosture(executionposture.Live))
			handle, err := runtimeeffects.BeginServeRegistration(ctx, []byte("registration request"), map[string]string{"binding_id": "hitl", "intent_id": uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatal(err)
			}
			pending := &PendingApply{handle: handle}
			if err := pending.SettleReadback(ctx, true, nil); err != nil {
				t.Fatalf("acknowledged readback settlement: %v", err)
			}
			if !pending.responseObserved || store.observationCalls != 1 {
				t.Fatalf("readback observation = observed:%t calls:%d", pending.responseObserved, store.observationCalls)
			}
			if err := harness.RequireState("provider_registration", runtimeeffects.StateSettled); err != nil {
				t.Fatal(err)
			}
		})
	}
}
