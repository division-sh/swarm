package cliapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type claimDeadlineClient struct {
	expected time.Time
	calls    int
}

func (c *claimDeadlineClient) call(ctx context.Context, method string, _ map[string]any, _ any) error {
	c.calls++
	deadline, ok := ctx.Deadline()
	if method != "channel.onboarding_get" || !ok || !deadline.Equal(c.expected) {
		return fmt.Errorf("claim RPC deadline %s, present=%t; expected stored/earlier deadline %s", deadline, ok, c.expected)
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestChannelClaimWaitUsesStoredExpiry(t *testing.T) {
	for _, earlierCancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("earlier_cancel=%t", earlierCancel), func(t *testing.T) {
			expires := time.Now().Add(50 * time.Millisecond)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &claimDeadlineClient{expected: expires}
			if earlierCancel {
				client.expected = time.Now().Add(10 * time.Millisecond)
				ctx, cancel = context.WithDeadline(ctx, client.expected)
				defer cancel()
			}
			begun := channelOnboardingResult{
				Operation:         channelonboarding.Operation{Provider: "whatsapp"},
				IdentityOperation: &operatorchannel.Operation{State: operatorchannel.StateAwaitingClaim, ExpiresAt: expires},
			}
			_, err := waitForChannelOnboardingClaim(ctx, client, begun, defaultRootCommandOptions())
			if client.calls != 1 || err == nil {
				t.Fatal("pending RPC did not stop at its only deadline", client.calls, err)
			}
			if earlierCancel {
				if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "stopped waiting") {
					t.Fatal("earlier caller cancellation was not preserved", err)
				}
			} else if !strings.Contains(err.Error(), expires.Local().Format(time.RFC3339Nano)) {
				t.Fatal("wait did not report its original stored expiry", err)
			}
		})
	}
}

func TestChannelClaimWaitRefusesAbsentOrExpiredDeadline(t *testing.T) {
	for _, expires := range []time.Time{{}, time.Now().Add(-time.Second)} {
		client := &claimDeadlineClient{}
		begun := channelOnboardingResult{
			Operation:         channelonboarding.Operation{Provider: "whatsapp"},
			IdentityOperation: &operatorchannel.Operation{State: operatorchannel.StateAwaitingClaim, ExpiresAt: expires},
		}
		if _, err := waitForChannelOnboardingClaim(t.Context(), client, begun, defaultRootCommandOptions()); err == nil || client.calls != 0 {
			t.Fatal("missing/expired deadline performed another claim read", expires, client.calls, err)
		}
	}
}

func TestWhatsAppCLIClaimCompletesWithoutSecondAnswer(t *testing.T) {
	var methods []string
	server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, _ int) map[string]any {
		methods = append(methods, req.Method)
		switch req.Method {
		case "channel.onboarding_get":
			result := sessionProgressCLIResult("whatsapp", "", "awaiting_operator_confirmation", false)
			identity := operatorChannelCLIOperationResult("awaiting_confirmation", 2, 0)
			identity["expires_at"] = time.Now().Add(time.Minute)
			result["identity_operation"] = identity
			return result
		case "channel.confirm":
			if req.Params["approve"] != true || req.Params["expected_revision"] != float64(2) || req.Params["operation_id"] != operatorChannelCLIOperation {
				t.Fatal("automatic claim completion lost exact confirmation", req.Params)
			}
			return map[string]any{"operation": operatorChannelCLIOperationResult("bound", 3, 1), "binding": map[string]any{"revision": 1}}
		case "channel.onboarding_retry":
			return sessionProgressCLIResult("whatsapp", "", "succeeded", true)
		default:
			t.Fatal("unexpected claim operation", req.Method)
			return nil
		}
	})
	input := &channelCredentialInput{}
	out, diagnostic, code := runChannelProgressCLI(t, server, input, "channel", "resume", operatorChannelCLIOperation)
	if code != 0 || diagnostic != "" || input.reads != 0 || !strings.Contains(out, "WhatsApp: READY") ||
		strings.Join(methods, ",") != "channel.onboarding_get,channel.confirm,channel.onboarding_retry" {
		t.Fatal("authenticated WhatsApp claim required another answer", code, out, diagnostic, input.reads, methods)
	}
}
