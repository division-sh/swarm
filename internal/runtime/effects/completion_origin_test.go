package effects

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestNormalCompletionOriginAdmission(t *testing.T) {
	runID := uuid.NewString()
	directive := agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()}
	claim := func(run, agent string, class deliverylifecycle.SubscriberClass) deliverylifecycle.Claim {
		value, err := deliverylifecycle.AdmitPersistedClaim(uuid.NewString(), run, "route", uuid.NewString(), 1, class, agent)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	delivery := deliverylifecycle.WithClaim(context.Background(), claim(runID, "agent", deliverylifecycle.SubscriberAgent))
	for _, tc := range []struct {
		name string
		ctx  context.Context
		kind CompletionOriginKind
		code string
	}{
		{"delivery", delivery, CompletionOriginDelivery, ""},
		{"directive", WithDirectiveCompletionOrigin(context.Background(), directive), CompletionOriginDirective, ""},
		{"absent", context.Background(), "", "completion_origin_missing_or_ambiguous"},
		{"nil", nil, "", "completion_origin_missing_or_ambiguous"},
		{"malformed_directive", WithDirectiveCompletionOrigin(context.Background(), agentcontrol.DirectiveExecutionOrigin{}), "", "completion_origin_invalid"},
		{"dual", WithDirectiveCompletionOrigin(delivery, directive), "", "completion_origin_missing_or_ambiguous"},
		{"dual_malformed", WithDirectiveCompletionOrigin(delivery, agentcontrol.DirectiveExecutionOrigin{}), "", "completion_origin_missing_or_ambiguous"},
		{"foreign_agent", deliverylifecycle.WithClaim(context.Background(), claim(runID, "other", deliverylifecycle.SubscriberAgent)), "", "completion_origin_delivery_claim_mismatch"},
		{"foreign_run", deliverylifecycle.WithClaim(context.Background(), claim(uuid.NewString(), "agent", deliverylifecycle.SubscriberAgent)), "", "completion_origin_delivery_claim_mismatch"},
		{"node", deliverylifecycle.WithClaim(context.Background(), claim(runID, "agent", deliverylifecycle.SubscriberNode)), "", "completion_origin_delivery_claim_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin, err := AgentCompletionOriginFromContext(tc.ctx, "agent", runID, "test")
			if tc.code != "" {
				if err == nil || !strings.Contains(err.Error(), tc.code) {
					t.Fatalf("origin=%+v error=%v, want %s", origin, err, tc.code)
				}
				return
			}
			if err != nil || origin.Kind != tc.kind || origin.Validate() != nil {
				t.Fatalf("origin=%+v error=%v", origin, err)
			}
			if tc.kind == CompletionOriginDirective && !origin.Directive.Same(directive) {
				t.Fatal("changed directive authority")
			}
			if tc.kind == CompletionOriginDelivery {
				exact, _ := deliverylifecycle.ClaimFromContext(delivery)
				if !origin.Delivery.Same(exact) {
					t.Fatal("changed delivery fence")
				}
			}
		})
	}
}
