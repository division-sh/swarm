package apiv1

import (
	"context"
	"encoding/json"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

type standingServiceControllerProbe struct {
	calls []string
}

func (p *standingServiceControllerProbe) result(operation runtimepipeline.StandingServiceOperation, transition, state string) runtimepipeline.StandingServiceReconciliation {
	p.calls = append(p.calls, transition)
	return runtimepipeline.StandingServiceReconciliation{
		ServiceID: operation.ServiceID, RunID: uuid.NewSHA1(uuid.MustParse(operation.ServiceID), []byte(transition)).String(),
		Generation: 2, EffectiveState: state, Transition: transition,
	}
}

func (p *standingServiceControllerProbe) SuspendStandingService(_ context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	return p.result(operation, "suspended", "suspended"), nil
}

func (p *standingServiceControllerProbe) ResumeStandingService(_ context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	return p.result(operation, "operator_resumed", "active"), nil
}

func (p *standingServiceControllerProbe) ResetStandingService(_ context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	return p.result(operation, "reset", "active"), nil
}

type directStandingIdempotencyStore struct{}

func (directStandingIdempotencyStore) WithAPIIdempotency(ctx context.Context, _ store.APIIdempotencyRequest, execute func(context.Context) (store.APIIdempotencyCompletion, error)) (store.APIIdempotencyCompletion, bool, error) {
	completion, err := execute(ctx)
	return completion, false, err
}

func TestOperatorStandingServiceHandlersUseCanonicalOwner(t *testing.T) {
	controller := &standingServiceControllerProbe{}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			StandingServices: controller,
			Idempotency:      directStandingIdempotencyStore{},
		}),
	})
	serviceID := uuid.NewString()
	for _, action := range []string{"suspend", "resume", "reset"} {
		body, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0", "id": action, "method": "standing." + action,
			"params": map[string]any{"service_id": serviceID, "reason": "test", "idempotency_key": "idem-" + action},
		})
		if err != nil {
			t.Fatal(err)
		}
		response := rpcCall(t, handler, string(body))
		if response.Error != nil {
			t.Fatalf("standing.%s error = %#v", action, response.Error)
		}
		result := asMap(t, response.Result)
		if result["service_id"] != serviceID || result["run_id"] == "" || result["generation"] != float64(2) {
			t.Fatalf("standing.%s result = %#v", action, result)
		}
	}
	if len(controller.calls) != 3 || controller.calls[0] != "suspended" || controller.calls[1] != "operator_resumed" || controller.calls[2] != "reset" {
		t.Fatalf("controller calls = %#v", controller.calls)
	}
}
