package agentpersistence

import (
	"context"

	"testing"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func TestComposedDirectiveOriginRefusesInactiveAttempt(t *testing.T) {
	ctx := context.Background()
	attempt := &mutationprotocol.Attempt{}
	origin := runtimeagentcontrol.DirectiveExecutionOrigin{}
	for _, test := range []struct {
		name string
		call func() error
	}{
		{"postgres/renew", func() error {
			return (&AgentPostgresOwner{}).RenewProviderDirectiveOriginTx(ctx, attempt, origin, time.Now(), time.Minute)
		}},
		{"sqlite/renew", func() error {
			return (&AgentSQLiteOwner{}).RenewProviderDirectiveOriginTx(ctx, attempt, origin, time.Now(), time.Minute)
		}},
		{"postgres/settle", func() error {
			return (&AgentPostgresOwner{}).SettleProviderDirectiveOriginTx(ctx, attempt, origin, runtimeagentcontrol.DirectiveOperationFailed, runtimefailures.Envelope{}, time.Now())
		}},
		{"sqlite/settle", func() error {
			return (&AgentSQLiteOwner{}).SettleProviderDirectiveOriginTx(ctx, attempt, origin, runtimeagentcontrol.DirectiveOperationFailed, runtimefailures.Envelope{}, time.Now())
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("inactive mutation attempt was accepted")
			}
		})
	}
}
