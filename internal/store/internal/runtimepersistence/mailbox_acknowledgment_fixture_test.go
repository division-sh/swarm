package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

type noticeAcknowledgmentFixtureOwner interface {
	EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
	AcknowledgeMailboxNotice(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error)
}

func noticeAcknowledgmentFixture(t testing.TB, ctx context.Context, selected noticeAcknowledgmentFixtureOwner, id string) func(context.Context) error {
	t.Helper()
	ctx = originalStoreFixtureContext(t, ctx, selected)
	requireDefaultSourceArtifactForTest(t, ctx, selected)
	principal, err := selected.EnsureOperatorPrincipal(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	request := apiidempotency.Request{Method: "mailbox.acknowledge", Actor: apiidempotency.PrincipalActor(principal.ID), ResourceID: id, RequestHash: "notice-fixture:" + id, Now: time.Now().UTC()}
	return func(caller context.Context) error {
		if !startupownership.HasMutationAuthority(caller) {
			caller = startupownership.CarryMutationAuthority(caller, ctx)
		}
		_, _, err := selected.AcknowledgeMailboxNotice(caller, request)
		return err
	}
}
