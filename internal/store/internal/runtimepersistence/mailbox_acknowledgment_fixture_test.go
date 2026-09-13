package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type noticeAcknowledgmentFixtureOwner interface {
	EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
	AcknowledgeMailboxNotice(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error)
}

func noticeAcknowledgmentFixture(t testing.TB, selected noticeAcknowledgmentFixtureOwner, id string) func(context.Context) error {
	t.Helper()
	ctx := testAuthorActivityContext()
	requireDefaultSourceArtifactForTest(t, ctx, selected)
	principal, err := selected.EnsureOperatorPrincipal(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	request := apiidempotency.Request{Method: "mailbox.acknowledge", Actor: apiidempotency.PrincipalActor(principal.ID), ResourceID: id, RequestHash: "notice-fixture:" + id, Now: time.Now().UTC()}
	return func(ctx context.Context) error {
		_, _, err := selected.AcknowledgeMailboxNotice(ctx, request)
		return err
	}
}
