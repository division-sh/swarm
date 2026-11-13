package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

// Invariant: both-store link-notice creation/replay is exact-audience fenced and leaves no unaddressed row.
func TestChannelLinkNoticeAdmissionAndReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openOperatorChannelContractFixture(t, backend)
			ctx, now := f.ctx, time.Now().UTC()
			mailbox := f.store.(runtimetools.MailboxPersistence)
			const text = "Swarm Notice: WhatsApp link down: connection reset"
			item := func(id string) runtimetools.MailboxItem {
				return runtimetools.MailboxItem{ID: id, Type: render.LinkNoticeType, Summary: text}
			}
			assertCount := func(want int) {
				t.Helper()
				if got, err := mailbox.CountMailboxItems(ctx, "pending"); err != nil || got != want {
					t.Fatal("mailbox creation count", got, want, err)
				}
			}
			if _, err := f.insertNotice(ctx, item(uuid.NewString())); err == nil {
				t.Fatal("generic caller bypassed the link audience guard")
			}
			assertCount(0)
			principal, err := f.store.EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			identity := operatorChannelContractIdentity("link-notice-" + backend)
			binding := operatorchannel.Binding{PrincipalID: principal.ID, Interface: identity, Revision: 1,
				Status: operatorchannel.BindingCurrent, ExternalAccountRef: "operator", ConversationRef: "room", ConversationScope: operatorchannel.ConversationScopeDirect}
			if _, err := f.insertNotice(render.WithLinkNoticeBinding(ctx, binding, true), item(uuid.NewString())); err == nil {
				t.Fatal("missing default created an unaddressed notice")
			}
			assertCount(0)
			confirm := func(kind operatorchannel.OperationKind, revision int64, conversation string) operatorchannel.Binding {
				t.Helper()
				id := uuid.NewString()
				op, err := f.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{OperationID: id, Kind: kind,
					PrincipalID: principal.ID, Interface: identity, ExpectedRevision: revision, RequestKeyHash: id, RequestHash: id,
					ProviderAuthority: operatorChannelProviderAuthority(), RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL)})
				if err != nil {
					t.Fatal(err)
				}
				claim, err := f.settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeDirect, "operator", conversation, uuid.NewString()), now.Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				_, b, err := f.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{OperationID: id, PrincipalID: principal.ID,
					ExpectedRevision: claim.Operation.Revision, Approve: true, ProviderAuthorityCurrent: true, ConfirmedAt: now.Add(2 * time.Second)})
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			binding = confirm(operatorchannel.OperationConnect, 0, "room")
			wrong := binding
			wrong.ConversationRef = "unrelated-room"
			if _, err := f.insertNotice(render.WithLinkNoticeBinding(ctx, wrong, true), item(uuid.NewString())); err == nil {
				t.Fatal("mismatched audience committed")
			}
			assertCount(0)
			request := item(uuid.NewString())
			create := render.WithLinkNoticeBinding(ctx, binding, true)
			if _, err := f.insertNotice(render.WithLinkNoticeBinding(ctx, binding, false), request); !errors.Is(err, render.ErrLinkNoticeAbsent) {
				t.Fatal("read-only reconciliation did not establish absence", err)
			}
			assertCount(0)
			if _, err := f.insertNotice(create, request); err != nil {
				t.Fatal(err)
			}
			// Lose the returned identity deliberately; replay must use the original request, not a new ID.
			if id, err := f.insertNotice(create, request); err != nil || id != request.ID {
				t.Fatal("exact creation replay refused", id, err)
			}
			assertCount(1)
			if n, err := f.countPlans(ctx, request.ID); err != nil || n != 1 {
				t.Fatal("exact replay added a delivery plan", n, err)
			}
			changed := request
			changed.Summary = "Swarm Notice: WhatsApp link needs attention: logged out"
			if _, err := f.insertNotice(create, changed); err == nil {
				t.Fatal("same identity accepted different immutable text")
			}
			next := confirm(operatorchannel.OperationRebind, binding.Revision, "new-room")
			if _, err := f.insertNotice(render.WithLinkNoticeBinding(ctx, binding, true), item(uuid.NewString())); err == nil {
				t.Fatal("stale destination created a new notice")
			}
			if id, err := f.insertNotice(render.WithLinkNoticeBinding(ctx, binding, false), request); err != nil || id != request.ID {
				t.Fatal("historical creation could not be reconciled without retargeting", err)
			}
			if _, err := f.insertNotice(render.WithLinkNoticeBinding(ctx, next, true), request); err == nil {
				t.Fatal("rebind redirected the old notice identity")
			}
			if n, err := f.countPlans(ctx, request.ID); err != nil || n != 1 {
				t.Fatal("rebind duplicated an old source", n, err)
			}
			assertCount(1)
			cancelled, cancel := context.WithCancel(render.WithLinkNoticeBinding(ctx, next, true))
			cancel()
			if _, err := f.insertNotice(cancelled, item(uuid.NewString())); err == nil {
				t.Fatal("cancelled creation admitted")
			}
			assertCount(1)
		})
	}
}
