//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func TestWhatsAppClaimCodeNoticeSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			identity := beginRuntimeClaimFixture(t, f)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			miss := "15000"
			if identity.Challenge == miss {
				miss = "15001"
			}
			message := incomingMessageFixture()
			for _, text := range []string{"hello", "SWARM-AAAAAAAAAAAAAAAA", "123456", "12 34", "\u0661\u0665\u0660\u0660\u0660"} {
				message.Message.Conversation = proto.String(text)
				if claim, err := c.incoming.pendingOperatorClaim(f.ctx, f.operation, message, true); claim || err != nil || c.claimCodeNotice.Load() != nil {
					t.Fatal("ordinary text created a claim/notice", text, claim, err)
				}
			}
			message.Message.Conversation = proto.String(miss)
			if claim, err := c.incoming.pendingOperatorClaim(f.ctx, f.operation, message, true); claim || err != nil {
				t.Fatal("numeric miss changed claim routing", claim, err)
			}
			first, err := c.ReadClaimCodeNotice(f.ctx, f.operation)
			if err != nil || first == nil || first.IdentityOperationID != identity.OperationID || uuid.Validate(first.Observation) != nil {
				t.Fatal("missing exact transient notice", first, err)
			}
			if claim, err := c.incoming.pendingOperatorClaim(f.ctx, f.operation, message, true); claim || err != nil {
				t.Fatal("another miss became a claim", err)
			}
			second, err := c.ReadClaimCodeNotice(f.ctx, f.operation)
			if err != nil || second == nil || second.Observation == first.Observation {
				t.Fatal("snapshot did not replace/coalesce the observation", second, err)
			}
			foreign := f.operation
			foreign.PrincipalID = uuid.NewString()
			if notice, err := c.ReadClaimCodeNotice(f.ctx, foreign); err != nil || notice != nil {
				t.Fatal("notice crossed operation/principal", notice, err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, err := c.ReadClaimCodeNotice(cancelled, f.operation); err == nil {
				t.Fatal("cancelled read remained current")
			}
			message.Message.Conversation = proto.String(identity.Challenge)
			if claim, err := c.incoming.pendingOperatorClaim(f.ctx, f.operation, message, true); !claim || err != nil || c.claimCodeNotice.Load().notice.Observation != second.Observation {
				t.Fatal("exact code changed notice rather than claiming", claim, err)
			}
			sendRuntimeIncomingFixture(t, f, c, "NOTICE_VALID_CLAIM", identity.Challenge)
			waitRuntimeIncomingReceipt(t, f, c, "NOTICE_VALID_CLAIM")
			if notice, err := c.ReadClaimCodeNotice(f.ctx, f.operation); err != nil || notice != nil {
				t.Fatal("settled claim retained its notice", notice, err)
			}
			expired := *c.claimCodeNotice.Load()
			expired.expires = time.Now().Add(-time.Second)
			c.claimCodeNotice.Store(&expired)
			if notice, err := c.ReadClaimCodeNotice(f.ctx, f.operation); err != nil || notice != nil {
				t.Fatal("expired observation remained visible", notice, err)
			}
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			fresh := openRuntimeIncomingFixture(t, f)
			if notice, err := fresh.ReadClaimCodeNotice(f.ctx, f.operation); err != nil || notice != nil {
				t.Fatal("new occurrence inherited a notice", notice, err)
			}
		})
	}
}

func TestWhatsAppClaimCodeNoticeDisconnectKeepsErrorsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			identity := beginRuntimeClaimFixture(t, f)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			c.observeClaimCodeMiss(identity)
			c.state.currentOccurrence().fenceAfterDisconnect(true, "network unreachable")
			if notice, err := c.ReadClaimCodeNotice(f.ctx, f.operation); err != nil || notice != nil {
				t.Fatal("optional notice converted disconnect to read failure", notice, err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, err := c.ReadClaimCodeNotice(cancelled, f.operation); !errors.Is(err, context.Canceled) {
				t.Fatal("disconnect hid caller cancellation", err)
			}
			original := c.operations
			failure := errors.New("selected notice currentness read failed")
			c.operations = &nonClaimObservationFailureStore{Store: f.selected, failure: failure}
			_, err := c.ReadClaimCodeNotice(f.ctx, f.operation)
			c.operations = original
			if !errors.Is(err, failure) {
				t.Fatal("disconnect hid selected-store currentness failure", err)
			}
		})
	}
}
