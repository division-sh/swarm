package channeldelivery

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

func TestCardActionDemandValidationPrecedesStoreAccess(t *testing.T) {
	base := render.CardActionDemand{CardID: uuid.NewString(), PrincipalID: uuid.NewString(),
		Method: "mailbox.decide", Verdict: "accept", ReceiptOperationID: uuid.NewString(), RenderHash: "hash"}
	for _, test := range []struct {
		name  string
		edit  func(*render.CardActionDemand)
		valid bool
	}{
		{"decide", func(*render.CardActionDemand) {}, true},
		{"bad_card", func(d *render.CardActionDemand) { d.CardID = "bad" }, false},
		{"bad_principal", func(d *render.CardActionDemand) { d.PrincipalID = "bad" }, false},
		{"unknown_method", func(d *render.CardActionDemand) { d.Method = "mailbox.other" }, false},
		{"decide_no_receipt", func(d *render.CardActionDemand) { d.ReceiptOperationID = "" }, false},
		{"decide_no_verdict", func(d *render.CardActionDemand) { d.Verdict = "" }, false},
		{"decide_no_hash", func(d *render.CardActionDemand) { d.RenderHash = "" }, false},
		{"begin_no_hash", func(d *render.CardActionDemand) { d.Method = "mailbox.begin_input"; d.RenderHash = "" }, true},
		{"begin_no_receipt", func(d *render.CardActionDemand) { d.Method = "mailbox.begin_input"; d.ReceiptOperationID = "" }, false},
		{"begin_no_verdict", func(d *render.CardActionDemand) { d.Method = "mailbox.begin_input"; d.Verdict = "" }, false},
		{"cancel_draft_only", func(d *render.CardActionDemand) {
			d.Method = "mailbox.cancel_input"
			d.DraftID = uuid.NewString()
			d.ReceiptOperationID = ""
			d.Verdict = ""
			d.RenderHash = ""
		}, true},
		{"cancel_no_draft", func(d *render.CardActionDemand) { d.Method = "mailbox.cancel_input" }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			demand := base
			test.edit(&demand)
			for _, postgres := range []bool{false, true} {
				for _, lock := range []bool{false, true} {
					// A valid demand reaches the unavailable store. An invalid demand
					// must retain its earlier, exact refusal on either path/dialect.
					err := RequireCardActionTx(context.Background(), nil, operatorchannel.ActionFact{}, demand, postgres, lock)
					if err == nil {
						t.Fatal("nil store was accepted")
					}
					incomplete := strings.Contains(err.Error(), "channel card action demand is incomplete")
					if incomplete == test.valid {
						t.Fatalf("valid=%t postgres=%t lock=%t: %v", test.valid, postgres, lock, err)
					}
				}
			}
		})
	}
}
