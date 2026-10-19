package serveapp

import (
	"context"
	"testing"
	"time"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

func textReplyCardCopies(t *testing.T, reader render.Observer, cardID string) []render.Candidate {
	t.Helper()
	var result []render.Candidate
	cursor := ""
	for {
		page, err := reader.ListCurrentChannelDeliveryPlans(context.Background(), cursor, 200)
		if err != nil {
			t.Fatal(err)
		}
		for _, plan := range page {
			if plan.SourceKind == "card" && plan.SourceID == cardID {
				result = append(result, plan)
			}
		}
		if len(page) < 200 {
			return result
		}
		next := page[len(page)-1].DeliveryID
		if next <= cursor {
			t.Fatal("channel observation did not advance")
		}
		cursor = next
	}
}

func waitTextReplyCardCopies(t *testing.T, reader render.Observer, cardID string, count int) []render.Candidate {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		plans := textReplyCardCopies(t, reader, cardID)
		if len(plans) > count {
			t.Fatal("duplicate request created an extra card copy")
		}
		sent := 0
		for _, plan := range plans {
			if plan.State == "sent" && plan.CurrentReceiptID != "" {
				sent++
			}
		}
		if sent == count {
			return plans
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("card %s did not retain %d accepted copies", cardID, count)
	return nil
}

func waitChannelPublicCard(t *testing.T, h *channelOnboardingE2EHarness, runID string, kind decisioncard.AnchorKind, flowInstance string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var matches []string
		cursor := ""
		for {
			var page struct {
				Items []struct {
					Kind string                `json:"kind"`
					Card decisioncard.ListItem `json:"decision_card"`
				} `json:"items"`
				Next string `json:"next_cursor"`
			}
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.list", map[string]any{
				"run_id": runID, "anchor_kind": string(kind), "status": "pending", "cursor": cursor, "limit": 200}, &page)
			for _, item := range page.Items {
				if item.Kind == "decision_card" && item.Card.RunID == runID && item.Card.Scope.FlowInstance == flowInstance {
					matches = append(matches, item.Card.CardID)
				}
			}
			if page.Next == "" {
				break
			}
			if page.Next == cursor {
				t.Fatal("public mailbox cursor did not advance")
			}
			cursor = page.Next
		}
		if len(matches) > 1 {
			t.Fatal("public baseline has ambiguous pending cards")
		}
		if len(matches) == 1 {
			return matches[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("public baseline card did not appear")
	return ""
}
