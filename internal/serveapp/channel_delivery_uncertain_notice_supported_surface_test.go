package serveapp

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil/telegramapi"
	"github.com/google/uuid"
)

func onlyPublicChannelNoticeID(t *testing.T, endpoint, excluded string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var listed map[string]any
		requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"status": "pending"}, &listed)
		items, _ := listed["items"].([]any)
		var ids []string
		for _, item := range items {
			entry, _ := item.(map[string]any)
			if entry["kind"] != "notice" {
				continue
			}
			notice, _ := entry["notice"].(map[string]any)
			id, _ := notice["mailbox_id"].(string)
			if uuid.Validate(id) != nil {
				t.Fatalf("public notice lacks its exact source identity: %v", entry)
			}
			if id != excluded {
				ids = append(ids, id)
			}
		}
		if len(ids) == 1 {
			return ids[0]
		}
		if len(ids) > 1 || time.Now().After(deadline) {
			t.Fatalf("expected one new authored notice, excluding %q: %v", excluded, listed)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func channelUncertaintySummaryEntry(t *testing.T, provider *telegramapi.Double) (int, string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		for index := 0; provider.Delivery(index) != nil; index++ {
			message := provider.Delivery(index)
			if !strings.Contains(fmt.Sprint(message["text"]), "waiting in your inbox") {
				continue
			}
			if token, found := telegramCallbackToken(message, "Open inbox"); found {
				return index + 1, token
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("actual preconnection notice did not produce a summary entry")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func proveChannelUncertainNoticeReadback(t *testing.T, provider *telegramapi.Double, backend, dsn, endpoint, seedID string, restart func()) {
	t.Helper()
	id := onlyPublicChannelNoticeID(t, endpoint, seedID)
	db, err := sql.Open(backend, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	summaryMessage, summaryToken := channelUncertaintySummaryEntry(t, provider)
	waitChannelUncertainNoticeFact(t, db, id, false, false)
	updateID := 905000
	readback := func(outcome string, resendable bool) {
		t.Helper()
		for _, entry := range []string{"summary", "native"} {
			updateID++
			assertChannelUncertainNoticeEntry(t, provider, id, outcome, resendable, entry, summaryMessage, summaryToken, updateID)
		}
	}
	readback("Not acknowledged", true)
	if got := len(channelPhysicalNoticeMessages(provider)); got != 1 {
		t.Fatalf("opening inbox granted fresh-copy consent: %d physical notices", got)
	}
	callback, signing, _ := provider.Registration()
	proveChannelManualResendAfterLostNotice(t, provider, callback, signing)
	waitChannelUncertainNoticeFact(t, db, id, false, true)
	readback("Not acknowledged", false)
	restart()
	readback("Not acknowledged", false)
	physical := channelPhysicalNoticeMessages(provider)
	if len(physical) != 2 {
		t.Fatalf("restart replayed the uncertain notice: %v", physical)
	}
	fresh := physical[1]
	acknowledge, found := telegramCallbackToken(provider.Delivery(fresh-1), "Acknowledge")
	if !found {
		t.Fatal("explicit fresh notice lacks its real acknowledgment control")
	}
	callback, signing, _ = provider.Registration()
	updateID++
	postChannelTelegramUpdate(t, callback, signing, map[string]any{
		"update_id": updateID,
		"callback_query": map[string]any{
			"id": fmt.Sprintf("uncertain-notice-%d", updateID), "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": fresh, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    acknowledge,
		},
	})
	waitChannelUncertainNoticeFact(t, db, id, true, true)
	readback("Acknowledged", false)
	restart()
	readback("Acknowledged", false)
	waitChannelUncertainNoticeFact(t, db, id, true, true)
	if got := len(channelPhysicalNoticeMessages(provider)); got != 2 {
		t.Fatalf("completion/restart replayed or revived the notice: %d physical copies", got)
	}
}

func channelPhysicalNoticeMessages(provider *telegramapi.Double) []int {
	var messages []int
	for index := 0; provider.Delivery(index) != nil; index++ {
		if strings.HasPrefix(fmt.Sprint(provider.Delivery(index)["text"]), "Notice: Observed ordinary business text") {
			messages = append(messages, index+1)
		}
	}
	return messages
}

func waitChannelUncertainNoticeFact(t *testing.T, db *sql.DB, id string, acknowledged, hasFresh bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var notified bool
		var uncertain, sent, total int
		if err := db.QueryRow(`SELECT COALESCE(notified,false) FROM mailbox WHERE item_id=$1`, id).Scan(&notified); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*),
			SUM(CASE WHEN state='uncertain' THEN 1 ELSE 0 END),
			SUM(CASE WHEN state='sent' THEN 1 ELSE 0 END)
			FROM channel_delivery_plans WHERE source_kind='notice' AND source_id=$1`, id).Scan(&total, &uncertain, &sent); err != nil {
			t.Fatal(err)
		}
		expectedTotal, expectedSent := 1, 0
		if hasFresh {
			expectedTotal, expectedSent = 2, 1
		}
		if notified == acknowledged && uncertain == 1 && sent == expectedSent && total == expectedTotal {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("canonical notice/effect facts did not settle: notified=%t, plans=%d uncertain=%d sent=%d", notified, total, uncertain, sent)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertChannelUncertainNoticeEntry(t *testing.T, provider *telegramapi.Double, id, outcome string, resendable bool, entry string, summaryMessage int, summaryToken string, updateID int) {
	t.Helper()
	before := 0
	for provider.Delivery(before) != nil {
		before++
	}
	callback, signing, _ := provider.Registration()
	update := map[string]any{"update_id": updateID}
	if entry == "summary" {
		update["callback_query"] = map[string]any{
			"id": fmt.Sprintf("uncertain-summary-%d", updateID), "from": map[string]any{"id": 7000},
			"message": map[string]any{"message_id": summaryMessage, "chat": map[string]any{"id": 1001, "type": "private"}},
			"data":    summaryToken,
		}
	} else {
		command := waitNativeInboxCommand(t, provider, "chat", "1001", "")
		update["message"] = map[string]any{
			"message_id": updateID, "from": map[string]any{"id": 7000},
			"chat": map[string]any{"id": 1001, "type": "private"}, "text": "/" + command,
		}
	}
	postChannelTelegramUpdate(t, callback, signing, update)
	deadline := time.Now().Add(15 * time.Second)
	for {
		for index := before; provider.Delivery(index) != nil; index++ {
			message := provider.Delivery(index)
			text := fmt.Sprint(message["text"])
			if !strings.Contains(text, "Older copies may be outdated. Current state:") {
				continue
			}
			if !strings.Contains(text, "notice ["+id[:8]+"] "+outcome) {
				t.Fatalf("%s entry lost the exact canonical notice outcome: %s", entry, text)
			}
			if !resendable {
				if telegramHasActionPrefix(message, "Resend notice") {
					t.Fatalf("%s entry made a linked/completed notice resendable", entry)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s entry omitted retained notice uncertainty after %q", entry, outcome)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func telegramHasActionPrefix(message map[string]any, label string) bool {
	markup, _ := message["reply_markup"].(map[string]any)
	rows, _ := markup["inline_keyboard"].([]any)
	for _, raw := range rows {
		row, _ := raw.([]any)
		for _, item := range row {
			button, _ := item.(map[string]any)
			text, _ := button["text"].(string)
			if strings.HasPrefix(text, label) {
				return true
			}
		}
	}
	return false
}

func TestChannelDeliveryResendControlAssertions(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		want       bool
	}{
		{"exact", "Resend notice", true},
		{"numbered", "Resend notice 01", true},
		{"other_source", "Resend card 01", false},
		{"unrelated", "Open inbox", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := map[string]any{"reply_markup": map[string]any{"inline_keyboard": []any{
				[]any{map[string]any{"text": tc.text, "callback_data": "token"}},
			}}}
			if got := telegramHasActionPrefix(message, "Resend notice"); got != tc.want {
				t.Fatalf("resend control %q matched=%t, want %t", tc.text, got, tc.want)
			}
		})
	}
	if telegramHasActionPrefix(nil, "Resend notice") {
		t.Fatal("absent markup was mistaken for a resend control")
	}
}
