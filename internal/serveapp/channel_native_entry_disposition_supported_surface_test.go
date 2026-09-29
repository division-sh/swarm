package serveapp

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelNativeEntryDispositionsPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, kind := range []string{"private", "group"} {
			t.Run(string(backend)+"/"+kind, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				h.start(t)
				defer h.stop(t)
				chat := int64(9593)
				if kind == "group" {
					chat = -9593
				}
				begun := startChannelOnboardingRPC(t, h, channelonboarding.VerbConnect, "entry-token", map[string]string{"client_language": "fr"})
				callback, signing, _ := h.provider.Registration()
				requireChannelClaimDisposition(t, "entry claimant", submitChannelOnboardingClaimWithChatType(t,
					callback, signing, begun.IdentityOperation.Challenge, 825000, 8593, chat, kind, "entry_operator"), "consumed_by_binding")
				claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
				var confirmation map[string]any
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
					"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
				}, &confirmation)
				retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
				qualified := waitNativeQualification(t, h, begun.Operation.OperationID, channelnative.QualificationQualified, time.Time{})
				command, err := channelnative.EntryCommand(qualified.SettingID, qualified.SettingGeneration)
				if err != nil {
					t.Fatal(err)
				}
				send := func(id, user int64, text string, expectedStatus ...int) {
					t.Helper()
					callback, signing, _ := h.provider.Registration()
					status := http.StatusAccepted
					if len(expectedStatus) != 0 {
						status = expectedStatus[0]
					}
					if names := postChannelTelegramUpdateStatus(t, callback, signing, map[string]any{
						"update_id": id, "message": map[string]any{
							"message_id": id, "from": map[string]any{"id": user},
							"chat": map[string]any{"id": chat, "type": kind}, "text": text,
						},
					}, status); len(names) != 0 {
						t.Fatalf("operator entry leaked business events: %v", names)
					}
				}
				driver := "sqlite"
				if backend == servedparity.BackendExplicitPostgres {
					driver = "postgres"
				}
				db, err := sql.Open(driver, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				// Every occurrence is admitted by verified ingress. SQL below is observation only.
				for index := int64(0); index < 201; index++ {
					send(825100+index, 8593, fmt.Sprintf("/unknown_%03d", index))
				}
				send(825401, 8593, "/"+command+"@ForeignBot")
				send(825402, 9000, "/"+command+"@SwarmTestBot")
				rejectedCount := 203
				if kind == "group" {
					send(825403, 8593, "/"+command)
					rejectedCount++
				}
				waitNativeIntentCount(t, db, "entry_rejected", rejectedCount)
				send(825100, 8593, "/unknown_000", http.StatusOK)
				waitNativeIntentCount(t, db, "entry_rejected", rejectedCount)
				var forbiddenPlans int
				if err := db.QueryRow(`SELECT COUNT(*) FROM channel_delivery_plans p JOIN operator_channel_text_intents i
					ON p.source_id=i.publication_id WHERE i.disposition='entry_rejected'`).Scan(&forbiddenPlans); err != nil || forbiddenPlans != 0 {
					t.Fatalf("rejection granted response authority: plans=%d err=%v", forbiddenPlans, err)
				}
				scope := map[string]any{"type": "chat", "chat_id": fmt.Sprint(chat)}
				if kind == "group" {
					scope["type"], scope["user_id"] = "chat_member", "8593"
				}
				if err := h.provider.SeedCommands("entry-token", scope, "fr", []map[string]any{{"command": "foreign", "description": "Foreign owner"}}); err != nil {
					t.Fatal(err)
				}
				waitNativeQualification(t, h, begun.Operation.OperationID, channelnative.QualificationInvalid, qualified.ObservedAt)
				text := "/" + command
				if kind == "group" {
					text += "@SwarmTestBot"
				}
				send(825500, 8593, text)
				waitNativeIntentCount(t, db, "", 1)
				h.stop(t)
				h.start(t)
				waitNativeQualification(t, h, begun.Operation.OperationID, channelnative.QualificationInvalid, time.Time{})
				waitNativeIntentCount(t, db, "", 1)
				waitNativeIntentCount(t, db, "entry", 0)
				cut := time.Now().UTC()
				if err := h.provider.SeedCommands("entry-token", scope, "fr", nil); err != nil {
					t.Fatal(err)
				}
				waitNativeQualification(t, h, begun.Operation.OperationID, channelnative.QualificationQualified, cut)
				waitNativeIntentCount(t, db, "entry", 1)
				waitNativeInboxDeliveries(t, h, fmt.Sprint(chat), 1)
				send(825501, 8593, text)
				waitNativeIntentCount(t, db, "entry", 2)
				waitNativeInboxDeliveries(t, h, fmt.Sprint(chat), 2)
				h.stop(t)
				h.start(t)
				waitNativeQualification(t, h, begun.Operation.OperationID, channelnative.QualificationQualified, time.Time{})
				waitNativeIntentCount(t, db, "entry_rejected", rejectedCount)
				waitNativeIntentCount(t, db, "entry", 2)
				send(825502, 8593, text)
				waitNativeIntentCount(t, db, "entry", 3)
				waitNativeInboxDeliveries(t, h, fmt.Sprint(chat), 3)
			})
		}
	}
}

func waitNativeInboxDeliveries(t *testing.T, h *channelOnboardingE2EHarness, conversation string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		count := 0
		for index := 0; ; index++ {
			message := h.provider.Delivery(index)
			if message == nil {
				break
			}
			text, _ := message["text"].(string)
			if strings.HasPrefix(text, "Inbox\n") {
				if fmt.Sprint(message["chat_id"]) != conversation {
					t.Fatalf("native inbox used another conversation: %v", message)
				}
				count++
			}
		}
		if count == want {
			return
		}
		if count > want || time.Now().After(deadline) {
			t.Fatalf("native inbox deliveries=%d want=%d", count, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitNativeIntentCount(t *testing.T, db *sql.DB, disposition string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	query := `SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='settled' AND disposition='` + disposition + `'`
	if disposition == "" {
		query = `SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='pending'`
	}
	for {
		var count int
		if err := db.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == want {
			return
		}
		if count > want || time.Now().After(deadline) {
			t.Fatalf("native intent disposition=%q count=%d want=%d", disposition, count, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
