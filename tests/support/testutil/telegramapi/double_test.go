package telegramapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestDoubleBotSettingsBelongToPhysicalResourceNotCredential(t *testing.T) {
	provider := &Double{}
	provider.SetResourceID("original", 9001)
	provider.SetResourceID("rotated", 9001)
	provider.SetResourceID("other-store", 9002)
	scope := map[string]any{"type": "chat", "chat_id": "42"}
	commands := []map[string]any{{"command": "inbox", "description": "Open inbox"}}
	if err := provider.SeedCommands("original", scope, "fr", commands); err != nil {
		t.Fatal(err)
	}
	if err := provider.SeedLauncher("original", "42", "web_app"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(provider)
	defer server.Close()
	for _, test := range []struct {
		credential string
		count      int
		launcher   string
	}{
		{"original", 1, "web_app"}, {"rotated", 1, "web_app"}, {"other-store", 0, "default"},
	} {
		t.Run(test.credential, func(t *testing.T) {
			read := func(method, body string, result any) {
				t.Helper()
				response, err := http.Post(server.URL+"/bot"+test.credential+"/"+method, "application/json", bytes.NewBufferString(body))
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if err := json.NewDecoder(response.Body).Decode(result); err != nil {
					t.Fatal(err)
				}
			}
			var commandResult struct {
				Result []map[string]any `json:"result"`
			}
			read("getMyCommands", `{"scope":{"type":"chat","chat_id":"42"},"language_code":"fr"}`, &commandResult)
			if len(commandResult.Result) != test.count {
				t.Fatalf("commands=%v want count=%d", commandResult.Result, test.count)
			}
			var launcherResult struct {
				Result struct {
					Type string `json:"type"`
				} `json:"result"`
			}
			read("getChatMenuButton", `{"chat_id":"42"}`, &launcherResult)
			if launcherResult.Result.Type != test.launcher {
				t.Fatalf("launcher=%q want=%q", launcherResult.Result.Type, test.launcher)
			}
		})
	}
}

func TestDoubleDelayedLostEditAppliesAfterSuccessor(t *testing.T) {
	provider := &Double{}
	server := httptest.NewServer(provider)
	defer server.Close()
	post := func(method, body string) error {
		response, err := http.Post(server.URL+"/botcredential/"+method, "application/json", bytes.NewBufferString(body))
		if err == nil {
			_ = response.Body.Close()
		}
		return err
	}
	if err := post("sendMessage", `{"chat_id":"42","text":"original"}`); err != nil {
		t.Fatal(err)
	}
	arrived, apply := provider.LoseNextEditAcknowledgmentBeforeApply()
	defer apply()
	if err := post("editMessageText", `{"chat_id":"42","message_id":1,"text":"old prompt"}`); err == nil {
		t.Fatal("delayed provider write retained its acknowledgment")
	}
	<-arrived
	if len(provider.Edits()) != 0 {
		t.Fatal("old provider write applied before explicit release")
	}
	if err := post("sendMessage", `{"chat_id":"42","text":"fresh copy"}`); err != nil {
		t.Fatal(err)
	}
	if err := post("editMessageText", `{"chat_id":"42","message_id":2,"text":"terminal successor"}`); err != nil {
		t.Fatal(err)
	}
	apply()
	deadline := time.Now().Add(time.Second)
	for len(provider.Edits()) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("provider did not apply its retained old write")
		}
		time.Sleep(time.Millisecond)
	}
	edits := provider.Edits()
	if edits[0]["text"] != "terminal successor" || edits[1]["text"] != "old prompt" {
		t.Fatalf("provider schedule did not apply the old write after successor progress: %v", edits)
	}
}

func TestDoubleRetainsEditAndCallbackEffectsAfterResponseLoss(t *testing.T) {
	provider := &Double{}
	server := httptest.NewServer(provider)
	defer server.Close()
	post := func(path, body string) (*http.Response, error) {
		return http.Post(server.URL+path, "application/json", bytes.NewBufferString(body))
	}
	response, err := post("/botcredential/sendMessage", `{"chat_id":"42","text":"first"}`)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("send: response=%v err=%v", response, err)
	}
	_ = response.Body.Close()
	provider.LoseNextEditAcknowledgment()
	response, err = post("/botcredential/editMessageText", `{"chat_id":"42","message_id":1,"text":"changed"}`)
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("accepted edit retained its response")
	}
	edits := provider.Edits()
	if len(edits) != 1 || edits[0]["text"] != "changed" {
		t.Fatalf("accepted edit effects = %#v", edits)
	}
	provider.LoseNextCallbackAcknowledgment()
	response, err = post("/botcredential/answerCallbackQuery", `{"callback_query_id":"tap-1"}`)
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("accepted callback acknowledgment retained its response")
	}
	acks := provider.Acknowledgments()
	if len(acks) != 1 || acks[0]["callback_query_id"] != "tap-1" {
		t.Fatalf("accepted callback effects = %#v", acks)
	}
	response, err = post("/botcredential/editMessageText", `{"chat_id":"42","message_id":1,"text":"final"}`)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		OK     bool `json:"ok"`
		Result struct {
			MessageID int `json:"message_id"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || !result.OK || result.Result.MessageID != 1 {
		t.Fatalf("edit response = %#v, err=%v", result, err)
	}
}

func TestDoubleRetainsDeliveryAfterResponseLoss(t *testing.T) {
	provider := &Double{}
	server := httptest.NewServer(provider)
	defer server.Close()
	provider.LoseNextDeliveryAcknowledgment()
	response, err := http.Post(server.URL+"/botcredential/sendMessage", "application/json", bytes.NewBufferString(`{"chat_id":"42","text":"first"}`))
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("accepted delivery retained its response")
	}
	if got := provider.Delivery(0); got == nil || got["text"] != "first" || provider.Delivery(1) != nil {
		t.Fatalf("accepted delivery effects = %#v", got)
	}
	if registrations, confirmations := provider.OnboardingCounts(); registrations != 0 || confirmations != 0 {
		t.Fatalf("unrelated delivery counted as onboarding: %d/%d", registrations, confirmations)
	}
	for _, text := range []string{"Swarm channel connected.", "Swarm channel connected. Future notices are visible to this group."} {
		body, err := json.Marshal(map[string]any{"chat_id": "42", "text": text})
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.Post(server.URL+"/botcredential/sendMessage", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	if registrations, deliveries := provider.Counts(); registrations != 0 || deliveries != 3 {
		t.Fatalf("all deliveries not retained: %d/%d", registrations, deliveries)
	}
	if registrations, confirmations := provider.OnboardingCounts(); registrations != 0 || confirmations != 2 {
		t.Fatalf("duplicate confirmations not counted: %d/%d", registrations, confirmations)
	}
}

func TestDoubleDeliveryBarrierSelectsExactlyOneMatchingMessage(t *testing.T) {
	provider := &Double{}
	arrived, release := provider.PauseDeliveryResponseMatching(func(payload map[string]any) bool {
		// Predicates may resolve persisted evidence without holding the double's lock.
		provider.Counts()
		return payload["text"] == "selected"
	})
	t.Cleanup(release)
	send := func(text string) <-chan *httptest.ResponseRecorder {
		response := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			body, err := json.Marshal(map[string]any{"chat_id": "42", "text": text})
			if err != nil {
				t.Error(err)
				return
			}
			writer := httptest.NewRecorder()
			provider.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/botcredential/sendMessage", bytes.NewReader(body)))
			response <- writer
		}()
		return response
	}
	wantResponse := func(response <-chan *httptest.ResponseRecorder, wantID int) {
		t.Helper()
		select {
		case writer := <-response:
			var result struct {
				OK     bool `json:"ok"`
				Result struct {
					MessageID int `json:"message_id"`
				} `json:"result"`
			}
			if err := json.Unmarshal(writer.Body.Bytes(), &result); err != nil || !result.OK || result.Result.MessageID != wantID {
				t.Fatalf("delivery response=%s err=%v want message=%d", writer.Body.String(), err, wantID)
			}
		case <-time.After(time.Second):
			t.Fatal("delivery response did not complete")
		}
	}
	wantResponse(send("unrelated before"), 1)
	select {
	case id := <-arrived:
		t.Fatalf("unmatched message %d consumed the barrier", id)
	default:
	}
	selected := send("selected")
	select {
	case id := <-arrived:
		if id != 2 {
			t.Fatalf("selected message=%d want=2", id)
		}
	case <-time.After(time.Second):
		t.Fatal("matching message did not reach the barrier")
	}
	wantResponse(send("unrelated after"), 3)
	wantResponse(send("selected"), 4)
	select {
	case <-selected:
		t.Fatal("another delivery released the selected response")
	default:
	}
	release()
	release()
	wantResponse(selected, 2)
	if _, count := provider.Counts(); count != 4 {
		t.Fatalf("recorded %d deliveries want=4", count)
	}
}

func TestDoubleNextDeliveryBarrierClaimsFirstAcceptedMessage(t *testing.T) {
	provider := &Double{}
	arrived, release := provider.PauseNextDeliveryResponse()
	var workers sync.WaitGroup
	t.Cleanup(func() { release(); workers.Wait() })
	const count = 16
	responses := make(chan *httptest.ResponseRecorder, count)
	start := make(chan struct{})
	for range count {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			writer := httptest.NewRecorder()
			provider.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/botcredential/sendMessage", bytes.NewBufferString(`{"chat_id":"42","text":"concurrent"}`)))
			responses <- writer
		}()
	}
	close(start)
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("first accepted delivery did not reach the barrier")
	}
	seen := map[int]bool{}
	readID := func() int {
		t.Helper()
		select {
		case response := <-responses:
			var result struct {
				Result struct {
					MessageID int `json:"message_id"`
				} `json:"result"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			id := result.Result.MessageID
			if id < 1 || id > count || seen[id] {
				t.Fatalf("invalid or duplicate delivery %d", id)
			}
			seen[id] = true
			return id
		case <-time.After(time.Second):
			t.Fatal("unpaused delivery did not finish")
			return 0
		}
	}
	for range count - 1 {
		if readID() == 1 {
			t.Fatal("first accepted delivery did not own the barrier")
		}
	}
	release()
	if id := readID(); id != 1 {
		t.Fatalf("released message=%d want=1", id)
	}
}
