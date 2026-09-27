package telegramapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
