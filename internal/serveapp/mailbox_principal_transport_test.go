package serveapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

func TestMailboxPrincipalConcurrentTransportBothStores(t *testing.T) {
	const secondToken = "mailbox-principal-second-credential"
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner, _ := newRetainedMailboxCompletionRuntime(t, backend, canonicalrouting.CopyMailboxNoticeCompletion(t), apiv1.DefaultLoopbackAPIToken, secondToken)
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect, "notice"} {
				methods := []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"}
				if kind == "notice" {
					methods = []string{"mailbox.acknowledge"}
				}
				for _, method := range methods {
					t.Run(string(kind)+"/"+method, func(t *testing.T) {
						f := mailboxCompletionFixtureInRuntime(t, rt, owner)
						params := mailboxPrincipalMutationParams(t, f, kind, method)
						key := fmt.Sprint(params["idempotency_key"])
						before := mailboxCompletionRunEffects(t, rt, f.base.RunID)
						for _, token := range []string{"", "not-an-admitted-token"} {
							for _, transport := range []string{"http", "ws"} {
								_, status, err := mailboxTransportRequest(context.Background(), rt.Endpoint, transport, token, method, params)
								if status != http.StatusUnauthorized {
									t.Fatalf("%s accepted invalid credential: status=%d err=%v", transport, status, err)
								}
							}
						}
						for _, token := range []string{apiv1.DefaultLoopbackAPIToken, secondToken} {
							response, status, err := mailboxTransportRequest(context.Background(), rt.Endpoint, "ws", token, method, params)
							if err != nil || status != http.StatusOK || response.Error == nil || response.Error.Code != -32601 {
								t.Fatalf("HTTP-only mutation admitted through WebSocket: status=%d err=%v rpc=%+v", status, err, response.Error)
							}
						}
						if after := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(before, after) {
							t.Fatalf("authentication/transport refusal changed run facts: %s", mailboxEffectsDifference(before, after))
						}
						type result struct {
							response servedJSONRPCEnvelope
							status   int
							err      error
						}
						results := make(chan result, 4)
						start := make(chan struct{})
						for i := range 4 {
							go func() {
								<-start
								token := apiv1.DefaultLoopbackAPIToken
								if i%2 != 0 {
									token = secondToken
								}
								response, status, err := mailboxTransportRequest(context.Background(), rt.Endpoint, "http", token, method, params)
								results <- result{response, status, err}
							}()
						}
						close(start)
						fresh := 0
						var original map[string]any
						collected := make([]result, 0, 4)
						for range 4 {
							collected = append(collected, <-results)
						}
						for _, got := range collected {
							if got.err != nil || got.status != http.StatusOK || got.response.Error != nil {
								t.Errorf("concurrent request failed: status=%d transport=%v rpc=%+v", got.status, got.err, got.response.Error)
							}
						}
						if t.Failed() {
							t.FailNow()
						}
						for _, got := range collected {
							var response map[string]any
							if err := json.Unmarshal(got.response.Result, &response); err != nil {
								t.Fatal(err)
							}
							if replayed, ok := response["idempotency_replayed"].(bool); !ok {
								t.Fatal("response omitted replay marker")
							} else if !replayed {
								fresh++
							}
							delete(response, "idempotency_replayed")
							if original == nil {
								original = response
							} else if !reflect.DeepEqual(original, response) {
								t.Fatalf("concurrent responses differ: %v / %v", original, response)
							}
						}
						if fresh != 1 {
							t.Fatalf("fresh executions=%d, want one", fresh)
						}
						waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, f.base.RunID)
						var principal, actor, actorKind, hash, resource, raw string
						if err := rt.DB.QueryRow(`SELECT principal_id FROM operator_principals`).Scan(&principal); err != nil {
							t.Fatal(err)
						}
						if err := rt.DB.QueryRow(`SELECT actor_kind,actor_id,request_hash,resource_id,CAST(response AS TEXT) FROM api_idempotency WHERE idempotency_key=$1`, key).Scan(&actorKind, &actor, &hash, &resource, &raw); err != nil {
							t.Fatal(err)
						}
						var stored map[string]any
						if err := json.Unmarshal([]byte(raw), &stored); err != nil {
							t.Fatal(err)
						}
						if actorKind != "operator_principal" || actor != principal || !reflect.DeepEqual(stored, original) {
							t.Fatalf("credential split completion: %s/%s %s", actorKind, actor, raw)
						}
						var count int
						if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, key).Scan(&count); err != nil || count != 1 {
							t.Fatalf("completion count=%d, %v", count, err)
						}
						req := apiidempotency.Request{Method: method, Actor: apiidempotency.PrincipalActor(principal), ResourceID: resource, RequestHash: hash, IdempotencyKey: key, Now: time.Now().UTC(), TTL: 24 * time.Hour}
						domain := mailboxCompletionRunEffects(t, rt, f.base.RunID)
						for _, change := range []string{"body", "resource"} {
							bad := req
							if change == "body" {
								bad.RequestHash = "different-semantic-body"
							} else {
								bad.ResourceID = uuid.NewString()
							}
							if _, _, err := mailboxTokenlessMutation(f.ctx, rt, owner, bad, params); err == nil {
								t.Fatalf("replay accepted changed %s identity", change)
							}
						}
						for _, invalid := range []apiidempotency.Actor{apiidempotency.PrincipalActor(""), apiidempotency.PrincipalActor(uuid.NewString()), apiidempotency.BearerActor(principal)} {
							bad := req
							bad.Actor = invalid
							if _, _, err := mailboxTokenlessMutation(f.ctx, rt, owner, bad, params); err == nil {
								t.Fatalf("invalid owner replay admitted: %+v", invalid)
							}
						}
						observed, replayed, err := mailboxTokenlessMutation(f.ctx, rt, owner, req, params)
						if err != nil || !replayed {
							t.Fatalf("tokenless principal replay: %t %v", replayed, err)
						}
						var tokenless map[string]any
						if err := json.Unmarshal(observed, &tokenless); err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(stored, tokenless) {
							t.Fatalf("tokenless response differs: %s", observed)
						}
						if after := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(domain, after) {
							t.Fatalf("principal refusal/replay changed committed domain: %s", mailboxEffectsDifference(domain, after))
						}
						t.Run("tokenless_first", func(t *testing.T) {
							fresh := mailboxCompletionFixtureInRuntime(t, rt, owner)
							params := mailboxPrincipalMutationParams(t, fresh, kind, method)
							req := mailboxPrincipalRequest(t, rt, method, params)
							response, replayed, err := mailboxTokenlessMutation(fresh.ctx, rt, owner, req, params)
							if err != nil || replayed {
								t.Fatalf("tokenless first execution: replay=%t err=%v", replayed, err)
							}
							var original map[string]any
							if err := json.Unmarshal(response, &original); err != nil {
								t.Fatal(err)
							}
							waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fresh.base.RunID)
							before := mailboxCompletionRunEffects(t, rt, fresh.base.RunID)
							for _, token := range []string{apiv1.DefaultLoopbackAPIToken, secondToken} {
								got, status, err := mailboxTransportRequest(fresh.ctx, rt.Endpoint, "http", token, method, params)
								if err != nil || status != http.StatusOK || got.Error != nil {
									t.Fatalf("tokenless-to-HTTP replay: status=%d err=%v rpc=%+v", status, err, got.Error)
								}
								var value map[string]any
								if err := json.Unmarshal(got.Result, &value); err != nil {
									t.Fatal(err)
								}
								if value["idempotency_replayed"] != true {
									t.Fatalf("HTTP re-executed tokenless completion: %s", got.Result)
								}
								delete(value, "idempotency_replayed")
								if !reflect.DeepEqual(original, value) {
									t.Fatalf("tokenless-to-HTTP response changed: %s / %s", response, got.Result)
								}
							}
							if after := mailboxCompletionRunEffects(t, rt, fresh.base.RunID); !reflect.DeepEqual(before, after) {
								t.Fatal("HTTP replay changed tokenless domain mutation")
							}
						})
					})
				}
			}
		})
	}
}

func mailboxPrincipalRequest(t *testing.T, rt servedControlProofRuntime, method string, params map[string]any) apiidempotency.Request {
	t.Helper()
	var principal string
	if err := rt.DB.QueryRow(`SELECT principal_id FROM operator_principals`).Scan(&principal); err != nil {
		t.Fatal(err)
	}
	// Compute the documented semantic request hash independently of HTTP admission.
	raw, err := json.Marshal(map[string]any{"method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = canonicaljson.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	resource, _ := params["card_id"].(string)
	if method == "mailbox.acknowledge" {
		resource, _ = params["mailbox_id"].(string)
	}
	key, _ := params["idempotency_key"].(string)
	return apiidempotency.Request{Method: method, Actor: apiidempotency.PrincipalActor(principal), ResourceID: resource, RequestHash: fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), IdempotencyKey: key, Now: time.Now().UTC(), TTL: 24 * time.Hour}
}

func TestMailboxUnkeyedMutationsRemainPrincipalOwnedBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input", "mailbox.acknowledge"} {
				t.Run(method, func(t *testing.T) {
					f := mailboxCompletionFixtureInRuntime(t, rt, owner)
					kind := decisioncard.AnchorKindStageGate
					if method == "mailbox.acknowledge" {
						kind = "notice"
					}
					params := mailboxPrincipalMutationParams(t, f, kind, method)
					delete(params, "idempotency_key")
					req := mailboxPrincipalRequest(t, rt, method, params)
					before := mailboxCompletionRunEffects(t, rt, f.base.RunID)
					bad := req
					bad.Actor = apiidempotency.PrincipalActor(uuid.NewString())
					if _, _, err := mailboxTokenlessMutation(f.ctx, rt, owner, bad, params); err == nil {
						t.Fatal("unkeyed request bypassed principal admission")
					}
					if after := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(before, after) {
						t.Fatalf("unkeyed principal refusal mutated domain: %s", mailboxEffectsDifference(before, after))
					}
					var result map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, method, params, &result)
					if result["idempotency_replayed"] != false || result["ok"] != true {
						t.Fatalf("unkeyed execution failed: %v", result)
					}
					var completions int
					if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE resource_id=$1 AND method=$2`, req.ResourceID, method).Scan(&completions); err != nil || completions != 0 {
						t.Fatalf("unkeyed request stored a replay response: count=%d err=%v", completions, err)
					}
				})
			}
		})
	}
}

func mailboxPrincipalMutationParams(t *testing.T, f cursorMailboxFixture, kind decisioncard.AnchorKind, method string) map[string]any {
	t.Helper()
	if kind == "notice" {
		return map[string]any{"mailbox_id": mailboxCompletionNotice(t, f), "idempotency_key": uuid.NewString()}
	}
	card := mailboxCompletionAnchorCard(t, f, kind)
	params := map[string]any{"card_id": card.CardID, "idempotency_key": uuid.NewString()}
	verdict := "reject"
	if kind == decisioncard.AnchorKindProposedEffect {
		verdict = "revise"
	}
	switch method {
	case "mailbox.decide":
		params["verdict"], params["observed_content_hash"] = "approve", card.CardContentHash
	case "mailbox.defer":
		params["until"] = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	case "mailbox.begin_input":
		params["verdict"], params["observed_content_hash"] = verdict, card.CardContentHash
	case "mailbox.cancel_input":
		var draft map[string]any
		requireServedJSONRPCResult(t, f.rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": card.CardID, "verdict": verdict, "observed_content_hash": card.CardContentHash, "idempotency_key": uuid.NewString()}, &draft)
		params["input_draft_id"] = draft["input_draft_id"]
	default:
		t.Fatalf("unsupported mailbox method %s", method)
	}
	return params
}

func mailboxTokenlessMutation(ctx context.Context, rt servedControlProofRuntime, owner cursorMailboxStore, req apiidempotency.Request, params map[string]any) (json.RawMessage, bool, error) {
	if req.Method == "mailbox.acknowledge" {
		port, ok := owner.(interface {
			AcknowledgeMailboxNotice(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error)
		})
		if !ok {
			return nil, false, fmt.Errorf("selected store has no notice completion owner")
		}
		completion, replay, err := port.AcknowledgeMailboxNotice(ctx, req)
		return completion.Response, replay, err
	}
	mutation, err := mailboxCardMutation(req, params)
	if err != nil {
		return nil, false, err
	}
	return rt.Runtime.Pipeline.CommitDecisionCardMutation(ctx, req, mutation)
}

func mailboxCardMutation(req apiidempotency.Request, params map[string]any) (runtimepipeline.DecisionCardMutation, error) {
	var mutation runtimepipeline.DecisionCardMutation
	text := func(key string) string { value, _ := params[key].(string); return value }
	switch req.Method {
	case "mailbox.decide":
		mutation = runtimepipeline.NewDecisionCardDecision(decisioncard.DecideRequest{CardID: req.ResourceID, PrincipalID: req.Actor.ID, Verdict: text("verdict"), ObservedContentHash: text("observed_content_hash"), Fields: semanticvalue.EmptyObject(), DecisionEventID: uuid.NewString(), Now: req.Now})
	case "mailbox.defer":
		until, err := time.Parse(time.RFC3339Nano, text("until"))
		if err != nil {
			return mutation, err
		}
		mutation = runtimepipeline.NewDecisionCardDeferral(decisioncard.DeferRequest{CardID: req.ResourceID, PrincipalID: req.Actor.ID, Until: until, Now: req.Now})
	case "mailbox.begin_input":
		mutation = runtimepipeline.NewDecisionCardInputBegin(decisioncard.BeginInputRequest{CardID: req.ResourceID, PrincipalID: req.Actor.ID, Verdict: text("verdict"), Now: req.Now}, text("observed_content_hash"))
	case "mailbox.cancel_input":
		mutation = runtimepipeline.NewDecisionCardInputCancellation(decisioncard.CancelInputRequest{CardID: req.ResourceID, PrincipalID: req.Actor.ID, InputDraftID: text("input_draft_id"), Now: req.Now})
	default:
		return mutation, fmt.Errorf("unknown mailbox method %s", req.Method)
	}
	return mutation, nil
}

func mailboxTransportRequest(ctx context.Context, endpoint, transport, token, method string, params map[string]any) (servedJSONRPCEnvelope, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	header := http.Header{}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	request := map[string]any{"jsonrpc": "2.0", "id": "principal-proof", "method": method, "params": params}
	var result servedJSONRPCEnvelope
	if transport == "ws" {
		url := "ws" + strings.TrimPrefix(strings.TrimSuffix(endpoint, "/v1/rpc"), "http") + "/v1/ws"
		conn, response, err := websocket.DefaultDialer.DialContext(ctx, url, header)
		if err != nil {
			status := 0
			if response != nil {
				status = response.StatusCode
				_ = response.Body.Close()
			}
			return result, status, err
		}
		defer conn.Close()
		deadline, _ := ctx.Deadline()
		_ = conn.SetWriteDeadline(deadline)
		_ = conn.SetReadDeadline(deadline)
		if err := conn.WriteJSON(request); err != nil {
			return result, 0, err
		}
		err = conn.ReadJSON(&result)
		return result, http.StatusOK, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return result, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return result, 0, err
	}
	req.Header = header
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return result, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, response.StatusCode, nil
	}
	err = json.NewDecoder(response.Body).Decode(&result)
	return result, response.StatusCode, err
}
