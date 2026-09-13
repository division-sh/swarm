package pipeline

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func TestDecisionCardRequestBinding(t *testing.T) {
	const principal = "11111111-1111-4111-8111-111111111111"
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, test := range []struct {
		method  string
		request any
		wrap    func(any) DecisionCardMutation
	}{
		{"mailbox.decide", decisioncard.DecideRequest{CardID: "card", PrincipalID: principal, Fields: semanticvalue.EmptyObject(), Now: now}, func(r any) DecisionCardMutation { return NewDecisionCardDecision(r.(decisioncard.DecideRequest)) }},
		{"mailbox.defer", decisioncard.DeferRequest{CardID: "card", PrincipalID: principal, Now: now}, func(r any) DecisionCardMutation { return NewDecisionCardDeferral(r.(decisioncard.DeferRequest)) }},
		{"mailbox.begin_input", decisioncard.BeginInputRequest{CardID: "card", PrincipalID: principal, Now: now}, func(r any) DecisionCardMutation {
			return NewDecisionCardInputBegin(r.(decisioncard.BeginInputRequest), "observed")
		}},
		{"mailbox.cancel_input", decisioncard.CancelInputRequest{CardID: "card", PrincipalID: principal, Now: now}, func(r any) DecisionCardMutation {
			return NewDecisionCardInputCancellation(r.(decisioncard.CancelInputRequest))
		}},
	} {
		t.Run(test.method, func(t *testing.T) {
			mutation := test.wrap(test.request)
			req := apiidempotency.Request{Method: test.method, Actor: apiidempotency.PrincipalActor(principal), ResourceID: "card", RequestHash: "hash"}
			if err := mutation.ValidateRequest(req); err != nil {
				t.Fatal(err)
			}
			if !mutation.SameRequest(mutation) {
				t.Fatal("request differs from itself")
			}
			for name, change := range map[string]func(*apiidempotency.Request){
				"method":     func(r *apiidempotency.Request) { r.Method = "mailbox.acknowledge" },
				"actor_kind": func(r *apiidempotency.Request) { r.Actor = apiidempotency.BearerActor(principal) },
				"principal": func(r *apiidempotency.Request) {
					r.Actor = apiidempotency.PrincipalActor("22222222-2222-4222-8222-222222222222")
				},
				"resource":     func(r *apiidempotency.Request) { r.ResourceID = "other" },
				"missing_hash": func(r *apiidempotency.Request) { r.RequestHash = "" },
			} {
				t.Run(name, func(t *testing.T) {
					bad := req
					change(&bad)
					if mutation.ValidateRequest(bad) == nil {
						t.Fatal("contradictory request admitted")
					}
				})
			}
			// Enumerate the entire typed request so newly added semantic fields cannot
			// silently escape the acquired-command identity check.
			typ := reflect.TypeOf(test.request)
			for i := 0; i < typ.NumField(); i++ {
				t.Run(typ.Field(i).Name, func(t *testing.T) {
					changed := reflect.New(typ).Elem()
					changed.Set(reflect.ValueOf(test.request))
					field := changed.Field(i)
					switch field.Interface().(type) {
					case string:
						field.SetString(field.String() + "changed")
					case time.Time:
						field.Set(reflect.ValueOf(now.Add(time.Second)))
					case time.Duration:
						field.SetInt(int64(time.Minute))
					case semanticvalue.Value:
						value, err := semanticvalue.String("changed")
						if err != nil {
							t.Fatal(err)
						}
						field.Set(reflect.ValueOf(value))
					default:
						t.Fatalf("unclassified request field %s", typ.Field(i).Name)
					}
					wantSame := test.method == "mailbox.begin_input" && typ.Field(i).Name == "TTL"
					if got := mutation.SameRequest(test.wrap(changed.Interface())); got != wantSame {
						t.Fatalf("same=%t want=%t", got, wantSame)
					}
				})
			}
		})
	}
	begin := decisioncard.BeginInputRequest{CardID: "card", PrincipalID: principal}
	if NewDecisionCardInputBegin(begin, "first").SameRequest(NewDecisionCardInputBegin(begin, "other")) {
		t.Fatal("observed hash escaped binding")
	}
	if (DecisionCardMutation{}).SameRequest(DecisionCardMutation{}) {
		t.Fatal("unknown mutation admitted")
	}
}
