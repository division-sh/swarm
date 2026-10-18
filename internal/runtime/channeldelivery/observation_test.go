package channeldelivery

import "testing"

func TestIntentObservationAdmissionHasOneExactSelector(t *testing.T) {
	for _, kind := range []IntentKind{IntentText, IntentAction} {
		if err := (IntentObservationQuery{Kind: kind, Provider: "whatsapp", ProviderEventID: "canonical-capture", InterfaceKey: "exact-interface"}).Validate(); err != nil {
			t.Fatal("existing exact publication selector was rejected", kind, err)
		}
	}
	exact := IntentObservationQuery{Kind: IntentText, Provider: "whatsapp", MessageReference: "SDK_MESSAGE",
		ConversationReference: "operator@s.whatsapp.net", InterfaceKey: "exact-interface"}
	if err := exact.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*IntentObservationQuery){
		"provider":             func(q *IntentObservationQuery) { q.Provider = "" },
		"interface":            func(q *IntentObservationQuery) { q.InterfaceKey = "" },
		"unknown kind":         func(q *IntentObservationQuery) { q.Kind = "invented" },
		"dual selector":        func(q *IntentObservationQuery) { q.ProviderEventID = "canonical-capture" },
		"missing conversation": func(q *IntentObservationQuery) { q.ConversationReference = "" },
		"missing message":      func(q *IntentObservationQuery) { q.MessageReference = "" },
		"action message":       func(q *IntentObservationQuery) { q.Kind = IntentAction },
	} {
		t.Run(name, func(t *testing.T) {
			bad := exact
			mutate(&bad)
			if err := bad.Validate(); err == nil {
				t.Fatal("ambiguous observation selector was admitted")
			}
		})
	}
}
