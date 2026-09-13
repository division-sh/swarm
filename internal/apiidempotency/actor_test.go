package apiidempotency

import "testing"

func TestReplayActorMethodBoundary(t *testing.T) {
	for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input", "mailbox.acknowledge"} {
		t.Run(method, func(t *testing.T) {
			if err := PrincipalActor("550e8400-e29b-41d4-a716-446655440000").ValidateMethod(method); err != nil {
				t.Fatal(err)
			}
			for _, actor := range []Actor{{}, BearerActor("real-token"), PrincipalActor("not-a-principal"), {Kind: "unknown", ID: "x"}} {
				if err := actor.ValidateMethod(method); err == nil {
					t.Fatalf("accepted actor %#v", actor)
				}
			}
		})
	}
	if err := BearerActor("token").ValidateMethod("event.publish"); err != nil {
		t.Fatal(err)
	}
	if err := PrincipalActor("550e8400-e29b-41d4-a716-446655440000").ValidateMethod("event.publish"); err == nil {
		t.Fatal("principal escaped finite mailbox scope")
	}
}
