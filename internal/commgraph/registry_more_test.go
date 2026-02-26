package commgraph

import "testing"

func TestRegistryExportHelpers_ReturnCopies(t *testing.T) {
	runtimeEvents := RuntimeEvents()
	humanEvents := HumanEvents()
	if len(runtimeEvents) == 0 || len(humanEvents) == 0 {
		t.Fatalf("expected non-empty event registries")
	}
	runtimeEvents[0] = "mutated.runtime"
	humanEvents[0] = "mutated.human"
	if RuntimeEvents()[0] == "mutated.runtime" {
		t.Fatal("RuntimeEvents should return a defensive copy")
	}
	if HumanEvents()[0] == "mutated.human" {
		t.Fatal("HumanEvents should return a defensive copy")
	}

	auth := MessageAuthorities()
	if len(auth) == 0 {
		t.Fatal("expected message authority registry entries")
	}
	origSender := auth[0].SenderRole
	auth[0].SenderRole = "mutated"
	if MessageAuthorities()[0].SenderRole != origSender {
		t.Fatal("MessageAuthorities should return a defensive copy")
	}

	roundTrips := MailboxRoundTrips()
	if len(roundTrips) == 0 {
		t.Fatal("expected mailbox round-trips")
	}
	origType := roundTrips[0].MailboxType
	roundTrips[0].MailboxType = "mutated"
	if MailboxRoundTrips()[0].MailboxType != origType {
		t.Fatal("MailboxRoundTrips should return a defensive copy")
	}
}
