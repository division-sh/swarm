package store

import "testing"

func TestStandaloneRuntimePlatformConvergenceRequiresExactClassAndProducerPair(t *testing.T) {
	base := standaloneRuntimePlatformRunRecord{
		RunID: "11111111-1111-4111-8111-111111111111", RunStatus: "running",
		EventID: "22222222-2222-4222-8222-222222222222", EventClass: "runtime_control",
		EventType: "platform.paused", ProducedBy: "runtime", ProducedByType: "platform",
		TriggerEventID: "22222222-2222-4222-8222-222222222222", TriggerEventType: "platform.paused",
	}
	if !isStandaloneRuntimePlatformRunRecord(base) {
		t.Fatal("exact runtime platform record was not recognized")
	}
	for _, mutate := range []func(*standaloneRuntimePlatformRunRecord){
		func(record *standaloneRuntimePlatformRunRecord) { record.EventClass = "root_ingress" },
		func(record *standaloneRuntimePlatformRunRecord) { record.ProducedByType = "external" },
		func(record *standaloneRuntimePlatformRunRecord) { record.ProducedBy = "other" },
	} {
		hostile := base
		mutate(&hostile)
		if isStandaloneRuntimePlatformRunRecord(hostile) {
			t.Fatalf("hostile standalone record acquired convergence authority: %#v", hostile)
		}
	}
}
