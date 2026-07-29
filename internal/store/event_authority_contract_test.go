package store

import (
	"testing"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestStandaloneRuntimePlatformConvergenceRequiresExactDurableCreationTrigger(t *testing.T) {
	origin := semanticEventRunOriginForTest(
		t,
		"22222222-2222-4222-8222-222222222222",
		"platform.paused",
	)
	base := standaloneRuntimePlatformRunRecord{
		RunID: "11111111-1111-4111-8111-111111111111", RunStatus: "running",
		Origin:  origin,
		EventID: "22222222-2222-4222-8222-222222222222", EventClass: "runtime_control",
		EventType: "platform.paused", ProducedBy: "runtime", ProducedByType: "platform",
	}
	if !isStandaloneRuntimePlatformRunRecord(base) {
		t.Fatal("exact runtime platform record was not recognized")
	}
	for _, mutate := range []func(*standaloneRuntimePlatformRunRecord){
		func(record *standaloneRuntimePlatformRunRecord) { record.EventClass = "root_ingress" },
		func(record *standaloneRuntimePlatformRunRecord) { record.ProducedByType = "external" },
		func(record *standaloneRuntimePlatformRunRecord) { record.ProducedBy = "other" },
		func(record *standaloneRuntimePlatformRunRecord) {
			record.SourceEventID = "33333333-3333-4333-8333-333333333333"
		},
		func(record *standaloneRuntimePlatformRunRecord) {
			record.Origin = semanticEventRunOriginForTest(
				t,
				"33333333-3333-4333-8333-333333333333",
				"platform.paused",
			)
		},
		func(record *standaloneRuntimePlatformRunRecord) {
			record.Origin = semanticEventRunOriginForTest(
				t,
				"22222222-2222-4222-8222-222222222222",
				"platform.other",
			)
		},
		func(record *standaloneRuntimePlatformRunRecord) {
			record.Origin = runtimerunlifecycle.ScenarioSetupRunOrigin()
		},
	} {
		hostile := base
		mutate(&hostile)
		if isStandaloneRuntimePlatformRunRecord(hostile) {
			t.Fatalf("hostile standalone record acquired convergence authority: %#v", hostile)
		}
	}
}
