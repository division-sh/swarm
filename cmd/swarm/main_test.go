package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"swarm/internal/store"
)

func TestPrintTraceReport(t *testing.T) {
	var buf bytes.Buffer
	report := store.TraceReport{
		TraceID: "trace-123",
		Events: []store.TraceEvent{
			{
				EventID:       "evt-1",
				EventName:     "scan.requested",
				SourceEventID: "",
				ProducedBy:    "campaign-coordinator",
				CreatedAt:     time.Unix(1700000000, 0).UTC(),
			},
		},
		Deliveries: []store.TraceDelivery{
			{EventID: "evt-1", SubscriberType: "agent", SubscriberID: "worker-1", Status: "pending", ReasonCode: "matched_agent_subscription"},
		},
		Receipts: []store.TraceReceipt{
			{EventID: "evt-1", SubscriberType: "platform", SubscriberID: "pipeline", Outcome: "success", ReasonCode: "pipeline_persisted"},
		},
	}

	printTraceReport(&buf, report)
	out := buf.String()
	for _, want := range []string{
		"Trace trace-123",
		"Summary: pending delivery agent/worker-1 reason=matched_agent_subscription for scan.requested",
		"scan.requested",
		"delivery  agent/worker-1  status=pending reason=matched_agent_subscription",
		"receipt   platform/pipeline  outcome=success reason=pipeline_persisted",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("trace output missing %q:\n%s", want, out)
		}
	}
}
