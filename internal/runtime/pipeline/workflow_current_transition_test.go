package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func TestWorkflowCurrentTransitionProducerBoundAndBytes(t *testing.T) {
	at := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	record := lifecycleTransitionRecordFixtureForTest(t, "review", "queued", "active", "event-0000", at)
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		WorkflowName: "review", WorkflowVersion: "fixture", StorageRef: "review/one", InstanceID: "one",
		CurrentState: "active", InstanceKind: "template", TemplateVersion: "v1", Status: "active",
		ParentFlowID: "owner", ParentFlowInstance: "owner/root", ParentEntityID: flowidentity.EntityID("owner/root"),
	})
	var oneRecordBytes int
	for _, count := range []int{0, 1, 2, 20, 200} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			instance.TransitionHistory = nil
			for index := 0; index < count; index++ {
				next := record
				next.TriggerEventID = fmt.Sprintf("event-%04d", index)
				// Later admitted events may have an older event timestamp.
				next.FiredAt = at.Add(-time.Duration(index) * time.Second)
				instance.TransitionHistory = append(instance.TransitionHistory, next)
			}
			before := append([]WorkflowTransitionRecord(nil), instance.TransitionHistory...)
			projection, err := workflowInstancePersistedProjectionFromInstance(instance, instance.StorageRef)
			if err != nil {
				t.Fatal(err)
			}
			wantCount := 0
			if count > 0 {
				wantCount = 1
			}
			if len(projection.Control.TransitionHistory) != wantCount {
				t.Fatalf("persisted history length = %d, want %d", len(projection.Control.TransitionHistory), wantCount)
			}
			if count > 0 && !reflect.DeepEqual(projection.Control.TransitionHistory[0], before[count-1]) {
				t.Fatal("producer did not retain the complete last appended record")
			}
			if !reflect.DeepEqual(before, instance.TransitionHistory) {
				t.Fatal("producer mutated its transient input")
			}
			raw, err := json.Marshal(projection.ConfigPayload(instance.WorkflowVersion))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("transitions=%d live_header_bytes=%d", count, len(raw))
			if count == 1 {
				oneRecordBytes = len(raw)
			} else if count > 1 && len(raw) != oneRecordBytes {
				t.Fatalf("equal-sized latest evidence grew header: %d bytes, want %d", len(raw), oneRecordBytes)
			}
			route := flowidentity.StoredRoute("review", "one", "review/one")
			decoded, err := DecodeWorkflowInstanceRecordedHeader(route, raw)
			if err != nil {
				t.Fatal(err)
			}
			projected, err := decoded.Project(route, decoded.ParentRoute())
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if err := json.Unmarshal(projected, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("historical projection changed complete header controls")
			}
		})
	}
}

func TestWorkflowCurrentTransitionProducerValidatesDiscardedPrefix(t *testing.T) {
	at := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	valid := lifecycleTransitionRecordFixtureForTest(t, "review", "queued", "active", "accepted", at)
	foreign := lifecycleTransitionRecordFixtureForTest(t, "sibling", "queued", "active", "foreign", at)
	malformed := valid
	malformed.TransitionID = "unowned"
	for name, prefix := range map[string]WorkflowTransitionRecord{"foreign": foreign, "malformed": malformed} {
		t.Run(name, func(t *testing.T) {
			instance := materializedWorkflowInstanceForTest(WorkflowInstance{
				WorkflowName: "review", WorkflowVersion: "fixture", StorageRef: "review/one", InstanceID: "one",
				TransitionHistory: []WorkflowTransitionRecord{prefix, valid},
			})
			if _, err := workflowInstancePersistedProjectionFromInstance(instance, instance.StorageRef); err == nil {
				t.Fatal("invalid discarded prefix was silently admitted")
			}
		})
	}
}

func TestWorkflowCurrentTransitionDecoderRejectsCumulativeEvidence(t *testing.T) {
	record := lifecycleTransitionRecordFixtureForTest(t, "review", "queued", "active", "accepted", time.Now().UTC())
	for _, count := range []int{2, 20, 200} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			history := make([]WorkflowTransitionRecord, count)
			for index := range history {
				history[index] = record
			}
			config := map[string]any{
				"workflow_version": "fixture", "instance_id": "one", "flow_path": "review/one",
				"storage_ref": "review/one", "transition_history": history,
			}
			if _, err := workflowInstanceTransitionHistoryFromConfig(config); err == nil || !strings.Contains(err.Error(), "at most one") {
				t.Fatalf("cumulative evidence read error = %v", err)
			}
			raw, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeWorkflowInstanceRecordedHeader(flowidentity.StoredRoute("review", "one", "review/one"), raw); err == nil || !strings.Contains(err.Error(), "at most one") {
				t.Fatalf("historical cumulative evidence read error = %v", err)
			}
		})
	}
}
