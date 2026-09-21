package cataloge2e

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRecordEvidenceReplacementUsesTypedStateAppendBothStores(t *testing.T) {
	fixture := catalogRuntimeFixture(t, "catalog.runtime.primitives", "test-record-evidence")
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			transcript := buildCatalogExecutionTranscript(t, fixture)
			h := newRuntimeHarnessFromTranscript(t, fixture.Root, backend, true, transcript)
			h.seedEntityFields(transcript.expected)
			first := transcript.groups[0].steps[0]
			assertFindings := func(want []any) {
				t.Helper()
				h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
				state, found, err := h.workflow.Load(h.ctx, catalogRootWorkflowRoute())
				if err != nil || !found || !reflect.DeepEqual(state.Fields["findings"], want) {
					t.Fatalf("findings=%#v want=%#v found=%v error=%v", state.Fields["findings"], want, found, err)
				}
			}
			want := []any{map[string]any{"finding": "missing-field", "severity": "high"}}
			h.publishAndWait(first, catalogRuntimePublishTimeout)
			assertFindings(want)
			h.publishAndWait(first, catalogRuntimePublishTimeout)
			assertFindings(want)
			h = h.reopenFromTranscript(transcript)
			h.publishAndWait(first, catalogRuntimePublishTimeout)
			assertFindings(want)
			second := first
			second.eventID, second.createdAt = uuid.NewString(), time.Now().UTC()
			second.Payload = map[string]any{"finding": "second-finding", "severity": "low"}
			h.publishAndWait(second, catalogRuntimePublishTimeout)
			want = append(want, map[string]any{"finding": "second-finding", "severity": "low"})
			assertFindings(want)
			lister, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			events, err := loadCatalogOperatorEvents(h.ctx, lister)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, event := range events {
				if event.EventName == "evidence.recorded" {
					count++
				}
			}
			if count != 2 {
				t.Fatalf("evidence publications=%d, want exactly two despite replay/restart", count)
			}
		})
	}
}
