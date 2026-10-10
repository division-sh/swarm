package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const nativeCatalogPublishedDeadLetterShape = `func assertPublishedEventDeadLetter(t testing.TB, selected catalogOperatorEventLister, publishedEventIDs map[string]struct{}, want bool) {
t.Helper()
count := 0
for eventID := range publishedEventIDs {
eventCount, err := storetest.CountDeadLettersForOriginalEvent(testAuthorActivityContext(context.Background()), selected, strings.TrimSpace(eventID))
if err != nil { t.Fatalf("query published-event dead letters for %s: %v", eventID, err) }
count += eventCount
}
if got := count > 0; got != want { t.Fatalf("published-event dead_letter = %v, want %v", got, want) }
}`

func TestNativeCatalogPublishedDeadLetterRecipePreservesExactSumAndRefusal(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	expected, err := canonicalFunction(nativeCatalogPublishedDeadLetterShape)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-published-deadletter" {
			continue
		}
		count++
		if row.File != "internal/runtime/cataloge2e/assertions_harness_test.go" || row.Function != "assertPublishedEventDeadLetter" {
			t.Fatal("unreviewed original-event relation recipe")
		}
		actual, err := canonicalFunction(row.After)
		if err != nil || actual != expected {
			t.Fatalf("original-event set, trim, count or error changed: %v", err)
		}
		for _, pair := range [][2]string{
			{"selected, strings.TrimSpace(eventID)", "foreignStore, otherEventID"},
			{"testAuthorActivityContext(context.Background())", "context.Background()"},
			{"count += eventCount", "count = eventCount"},
			{"count > 0", "count > 1"},
			{"if err != nil", "if false"},
		} {
			changed := strings.Replace(row.After, pair[0], pair[1], 1)
			actual, err := canonicalFunction(changed)
			if changed == row.After || (err == nil && actual == expected) {
				t.Fatalf("wrong owner/event, lost sum/refusal or weakened count accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("original-event relation recipes=%d, want one shared consumer", count)
	}
}
