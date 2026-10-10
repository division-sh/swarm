package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeCatalogEntityDeadLetterRecipesPreserveRelationAndDiagnostics(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	shapes := map[string]string{
		"catalogHasDeadLetterRelation": `func catalogHasDeadLetterRelation(t testing.TB, selected catalogOperatorEventLister, since time.Time, entityID string) bool {
t.Helper()
count, err := storetest.CountDeadLetterEntityRelationsSince(testAuthorActivityContext(context.Background()), selected, since.UTC(), strings.TrimSpace(entityID))
if err != nil { t.Fatalf("query dead_letters: %v", err) }
return count > 0
}`,
		"assertDeadLetter": `func assertDeadLetter(t testing.TB, selected catalogOperatorEventLister, since time.Time, entityID string, want bool) {
t.Helper()
got := catalogHasDeadLetterRelation(t, selected, since, entityID)
if got != want {
rows, readErr := storetest.ReadDeadLetterObservationRows(testAuthorActivityContext(context.Background()), selected)
var evidence []string
for _, row := range rows {
evidence = append(evidence, fmt.Sprintf("event=%s stored_entity=%q payload_entity=%q node=%q", row.OriginalEventID, row.StoredEntityID, row.PayloadEntityID, row.HandlerNode))
}
t.Fatalf("dead_letter = %v, want %v for entity %q; evidence=%v read_error=%v", got, want, entityID, evidence, readErr)
}
}`,
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-entity-deadletter" {
			continue
		}
		count++
		if shape, exists := shapes[row.Function]; exists {
			want, err := canonicalFunction(shape)
			got, parseErr := canonicalFunction(row.After)
			if err != nil || parseErr != nil || got != want {
				t.Fatalf("%s changed relation or failure evidence: %v / %v", row.Function, err, parseErr)
			}
			for _, pair := range [][2]string{
				{"selected", "foreignStore"}, {"since.UTC()", "since.Add(time.Second)"},
				{"strings.TrimSpace(entityID)", "otherEntityID"}, {"count > 0", "count > 1"},
				{"if err != nil", "if false"}, {"got != want", "got == want"},
				{"row.StoredEntityID", "row.PayloadEntityID"}, {"evidence, readErr", "nil, nil"},
			} {
				changed := strings.Replace(row.After, pair[0], pair[1], 1)
				if changed == row.After {
					continue
				}
				got, err := canonicalFunction(changed)
				if err == nil && got == want {
					t.Fatalf("lost owner, relation or failure detail accepted: %v", pair)
				}
			}
			continue
		}
		if row.Function != "assertEntityDeadLetterOutcome" && row.Function != "TestCatalogDeadLetterRelation_DiagnosticAloneGetsNoCredit" {
			t.Fatal("unreviewed entity dead-letter propagation")
		}
		want, err := canonicalFunction(row.Before)
		got, parseErr := canonicalFunction(undoNativeCatalogReaderProjection(t, row))
		if err != nil || parseErr != nil || got != want {
			t.Fatalf("%s changed assertions beyond native read propagation: %v / %v", row.Function, err, parseErr)
		}
	}
	if count != 4 {
		t.Fatalf("entity dead-letter recipes=%d, want four", count)
	}
}

func TestNativeCatalogEntityDeadLetterOwnerPreservesExactPhysicalCuts(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "delivery", "dead_letter_owner.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"COALESCE(NULLIF(dl.original_payload->>'entity_id',''),COALESCE(dl.entity_id::text,''))=$1 AND dl.created_at >= $2",
		"COALESCE(NULLIF(json_extract(dl.original_payload,'$.entity_id'),''),COALESCE(dl.entity_id,''))=? AND dl.created_at >= ?",
		"FROM dead_letters dl ORDER BY dl.created_at,dl.dead_letter_id",
		"defer rows.Close()", "if err := rows.Err(); err != nil", "rows.Scan(&row.OriginalEventID, &row.StoredEntityID, &row.PayloadEntityID, &row.HandlerNode)",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("physical relation/diagnostic contract missing %q", fragment)
		}
	}
}
