package main

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nativeCatalogPublicationIdentityShape = `func (h *runtimeHarness) refreshPublishedEventEntityID(eventID string) {
h.t.Helper()
eventID = strings.TrimSpace(eventID)
if h == nil || (h.pg == nil && h.sqlite == nil) || eventID == "" { return }
var selected any = h.pg
if h.pg == nil { selected = h.sqlite }
event, found, err := storetest.ReadCanonicalEventRecord(h.ctx, selected, eventID)
if err != nil { h.t.Fatalf("query published event entity_id for %s: %v", eventID, err) }
if !found { return }
entityID := strings.TrimSpace(event.EntityID())
if entityID == "" { return }
h.mu.Lock()
h.eventEntityIDs[eventID] = entityID
h.mu.Unlock()
}`

func TestNativeCatalogPublicationIdentityRecipeRetainsExactCacheContract(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	expected, err := canonicalFunction(nativeCatalogPublicationIdentityShape)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-publication-identity" {
			continue
		}
		count++
		if row.File != "internal/runtime/cataloge2e/runtime_harness_test.go" || row.Function != "refreshPublishedEventEntityID" {
			t.Fatal("unreviewed catalog publication identity recipe")
		}
		actual, err := canonicalFunction(row.After)
		if err != nil || actual != expected {
			t.Fatalf("canonical header, missing result, cache or mutex contract changed: %v", err)
		}
		for _, pair := range [][2]string{
			{"ReadCanonicalEventRecord(h.ctx, selected, eventID)", "ReadCanonicalEventRecord(context.Background(), foreignStore, eventID)"},
			{"event.EntityID()", "event.ParentEventID()"},
			{"if !found", "if found"},
			{"h.mu.Lock()", ""},
			{"h.eventEntityIDs[eventID] = entityID", "h.eventEntityIDs[eventID] = payloadEntityID"},
		} {
			changed := strings.Replace(row.After, pair[0], pair[1], 1)
			if changed == row.After {
				t.Fatalf("negative control did not change source: %v", pair)
			}
			actual, err := canonicalFunction(changed)
			if err == nil && actual == expected {
				t.Fatalf("lost native read, header authority or cache preservation accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("publication identity recipe count=%d, want one shared consumer", count)
	}
}

func TestNativeOptionalCanonicalEventBridgeConsumesExistingOwner(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "storetest", "event.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "ReadCanonicalEventRecord")
	if err != nil {
		t.Fatal(err)
	}
	want := `func ReadCanonicalEventRecord(ctx context.Context, selectedStore any, eventID string) (events.Event, bool, error) {
return private.ReadCanonicalEventRecordForTest(ctx, selectedStore, eventID)
}`
	if formattedNativeReadNode(fn) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
		t.Fatal("optional canonical observation gained another read or outcome interpreter")
	}
}
