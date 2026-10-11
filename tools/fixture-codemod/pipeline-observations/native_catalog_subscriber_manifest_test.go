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

const nativeSubscriberManifestShape = `func assertAgentReceived(t testing.TB,h *runtimeHarness,since time.Time,want map[string][]string) {
t.Helper()
if len(want) == 0 { return }
for agentID,expectedEvents := range want {
agentID = strings.TrimSpace(agentID)
if agentID == "" { continue }
reader,err := h.catalogOperatorEventLister()
if err != nil { t.Fatalf("query agent_received for %s: %v",agentID,err) }
rows,err := storetest.ReadSubscriberEventNamesCreatedSince(testAuthorActivityContext(context.Background()),reader,since,agentID)
if err != nil { t.Fatalf("query agent_received for %s: %v",agentID,err) }
got := make([]string,0,len(expectedEvents))
for _,eventName := range rows { got = append(got,strings.TrimSpace(eventName)) }
if fmt.Sprintf("%q",got) != fmt.Sprintf("%q",expectedEvents) { t.Fatalf("agent_received[%s] = %v, want %v",agentID,got,expectedEvents) }
}
}`

func subscriberManifestConsumerPreserved(source string) bool {
	want, err := canonicalFunction(nativeSubscriberManifestShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogSubscriberManifestPreservesEmptyTrimOrderAndAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-subscriber-manifest-observation" {
			continue
		}
		count++
		if !subscriberManifestConsumerPreserved(row.After) {
			t.Fatal("subscriber manifest changed empty/blank behavior, order, trim, native owner or complete assertion")
		}
		for _, pair := range [][2]string{
			{"if len(want) == 0", "if len(want) <= 1"}, {"if agentID == \"\"", "if false"},
			{"strings.TrimSpace(agentID)", "agentID"}, {"strings.TrimSpace(eventName)", "eventName"},
			{"reader, since, agentID", "reader, since, otherSubscriber"},
			{"h.catalogOperatorEventLister()", "foreignHarness.catalogOperatorEventLister()"},
			{"if err != nil", "if false"}, {"fmt.Sprintf(\"%q\", got) !=", "fmt.Sprintf(\"%q\", got) =="},
		} {
			mutant := strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant == row.After || subscriberManifestConsumerPreserved(mutant) {
				t.Fatalf("weakened subscriber witness accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("subscriber manifest recipes=%d, want one complete consumer", count)
	}
}

func TestNativeCatalogSubscriberManifestOwnerKeepsPhysicalPredicateAndNoLocalSQL(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original string
	for _, row := range rows {
		if row.Family == "native-catalog-subscriber-manifest-observation" {
			original = causalDeliveryQueryLiteral(t, row.Before)
		}
	}
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "delivery", "read_projections.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "ReadSubscriberEventNamesCreatedSince")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
	if original == "" || causalDeliveryQueryLiteral(t, source) != original {
		t.Fatal("physical manifest changed inclusive cut, subscriber filter, join or tie order")
	}
	consumer, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "runtime", "cataloge2e", "assertions_harness_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{".QueryContext(", ".QueryRowContext(", "*sql.DB", "*sql.Tx"} {
		if strings.Contains(string(consumer), raw) {
			t.Fatalf("assertion harness retains raw authority: %s", raw)
		}
	}
}
