package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func restoreRootReplyProjection(row recipe, source, begin, end, expected, oldBegin string) (string, bool) {
	start := strings.Index(source, begin)
	oldStart := strings.LastIndex(row.Before, oldBegin)
	if start < 0 || oldStart < 0 {
		return "", false
	}
	stop := strings.Index(source[start:], end)
	oldStop := strings.Index(row.Before[oldStart:], end)
	if stop < 0 || oldStop < 0 {
		return "", false
	}
	actual := source[start : start+stop]
	want, err := canonicalFunction("func witness() {" + expected + "}")
	got, parseErr := canonicalFunction("func witness() {" + actual + "}")
	if err != nil || parseErr != nil || want != got {
		return "", false
	}
	return strings.Replace(source, actual, row.Before[oldStart:oldStart+oldStop], 1), true
}

func catalogRootReplyRecipePreservesAssertions(row recipe) bool {
	source, valid := row.After, true
	switch row.Function {
	case "proveRootReplyBoundary":
		source, valid = restoreRootReplyProjection(row, source, "\t\t\t\t\treader, err :=", "\n\t\t\t\t})", `reader, err := h.catalogOperatorEventLister()
if err != nil { t.Logf("reply publication diagnostic: %v", err); return }
rows, err := storetest.ReadEarliestEventPipelineReceiptRows(context.Background(), reader)
if err != nil { t.Logf("reply publication diagnostic: %v", err); return }
for _, row := range rows { t.Logf("reply publication: event=%s name=%s outcome=%s reason=%s", row.ID, row.Name, row.Outcome, row.Reason) }`, "\t\t\t\t\trows, err := h.db.Query(")
	case "assertConstructedRootReplyOrigins":
		source, valid = restoreRootReplyProjection(row, source, "\tselected, err :=", "\twant := events.RouteIdentity", `selected, err := h.catalogOperatorEventLister()
if err != nil { t.Fatal(err) }
ids, err := storetest.ReadReplyContextRequestIDs(h.ctx, selected, catalogRuntimeRunID)
if err != nil { t.Fatal(err) }
`, "\trows, err := h.db.QueryContext(")
	case "assertRootReplyRefusals":
		source, valid = restoreRootReplyProjection(row, source, "\tselected, err :=", "\trequest, found, err :=", `selected, err := h.catalogOperatorEventLister()
if err != nil { t.Fatal(err) }
stored, err := storetest.ReadFirstReplyContextStorage(h.ctx, selected, catalogRuntimeRunID)
if err != nil { t.Fatal(err) }
contextID, requestID, acceptedID := stored.ContextID, stored.RequestEventID, stored.AcceptedReplyEventID
`, "\tvar contextID, requestID, acceptedID string")
		if !valid {
			return false
		}
		source, valid = restoreRootReplyProjection(row, source, "\t\t\tbefore, err :=", "\t\t\tplan, err :=", `before, err := storetest.CountPhysicalRunEvents(h.ctx, selected, catalogRuntimeRunID)
if err != nil { t.Fatal(err) }
`, "\t\t\tvar before, after int")
		if !valid {
			return false
		}
		source, valid = restoreRootReplyProjection(row, source, "\t\t\tafter, err :=", "\t\t\tif before != after", `after, err := storetest.CountPhysicalRunEvents(h.ctx, selected, catalogRuntimeRunID)
if err != nil { t.Fatal(err) }
`, "\t\t\tif err := h.db.QueryRowContext(h.ctx, `SELECT count(*) FROM events WHERE run_id=$1`")
	case "assertRootReplyEvidence":
		source, valid = restoreRootReplyProjection(row, source, "\tlister, err :=", "\tif total != want", `lister, err := h.catalogOperatorEventLister()
if err != nil { t.Fatal(err) }
counts, err := storetest.ReadReplyContextStorageCounts(h.ctx, lister, catalogRuntimeRunID)
if err != nil { t.Fatal(err) }
total, accepted, distinctRequests, distinctReplies := counts.Total, counts.Accepted, counts.DistinctRequests, counts.DistinctReplies
`, "\tvar total, accepted, distinctRequests, distinctReplies int")
		if !valid {
			return false
		}
		source = strings.Replace(source, "\tpublic, err :=", "\tlister, err := h.catalogOperatorEventLister()\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\tpublic, err :=", 1)
		source, valid = restoreRootReplyProjection(row, source, "\t\tpublications, err :=", "\t\tvisible :=", `publications, err := storetest.CountRunEventNameStorage(h.ctx, lister, catalogRuntimeRunID, name)
if err != nil { t.Fatal(err) }
`, "\t\tvar publications int")
	default:
		return false
	}
	if !valid {
		return false
	}
	want, err := canonicalFunction(row.Before)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogRootReplyRecipesPreserveRoutingRefusalAndNoRedispatch(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-root-reply-observation" {
			continue
		}
		count++
		if !catalogRootReplyRecipePreservesAssertions(row) {
			t.Fatalf("%s changed root construction, live refusal, replay/reopen, workload or assertions", row.Function)
		}
		for _, pair := range [][2]string{
			{"h.ctx, selected, catalogRuntimeRunID", "h.ctx, foreignOwner, foreignRunID"},
			{"h.ctx, lister, catalogRuntimeRunID, name", "h.ctx, lister, foreignRunID, name"},
			{"before != after", "before == after"}, {"len(request.DeliveryRoutes) != 1", "len(request.DeliveryRoutes) > 1"},
			{"publications != want", "publications > want"}, {"AcceptedReplyEventID != acceptedID", "AcceptedReplyEventID == acceptedID"},
			{"context.Background(), reader", "context.Background(), foreignReader"},
		} {
			mutant := row
			mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant.After != row.After && catalogRootReplyRecipePreservesAssertions(mutant) {
				t.Fatalf("lost ownership, scope, conservation or replay assertion accepted: %v", pair)
			}
		}
	}
	if count != 4 {
		t.Fatalf("root reply recipes=%d, want all four consumers", count)
	}
}

func TestNativeCatalogRootReplyOwnerPreservesPhysicalCardinalityAndOrder(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "replycontext", "owner.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"SELECT CAST(request_event_id AS TEXT) FROM reply_contexts WHERE run_id=$1 ORDER BY request_event_id",
		"FROM reply_contexts WHERE run_id=$1 ORDER BY reply_context_id LIMIT 1",
		"SELECT count(*),count(accepted_reply_event_id),count(DISTINCT request_event_id),count(DISTINCT accepted_reply_event_id)",
		"if err := rows.Err(); err != nil", "if err := rows.Close(); err != nil",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("reply physical read contract missing %q", fragment)
		}
	}
}
