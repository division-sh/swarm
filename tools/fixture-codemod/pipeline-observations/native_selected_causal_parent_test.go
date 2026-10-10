package main

import (
	"strings"
	"testing"
)

const selectedCausalParentBefore = `var exists int
if err := g.probe.h.db.QueryRowContext(ctx, \x60SELECT COUNT(*) FROM events WHERE event_id=$1 AND run_id=$2\x60, parent, req.Identity.RunID).Scan(&exists); err != nil || exists != 1 {
return manager.AgentLifecycleTransitionResult{}, fmt.Errorf("selected parent was not actually published: count=%d err=%v", exists, err)
}`

const selectedCausalParentAfter = `reader, err := g.probe.h.catalogOperatorEventLister()
if err != nil { return manager.AgentLifecycleTransitionResult{}, err }
publishedParent, found, err := storetest.ReadCanonicalEventRecord(ctx, reader, parent)
if err != nil || !found || publishedParent.RunID() != req.Identity.RunID {
return manager.AgentLifecycleTransitionResult{}, fmt.Errorf("selected parent was not actually published: found=%t run=%s err=%v", found, publishedParent.RunID(), err)
}`

func selectedCausalParentNormalized(t *testing.T, source string) string {
	t.Helper()
	before, err := canonicalFunction("func parent() {" + strings.ReplaceAll(selectedCausalParentBefore, "\\x60", "`") + "}")
	if err != nil {
		t.Fatal(err)
	}
	after, err := canonicalFunction("func parent() {" + selectedCausalParentAfter + "}")
	if err != nil {
		t.Fatal(err)
	}
	before = strings.TrimSuffix(strings.TrimPrefix(before, "func parent() {\n"), "\n}")
	after = strings.TrimSuffix(strings.TrimPrefix(after, "func parent() {\n"), "\n}")
	actual, err := canonicalFunction(source)
	if err != nil {
		return "parse-refused"
	}
	actual = strings.Replace(actual, selectedCausalFragment(t, selectedCausalConservationBefore), selectedCausalFragment(t, selectedCausalConservationAfter), 1)
	return strings.Replace(actual, before, after, 1)
}

func selectedCausalFragment(t *testing.T, source string) string {
	t.Helper()
	canonical, err := canonicalFunction("func fragment() {" + strings.ReplaceAll(source, "\\x60", "`") + "}")
	if err != nil {
		t.Fatal(err)
	}
	fragment := strings.TrimSuffix(strings.TrimPrefix(canonical, "func fragment() {\n"), "\n}")
	return "\t\t\t\t" + strings.ReplaceAll(fragment, "\n", "\n\t\t\t\t")
}

const selectedCausalConservationBefore = `					query := \x60SELECT COUNT(*) FROM events WHERE json_extract(payload,'$.details.outbox_id')=$1\x60
					if g.probe.h.pg != nil {
						query = \x60SELECT COUNT(*) FROM events WHERE payload->'details'->>'outbox_id'=$1\x60
					}
					var count int
					if err := g.probe.h.db.QueryRowContext(ctx, query, item.OutboxID).Scan(&count); err != nil || count != 0 {
						return result, fmt.Errorf("missing parent emitted diagnostic: count=%d err=%v", count, err)
					}
					var pendingCount int
					if err := g.probe.h.db.QueryRowContext(ctx, \x60SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1 AND projected_at IS NULL\x60, item.OutboxID).Scan(&pendingCount); err != nil || pendingCount != 1 {
						return result, fmt.Errorf("missing parent acknowledged diagnostic: pending=%d err=%v", pendingCount, err)
					}
`

const selectedCausalConservationAfter = `					observed, err := storetest.ReadSelectedCausalDiagnosticConservation(ctx, diagnostics, item.OutboxID)
					count := observed.Events
					if err != nil || count != 0 {
						return result, fmt.Errorf("missing parent emitted diagnostic: count=%d err=%v", count, err)
					}
					pendingCount := observed.Pending
					if pendingCount != 1 {
						return result, fmt.Errorf("missing parent acknowledged diagnostic: pending=%d err=%v", pendingCount, err)
					}
`

func TestNativeSelectedCausalParentPreservesCompleteLifecycleCallback(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-selected-causal-parent-read")
	if selectedCausalParentNormalized(t, row.Before) != selectedCausalParentNormalized(t, row.After) {
		t.Fatal("selected grant, parent identity, lineage, lifecycle commit, pending diagnostics or refusal assertions changed")
	}
	for _, pair := range [][2]string{
		{"g.probe.h.catalogOperatorEventLister()", "foreignHarness.catalogOperatorEventLister()"},
		{"ctx, reader, parent", "ctx, reader, otherEvent"},
		{"!found", "false"}, {"publishedParent.RunID() != req.Identity.RunID", "false"},
		{"evidence.SelectedFork.ForkRunID != req.Identity.RunID", "false"},
		{"g.GenerationGrant.CommitAgentLifecycleTransition(correlation.WithRuntimeLineage(ctx, lineage), req)", "fakeCommit(ctx,req)"},
		{"pendingCount != 1", "pendingCount != 0"}, {"attempt < 2", "attempt < 1"},
	} {
		mutant := strings.Replace(row.After, pair[0], pair[1], 1)
		if mutant == row.After || selectedCausalParentNormalized(t, row.Before) == selectedCausalParentNormalized(t, mutant) {
			t.Fatalf("weakened lifecycle/parent cut admitted: %v", pair)
		}
	}
}
