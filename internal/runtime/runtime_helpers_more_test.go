package runtime

import (
	"context"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/testutil"
)

func TestAgentRuntimeHelpers_InferAndBudgetMapping(t *testing.T) {
	if got := inferDiscoveryMode("local services in argentina"); got != "local_services" {
		t.Fatalf("expected local_services, got %q", got)
	}
	if got := inferDiscoveryMode("follow saas_trend signals"); got != "saas_trend" {
		t.Fatalf("expected saas_trend, got %q", got)
	}
	if got := inferDiscoveryMode("generic directive"); got != "saas_gap" {
		t.Fatalf("expected default saas_gap, got %q", got)
	}

	if got := inferGeographyHint("SaaS in Paraguay"); got != "paraguay" {
		t.Fatalf("expected recognized geography paraguay, got %q", got)
	}
	if got := inferGeographyHint("Focus LATAM"); got != "Focus LATAM" {
		t.Fatalf("expected passthrough geography hint, got %q", got)
	}
	if got := inferGeographyHint(" "); got != "" {
		t.Fatalf("expected empty hint for blank input, got %q", got)
	}

	for state, evtType := range map[string]events.EventType{
		"warning":   events.EventType("budget.warning"),
		"throttle":  events.EventType("budget.throttle"),
		"emergency": events.EventType("budget.emergency"),
		"resumed":   events.EventType("budget.resumed"),
		"ok":        events.EventType("budget.resumed"),
	} {
		raw := mustJSON(map[string]any{"state": state})
		if got := budgetEventTypeFromThresholdPayload(raw); got != evtType {
			t.Fatalf("state %q expected %q, got %q", state, evtType, got)
		}
	}
	if got := budgetEventTypeFromThresholdPayload(mustJSON(map[string]any{"state": "unknown"})); got != "" {
		t.Fatalf("expected empty event type for unknown state, got %q", got)
	}

	if got := fieldStringFromJSON(mustJSON(map[string]any{"k": " v "}), "k"); got != "v" {
		t.Fatalf("expected trimmed field string, got %q", got)
	}
	if got := fieldStringFromJSON([]byte("{"), "k"); got != "" {
		t.Fatalf("expected empty for invalid json, got %q", got)
	}
}

func TestPipelineHelpers_NormalizationAndSimilarity(t *testing.T) {
	if got := normalizeName("  Dental-Clinic  Scheduling!! "); got != "dental clinic scheduling" {
		t.Fatalf("normalizeName mismatch: %q", got)
	}
	if slug := buildVerticalSlug("Dental Clinic Scheduling", "1234567890abcdef"); slug != "dental-clinic-scheduling-12345678" {
		t.Fatalf("unexpected slug %q", slug)
	}
	best, score := fuzzyBestMatch("Dental Clinic Scheduling SaaS", []verticalCandidate{
		{ID: "v1", Name: "Dental Clinic Scheduling"},
		{ID: "v2", Name: "Restaurant Ordering"},
	})
	if best.ID != "v1" || score <= 0.7 {
		t.Fatalf("expected v1 fuzzy match above threshold, got best=%+v score=%.2f", best, score)
	}
	if got := jaccard(tokenSet("a b"), tokenSet("b c")); got <= 0 || got >= 1 {
		t.Fatalf("expected partial overlap jaccard in (0,1), got %.2f", got)
	}
	merged := parsePayloadMap(mergeRawPayload(mustJSON(map[string]any{"a": 1, "b": 1}), mustJSON(map[string]any{"b": 2, "c": 3})))
	if asInt(merged["a"]) != 1 || asInt(merged["b"]) != 2 || asInt(merged["c"]) != 3 {
		t.Fatalf("unexpected merged payload: %+v", merged)
	}
}

func TestPipelineHelpers_DeriveDiscoveryCandidateName(t *testing.T) {
	name := deriveDiscoveryCandidateName(map[string]any{
		"trend_category":         "instant_payments",
		"trend_description":      "Paraguay's instant payment system is experiencing explosive growth with regulatory mandates and interoperability standards.",
		"opportunity_hypothesis": "Build a complete all-in-one orchestration layer for instant payment operators.",
	})
	if name != "Instant Payments" {
		t.Fatalf("expected concise taxonomy-derived name, got %q", name)
	}

	if got := deriveDiscoveryCandidateName(map[string]any{
		"opportunity_hypothesis": strings.Repeat("very long narrative hypothesis ", 8),
	}); got != "" {
		t.Fatalf("expected long narrative-only payload to be rejected, got %q", got)
	}
}

func TestPipelineHelpers_BuildVerticalSlugCapsLongBase(t *testing.T) {
	slug := buildVerticalSlug(strings.Repeat("instant-payment-growth-opportunity-", 6), "abcdef1234567890")
	if !strings.HasSuffix(slug, "-abcdef12") {
		t.Fatalf("expected stable id suffix, got %q", slug)
	}
	if len(slug) > maxVerticalSlugLen+1+8 {
		t.Fatalf("expected slug length cap <= %d, got %d (%q)", maxVerticalSlugLen+1+8, len(slug), slug)
	}
}

func TestDBTxContextWrappers_UseTransactionFromContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS tx_probe (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	txCtx := withSQLTxContext(ctx, tx)
	if _, err := dbExecContext(txCtx, db, `INSERT INTO tx_probe (id) VALUES (1)`); err != nil {
		t.Fatalf("insert in tx context: %v", err)
	}

	var inTxCount int
	if err := dbQueryRowContext(txCtx, db, `SELECT count(*) FROM tx_probe`).Scan(&inTxCount); err != nil {
		t.Fatalf("count in tx: %v", err)
	}
	if inTxCount != 1 {
		t.Fatalf("expected in-tx count=1, got %d", inTxCount)
	}

	rows, err := dbQueryContext(txCtx, db, `SELECT id FROM tx_probe`)
	if err != nil {
		t.Fatalf("query rows in tx: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row in tx query")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	var postRollbackCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tx_probe`).Scan(&postRollbackCount); err != nil {
		t.Fatalf("count after rollback: %v", err)
	}
	if postRollbackCount != 0 {
		t.Fatalf("expected rollback to clear insert, got count=%d", postRollbackCount)
	}
}
