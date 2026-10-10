package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const originalScatterFrontierSQL = `WITH RECURSIVE run_events AS MATERIALIZED (
	SELECT event_id,source_event_id FROM events WHERE run_id=$1 AND event_name<>'platform.runtime_log'
), descendants(event_id) AS (
	SELECT event_id FROM run_events WHERE event_id=$2
	UNION
	SELECT e.event_id FROM run_events e JOIN descendants p ON e.source_event_id=p.event_id
), delivery_counts AS (
	SELECT p.event_id, COUNT(d.delivery_id) AS total,
		COALESCE(SUM(CASE WHEN d.status<>'delivered' THEN 1 ELSE 0 END),0) AS unsettled
	FROM descendants p LEFT JOIN event_deliveries d ON d.event_id=p.event_id
	GROUP BY p.event_id
)
SELECT (SELECT COUNT(*) FROM descendants),
	(SELECT COUNT(*) FROM delivery_counts WHERE total<>1 OR unsettled<>0),
	(SELECT COUNT(*) FROM dead_letters d JOIN descendants p ON d.original_event_id=p.event_id)`

const nativeFrontierProbeConsumer = `func TestScatterGatherFrontierAggregatePreservesRefusalsBothStores(t *testing.T) {
for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite,catalogBackendPostgres} {
t.Run(string(backend),func(t *testing.T) {
h := newRuntimeHarnessForBackend(t,filepath.Join(canonicalrouting.RepoRoot(t),"internal/runtime/cataloge2e/testdata/scatter-gather-safety"),backend,true)
reader,err := h.catalogOperatorEventLister()
if err != nil { t.Fatal(err) }
for _,tc := range []struct{name string;probe storetest.CausalDeliveryFrontierProbe;unsettled int}{
{"complete",storetest.FrontierComplete,0},
{"missing",storetest.FrontierMissing,2},
{"pending",storetest.FrontierPending,1},
{"duplicate-delivery",storetest.FrontierDuplicateDelivery,1},
{"dead-lettered",storetest.FrontierDeadLetterDelivery,1},
} {
t.Run(tc.name,func(t *testing.T){
frontier,err := storetest.ProbeCausalDeliveryFrontier(h.ctx,reader,tc.probe)
if err != nil { t.Fatal(err) }
observed,unsettled,dead := frontier.Observed,frontier.Unsettled,frontier.DeadLetters
if observed != 3 || unsettled != tc.unsettled || dead != 2 { t.Fatalf("frontier=%d unsettled=%d dead=%d; want 3/%d/2",observed,unsettled,dead,tc.unsettled) }
})
}
})
}
}`

func nativeFrontierProbeConsumerPreserved(source string) bool {
	want, err := canonicalFunction(nativeFrontierProbeConsumer)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogScatterProbeConsumerKeepsAllFiveExactAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-scatter-frontier-observation" || row.Function != "TestScatterGatherFrontierAggregatePreservesRefusalsBothStores" {
			continue
		}
		count++
		if !nativeFrontierProbeConsumerPreserved(row.After) {
			t.Fatal("frontier control changed real fixture, native owner, five cases or complete assertions")
		}
		for _, pair := range [][2]string{
			{"storetest.FrontierMissing, 2", "storetest.FrontierMissing, 1"},
			{"observed != 3", "observed < 3"}, {"unsettled != tc.unsettled", "unsettled > tc.unsettled"},
			{"dead != 2", "dead == 0"}, {"if err != nil", "if false"},
			{"h.ctx, reader, tc.probe", "h.ctx, foreignOwner, tc.probe"},
		} {
			mutant := strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant == row.After || nativeFrontierProbeConsumerPreserved(mutant) {
				t.Fatalf("weakened negative control accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("frontier probe consumers=%d, want one complete migration", count)
	}
}

func scatterWaitRecipePreserved(row recipe) bool {
	source := row.After
	binding := "\treader, err := h.catalogOperatorEventLister()\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n"
	if strings.Count(source, binding) != 1 {
		return false
	}
	source = strings.Replace(source, binding, "", 1)
	for _, indent := range []string{"\t\t\t", "\t\t\t\t"} {
		start := indent + "count, err := storetest.CountClosedSourceFanOutIssuance(ctx, reader, catalogRuntimeRunID, step.eventID, cardinality)\n" + indent + "if err != nil {\n"
		if strings.Count(source, start) != 1 {
			return false
		}
		original := indent + "var count int\n" + indent + "if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2 AND status='closed' AND cardinality=$3 AND cursor=$3 AND claim_owner IS NULL`, catalogRuntimeRunID, step.eventID, cardinality).Scan(&count); err != nil {\n"
		source = strings.Replace(source, start, original, 1)
	}
	native := "\t\tfrontier, err := storetest.ReadCausalDeliveryFrontier(ctx, reader, catalogRuntimeRunID, step.eventID)\n\t\tobserved, unsettled, deadLetters := frontier.Observed, frontier.Unsettled, frontier.DeadLetters\n"
	if strings.Count(source, native) != 1 {
		return false
	}
	source = strings.Replace(source, native, "\t\tvar observed, unsettled, deadLetters int\n\t\terr := h.db.QueryRowContext(ctx, scatterGatherFrontierQuery, catalogRuntimeRunID, step.eventID).Scan(&observed, &unsettled, &deadLetters)\n", 1)
	want, err := canonicalFunction(row.Before)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogScatterWaitPreservesEveryGateClockAndPublicAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-scatter-frontier-observation" || row.Function != "scatterGatherWait" {
			continue
		}
		count++
		if !scatterWaitRecipePreserved(row) {
			t.Fatal("scatter wait changed issuance/convergence gates, scope, poll/deadline or public assertions")
		}
		for _, pair := range [][2]string{
			{"reader, catalogRuntimeRunID, step.eventID, cardinality", "reader, otherRunID, step.eventID, cardinality"},
			{"reader, catalogRuntimeRunID, step.eventID)", "reader, catalogRuntimeRunID, otherEventID)"},
			{"if count != 1", "if count > 1"}, {"deadLetters != 0", "deadLetters > 1"},
			{"observed != want+1 || unsettled != 0", "observed < want+1"},
			{"10 * time.Millisecond", "time.Second"}, {"20 * time.Second", "30 * time.Second"},
			{"len(event.Deliveries) != 1", "len(event.Deliveries) > 1"},
			{"event.Deliveries[0].Status != \"delivered\"", "event.Deliveries[0].Status == \"pending\""},
			{"started, deadline = phaseStart, phaseDeadline", "started, deadline = time.Now(), time.Now().Add(time.Minute)"},
		} {
			mutant := row
			mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant.After == row.After || scatterWaitRecipePreserved(mutant) {
				t.Fatalf("weakened scatter proof accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("scatter wait recipes=%d, want exact owner migration", count)
	}
}

func frontierProbeSQLValues(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", "package witness\n"+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(text, "('r','root'") || strings.HasPrefix(text, "WITH RECURSIVE events(") {
			out = append(out, strings.Join(strings.Fields(text), ""))
		}
		return true
	})
	return out
}

func TestNativeCatalogScatterFrontierKeepsActualProbeCasesAndOneSQLBody(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
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
	queryMatched := false
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || spec.Names[0].Name != "causalDeliveryFrontierQuery" || len(spec.Values) != 1 {
			return true
		}
		literal, ok := spec.Values[0].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			t.Fatal("canonical frontier query is not the fixed literal")
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		queryMatched = strings.Join(strings.Fields(text), "") == strings.Join(strings.Fields(originalScatterFrontierSQL), "")
		return false
	})
	if !queryMatched {
		t.Fatal("canonical frontier SQL changed any original predicate, aggregate or materialization")
	}
	fn, err := uniqueFunction(file, "ProbeCausalDeliveryFrontier")
	if err != nil {
		t.Fatal(err)
	}
	probeSource := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
	var original []string
	for _, row := range rows {
		if row.Family == "native-catalog-scatter-frontier-observation" && row.Function == "TestScatterGatherFrontierAggregatePreservesRefusalsBothStores" {
			original = frontierProbeSQLValues(t, row.Before)
		}
	}
	values := frontierProbeSQLValues(t, probeSource)
	if len(values) != 6 || len(original) != 6 {
		t.Fatalf("probe input inventory=%d/%d, want prefix and five cases", len(values), len(original))
	}
	// The original prefix precedes its table; the private switch precedes prefix.
	for i := 0; i < 5; i++ {
		if values[i] != original[i+1] {
			t.Fatalf("probe case%d lost missing/pending/duplicate/dead-letter evidence", i)
		}
	}
	if values[5] != original[0] {
		t.Fatal("probe source graph, runtime logs or duplicate dead-letter rows changed")
	}
	for _, fragment := range []string{"strings.TrimPrefix(causalDeliveryFrontierQuery, \"WITH RECURSIVE \")", "default:", "unsupported causal frontier probe", "q.QueryRowContext(ctx, query, \"run\", \"root\")"} {
		if !strings.Contains(probeSource, fragment) {
			t.Fatalf("probe no longer executes one exact owner query or refuses unsupported cases: %q", fragment)
		}
	}
	for _, fragment := range []string{
		"SELECT event_id,source_event_id FROM events WHERE run_id=$1 AND event_name<>'platform.runtime_log'",
		"FROM descendants p LEFT JOIN event_deliveries d ON d.event_id=p.event_id",
		"WHERE total<>1 OR unsettled<>0", "dead_letters d JOIN descendants p ON d.original_event_id=p.event_id",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("exact frontier predicate lost: %q", fragment)
		}
	}
	consumer, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "runtime", "cataloge2e", "scatter_gather_safety_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(consumer), "scatterGatherFrontierQuery") {
		t.Fatal("local frontier SQL interpreter survived")
	}
}
