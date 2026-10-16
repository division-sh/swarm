package contracts

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func Test2293Q1AccumulateStateListBundleAdmission(t *testing.T) {
	for _, collect := range []bool{false, true} {
		name := "ordinary_handler_control"
		if collect {
			name = "canonical_state_list"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: state-list\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "types.yaml"), "Review: {reviewer_id: text, message: text}\n")
			writeFixtureFile(t, filepath.Join(root, "entities.yaml"), "State:\n  reviews: [Review]\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), "review.received: {reviewer_id: text, message: text}\nreview.recorded:\n")
			node := "collector:\n  execution_type: system_node\n  subscribes_to: [review.received]\n  event_handlers:\n    review.received:\n      emit: review.recorded\n"
			if collect {
				node += "      accumulate: {target: reviews, key: reviewer_id}\n"
			}
			writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), node)
			repo := repoRootForContractsTest(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("supported bundle entrance refused %s: %v", name, err)
			}
			if bundle == nil {
				t.Fatal("loader returned no admitted bundle")
			}
			if collect {
				spec := bundle.Nodes["collector"].EventHandlers["review.received"].Accumulate
				if spec == nil || spec.Target != "reviews" || spec.Key != "reviewer_id" {
					t.Fatalf("admitted bundle lost accumulation: %#v", spec)
				}
			}
		})
	}
}

// The declaration grammar is ruled independently of the pending catalog layout.
func Test2293Q1AccumulateStateListSourceShape(t *testing.T) {
	for _, source := range []string{
		"accumulate: {target: reviews, key: reviewer_id}\n",
		"accumulate:\n  target: reviews\n  key: reviewer_id\n",
	} {
		t.Run(source, func(t *testing.T) {
			out, err := projectNodeAccumulateValue(accumulateQ1Source(t, source))
			if err != nil {
				t.Fatalf("canonical state-list declaration refused: %v", err)
			}
			if out == nil || out.Target != "reviews" || out.Key != "reviewer_id" {
				t.Fatalf("authored item key not preserved: %#v", out)
			}
		})
	}
}

func Test2293Q1AccumulateStateListSourceRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, source, teaching string
	}{
		{"missing_key", "{target: reviews}", "key: <item field>"},
		{"missing_target", "{key: reviewer_id}", "target"},
		{"empty", "{}", "target"},
		{"null", "null", "mapping"},
		{"empty_key", "{target: reviews, key: ''}", "key"},
		{"null_key", "{target: reviews, key: null}", "key"},
		{"integer_key", "{target: reviews, key: 0}", "key"},
		{"list_key", "{target: reviews, key: [reviewer_id]}", "key"},
		{"rooted_key", "{target: reviews, key: payload.reviewer_id}", "key: reviewer_id"},
		{"nested_key", "{target: reviews, key: reviewer.id}", "item field"},
		{"rooted_target", "{target: state.reviews, key: reviewer_id}", "target: reviews"},
		{"into", "{into: reviews, key: reviewer_id}", "target: reviews"},
		{"from", "{target: reviews, key: reviewer_id, from: payload}", "from"},
		{"description", "{target: reviews, key: reviewer_id, description: review}", "description"},
		{"duplicate_key", "{target: reviews, key: reviewer_id, key: other}", "duplicate"},
		{"duplicate_target", "{target: reviews, target: other, key: reviewer_id}", "duplicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := projectNodeAccumulateValue(accumulateQ1Source(t, "accumulate: "+tc.source+"\n"))
			if err == nil || !strings.Contains(err.Error(), tc.teaching) || !strings.Contains(err.Error(), "nodes.yaml:") {
				t.Fatalf("want source-located refusal teaching %q, got %v", tc.teaching, err)
			}
		})
	}
}

func accumulateQ1Source(t *testing.T, source string) yamlsource.Value {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	field, err := snapshot.Document("nodes.yaml").Root().Lookup("accumulate")
	if err != nil {
		t.Fatal(err)
	}
	return field.Value
}
