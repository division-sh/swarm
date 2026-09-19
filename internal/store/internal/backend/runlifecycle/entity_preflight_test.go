package runlifecycle

import (
	"testing"

	runtimeentity "github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestEntityWorkBlocksCompletionValidatesExactOwnerSummary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		summary   runtimeentity.RunSummary
		pending   bool
		wantError bool
	}{
		{"nonterminal", runtimeentity.RunSummary{RunID: "run", Total: 1, Nonterminal: 1}, true, false},
		{"terminal", runtimeentity.RunSummary{RunID: "run", Total: 1, Terminal: 1}, false, false},
		{"mixed", runtimeentity.RunSummary{RunID: "run", Total: 2, Terminal: 1, Nonterminal: 1}, true, false},
		{"empty_is_not_ready", runtimeentity.RunSummary{RunID: "run"}, true, false},
		{"malformed_descriptor_blocks", runtimeentity.RunSummary{RunID: "run", Total: 1, Malformed: 1}, true, false},
		{"foreign_terminal", runtimeentity.RunSummary{RunID: "other", Total: 1, Terminal: 1}, false, true},
		{"foreign_nonterminal", runtimeentity.RunSummary{RunID: "other", Total: 1, Nonterminal: 1}, false, true},
		{"missing_run", runtimeentity.RunSummary{Total: 1, Nonterminal: 1}, false, true},
		{"negative_total", runtimeentity.RunSummary{RunID: "run", Total: -1}, false, true},
		{"negative_terminal", runtimeentity.RunSummary{RunID: "run", Terminal: -1, Nonterminal: 1}, false, true},
		{"negative_nonterminal", runtimeentity.RunSummary{RunID: "run", Terminal: 1, Nonterminal: -1}, false, true},
		{"negative_malformed", runtimeentity.RunSummary{RunID: "run", Terminal: 1, Malformed: -1}, false, true},
		{"undercount", runtimeentity.RunSummary{RunID: "run", Total: 2, Terminal: 1}, false, true},
		{"overcount", runtimeentity.RunSummary{RunID: "run", Total: 1, Terminal: 1, Nonterminal: 1}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pending, err := entityWorkBlocksCompletion("run", tc.summary)
			if pending != tc.pending || (err != nil) != tc.wantError {
				t.Fatalf("entity preflight pending=%v err=%v want pending=%v error=%v", pending, err, tc.pending, tc.wantError)
			}
		})
	}
}
