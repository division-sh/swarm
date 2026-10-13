package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"slices"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const predecessor = "bb42d634bc2810223a91ded22e26182a27bcea90"
const destination = "tools/fixture-codemod/pipeline-observations/family145-stop-witness-amendment.json"

type pin struct{ File, Function, Before, After, SHA256 string }

var functions = map[string][]string{
	"internal/store/internal/backend/delivery/read_projections.go":                      {"ReadStopPendingDeliveryCountTx"},
	"internal/serveapp/fan_out_stop_http_test.go":                                       {"TestIssue2394ServedStopTwentyBothStores", "TestIssue2394SelectedStopTwentyBothStores", "readIssue2394StopSnapshot", "assertIssue2394StopUnchanged"},
	"internal/serveapp/fan_out_stop_failure_test.go":                                    {"TestIssue2394StopFailureEnvelopeBothStores"},
	"internal/store/internal/runtimepersistence/run_stop_commit_boundary_test.go":       {"TestRunStopCommitUncertaintyPreservesRealOutcomeBothStores", "TestRunStopKnownCommitRejectionBothStores", "TestIssue2394StopCommitRaceBothStores", "readStopCommitEvidence"},
	"internal/store/internal/runtimepersistence/b16_independent_parent_stop_test.go":    {"TestB16IndependentPostgresParentStopRetainedGroup"},
	"internal/store/internal/runtimepersistence/b16_parent_stop_group_race_test.go":     {"TestB16ParentStopVersusPublicationGroupBothCommitOrders", "TestB16ParentStopContendsWithGroupSettlementCommit"},
	"internal/store/internal/runtimepersistence/b18_group_control_preservation_test.go": {"TestB18PausedPreparedGroupRetainsAcceptedWorkBothStores"},
	"internal/store/internal/runtimepersistence/run_stop_storage_test.go":               {"TestRunStopStoragePreservesExactRunCountsAndOriginalReadBothStores"},
}

// This records the explicitly authorized semantic amendment, not query equivalence.
// Complete preimages and outputs keep the unrelated temporal cuts and assertions pinned.
func main() {
	if len(os.Args) != 1 && (len(os.Args) != 2 || os.Args[1] != "-write") {
		panic("usage: capture-stop-witness [-write]")
	}
	var paths []string
	for path := range functions {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	var pins []pin
	for _, path := range paths {
		before, err := exec.Command("git", "show", predecessor+":"+path).Output()
		must(err)
		after, err := os.ReadFile(path)
		must(err)
		original, current := declarations(before), declarations(after)
		for _, function := range functions[path] {
			if original[function] == "" || current[function] == "" || original[function] == current[function] {
				panic("missing exact changed source: " + path + "/" + function)
			}
			pins = append(pins, pin{path, function, original[function], current[function], fmt.Sprintf("%x", sha256.Sum256([]byte(original[function])))})
		}
	}
	if len(pins) != 15 {
		panic("stop witness amendment must retain all fifteen exact declarations")
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("declarations=%d sha256=%x\n", len(pins), sha256.Sum256(raw))
	if len(os.Args) == 2 {
		must(os.WriteFile(destination, raw, 0644))
	}
}

func declarations(raw []byte) map[string]string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	out := map[string]string{}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil {
			out[fn.Name.Name] = requestkey.Node(fn)
		}
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
