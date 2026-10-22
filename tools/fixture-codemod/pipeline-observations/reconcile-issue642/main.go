package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"reflect"
	"strings"
)

const source = "3e84acd02f4cc3c02e2b26be494e74be1a035f0c"
const completion = "098a84acd3cff157fb6bcaefb1d55ee15994cdb7"
const retainedJoinNative = "29c953af346d076cfba81f7c93ef2ced35b41e4a"
const root = "tools/fixture-codemod/pipeline-observations/"

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string          `json:"Successor,omitempty"`
	Removed                               bool            `json:"Removed,omitempty"`
	Mechanical                            json.RawMessage `json:"Mechanical,omitempty"`
}

type edit struct {
	Start          int
	Current, Prior []string
}

type successor struct {
	Original  recipe
	Candidate string
	Edits     []edit
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-integrate-2672" {
		integrate2672()
		return
	}
	var rows []recipe
	var history []successor
	decode(read(source, root+"recipes.json"), &rows)
	oldHistory := read(source, root+"testdata/fixture_setup_successors_2617.json")
	decode(oldHistory, &history)
	if len(rows) != 893 || len(history) != 36 || fmt.Sprintf("%x", sha256.Sum256(oldHistory)) != "1cafae311b85749160553e56ca1a3123336ba397882464c9c0709a16d04e10ca" {
		panic("reviewed historical inventory changed")
	}
	history = appendOriginalSuccessors(rows, history)
	// #642/6191161328 and 6191625495 append to the prior 39 receipts.
	prior := encode(history, false)
	if !bytes.Equal(prior, read(completion, root+"testdata/fixture_setup_successors_2617.json")) {
		panic("reviewed completion predecessor changed")
	}
	history = appendCompletionSuccessors(rows, history)
	// LIGHT: existing native observation composition under #642/6191161328
	// and6192796272. Preserve every preceding endpoint and the refusal branch.
	if prior := encode(history, false); fmt.Sprintf("%x", sha256.Sum256(prior)) != "dc9462f1af05ec13e8fc9c03570c48082fe74b126bb6d0da8597dd4b2689ba62" {
		panic("reviewed 43-row predecessor changed")
	}
	history = appendRetainedJoinSuccessor(rows, history)
	printReviewedPins()
	packet := encode(history, false)
	fmt.Printf("append-only history=%d sha256=%x\n", len(history), sha256.Sum256(packet))
	if len(os.Args) == 2 && os.Args[1] == "-write" {
		must(os.WriteFile(root+"recipes.json", encode(rows, true), 0644))
		must(os.WriteFile(root+"testdata/fixture_setup_successors_2617.json", packet, 0644))
	}
}

func appendOriginalSuccessors(rows []recipe, history []successor) []successor {
	for _, target := range [][2]string{
		{"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "selectedContractExecutionOwnerForCatalogTest"},
		{"internal/serveapp/fork_retained_join_test.go", "testServedJoinWriterCompletedHistoryRefusal"},
		{"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "selectedContractExecutionOwnerForCatalogHarness"},
	} {
		matches := 0
		for i := range rows {
			row := &rows[i]
			if row.File != target[0] || row.Function != target[1] {
				continue
			}
			matches++
			candidate := body(read(source, row.File), row.Function)
			current, err := os.ReadFile(row.File)
			must(err)
			currentCandidate := candidate
			if row.File == "internal/serveapp/fork_retained_join_test.go" && row.Function == "testServedJoinWriterCompletedHistoryRefusal" {
				currentCandidate = body(read(retainedJoinNative, row.File), row.Function)
			}
			if body(current, row.Function) != currentCandidate || row.Successor == "" || row.Successor == candidate {
				panic("unreviewed or unchanged successor: " + row.Function)
			}
			history = append(history, successor{Original: *row, Candidate: candidate, Edits: []edit{{
				Current: strings.SplitAfter(candidate, "\n"), Prior: strings.SplitAfter(row.Successor, "\n"),
			}}})
			row.Successor = candidate
		}
		if matches != 1 {
			panic("missing/ambiguous reviewed endpoint: " + target[1])
		}
	}
	return history
}

func appendCompletionSuccessors(rows []recipe, history []successor) []successor {
	for _, target := range [][2]string{
		{"internal/runtime/pipeline/workflow_timer_replay_selected_store_external_test.go", "TestWorkflowTimerCauseReplayReopenAndIsolationBothStores"},
		{"internal/runtime/runforkexecution/constructed_agent_input_execution_test.go", "proveSelectedConstructedAgentInput"},
		{"internal/runtime/runforkexecution/execution_test.go", "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation"},
		{"internal/runtime/runforkexecution/runtime_outcome_test.go", "seedSelectedRuntimeOutcomeSource"},
	} {
		matches := 0
		for i := range rows {
			row := &rows[i]
			if row.File != target[0] || row.Function != target[1] {
				continue
			}
			matches++
			candidate := body(read(completion, row.File), row.Function)
			current, err := os.ReadFile(row.File)
			must(err)
			previous := row.Successor
			if previous == "" {
				previous = row.After
			}
			if body(current, row.Function) != candidate || previous == candidate {
				panic("unreviewed or unchanged completion successor: " + row.Function)
			}
			history = append(history, successor{Original: *row, Candidate: candidate, Edits: []edit{{
				Current: strings.SplitAfter(candidate, "\n"), Prior: strings.SplitAfter(previous, "\n"),
			}}})
			row.Successor = candidate
		}
		if matches != 1 {
			panic("missing/ambiguous completion endpoint: " + target[1])
		}
	}
	return history
}

func appendRetainedJoinSuccessor(rows []recipe, history []successor) []successor {
	matches := 0
	for i := range rows {
		row := &rows[i]
		if row.File != "internal/serveapp/fork_retained_join_test.go" || row.Function != "testServedJoinWriterCompletedHistoryRefusal" {
			continue
		}
		matches++
		candidate := body(read(retainedJoinNative, row.File), row.Function)
		current, err := os.ReadFile(row.File)
		must(err)
		if body(current, row.Function) != candidate || row.Successor == "" || row.Successor == candidate {
			panic("unreviewed or unchanged native retained join successor")
		}
		history = append(history, successor{Original: *row, Candidate: candidate, Edits: []edit{{
			Current: strings.SplitAfter(candidate, "\n"), Prior: strings.SplitAfter(row.Successor, "\n"),
		}}})
		row.Successor = candidate
	}
	if matches != 1 {
		panic("missing/ambiguous native retained join endpoint")
	}
	return history
}

func printReviewedPins() {
	fmt.Printf("pin requireServedRetainedJoinForkSuccess %x\n", sha256.Sum256([]byte(canonical(body(read(retainedJoinNative, "internal/serveapp/fork_retained_join_test.go"), "requireServedRetainedJoinForkSuccess")))))
	for _, target := range [][2]string{
		{"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "selectedContractExecutionOwnerForCatalogTestWithReplay"},
		{"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "selectedContractExecutionOwnerForCatalogHarnessWithReplay"},
		{"internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go", "constructCatalogSelectedExecutionOwner"},
		{"internal/store/internal/runtimepersistence/stop_commit_fork_witness_test.go", "*stopCommitForkWitness.decode"},
		{"internal/store/internal/runtimepersistence/stop_commit_fork_witness_test.go", "*stopCommitWitnessNativeConn.ExecContext"},
		{"internal/store/internal/runtimepersistence/stop_commit_fork_witness_test.go", "TestStopCommitForkWitnessObservesOnlySuccessfulExactTransaction"},
	} {
		fmt.Printf("pin %s %x\n", target[1], sha256.Sum256([]byte(canonical(body(read(source, target[0]), target[1])))))
	}
	const scanFile = "internal/runtime/runforkexecution/constructed_agent_input_execution_test.go"
	fmt.Printf("pin selectedHistoricalScanProbe.CommitSelectedForkEvent %x\n", sha256.Sum256([]byte(canonical(body(read(completion, scanFile), "selectedHistoricalScanProbe.CommitSelectedForkEvent")))))
}

// Compose the two independently reviewed receipt chains without rewriting
// either predecessor packet or inferring new endpoints from the working tree.
func integrate2672() {
	const lane = "cc446dd0065c86e1c117f02b4b31f4106624f9c1"
	const master = "3a6de3d3b224fb74003c6da3377a8b85d1fd3b9e"
	const base = "98feeaa8de97bd15e3f93c7d110a3dda6af4a783"
	packetPath := root + "testdata/fixture_setup_successors_2617.json"
	lanePacket, masterPacket := read(lane, packetPath), read(master, packetPath)
	if fmt.Sprintf("%x", sha256.Sum256(lanePacket)) != "3698f8e49a693211ba853bb110ce400011ec3c828d3af40289505711202daf28" ||
		fmt.Sprintf("%x", sha256.Sum256(masterPacket)) != "501485801119fc04c640401945b123bff6fd2644afad7e48ad7b85d548a36528" {
		panic("integration predecessor packet changed")
	}
	var lanePins, masterPins []successor
	decode(lanePacket, &lanePins)
	decode(masterPacket, &masterPins)
	if len(lanePins) != 44 || len(masterPins) != 39 || !reflect.DeepEqual(lanePins[:36], masterPins[:36]) {
		panic("integration receipt chains do not share their exact 36-row origin")
	}
	for _, incoming := range masterPins[36:] {
		for _, existing := range lanePins[36:] {
			if incoming.Original.File == existing.Original.File && incoming.Original.Function == existing.Original.Function {
				panic("overlapping receipt endpoints require explicit reconciliation")
			}
		}
	}
	combinedPins := append(append([]successor(nil), lanePins...), masterPins[36:]...)
	if !bytes.Equal(encode(combinedPins[:44], false), lanePacket) ||
		!bytes.Equal(encode(append(append([]successor(nil), combinedPins[:36]...), combinedPins[44:]...), false), masterPacket) {
		panic("integration changed a reviewed predecessor")
	}

	var baseRows, laneRows, masterRows []rowMap
	decode(read(base, root+"recipes.json"), &baseRows)
	decode(read(lane, root+"recipes.json"), &laneRows)
	decode(read(master, root+"recipes.json"), &masterRows)
	if len(baseRows) != 893 || len(laneRows) != len(baseRows) || len(masterRows) != len(baseRows) {
		panic("integration recipe inventory changed")
	}
	mergeReviewedRecipeSuccessors(baseRows, laneRows, masterRows)
	// Keep the established recipe field order and escaping, rather than the
	// alphabetical map order used for the three-way validation above.
	var combinedRows []recipe
	decode(encode(laneRows, true), &combinedRows)
	packet := encode(combinedPins, false)
	must(os.WriteFile(root+"recipes.json", encode(combinedRows, true), 0644))
	must(os.WriteFile(packetPath, packet, 0644))
	fmt.Printf("integrated history=%d sha256=%x\n", len(combinedPins), sha256.Sum256(packet))
	var mechanical []recipe
	for _, row := range combinedRows {
		if len(row.Mechanical) != 0 {
			mechanical = append(mechanical, row)
		}
	}
	data, err := json.Marshal(mechanical)
	must(err)
	fmt.Printf("integrated mechanical=%d sha256=%x\n", len(mechanical), sha256.Sum256(data))
}

type rowMap = map[string]json.RawMessage

func mergeReviewedRecipeSuccessors(baseRows, laneRows, masterRows []rowMap) {
	for i := range baseRows {
		for _, key := range []string{"Family", "File", "Function", "Before", "After", "Mechanical", "Removed"} {
			if !reflect.DeepEqual(baseRows[i][key], laneRows[i][key]) || !reflect.DeepEqual(baseRows[i][key], masterRows[i][key]) {
				panic(fmt.Sprintf("integration changed historical recipe %d/%s", i, key))
			}
		}
		baseSuccessor, laneSuccessor, masterSuccessor := baseRows[i]["Successor"], laneRows[i]["Successor"], masterRows[i]["Successor"]
		if !reflect.DeepEqual(masterSuccessor, baseSuccessor) {
			if !reflect.DeepEqual(laneSuccessor, baseSuccessor) && !reflect.DeepEqual(laneSuccessor, masterSuccessor) {
				panic(fmt.Sprintf("overlapping recipe successor %d", i))
			}
			laneRows[i]["Successor"] = masterSuccessor
		}
	}
}

func read(commit, path string) []byte {
	raw, err := exec.Command("git", "show", commit+":"+path).Output()
	must(err)
	return raw
}

func body(raw []byte, name string) string {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "proof.go", raw, parser.AllErrors)
	must(err)
	var result string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		identity := fn.Name.Name
		if fn.Recv != nil {
			var receiver bytes.Buffer
			must(format.Node(&receiver, set, fn.Recv.List[0].Type))
			identity = receiver.String() + "." + identity
		}
		if identity != name {
			continue
		}
		if result != "" {
			panic("ambiguous function: " + name)
		}
		result = string(raw[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
	}
	if result == "" {
		panic("missing function: " + name)
	}
	return result
}

func decode(raw []byte, value any) { must(json.Unmarshal(raw, value)) }

func canonical(source string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "pin.go", "package pin\n"+source, parser.AllErrors)
	must(err)
	var out bytes.Buffer
	must(format.Node(&out, token.NewFileSet(), file.Decls[0]))
	return out.String()
}

func encode(value any, escape bool) []byte {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(escape)
	encoder.SetIndent("", "  ")
	must(encoder.Encode(value))
	return out.Bytes()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
