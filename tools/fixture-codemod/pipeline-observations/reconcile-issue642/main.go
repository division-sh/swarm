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
	"strings"
)

const source = "3e84acd02f4cc3c02e2b26be494e74be1a035f0c"
const completion = "098a84acd3cff157fb6bcaefb1d55ee15994cdb7"
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
	var rows []recipe
	var history []successor
	decode(read(source, root+"recipes.json"), &rows)
	oldHistory := read(source, root+"testdata/fixture_setup_successors_2617.json")
	decode(oldHistory, &history)
	if len(rows) != 893 || len(history) != 36 || fmt.Sprintf("%x", sha256.Sum256(oldHistory)) != "1cafae311b85749160553e56ca1a3123336ba397882464c9c0709a16d04e10ca" {
		panic("reviewed historical inventory changed")
	}
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
			if body(current, row.Function) != candidate || row.Successor == "" || row.Successor == candidate {
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
	// #642/6191161328 and 6191625495 append to the prior 39 receipts.
	prior := encode(history, false)
	if !bytes.Equal(prior, read(completion, root+"testdata/fixture_setup_successors_2617.json")) {
		panic("reviewed completion predecessor changed")
	}
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
	packet := encode(history, false)
	fmt.Printf("append-only history=%d sha256=%x\n", len(history), sha256.Sum256(packet))
	if len(os.Args) == 2 && os.Args[1] == "-write" {
		must(os.WriteFile(root+"recipes.json", encode(rows, true), 0644))
		must(os.WriteFile(root+"testdata/fixture_setup_successors_2617.json", packet, 0644))
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
