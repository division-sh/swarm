//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

func preflightStatusFixture(t *testing.T) (string, preflightReceipt, string) {
	t.Helper()
	repo, head := completionRepo(t)
	completionGit(t, repo, "remote", "add", "origin", "git@github.com:division-sh/swarm.git")
	t.Chdir(repo)
	build, err := testplanning.EffectiveBuildContext(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	const pkg = "github.com/division-sh/swarm/cmd/swarm-test"
	plan := testplanning.PreflightPlan{Version: 1, HeadSHA: head, BaseSHA: head, BuildContext: build,
		Packages: []testplanning.PreflightPackage{{Package: pkg, Run: "^(TestGuard)$", Required: []testplanning.RequiredTest{
			{TestRoot: testplanning.TestRoot{Package: pkg, Name: "TestGuard"}},
		}}},
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Digest = fmt.Sprintf("%x", sha256.Sum256(raw))
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	const attemptID = "90ca6bd3-114d-4c7a-8af6-30e173bb1b6b"
	receiptID := started.Format("20060102T150405.000000000") + "-" + attemptID
	receipt := preflightReceipt{Plan: plan, Started: started, Complete: true, Attestation: preflightStatusIdentity{
		Repository: "division-sh/swarm", RepositoryNodeID: "R_fixture", AttemptID: attemptID, ReceiptID: receiptID, StartStatusID: 101,
	}}
	for i, phase := range preflightPhases(plan) {
		at := started.Add(time.Duration(i) * time.Second)
		receipt.Phases = append(receipt.Phases, preflightPhaseReceipt{Name: phase.Name, Args: slices.Clone(phase.Args), Started: at, Completed: at.Add(time.Millisecond), Passed: true})
	}
	directory := filepath.Join(repo, "test-results", "preflight", receiptID)
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	writePreflightStatusFixtureReceipt(t, directory, receipt)
	bin := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nexec \"$PREFLIGHT_STATUS_TEST_BINARY\" -test.run='^TestPreflightStatusCommandProcess$' -- \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(directory, "api-calls.jsonl")
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("PREFLIGHT_STATUS_TEST_BINARY", executable)
	t.Setenv("PREFLIGHT_STATUS_TEST_PROCESS", "1")
	t.Setenv("PREFLIGHT_STATUS_TEST_HEAD", head)
	t.Setenv("PREFLIGHT_STATUS_TEST_CALLS", calls)
	t.Setenv("PREFLIGHT_STATUS_TEST_RECEIPT", filepath.Join(directory, "receipt.json"))
	t.Setenv("PREFLIGHT_STATUS_TEST_MODE", "")
	t.Setenv("GH_REPO", "")
	t.Setenv("GH_HOST", "")
	return directory, receipt, calls
}

func startPreflightStatusFixture(t *testing.T, receipt preflightReceipt) *preflightStatusAttempt {
	t.Helper()
	attempt, err := beginPreflightStatus(context.Background(), receipt.Plan.HeadSHA, receipt.Attestation.ReceiptID, receipt.Attestation.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.identity != receipt.Attestation {
		t.Fatalf("unexpected start identity: %+v", attempt.identity)
	}
	if err := attempt.bindPlan(receipt.Plan); err != nil {
		t.Fatal(err)
	}
	return attempt
}

func writePreflightStatusFixtureReceipt(t *testing.T, directory string, receipt preflightReceipt) {
	t.Helper()
	raw, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "receipt.json"), append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func readPreflightStatusCalls(t *testing.T, path string) []struct {
	Args  []string        `json:"args"`
	Input json.RawMessage `json:"input,omitempty"`
} {
	t.Helper()
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var calls []struct {
		Args  []string        `json:"args"`
		Input json.RawMessage `json:"input,omitempty"`
	}
	decoder := json.NewDecoder(file)
	for {
		var call struct {
			Args  []string        `json:"args"`
			Input json.RawMessage `json:"input,omitempty"`
		}
		if err := decoder.Decode(&call); errors.Is(err, io.EOF) {
			return calls
		} else if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
}

func TestPreflightStatusPublishesOnlyCompletedExactHead(t *testing.T) {
	directory, receipt, callsPath := preflightStatusFixture(t)
	attempt := startPreflightStatusFixture(t, receipt)
	statusID, err := attempt.finish(context.Background(), directory, receipt)
	if err != nil || statusID != 123 {
		t.Fatalf("exact attestation: %d / %v", statusID, err)
	}
	calls := readPreflightStatusCalls(t, callsPath)
	want := [][]string{
		{"repo", "view", "git@github.com:division-sh/swarm.git", "--json", "nameWithOwner,url,id"},
		{"api", "--hostname", "github.com", "repos/division-sh/swarm/commits/" + receipt.Plan.HeadSHA},
		{"api", "--hostname", "github.com", "--method", "POST", "repos/division-sh/swarm/statuses/" + receipt.Plan.HeadSHA, "--input", "-"},
		{"repo", "view", "git@github.com:division-sh/swarm.git", "--json", "nameWithOwner,url,id"},
		{"api", "--hostname", "github.com", "--method", "POST", "repos/division-sh/swarm/statuses/" + receipt.Plan.HeadSHA, "--input", "-"},
	}
	if len(calls) != len(want) {
		t.Fatalf("calls=%+v", calls)
	}
	for i := range want {
		if !slices.Equal(calls[i].Args, want[i]) {
			t.Fatalf("call%d=%v want=%v", i, calls[i].Args, want[i])
		}
	}
	for i, state := range []string{"pending", "success"} {
		var posted map[string]string
		if err := json.Unmarshal(calls[2+i*2].Input, &posted); err != nil || !reflect.DeepEqual(posted, map[string]string{
			"state": state, "context": "preflight/local", "description": "attempt:" + receipt.Attestation.AttemptID + " receipt:" + receipt.Attestation.ReceiptID,
		}) {
			t.Fatalf("status body=%v err=%v", posted, err)
		}
	}
	if _, err := attempt.finish(context.Background(), directory, receipt); err == nil || len(readPreflightStatusCalls(t, callsPath)) != 5 {
		t.Fatalf("ended attempt gained repeat authority: %v", err)
	}
}

func TestPreflightStatusRefusesIncompleteOrChangedEvidence(t *testing.T) {
	for _, mode := range []string{"problem", "unstarted", "missing-phase", "failed-phase", "foreign-phase", "foreign-command", "bad-chronology", "invalid-plan", "different-valid-plan", "missing-receipt", "corrupt-receipt", "changed-receipt", "wrong-id", "foreign-attempt", "dirty-source", "changed-head", "changed-build", "cancelled", "changed-origin", "repository-override", "host-override"} {
		t.Run(mode, func(t *testing.T) {
			directory, receipt, calls := preflightStatusFixture(t)
			attempt := startPreflightStatusFixture(t, receipt)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "problem":
				receipt.Problem = "failed proof"
			case "unstarted":
				receipt.Unstarted = []string{"vet"}
			case "missing-phase":
				receipt.Phases = receipt.Phases[:len(receipt.Phases)-1]
			case "failed-phase":
				receipt.Phases[0].Passed = false
			case "foreign-phase":
				receipt.Phases[0].Name = "invented"
			case "foreign-command":
				receipt.Phases[0].Args = []string{"true"}
			case "bad-chronology":
				receipt.Phases[0].Completed = receipt.Phases[0].Started.Add(-time.Second)
			case "invalid-plan":
				receipt.Plan.HeadSHA = strings.Repeat("a", 40)
			case "different-valid-plan":
				receipt.Plan.BaseSHA = strings.Repeat("b", 40)
				receipt.Plan.Digest = ""
				raw, err := json.Marshal(receipt.Plan)
				if err != nil {
					t.Fatal(err)
				}
				receipt.Plan.Digest = fmt.Sprintf("%x", sha256.Sum256(raw))
				if err := receipt.Plan.Validate(); err != nil {
					t.Fatal(err)
				}
			case "wrong-id":
				receipt.Started = receipt.Started.Add(time.Second)
			case "foreign-attempt":
				receipt.Attestation.AttemptID = "62a856ec-3954-46db-a68d-0a8c93537969"
			case "dirty-source":
				if err := os.WriteFile("source_test.go", []byte("package proof\n// changed\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "changed-head":
				completionGit(t, ".", "commit", "--allow-empty", "--quiet", "-m", "test: changed head")
			case "changed-build":
				t.Setenv("GOFLAGS", "-tags=foreign_preflight")
			case "cancelled":
				cancel()
			case "changed-origin":
				completionGit(t, ".", "remote", "set-url", "origin", "git@github.com:foreign/repository.git")
			case "repository-override":
				t.Setenv("GH_REPO", "foreign/repository")
			case "host-override":
				t.Setenv("GH_HOST", "foreign.invalid")
			}
			writePreflightStatusFixtureReceipt(t, directory, receipt)
			switch mode {
			case "missing-receipt":
				if err := os.Remove(filepath.Join(directory, "receipt.json")); err != nil {
					t.Fatal(err)
				}
			case "corrupt-receipt", "changed-receipt":
				raw := []byte("{broken")
				if mode == "changed-receipt" {
					changed := receipt
					changed.EndToEndSeconds++
					raw, _ = json.Marshal(changed)
				}
				if err := os.WriteFile(filepath.Join(directory, "receipt.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			statusID, err := attempt.finish(ctx, directory, receipt)
			if err == nil || statusID != 0 || len(readPreflightStatusCalls(t, calls)) != 3 {
				t.Fatalf("refusal gained terminal authority: %d / %v", statusID, err)
			}
			if mode == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
}

func TestPreflightStatusRefusesFailedOrContradictoryAPI(t *testing.T) {
	for _, mode := range []string{"auth-error", "bad-json", "foreign-host", "bad-repository", "unpublished", "foreign-commit", "mutate-source", "mutate-receipt", "post-error", "bad-status", "wrong-context", "wrong-description", "non-success", "changed-repository-id", "changed-repository-name"} {
		t.Run(mode, func(t *testing.T) {
			directory, receipt, callsPath := preflightStatusFixture(t)
			late := slices.Contains([]string{"mutate-receipt", "post-error", "bad-status", "wrong-context", "wrong-description", "non-success", "changed-repository-id", "changed-repository-name"}, mode)
			var attempt *preflightStatusAttempt
			if late {
				attempt = startPreflightStatusFixture(t, receipt)
			}
			t.Setenv("PREFLIGHT_STATUS_TEST_MODE", mode)
			var err error
			if late {
				if mode == "mutate-receipt" {
					if err := os.WriteFile(filepath.Join(directory, "receipt.json"), []byte("{corrupt"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				_, err = attempt.finish(context.Background(), directory, receipt)
			} else {
				_, err = beginPreflightStatus(context.Background(), receipt.Plan.HeadSHA, receipt.Attestation.ReceiptID, receipt.Attestation.AttemptID)
			}
			if err == nil || strings.Contains(err.Error(), "SENSITIVE_AUTH_DIAGNOSTIC") {
				t.Fatalf("invalid API result accepted or disclosed: %v", err)
			}
			posts := 0
			for _, call := range readPreflightStatusCalls(t, callsPath) {
				if slices.Contains(call.Args, "POST") {
					posts++
				}
			}
			want := 0
			if late {
				want++
			}
			if slices.Contains([]string{"post-error", "bad-status", "wrong-context", "wrong-description", "non-success"}, mode) {
				want++
			}
			if posts != want {
				t.Fatalf("status writes=%d want=%d; no automatic replay allowed", posts, want)
			}
		})
	}
}

func TestPreflightStatusFailureAndInterruptedStartsNeverAttestSuccess(t *testing.T) {
	for _, mode := range []string{"failure", "cancelled", "hard-death", "pending-post-error", "pending-lost-ack", "pending-wrong-context", "terminal-lost-ack"} {
		t.Run(mode, func(t *testing.T) {
			directory, receipt, callsPath := preflightStatusFixture(t)
			if strings.HasPrefix(mode, "pending-") {
				transport := strings.TrimPrefix(mode, "pending-")
				if transport == "lost-ack" {
					transport = "bad-json-post"
				}
				t.Setenv("PREFLIGHT_STATUS_TEST_MODE", transport)
				if attempt, err := beginPreflightStatus(context.Background(), receipt.Plan.HeadSHA, receipt.Attestation.ReceiptID, receipt.Attestation.AttemptID); err == nil || attempt != nil {
					t.Fatalf("unacknowledged start admitted: %v", err)
				}
			} else if mode == "hard-death" {
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				command := exec.Command(binary, "-test.run=^TestPreflightStatusDiesAfterAcknowledgedStart$")
				command.Env = append(os.Environ(), "PREFLIGHT_STATUS_START_THEN_DIE=1", "PREFLIGHT_STATUS_TEST_ATTEMPT="+receipt.Attestation.AttemptID, "PREFLIGHT_STATUS_TEST_RECEIPT_ID="+receipt.Attestation.ReceiptID)
				if out, err := command.CombinedOutput(); err == nil {
					t.Fatalf("death probe did not die: %s", out)
				}
			} else {
				attempt := startPreflightStatusFixture(t, receipt)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				receipt.Complete = false
				receipt.Problem = "failed phase"
				writePreflightStatusFixtureReceipt(t, directory, receipt)
				if mode == "cancelled" {
					cancel()
				} else if mode == "terminal-lost-ack" {
					t.Setenv("PREFLIGHT_STATUS_TEST_MODE", "bad-json-post")
				}
				_, err := attempt.finish(ctx, directory, receipt)
				if (err == nil) != (mode == "failure") {
					t.Fatalf("failure publication outcome: %v", err)
				}
				if _, err := attempt.finish(context.Background(), directory, receipt); err == nil {
					t.Fatal("failed/interrupted publication automatically replayed")
				}
			}
			var states []string
			for _, call := range readPreflightStatusCalls(t, callsPath) {
				if len(call.Input) == 0 {
					continue
				}
				var body map[string]string
				if err := json.Unmarshal(call.Input, &body); err != nil {
					t.Fatal(err)
				}
				states = append(states, body["state"])
			}
			want := []string{"pending"}
			if mode == "failure" || mode == "terminal-lost-ack" {
				want = append(want, "failure")
			}
			if !slices.Equal(states, want) {
				t.Fatalf("failure/death created success or replay: %v want=%v", states, want)
			}
		})
	}
}

func TestPreflightStatusDiesAfterAcknowledgedStart(t *testing.T) {
	if os.Getenv("PREFLIGHT_STATUS_START_THEN_DIE") != "1" {
		return
	}
	_, err := beginPreflightStatus(context.Background(), os.Getenv("PREFLIGHT_STATUS_TEST_HEAD"), os.Getenv("PREFLIGHT_STATUS_TEST_RECEIPT_ID"), os.Getenv("PREFLIGHT_STATUS_TEST_ATTEMPT"))
	if err != nil {
		t.Fatal(err)
	}
	os.Exit(13)
}

func TestPreflightStatusConcurrentFinishHasOneTerminalOwner(t *testing.T) {
	directory, receipt, calls := preflightStatusFixture(t)
	attempt := startPreflightStatusFixture(t, receipt)
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := attempt.finish(context.Background(), directory, receipt)
			results <- err
		}()
	}
	passed := 0
	for range 2 {
		if err := <-results; err == nil {
			passed++
		}
	}
	if passed != 1 || len(readPreflightStatusCalls(t, calls)) != 5 {
		t.Fatalf("concurrent terminal publication owners=%d", passed)
	}
}

func TestPreflightStatusCannotImportOrRebindSuccessPlan(t *testing.T) {
	directory, receipt, calls := preflightStatusFixture(t)
	attempt, err := beginPreflightStatus(context.Background(), receipt.Plan.HeadSHA, receipt.Attestation.ReceiptID, receipt.Attestation.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := attempt.finish(context.Background(), directory, receipt); err == nil || len(readPreflightStatusCalls(t, calls)) != 3 {
		t.Fatal("imported completed receipt gained success without this attempt's plan")
	}
	if err := attempt.bindPlan(receipt.Plan); err == nil {
		t.Fatal("ended attempt adopted a success plan")
	}
	attempt = startPreflightStatusFixture(t, receipt)
	if err := attempt.bindPlan(receipt.Plan); err == nil {
		t.Fatal("attempt replaced its frozen plan")
	}
}

func TestPreflightStatusRejectsEarlyOverridesAndInvalidAttempts(t *testing.T) {
	for _, mode := range []string{"repository", "host", "invalid-attempt", "foreign-receipt", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			_, receipt, calls := preflightStatusFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "repository":
				t.Setenv("GH_REPO", "foreign/repository")
			case "host":
				t.Setenv("GH_HOST", "foreign.invalid")
			case "invalid-attempt":
				receipt.Attestation.AttemptID = "invented"
			case "foreign-receipt":
				receipt.Attestation.ReceiptID = "foreign"
			case "cancelled":
				cancel()
			}
			attempt, err := beginPreflightStatus(ctx, receipt.Plan.HeadSHA, receipt.Attestation.ReceiptID, receipt.Attestation.AttemptID)
			if err == nil || attempt != nil || len(readPreflightStatusCalls(t, calls)) != 0 {
				t.Fatalf("early refusal gained publication authority: %v", err)
			}
		})
	}
}

func TestPreflightStatusWriterFeedsActualCIReader(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct{ Name, Run string }
		}
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	steps := workflow.Jobs["ci-preflight"].Steps
	if len(steps) != 1 || steps[0].Name != "Admit exact source before checkout" {
		t.Fatal("missing actual CI status-consumption owner")
	}
	directory, receipt, _ := preflightStatusFixture(t)
	attempt := startPreflightStatusFixture(t, receipt)
	event, err := json.Marshal(map[string]any{"repository": map[string]string{"full_name": "division-sh/swarm"}, "action": "synchronize",
		"pull_request": map[string]any{"draft": false, "head": map[string]any{"repo": map[string]string{"full_name": "division-sh/swarm"}, "sha": receipt.Plan.HeadSHA, "ref": "agent-f/topic"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	eventPath := filepath.Join(directory, "event.json")
	if err := os.WriteFile(eventPath, event, 0600); err != nil {
		t.Fatal(err)
	}
	read := func() error {
		command := exec.Command("bash", "-c", steps[0].Run)
		command.Env = append(os.Environ(), "EVENT_NAME=pull_request", "GITHUB_REF=refs/pull/17/merge", "GITHUB_SHA="+strings.Repeat("b", 40),
			"GITHUB_REPOSITORY=division-sh/swarm", "GITHUB_EVENT_PATH="+eventPath, "RUNNER_TEMP="+directory, "GITHUB_STEP_SUMMARY="+filepath.Join(directory, "summary"))
		out, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("actual CI reader: %w: %s", err, out)
		}
		return nil
	}
	if err := read(); err == nil {
		t.Fatal("writer's pending attempt gained CI admission")
	}
	if _, err := attempt.finish(context.Background(), directory, receipt); err != nil {
		t.Fatal(err)
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
}

func TestPreflightStatusCommandProcess(t *testing.T) {
	if os.Getenv("PREFLIGHT_STATUS_TEST_PROCESS") != "1" {
		return
	}
	args := os.Args[slices.Index(os.Args, "--")+1:]
	mode := os.Getenv("PREFLIGHT_STATUS_TEST_MODE")
	var input json.RawMessage
	if slices.Contains(args, "POST") {
		var err error
		input, err = io.ReadAll(os.Stdin)
		if err != nil {
			os.Exit(2)
		}
	}
	file, err := os.OpenFile(os.Getenv("PREFLIGHT_STATUS_TEST_CALLS"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(2)
	}
	err = json.NewEncoder(file).Encode(struct {
		Args  []string        `json:"args"`
		Input json.RawMessage `json:"input,omitempty"`
	}{args, input})
	if err = errors.Join(err, file.Close()); err != nil {
		os.Exit(2)
	}
	fail := func() { fmt.Fprintln(os.Stderr, "SENSITIVE_AUTH_DIAGNOSTIC"); os.Exit(1) }
	var response any
	switch {
	case slices.Contains(args, "--slurp"):
		var statuses []map[string]any
		for _, call := range readPreflightStatusCalls(t, os.Getenv("PREFLIGHT_STATUS_TEST_CALLS")) {
			if len(call.Input) == 0 {
				continue
			}
			var body map[string]string
			if json.Unmarshal(call.Input, &body) != nil {
				os.Exit(2)
			}
			id := 101
			if body["state"] != "pending" {
				id = 123
			}
			statuses = append(statuses, map[string]any{"id": id, "state": body["state"], "context": body["context"], "description": body["description"]})
		}
		slices.Reverse(statuses)
		response = [][]map[string]any{statuses}
	case len(args) > 1 && args[0] == "repo":
		if mode == "auth-error" {
			fail()
		}
		if mode == "bad-json" {
			fmt.Print("{bad")
			os.Exit(0)
		}
		name, coordinate := "division-sh/swarm", "https://github.com/division-sh/swarm"
		if mode == "foreign-host" {
			coordinate = "https://example.invalid/division-sh/swarm"
		}
		if mode == "bad-repository" {
			name = "division-sh/swarm/other"
		}
		if mode == "changed-repository-name" {
			name, coordinate = "foreign/repository", "https://github.com/foreign/repository"
		}
		id := "R_fixture"
		if mode == "changed-repository-id" {
			id = "R_recreated_repository"
		}
		response = map[string]string{"nameWithOwner": name, "url": coordinate, "id": id}
	case slices.Contains(args, "POST"):
		if mode == "post-error" {
			fail()
		}
		if mode == "bad-json-post" {
			fmt.Print("{lost acknowledgment")
			os.Exit(0)
		}
		var body map[string]string
		if json.Unmarshal(input, &body) != nil {
			os.Exit(2)
		}
		response = map[string]any{"id": 123, "state": body["state"], "context": body["context"], "description": body["description"]}
		result := response.(map[string]any)
		if body["state"] == "pending" {
			result["id"] = 101
		}
		switch mode {
		case "bad-status":
			result["id"] = 0
		case "wrong-context":
			result["context"] = "foreign/context"
		case "wrong-description":
			result["description"] = "receipt:foreign"
		case "non-success":
			result["state"] = "pending"
		}
	default:
		if mode == "unpublished" {
			fail()
		}
		head := os.Getenv("PREFLIGHT_STATUS_TEST_HEAD")
		if mode == "foreign-commit" {
			head = strings.Repeat("b", 40)
		}
		if mode == "mutate-source" {
			if os.WriteFile("source_test.go", []byte("package proof\n// late mutation\n"), 0600) != nil {
				os.Exit(2)
			}
		}
		if mode == "mutate-receipt" && os.WriteFile(os.Getenv("PREFLIGHT_STATUS_TEST_RECEIPT"), []byte("{corrupt"), 0600) != nil {
			os.Exit(2)
		}
		response = map[string]string{"sha": head}
	}
	if json.NewEncoder(os.Stdout).Encode(response) != nil {
		os.Exit(2)
	}
	os.Exit(0)
}
