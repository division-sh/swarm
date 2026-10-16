package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

// These are bound by the supervisor's build from approved immutable source.
// An internal invocation is not itself a certificate trust boundary.
var accountingExecutorSource, accountingExecutorDigest, accountingExecutorRoot string

type accountingPlan struct {
	Version    int                         `json:"version"`
	Base       string                      `json:"base"`
	Head       string                      `json:"head"`
	SourceTree string                      `json:"source_tree"`
	Policy     accountingPolicyObservation `json:"policy"`
	Previous   formatproof.History         `json:"previous"`
	Output     formatproof.History         `json:"output"`
	Rows       int                         `json:"rows"`
	Planner    []testplanning.TestRoot     `json:"planner_read_only"`
	Coverage   testplanning.PreflightPlan  `json:"planner_coverage_read_only"`
}

func runAccounting(ctx context.Context, args []string) int {
	err := superviseAccounting(ctx, args, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "accounting refused:", err)
		return 1
	}
	return 0
}

func superviseAccounting(ctx context.Context, args []string, out, stderr io.Writer) (result error) {
	if len(args) == 0 || (args[0] != "plan" && args[0] != "apply" && args[0] != "certify") {
		return fmt.Errorf("usage: swarm-test accounting plan|apply|certify [--plan file | --receipt file]")
	}
	policy, err := observeAccountingPolicy(ctx, accountingGitHub)
	if err != nil {
		return err
	}
	root, err := accountingGit(ctx, ".", "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	repo := strings.TrimSpace(string(root))
	frozen, err := completionSource(repo, "", true)
	if err != nil {
		return err
	}
	if err = fetchAccountingExecutor(ctx, repo, policy.Policy, accountingGitHub); err != nil {
		return err
	}
	directory, err := archiveAccountingExecutor(ctx, repo, policy.Policy.ExecutorSource)
	if err != nil {
		return err
	}
	defer func() {
		if directory != "" {
			result = errors.Join(result, os.RemoveAll(directory))
		}
	}()
	inputs, err := accountingClosure(ctx, directory)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(inputs, policy.Policy.Inputs) {
		return fmt.Errorf("approved accounting dependency names or bytes changed")
	}
	goVersion, err := accountingGitHubToolchain(ctx, directory)
	if err != nil || goVersion != "go1.26.8" {
		return fmt.Errorf("accounting requires its approved Go1.26.8 toolchain: %v", err)
	}
	binary := filepath.Join(directory, "accounting-executor")
	link := "-X main.accountingExecutorSource=" + policy.Policy.ExecutorSource + " -X main.accountingExecutorDigest=" + policy.Policy.DependencyDigest + " -X main.accountingExecutorRoot=" + directory
	build := []string{"go", "-C", directory, "build", "-mod=readonly", "-trimpath", "-ldflags", link, "-o", binary, "./cmd/swarm-test"}
	if _, err = runPreflightProcessEnvironment(ctx, build, stderr, stderr, accountingBuildEnv(runtime.GOOS, runtime.GOARCH)); err != nil {
		return err
	}
	after, err := accountingClosure(ctx, directory)
	if err != nil || !reflect.DeepEqual(inputs, after) {
		return fmt.Errorf("accounting executor closure changed during build: %v", err)
	}
	if err = requireAccountingPolicy(ctx, accountingGitHub, policy); err != nil {
		return err
	}
	// The candidate executable is not used to evaluate candidate equivalence.
	var completed bytes.Buffer
	if err = runApprovedAccountingProcess(ctx, binary, args, &completed, stderr, policy, accountingGitHub); err != nil {
		return err
	}
	if err = os.RemoveAll(directory); err != nil {
		return err
	}
	directory = ""
	if err = checkAccountingWorkingSource(ctx, repo, frozen, args[0] == "apply"); err != nil {
		return err
	}
	if err = requireAccountingPolicy(ctx, accountingGitHub, policy); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	_, err = io.Copy(out, &completed)
	return err
}

func checkAccountingWorkingSource(ctx context.Context, root, head string, applying bool) error {
	if _, err := completionSource(root, head, !applying); err != nil {
		return err
	}
	if !applying {
		return nil
	}
	status, err := accountingGit(ctx, root, "status", "--porcelain", "-z", "--untracked-files=all")
	if err != nil {
		return err
	}
	if string(status) != " M "+formatproof.Successors+"\x00" {
		return fmt.Errorf("accounting apply changed more than its single unstaged successor output")
	}
	raw, err := os.ReadFile(filepath.Join(root, formatproof.Successors))
	if err != nil {
		return err
	}
	history, err := formatproof.Read(raw)
	if err != nil {
		return err
	}
	endpoint, _, err := history.Endpoint()
	if err != nil || endpoint == nil {
		return fmt.Errorf("accounting apply lacks its actual endpoint: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(root, formatproof.Source))
	if err != nil || !bytes.Equal(body, endpoint) {
		return fmt.Errorf("accounting apply source changed before completion: %v", err)
	}
	return nil
}

func runApprovedAccountingProcess(ctx context.Context, binary string, args []string, out, stderr io.Writer, policy accountingPolicyObservation, remote accountingRemote) error {
	var completed bytes.Buffer
	_, err := runPreflightProcessEnvironment(ctx, append([]string{binary, "--internal-accounting-executor"}, args...), &completed, stderr, accountingBuildEnv(runtime.GOOS, runtime.GOARCH))
	if err != nil {
		return err
	}
	if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	_, err = io.Copy(out, &completed)
	return err
}

func accountingGitHubToolchain(ctx context.Context, root string) (string, error) {
	command := execAccountingGo(ctx, root, "env", "GOVERSION")
	raw, err := command.Output()
	return strings.TrimSpace(string(raw)), err
}

func execAccountingGo(ctx context.Context, root string, args ...string) *exec.Cmd {
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = root
	command.Env = accountingBuildEnv(runtime.GOOS, runtime.GOARCH)
	return command
}

func runAccountingExecutor(ctx context.Context, args []string) int {
	err := executeAccounting(ctx, args, os.Stdout, accountingGitHub)
	if err != nil {
		fmt.Fprintln(os.Stderr, "accounting refused:", err)
		return 1
	}
	return 0
}

func executeAccounting(ctx context.Context, args []string, out io.Writer, remote accountingRemote) error {
	if len(args) == 0 {
		return fmt.Errorf("accounting operation is required")
	}
	policy, err := observeAccountingPolicy(ctx, remote)
	if err != nil {
		return err
	}
	if accountingExecutorSource != policy.Policy.ExecutorSource || accountingExecutorDigest != policy.Policy.DependencyDigest || accountingExecutorRoot == "" {
		return fmt.Errorf("candidate executable cannot approve its own accounting")
	}
	inputs, err := accountingClosure(ctx, accountingExecutorRoot)
	if err != nil || !reflect.DeepEqual(inputs, policy.Policy.Inputs) {
		return fmt.Errorf("executor no longer matches approved closure: %v", err)
	}
	flags := flag.NewFlagSet("accounting "+args[0], flag.ContinueOnError)
	planFile := flags.String("plan", "", "frozen read-only accounting plan")
	receiptFile := flags.String("receipt", "", "existing certificate to independently verify")
	if err = flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || (*planFile != "" && *receiptFile != "") {
		return fmt.Errorf("accounting accepts one exact plan or receipt, not overrides")
	}
	head, err := completionSource(".", "", true)
	if err != nil {
		return err
	}
	rootRaw, err := accountingGit(ctx, ".", "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	root := strings.TrimSpace(string(rootRaw))
	switch args[0] {
	case "plan":
		if *planFile != "" || *receiptFile != "" {
			return fmt.Errorf("plan reads the clean source and has no input override")
		}
		return planApprovedAccounting(ctx, root, head, out, remote, policy)
	case "apply":
		return applyApprovedAccounting(ctx, root, head, *planFile, out, remote, policy)
	case "certify":
		if *receiptFile != "" {
			return verifyAccountingReceipt(ctx, root, head, *receiptFile, out, remote, policy)
		}
		if *planFile == "" {
			return fmt.Errorf("certify requires its plan or prior certificate")
		}
		var plan accountingPlan
		if err = readAccountingJSON(*planFile, &plan); err != nil {
			return err
		}
		return certifyAccounting(ctx, root, head, plan, out, remote, policy)
	default:
		return fmt.Errorf("unknown accounting operation")
	}
}

func planApprovedAccounting(ctx context.Context, root, head string, out io.Writer, remote accountingRemote, policy accountingPolicyObservation) error {
	originRaw, err := accountingGit(ctx, root, "rev-parse", "origin/master^{commit}")
	if err != nil {
		return err
	}
	base, _, err := observePreflightComparison(ctx, head, strings.TrimSpace(string(originRaw)))
	if err != nil {
		return err
	}
	plan, err := planAccounting(ctx, root, base, head, policy)
	if err != nil {
		return err
	}
	if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	return writeAccountingJSON(out, plan)
}

func applyApprovedAccounting(ctx context.Context, root, head, path string, out io.Writer, remote accountingRemote, policy accountingPolicyObservation) error {
	if path == "" {
		return fmt.Errorf("apply requires its exact frozen plan")
	}
	var plan accountingPlan
	if err := readAccountingJSON(path, &plan); err != nil {
		return err
	}
	if head != plan.Head {
		return fmt.Errorf("accounting source moved before apply")
	}
	if err := replayAccountingPlan(ctx, root, plan, policy); err != nil {
		return err
	}
	var err error
	if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	if plan.Rows == 0 {
		return fmt.Errorf("accounting is not applicable; no rows regenerated")
	}
	if err = writeFiniteHistory(root, plan.Output); err != nil {
		return err
	}
	if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "accounting successor prepared; commit and certify the frozen head; no certification yet")
	return err
}

func verifyAccountingPolicyHistory(ctx context.Context, root string, expected, current accountingPolicyObservation) error {
	if expected.Digest != current.Digest || expected.Blob != current.Blob || !reflect.DeepEqual(expected.Policy, current.Policy) {
		return fmt.Errorf("historical accounting approval differs from fresh protected policy")
	}
	if err := checkAccountingBase(ctx, root, expected.Commit, current); err != nil {
		return err
	}
	raw, err := gitAccountingBlob(ctx, root, expected.Commit, accountingPolicyPath)
	if err != nil || formatproof.Digest(raw) != current.Digest {
		return fmt.Errorf("claimed historical approval was not the actual protected policy: %v", err)
	}
	return nil
}

func replayAccountingPlan(ctx context.Context, root string, plan accountingPlan, current accountingPolicyObservation) error {
	if err := verifyAccountingPolicyHistory(ctx, root, plan.Policy, current); err != nil {
		return err
	}
	fresh, err := planAccounting(ctx, root, plan.Base, plan.Head, current)
	if err != nil {
		return err
	}
	// Master may advance without changing the policy. Preserve the authenticated
	// planning observation, not a caller-chosen replacement for current authority.
	fresh.Policy = plan.Policy
	if !reflect.DeepEqual(fresh, plan) {
		return fmt.Errorf("accounting plan changed")
	}
	return nil
}

func readAccountingJSON(path string, out any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("accounting input is not a regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return formatproof.Decode(raw, out)
}

func writeAccountingJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func gitAccountingBlob(ctx context.Context, root, commit, path string) ([]byte, error) {
	if !hexIdentity(commit, 40) || !filepath.IsLocal(path) {
		return nil, fmt.Errorf("invalid accounting source identity")
	}
	entry, err := accountingGit(ctx, root, "ls-tree", commit, "--", path)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(string(entry), "100644 blob ") && !strings.HasPrefix(string(entry), "100755 blob ") {
		return nil, fmt.Errorf("accounting source is missing or nonregular: %s", path)
	}
	return accountingGit(ctx, root, "show", commit+":"+path)
}

func accountingHistoryAt(ctx context.Context, root, head string) (formatproof.History, error) {
	raw, err := gitAccountingBlob(ctx, root, head, formatproof.Successors)
	if err != nil {
		return formatproof.History{}, err
	}
	return formatproof.Read(raw)
}

func accountingSourceTree(ctx context.Context, root, head string) (string, error) {
	raw, err := accountingGit(ctx, root, "ls-tree", "-r", "-z", head)
	if err != nil {
		return "", err
	}
	var retained []byte
	for _, entry := range bytes.Split(raw, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		_, name, ok := bytes.Cut(entry, []byte{'\t'})
		if !ok {
			return "", fmt.Errorf("invalid accounting tree entry")
		}
		if string(name) == formatproof.Successors {
			continue
		}
		retained = append(retained, entry...)
		retained = append(retained, 0)
	}
	return formatproof.Digest(retained), nil
}

func planAccounting(ctx context.Context, root, base, head string, policy accountingPolicyObservation) (accountingPlan, error) {
	if err := checkAccountingBase(ctx, root, base, policy); err != nil {
		return accountingPlan{}, err
	}
	result, err := deriveFiniteAccountingPlan(ctx, root, base, head, policy)
	if err != nil {
		return result, err
	}
	// P1 is report-only; the original scanner/coverage envelope is not rewritten.
	result.Planner, err = testplanning.StaticAuthorityGuardRoots(root)
	if err != nil {
		return result, err
	}
	observedBase, comparison, err := derivePreflightComparison(ctx, head, base)
	if err != nil || observedBase != base {
		return result, fmt.Errorf("frozen accounting comparison changed: %v", err)
	}
	result.Coverage, err = buildPreflightPlan(ctx, head, base, comparison)
	if err != nil {
		return result, err
	}
	for _, identity := range result.Planner {
		covered := 0
		for _, pkg := range result.Coverage.Packages {
			for _, selected := range pkg.Required {
				if selected.Package == identity.Package && selected.Name == identity.Name {
					covered++
				}
			}
		}
		if covered != 1 {
			return result, fmt.Errorf("planner read-only identity %s/%s has coverage multiplicity %d", identity.Package, identity.Name, covered)
		}
	}
	return result, nil
}

func checkAccountingBase(ctx context.Context, root, base string, policy accountingPolicyObservation) error {
	if !hexIdentity(base, 40) || !hexIdentity(policy.Commit, 40) {
		return fmt.Errorf("unbound accounting trusted base")
	}
	if _, err := accountingGit(ctx, root, "cat-file", "-e", policy.Commit+"^{commit}"); err != nil {
		if _, err = accountingGit(ctx, root, "fetch", "--no-tags", "--depth=1", "https://github.com/"+accountingRepository+".git", policy.Commit); err != nil {
			return fmt.Errorf("authenticated policy ancestry unavailable: %w", err)
		}
	}
	if _, err := accountingGit(ctx, root, "merge-base", "--is-ancestor", base, policy.Commit); err != nil {
		return fmt.Errorf("comparison base is not authenticated protected-master history")
	}
	return nil
}

func deriveFiniteAccountingPlan(ctx context.Context, root, base, head string, policy accountingPolicyObservation) (accountingPlan, error) {
	result := accountingPlan{Version: 1, Base: base, Head: head, Policy: policy}
	if err := policy.Policy.validate(); err != nil {
		return result, err
	}
	if err := checkFiniteAccountingDiff(ctx, root, base, head, policy.Policy.Inputs); err != nil {
		return result, err
	}
	if err := checkImmutableFiniteLedger(ctx, root, base, head); err != nil {
		return result, err
	}
	previous, err := finiteAccountingPredecessor(ctx, root, base, head)
	if err != nil {
		return result, err
	}
	before, err := gitAccountingBlob(ctx, root, base, formatproof.Source)
	if err != nil {
		return result, err
	}
	after, err := gitAccountingBlob(ctx, root, head, formatproof.Source)
	if err != nil {
		return result, err
	}
	endpoint, _, err := previous.Endpoint()
	if err != nil {
		return result, err
	}
	if endpoint == nil {
		if formatproof.Digest(before) != formatproof.Origin {
			return result, fmt.Errorf("trusted base already has unexplained finite drift")
		}
	} else if !bytes.Equal(endpoint, before) {
		return result, fmt.Errorf("trusted finite base is stale")
	}
	result.Previous, result.Output = previous, previous
	if !bytes.Equal(before, after) {
		result.Output, err = previous.Append(before, after)
		if err != nil {
			return result, err
		}
		result.Rows = 1
	}
	result.SourceTree, err = accountingSourceTree(ctx, root, head)
	return result, err
}

func checkFiniteAccountingDiff(ctx context.Context, root, base, head string, inputs []accountingInput) error {
	if !hexIdentity(base, 40) || !hexIdentity(head, 40) {
		return fmt.Errorf("unbound accounting comparison")
	}
	if _, err := accountingGit(ctx, root, "merge-base", "--is-ancestor", base, head); err != nil {
		return fmt.Errorf("accounting base is not a predecessor")
	}
	changed, err := accountingGit(ctx, root, "diff", "--name-only", "-z", base, head, "--")
	if err != nil {
		return err
	}
	for _, name := range strings.Split(strings.TrimSuffix(string(changed), "\x00"), "\x00") {
		if name != "" && name != formatproof.Source {
			return fmt.Errorf("F1 initial rule refuses another changed source or accounting row: %s", name)
		}
	}
	for _, revision := range []string{base, head} {
		entry, err := accountingGit(ctx, root, "ls-tree", revision, "--", formatproof.Source)
		if err != nil || !strings.HasPrefix(string(entry), "100644 blob ") {
			return fmt.Errorf("F1 refuses noncanonical source mode or inclusion")
		}
	}
	for _, input := range inputs {
		if input.Path == formatproof.Source || input.Path == formatproof.Successors {
			continue
		}
		raw, err := gitAccountingBlob(ctx, root, head, input.Path)
		if err != nil || formatproof.Digest(raw) != input.SHA256 {
			return fmt.Errorf("candidate changed approved owner/guard dependency %s", input.Path)
		}
	}
	return nil
}

func checkImmutableFiniteLedger(ctx context.Context, root, base, head string) error {
	oldLedger, err := gitAccountingBlob(ctx, root, base, formatproof.Ledger)
	if err != nil {
		return err
	}
	newLedger, err := gitAccountingBlob(ctx, root, head, formatproof.Ledger)
	if err != nil || !bytes.Equal(oldLedger, newLedger) {
		return fmt.Errorf("immutable finite ledger changed")
	}
	// The source owner uses snake-case fields; inspect only its closed row identity.
	var rawLedger struct {
		Changes []struct {
			File      string `json:"file"`
			AfterHash string `json:"after_hash"`
		} `json:"changes"`
	}
	if err = json.Unmarshal(oldLedger, &rawLedger); err != nil {
		return err
	}
	matched := 0
	for _, row := range rawLedger.Changes {
		if row.File == formatproof.Source && row.AfterHash == formatproof.Origin {
			matched++
		}
	}
	if matched != 1 {
		return fmt.Errorf("finite ledger has no unique approved origin")
	}
	return nil
}

func finiteAccountingPredecessor(ctx context.Context, root, base, head string) (formatproof.History, error) {
	previous, err := accountingHistoryAt(ctx, root, base)
	if err != nil {
		return previous, err
	}
	current, err := accountingHistoryAt(ctx, root, head)
	if err != nil {
		return previous, err
	}
	if !reflect.DeepEqual(previous, current) {
		return previous, fmt.Errorf("plan cannot authorize prewritten successor edits")
	}
	return previous, nil
}

func writeFiniteHistory(root string, history formatproof.History) error {
	if _, _, err := history.Endpoint(); err != nil {
		return err
	}
	path := filepath.Join(root, formatproof.Successors)
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("finite output is not regular")
	}
	var buffer bytes.Buffer
	if err = writeAccountingJSON(&buffer, history); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".accounting-successor-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	_, writeErr := file.Write(buffer.Bytes())
	closeErr := errors.Join(file.Sync(), file.Close())
	if err = errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
