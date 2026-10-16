package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

type accountingGuardReceipt struct {
	Package string   `json:"package"`
	Run     string   `json:"run"`
	Roots   []string `json:"roots"`
	Log     string   `json:"log"`
	SHA256  string   `json:"sha256"`
}

type accountingCertificate struct {
	Version         int                         `json:"version"`
	Head            string                      `json:"head"`
	Tree            string                      `json:"tree"`
	Plan            accountingPlan              `json:"plan"`
	Policy          accountingPolicyObservation `json:"policy"`
	ProofSource     accountingProofManifest     `json:"proof_source"`
	Guards          []accountingGuardReceipt    `json:"guards"`
	Preflight       string                      `json:"preflight"`
	PreflightSHA256 string                      `json:"preflight_sha256"`
	Sample          []string                    `json:"sample"`
	HumanAudit      string                      `json:"human_audit"`
}

func accountingSample(head, base, policy string, records []formatproof.Record) []string {
	type ranked struct{ Rank, Identity string }
	var rows []ranked
	for _, record := range records {
		id := formatproof.RecordID(record)
		rows = append(rows, ranked{formatproof.Digest([]byte("c2-audit-v1\x00" + base + "\x00" + head + "\x00" + policy + "\x00" + id)), id})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Rank == rows[j].Rank {
			return rows[i].Identity < rows[j].Identity
		}
		return rows[i].Rank < rows[j].Rank
	})
	var sample []string
	for _, row := range rows[:(len(rows)+3)/4] {
		sample = append(sample, row.Identity)
	}
	return sample
}

func checkAccountingFinalSource(ctx context.Context, root, head string, plan accountingPlan, policy accountingPolicyObservation) error {
	if plan.Version != 1 || plan.Rows != 1 || plan.Policy.Digest != policy.Digest || !reflect.DeepEqual(plan.Policy.Policy, policy.Policy) {
		return fmt.Errorf("accounting plan is foreign, empty or no longer approved")
	}
	if err := replayAccountingPlan(ctx, root, plan, policy); err != nil {
		return err
	}
	actual, err := accountingHistoryAt(ctx, root, head)
	if err != nil || !reflect.DeepEqual(actual, plan.Output) {
		return fmt.Errorf("final successor differs from approved deterministic output: %v", err)
	}
	tree, err := accountingSourceTree(ctx, root, head)
	if err != nil || tree != plan.SourceTree {
		return fmt.Errorf("source changed after accounting plan")
	}
	if _, err = accountingSource(ctx, root, head, true); err != nil {
		return err
	}
	return nil
}

func certifyAccounting(ctx context.Context, root, head string, plan accountingPlan, out io.Writer, remote accountingRemote, policy accountingPolicyObservation) error {
	if accountingEvidenceRoot == "" || root != accountingProofRoot {
		return fmt.Errorf("accounting certification requires owned proof and evidence directories")
	}
	if err := verifyFrozenAccountingSource(ctx, root); err != nil {
		return err
	}
	if err := checkAccountingFinalSource(ctx, root, head, plan, policy); err != nil {
		return err
	}
	// No refresh environment reaches even the first independent guard.
	for _, entry := range os.Environ() {
		name, value, _ := strings.Cut(entry, "=")
		if value != "" && (strings.HasPrefix(name, "SWARM_UPDATE") || strings.HasPrefix(name, "SWARM_REFRESH") || strings.HasPrefix(name, "SWARM_DEBT_CACHE_PUBLISH")) {
			return fmt.Errorf("accounting refuses ambient update authority %s", name)
		}
	}
	directory, err := os.MkdirTemp(accountingEvidenceRoot, "certificate-")
	if err != nil {
		return err
	}
	certificate := accountingCertificate{Version: 1, Head: head, Plan: plan, Policy: policy, HumanAudit: "pending"}
	certificate.ProofSource, err = accountingTreeInputs(ctx, root, head, nil)
	if err != nil {
		return err
	}
	tree, err := accountingGit(ctx, root, "rev-parse", head+"^{tree}")
	if err != nil {
		return err
	}
	certificate.Tree = strings.TrimSpace(string(tree))
	guards, err := accountingGuardMap(ctx, root)
	if err != nil {
		return err
	}
	for _, required := range guards {
		if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
			return err
		}
		guard, err := runAccountingGuards(ctx, root, directory, required.Package, required.Run, required.Roots)
		if err != nil {
			return err
		}
		certificate.Guards = append(certificate.Guards, guard)
	}
	if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	if err = verifyFrozenAccountingSource(ctx, root); err != nil {
		return err
	}
	preflightLog, err := os.OpenFile(filepath.Join(directory, "preflight-command.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, runErr := runPreflightProcessDirectory(ctx, root, []string{"go", "run", "./cmd/swarm-test", "preflight"}, preflightLog, preflightLog, accountingBuildEnv(runtime.GOOS, runtime.GOARCH))
	if err = errors.Join(runErr, preflightLog.Close(), verifyFrozenAccountingSource(ctx, root)); err != nil {
		return fmt.Errorf("canonical preflight refused accounting certification: %w", err)
	}
	preflight, err := newestAccountingPreflight(root, head, plan.Base)
	if err != nil {
		return err
	}
	certificate.Preflight, err = retainAccountingPreflight(root, preflight, directory)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(root, preflight))
	if err != nil {
		return err
	}
	certificate.PreflightSHA256 = formatproof.Digest(raw)
	certificate.Sample = accountingSample(head, plan.Base, policy.Digest, plan.Output.Records[len(plan.Previous.Records):])
	if err = checkAccountingFinalSource(ctx, root, head, plan, policy); err != nil {
		return err
	}
	if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	path := filepath.Join(directory, "receipt.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	err = errors.Join(writeAccountingJSON(file, certificate), file.Close())
	if err != nil {
		return err
	}
	// Only this owned execution can publish. A saved JSON/log pair is not an
	// attestation; subsequent use replays provenance and executes the guards anew.
	if err = validateAccountingReceipt(ctx, root, head, path, remote, policy); err != nil {
		return err
	}
	return publishAccountingCertificate(ctx, out, remote, policy, certificate)
}

func accountingGuardMap(ctx context.Context, root string) ([]accountingGuardReceipt, error) {
	inventory, err := testplanning.DiscoverRootInventory(ctx, root)
	if err != nil {
		return nil, err
	}
	var required []accountingGuardReceipt
	for _, pkg := range []string{"github.com/division-sh/swarm/scripts/rewrite-stages-2566", "github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"} {
		roots := append([]string{}, inventory.Packages[pkg].Roots...)
		sort.Strings(roots)
		if len(roots) == 0 {
			return nil, fmt.Errorf("missing canonical finite guard package %s", pkg)
		}
		required = append(required, accountingGuardReceipt{Package: pkg, Run: ".", Roots: roots})
	}
	roots := []string{"TestPersistenceAuthorityFindingRegistry", "TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority", "TestPersistenceAuthorityDebtRatchet", "TestNativeFixtureFamiliesDoNotReceiveRawAuthority"}
	for _, name := range roots {
		found := false
		for _, actual := range inventory.Packages["github.com/division-sh/swarm/internal/store"].Roots {
			if name == actual {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("missing independent accounting guard %s", name)
		}
	}
	return append(required, accountingGuardReceipt{Package: "github.com/division-sh/swarm/internal/store", Run: "^(" + strings.Join(roots, "|") + ")$", Roots: roots}), nil
}

func runAccountingGuards(ctx context.Context, root, directory, pkg, run string, roots []string) (accountingGuardReceipt, error) {
	result := accountingGuardReceipt{Package: pkg, Run: run, Roots: append([]string{}, roots...)}
	if err := verifyFrozenAccountingSource(ctx, root); err != nil {
		return result, err
	}
	file, err := os.CreateTemp(directory, "guard-*.jsonl")
	if err != nil {
		return result, err
	}
	result.Log = file.Name()
	args := []string{"go", "run", "./cmd/swarm-test", "--", pkg, "-p=1", "-count=1", "-json", "-run", run}
	_, runErr := runPreflightProcessDirectory(ctx, root, args, file, os.Stderr, accountingBuildEnv(runtime.GOOS, runtime.GOARCH))
	err = errors.Join(runErr, file.Close(), verifyFrozenAccountingSource(ctx, root))
	if err != nil {
		return result, err
	}
	raw, err := os.ReadFile(result.Log)
	if err != nil {
		return result, err
	}
	if err = validateAccountingGuardLog(raw, pkg, roots); err != nil {
		return result, err
	}
	result.SHA256 = formatproof.Digest(raw)
	return result, nil
}

func validateAccountingGuardLog(raw []byte, pkg string, roots []string) error {
	if len(roots) == 0 {
		return fmt.Errorf("empty accounting guard selection")
	}
	passed := map[string]int{}
	packagePass := 0
	scan := bufio.NewScanner(bytes.NewReader(raw))
	scan.Buffer(make([]byte, 65536), 16<<20)
	for scan.Scan() {
		var event struct{ Action, Package, Test string }
		if err := json.Unmarshal(scan.Bytes(), &event); err != nil {
			return err
		}
		if event.Action == "fail" || event.Action == "skip" {
			return fmt.Errorf("accounting guard failed/skipped: %s/%s", event.Package, event.Test)
		}
		if event.Action == "pass" {
			if event.Package != pkg {
				return fmt.Errorf("foreign accounting guard package")
			}
			if event.Test == "" {
				packagePass++
			} else {
				passed[event.Test]++
			}
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if packagePass != 1 {
		return fmt.Errorf("accounting guard process lacks one joined package PASS")
	}
	for _, root := range roots {
		if passed[root] != 1 {
			return fmt.Errorf("accounting guard root missing or repeated: %s", root)
		}
	}
	for name := range passed {
		root := strings.SplitN(name, "/", 2)[0]
		found := false
		for _, required := range roots {
			if root == required {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("unplanned accounting guard root %s", root)
		}
	}
	return nil
}

func newestAccountingPreflight(root, head, base string) (string, error) {
	paths, err := filepath.Glob(filepath.Join(root, "test-results/preflight/*/receipt.json"))
	if err != nil {
		return "", err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	for _, path := range paths {
		var receipt preflightReceipt
		if err := readAccountingJSON(path, &receipt); err != nil {
			return "", err
		}
		if receipt.Plan.HeadSHA != head {
			continue
		}
		if receipt.Plan.BaseSHA != base {
			return "", fmt.Errorf("preflight used a different native comparison base")
		}
		if err := validatePreflightStatusReceipt(filepath.Dir(path), receipt); err != nil {
			return "", err
		}
		return filepath.Rel(root, path)
	}
	return "", fmt.Errorf("no exact-head canonical preflight receipt")
}

func verifyAccountingReceipt(ctx context.Context, root, head, path string, out io.Writer, remote accountingRemote, policy accountingPolicyObservation) error {
	if err := validateAccountingReceipt(ctx, root, head, path, remote, policy); err != nil {
		return err
	}
	var receipt accountingCertificate
	if err := readAccountingJSON(path, &receipt); err != nil {
		return err
	}
	// Hashes establish integrity, not who ran a test. Never accept caller-supplied
	// PASS JSON as execution authority, even when it matches the mandatory map.
	return certifyAccounting(ctx, root, head, receipt.Plan, out, remote, policy)
}

func validateAccountingReceipt(ctx context.Context, root, head, path string, remote accountingRemote, policy accountingPolicyObservation) error {
	var receipt accountingCertificate
	if err := readAccountingJSON(path, &receipt); err != nil {
		return err
	}
	if receipt.Version != 1 || receipt.Head != head || receipt.HumanAudit != "pending" || receipt.Policy.Digest != policy.Digest || !reflect.DeepEqual(receipt.Policy.Policy, policy.Policy) {
		return fmt.Errorf("accounting receipt is foreign, stale or claims an unproved audit")
	}
	manifest, err := accountingTreeInputs(ctx, root, head, nil)
	if err != nil || !reflect.DeepEqual(manifest, receipt.ProofSource) {
		return fmt.Errorf("accounting receipt proof input manifest changed: %v", err)
	}
	if err := verifyAccountingPolicyHistory(ctx, root, receipt.Policy, policy); err != nil {
		return err
	}
	if err := checkAccountingFinalSource(ctx, root, head, receipt.Plan, policy); err != nil {
		return err
	}
	tree, err := accountingGit(ctx, root, "rev-parse", head+"^{tree}")
	if err != nil || strings.TrimSpace(string(tree)) != receipt.Tree {
		return fmt.Errorf("accounting receipt tree mismatch")
	}
	required, err := accountingGuardMap(ctx, root)
	if err != nil {
		return err
	}
	if err = validateAccountingGuards(filepath.Dir(path), required, receipt.Guards); err != nil {
		return err
	}
	if err = validateBoundAccountingPreflight(receipt.Preflight, head, receipt.Plan.Base); err != nil {
		return err
	}
	raw, err := os.ReadFile(receipt.Preflight)
	if err != nil || formatproof.Digest(raw) != receipt.PreflightSHA256 {
		return fmt.Errorf("accounting preflight receipt changed")
	}
	sample := accountingSample(head, receipt.Plan.Base, policy.Digest, receipt.Plan.Output.Records[len(receipt.Plan.Previous.Records):])
	if !reflect.DeepEqual(sample, receipt.Sample) {
		return fmt.Errorf("accounting sample is stale or author-selected")
	}
	if err = requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	return nil
}

func validateAccountingGuards(directory string, required, actual []accountingGuardReceipt) error {
	if len(required) == 0 || len(actual) != len(required) {
		return fmt.Errorf("accounting receipt lacks the complete guard map")
	}
	for i, guard := range actual {
		if guard.Package != required[i].Package || guard.Run != required[i].Run || !reflect.DeepEqual(guard.Roots, required[i].Roots) {
			return fmt.Errorf("accounting guard selection was altered")
		}
		if filepath.Dir(guard.Log) != directory {
			return fmt.Errorf("accounting guard evidence escaped its owned directory")
		}
		info, err := os.Lstat(guard.Log)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("accounting guard evidence missing or nonregular")
		}
		raw, err := os.ReadFile(guard.Log)
		if err != nil || formatproof.Digest(raw) != guard.SHA256 {
			return fmt.Errorf("accounting guard log missing or changed")
		}
		if err = validateAccountingGuardLog(raw, guard.Package, guard.Roots); err != nil {
			return err
		}
	}
	return nil
}

func publishAccountingCertificate(ctx context.Context, out io.Writer, remote accountingRemote, policy accountingPolicyObservation, receipt accountingCertificate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireAccountingPolicy(ctx, remote, policy); err != nil {
		return err
	}
	if _, err := accountingSource(ctx, ".", receipt.Head, true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manifest, _ := json.Marshal(receipt.Plan.Output)
	_, err := fmt.Fprintf(out, "regenerated, no raise: head=%s base=%s rows=%d manifest=%s guards=pass sample=%s human-audit=pending\n", receipt.Head, receipt.Plan.Base, receipt.Plan.Rows, formatproof.Digest(manifest), strings.Join(receipt.Sample, ","))
	return err
}
