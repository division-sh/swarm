package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

func approvedAccountingPolicy(t *testing.T) accountingPolicy {
	t.Helper()
	inputs := []accountingInput{{Path: "go.mod", SHA256: strings.Repeat("1", 64)}}
	raw, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	return accountingPolicy{Version: 1, Category: "finite-go-format", Status: "active", Rule: formatproof.Rule, ExecutorSource: strings.Repeat("a", 40), DependencyDigest: formatproof.Digest(raw), Inputs: inputs, Variants: append([]string{}, accountingVariants...)}
}

func accountingPolicyRemote(t *testing.T, policy *accountingPolicy, master *string) accountingRemote {
	t.Helper()
	return func(ctx context.Context, path string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch path {
		case "git/ref/heads/master":
			return json.Marshal(map[string]any{"ref": "refs/heads/master", "object": map[string]string{"sha": *master, "type": "commit"}})
		case "contents/" + accountingPolicyPath + "?ref=" + *master:
			raw, err := json.Marshal(*policy)
			if err != nil {
				return nil, err
			}
			object := append([]byte(fmt.Sprintf("blob %d\x00", len(raw))), raw...)
			return json.Marshal(map[string]string{"type": "file", "path": accountingPolicyPath, "sha": fmt.Sprintf("%x", sha1.Sum(object)), "encoding": "base64", "content": base64.StdEncoding.EncodeToString(raw)})
		default:
			return nil, fmt.Errorf("unexpected remote path %q", path)
		}
	}
}

func TestAccountingFreshPolicyDistinguishesComparisonFromApproval(t *testing.T) {
	policy := approvedAccountingPolicy(t)
	master := strings.Repeat("b", 40)
	remote := accountingPolicyRemote(t, &policy, &master)
	observed, err := observeAccountingPolicy(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Commit == policy.ExecutorSource {
		t.Fatal("fixture must distinguish executor approval from latest policy tip")
	}
	master = strings.Repeat("c", 40)
	if err := requireAccountingPolicy(context.Background(), remote, observed); err != nil {
		t.Fatalf("unrelated master advance must not rebind executor or comparison: %v", err)
	}
	policy.Status = "suspended"
	if err := requireAccountingPolicy(context.Background(), remote, observed); err == nil {
		t.Fatal("revocation after validation accepted")
	}
	policy.Status = "active"
	policy.Rule = "unknown"
	if err := requireAccountingPolicy(context.Background(), remote, observed); err == nil {
		t.Fatal("changed rule accepted")
	}
	offline := func(context.Context, string) ([]byte, error) { return nil, errors.New("offline") }
	if err := requireAccountingPolicy(context.Background(), offline, observed); err == nil {
		t.Fatal("cached active fallback accepted")
	}
}

func TestAccountingPolicyRefusesUnknownInactiveAndUnboundInputs(t *testing.T) {
	for name, mutate := range map[string]func(*accountingPolicy){
		"inactive":       func(p *accountingPolicy) { p.Status = "inactive" },
		"category":       func(p *accountingPolicy) { p.Category = "debt" },
		"rule":           func(p *accountingPolicy) { p.Rule = "r1" },
		"context":        func(p *accountingPolicy) { p.Variants = p.Variants[:1] },
		"missing_source": func(p *accountingPolicy) { p.ExecutorSource = "" },
		"missing_inputs": func(p *accountingPolicy) { p.Inputs = nil },
		"digest":         func(p *accountingPolicy) { p.Inputs[0].SHA256 = strings.Repeat("2", 64) },
		"duplicate":      func(p *accountingPolicy) { p.Inputs = append(p.Inputs, p.Inputs[0]) },
		"escape":         func(p *accountingPolicy) { p.Inputs[0].Path = "../go.mod" },
	} {
		t.Run(name, func(t *testing.T) {
			p := approvedAccountingPolicy(t)
			mutate(&p)
			if p.validate() == nil {
				t.Fatal("unapproved policy accepted")
			}
		})
	}
}

func TestAccountingPublicationRechecksSuspensionAndCancellation(t *testing.T) {
	policy := approvedAccountingPolicy(t)
	master := strings.Repeat("b", 40)
	remote := accountingPolicyRemote(t, &policy, &master)
	observed, err := observeAccountingPolicy(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	certificate := accountingCertificate{Version: 1, Head: strings.Repeat("d", 40), Plan: accountingPlan{Base: strings.Repeat("e", 40), Rows: 1}}
	for _, state := range []string{"suspended", "inactive"} {
		policy.Status = state
		var out bytes.Buffer
		if err := publishAccountingCertificate(context.Background(), &out, remote, observed, certificate); err == nil || out.Len() != 0 {
			t.Fatal("success published after category withdrawal")
		}
	}
	policy.Status = "active"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := publishAccountingCertificate(ctx, &out, remote, observed, certificate); !errors.Is(err, context.Canceled) || out.Len() != 0 {
		t.Fatal("canceled receipt published success")
	}
}

func TestAccountingSampleIsHeadBoundAndNotAuthorChosen(t *testing.T) {
	var records []formatproof.Record
	for i := 0; i < 9; i++ {
		records = append(records, formatproof.Record{File: fmt.Sprint(i)})
	}
	first := accountingSample("head", "base", "policy", records)
	if len(first) != 3 {
		t.Fatalf("sample=%v, want ceiling of 1/4", first)
	}
	for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
		records[i], records[j] = records[j], records[i]
	}
	if !reflect.DeepEqual(first, accountingSample("head", "base", "policy", records)) {
		t.Fatal("sample depends on author row order")
	}
	if reflect.DeepEqual(first, accountingSample("changed-head", "base", "policy", records)) {
		t.Fatal("sample was not selected from frozen head")
	}
}

func TestAccountingGuardEvidenceRefusesPartialMissingSkippedAndForeign(t *testing.T) {
	const pkg = "github.com/division-sh/swarm/scripts/rewrite-stages-2566"
	line := func(action, pkg, test string) string {
		raw, _ := json.Marshal(map[string]string{"Action": action, "Package": pkg, "Test": test})
		return string(raw) + "\n"
	}
	good := line("pass", pkg, "TestOne") + line("pass", pkg, "TestOne/child") + line("pass", pkg, "")
	if err := validateAccountingGuardLog([]byte(good), pkg, []string{"TestOne"}); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"empty": "", "partial": line("pass", pkg, "TestOne"), "missing": line("pass", pkg, ""),
		"skipped": line("skip", pkg, "TestOne") + good, "failed": line("fail", pkg, "TestOne") + good,
		"foreign": good + line("pass", "other", ""), "extra_root": good + line("pass", pkg, "TestTwo"),
		"repeated": good + line("pass", pkg, "TestOne"), "non_json": "PASS\n",
	} {
		t.Run(name, func(t *testing.T) {
			if validateAccountingGuardLog([]byte(raw), pkg, []string{"TestOne"}) == nil {
				t.Fatal("incomplete guard evidence accepted")
			}
		})
	}
	if validateAccountingGuardLog([]byte(good), pkg, nil) == nil {
		t.Fatal("empty required selection accepted")
	}
}

func TestAccountingBootstrapCannotCertifyItsOwnNewPolicy(t *testing.T) {
	var policy accountingPolicy
	raw, err := os.ReadFile(filepath.Join("..", "..", accountingPolicyPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := formatproof.Decode(raw, &policy); err != nil {
		t.Fatal(err)
	}
	// An active policy can appear only in a separately reviewed later activation.
	// This proof concerns the unbound bootstrap, not a permanent activation ban.
	if policy.Status == "active" {
		if err := policy.validate(); err != nil {
			t.Fatal(err)
		}
	} else if policy.Status != "inactive" {
		t.Fatal("unexpected bootstrap policy state")
	}
	policy.Status = "active"
	policy.ExecutorSource = ""
	policy.DependencyDigest = ""
	policy.Inputs = nil
	if policy.validate() == nil {
		t.Fatal("inactive bootstrap accepted as certification authority")
	}
}

func TestAccountingOwnedProcessBuffersSuccessUntilJoinedAndFresh(t *testing.T) {
	policy := approvedAccountingPolicy(t)
	master := strings.Repeat("b", 40)
	remote := accountingPolicyRemote(t, &policy, &master)
	observed, err := observeAccountingPolicy(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, program string
		suspend       bool
		want          bool
	}{
		{"failed", "#!/bin/sh\nprintf 'regenerated, no raise\\n'\nexit 1\n", false, false},
		{"revoked", "#!/bin/sh\nprintf 'regenerated, no raise\\n'\n", true, false},
		{"joined", "#!/bin/sh\nprintf 'regenerated, no raise\\n'\n", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "approved-fixture")
			if err := os.WriteFile(binary, []byte(test.program), 0700); err != nil {
				t.Fatal(err)
			}
			policy.Status = "active"
			if test.suspend {
				policy.Status = "suspended"
			}
			var out bytes.Buffer
			err := runApprovedAccountingProcess(context.Background(), binary, []string{"certify"}, &out, io.Discard, observed, remote)
			if test.want {
				if err != nil || out.String() != "regenerated, no raise\n" {
					t.Fatalf("joined result lost: %q %v", out.String(), err)
				}
			} else if err == nil || out.Len() != 0 {
				t.Fatalf("premature success escaped: %q %v", out.String(), err)
			}
		})
	}
}

func TestAccountingApprovedClosureCoversAllClosedContextsAndCanonicalOwners(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := accountingClosure(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, input := range inputs {
		if found[input.Path] || !hexIdentity(input.SHA256, 64) {
			t.Fatalf("duplicate/unbound closure input %#v", input)
		}
		found[input.Path] = true
	}
	for _, path := range []string{"cmd/swarm-test/accounting.go", "cmd/swarm-test/preflight_process_unix.go", "cmd/swarm-test/preflight_process_other.go", "internal/testplanning/static_authority_guards.go", "internal/store/persistence_authority_registry_test.go", "internal/store/persistence_authority_debt_test.go", formatproof.Ledger, formatproof.Source, "scripts/rewrite-stages-2566/formatproof/testdata/original.go.txt", ".github/test-proof-history-inputs.json"} {
		if !found[path] {
			t.Fatalf("canonical executable/guard/history input omitted: %s", path)
		}
	}
	if found[accountingPolicyPath] {
		t.Fatal("policy/status digest must remain separate from executor closure")
	}
	t.Logf("approved closure: inputs=%d contexts=%v", len(inputs), accountingVariants)
}

func TestAccountingReceiptCannotSelectItsOwnGuardsOrImportExecutionAuthority(t *testing.T) {
	const pkg = "fixture"
	directory := t.TempDir()
	path := filepath.Join(directory, "guard.jsonl")
	raw := []byte("{\"Action\":\"pass\",\"Package\":\"fixture\",\"Test\":\"TestRequired\"}\n{\"Action\":\"pass\",\"Package\":\"fixture\"}\n")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	required := []accountingGuardReceipt{{Package: pkg, Run: "^TestRequired$", Roots: []string{"TestRequired"}}}
	good := required[0]
	good.Log = path
	good.SHA256 = formatproof.Digest(raw)
	if err := validateAccountingGuards(directory, required, []accountingGuardReceipt{good}); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*accountingGuardReceipt){
		"empty_roots": func(g *accountingGuardReceipt) { g.Roots = nil }, "foreign_package": func(g *accountingGuardReceipt) { g.Package = "foreign" },
		"weaker_run": func(g *accountingGuardReceipt) { g.Run = "^$" }, "foreign_root": func(g *accountingGuardReceipt) { g.Roots = []string{"TestFake"} },
		"forged_digest": func(g *accountingGuardReceipt) { g.SHA256 = strings.Repeat("0", 64) }, "escaped_log": func(g *accountingGuardReceipt) { g.Log = filepath.Join(t.TempDir(), "other") },
	} {
		t.Run(name, func(t *testing.T) {
			copy := good
			mutate(&copy)
			if validateAccountingGuards(directory, required, []accountingGuardReceipt{copy}) == nil {
				t.Fatal("imported receipt weakened the canonical guard map")
			}
		})
	}
	if validateAccountingGuards(directory, required, nil) == nil {
		t.Fatal("missing guard map accepted")
	}
	linked := filepath.Join(directory, "linked.jsonl")
	if err := os.Symlink(path, linked); err != nil {
		t.Fatal(err)
	}
	copy := good
	copy.Log = linked
	if validateAccountingGuards(directory, required, []accountingGuardReceipt{copy}) == nil {
		t.Fatal("symlink evidence accepted")
	}
	// The actual saved-receipt consumer must replay owned execution, rather than
	// promote integrity-checked PASS JSON into an attestation. Keep its sole
	// publication path closed while the bootstrap itself remains inactive.
	file, err := parser.ParseFile(token.NewFileSet(), "accounting_receipt.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found, replay := false, 0
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Name.Name != "verifyAccountingReceipt" {
			continue
		}
		found = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			if name.Name == "publishAccountingCertificate" {
				t.Fatal("saved receipt directly authorizes a success footer")
			}
			if name.Name == "certifyAccounting" {
				replay++
			}
			return true
		})
	}
	if !found || replay != 1 {
		t.Fatal("actual receipt consumer lost its single owned re-execution path")
	}
}

func TestAccountingRemotePolicyRequiresAuthenticatedExactBlob(t *testing.T) {
	policy := approvedAccountingPolicy(t)
	master := strings.Repeat("b", 40)
	healthy := accountingPolicyRemote(t, &policy, &master)
	for _, fault := range []string{"wrong_ref", "wrong_type", "wrong_path", "wrong_hash", "non_base64", "missing", "unknown_field"} {
		t.Run(fault, func(t *testing.T) {
			remote := func(ctx context.Context, path string) ([]byte, error) {
				raw, err := healthy(ctx, path)
				if err != nil {
					return nil, err
				}
				var object map[string]any
				if err = json.Unmarshal(raw, &object); err != nil {
					return nil, err
				}
				if path == "git/ref/heads/master" {
					if fault == "wrong_ref" {
						object["ref"] = "refs/heads/candidate"
					}
					if fault == "wrong_type" {
						object["object"].(map[string]any)["type"] = "tag"
					}
				} else {
					switch fault {
					case "missing":
						return nil, errors.New("missing policy")
					case "wrong_path":
						object["path"] = "candidate-policy.json"
					case "wrong_hash":
						object["sha"] = strings.Repeat("0", 40)
					case "non_base64":
						object["encoding"] = "none"
					case "unknown_field":
						body, err := base64.StdEncoding.DecodeString(object["content"].(string))
						if err != nil {
							return nil, err
						}
						body = bytes.Replace(body, []byte("{\"version\":"), []byte("{\"new_exception\":true,\"version\":"), 1)
						object["content"] = base64.StdEncoding.EncodeToString(body)
						object["sha"] = fmt.Sprintf("%x", sha1.Sum(append([]byte(fmt.Sprintf("blob %d\x00", len(body))), body...)))
					}
				}
				return json.Marshal(object)
			}
			if _, err := observeAccountingPolicy(context.Background(), remote); err == nil {
				t.Fatal("foreign or unclassified remote approval accepted")
			}
		})
	}
}
