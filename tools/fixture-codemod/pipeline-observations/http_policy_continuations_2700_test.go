package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func http2700PriorRecipe(row recipe, actual string) (recipe, string, error) {
	raw, err := json.Marshal(row)
	if err != nil {
		return recipe{}, "", err
	}
	prior, body, _, err := selectedreadrole.HTTP2700PriorRecipe("../../..", raw, actual)
	if err != nil {
		return recipe{}, "", err
	}
	var previous recipe
	err = json.Unmarshal(prior, &previous)
	return previous, body, err
}

func proveHTTP2700Continuations(t *testing.T) {
	t.Helper()
	packet, err := selectedreadrole.HTTP2700Correspondences()
	if err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, pin := range packet.Rows {
		var original recipe
		if err := json.Unmarshal(pin.Original, &original); err != nil {
			t.Fatal(err)
		}
		key := original.Family + "/" + original.File + "/" + original.Function
		if seen[key] || pin.Multiplicity != 1 || pin.CurrentSourceRef != packet.FrozenSource {
			t.Fatalf("HTTP continuation identity/multiplicity changed: %s", key)
		}
		seen[key] = true
		t.Run("http2700/"+original.Function, func(t *testing.T) {
			prior, err := selectedreadrole.HTTP2700Reverse(pin)
			if err != nil {
				t.Fatal(err)
			}
			proveHTTP2700SourcePins(t, pin, original, prior)
			proveHTTP2700Class(t, pin, prior)
			row := rows[pin.Index]
			actual := selectedCausalObservationBody(t, original.File, projectionShapeFunction(t, pin.Candidate).Name.Name)
			previous, restored, err := http2700PriorRecipe(row, actual)
			if err != nil || restored != prior || catchUp2542RecipeDigest(previous) != catchUp2542RecipeDigest(original) {
				t.Fatalf("HTTP continuation failed its exact prior pair: %v", err)
			}
			for name, changed := range map[string]string{
				"stale": prior, "extra_call": strings.Replace(actual, "{", "{ unrelated();", 1),
				"changed_call_argument": http2700StatementMutant(t, actual, false),
				"changed_assertion":     http2700StatementMutant(t, actual, true),
			} {
				t.Run(name, func(t *testing.T) {
					if _, _, err := http2700PriorRecipe(row, changed); err == nil {
						t.Fatal("changed/stale HTTP continuation input accepted")
					}
				})
			}
			for _, field := range []string{"Family", "File", "Function"} {
				t.Run("foreign_"+field, func(t *testing.T) {
					raw, err := json.Marshal(row)
					if err != nil {
						t.Fatal(err)
					}
					var changed map[string]any
					if err := json.Unmarshal(raw, &changed); err != nil {
						t.Fatal(err)
					}
					changed[field] = "foreign"
					foreign, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					got, source, recognized, err := selectedreadrole.HTTP2700PriorRecipe("../../..", foreign, actual)
					if err != nil || recognized || !bytes.Equal(got, foreign) || source != actual {
						t.Fatal("foreign recipe received a continuation")
					}
				})
			}
			for _, field := range []string{"before", "after", "missing", "reverse", "unknown", "duplicate"} {
				t.Run(field, func(t *testing.T) {
					changed := row
					switch field {
					case "before":
						changed.Before += "\n// changed"
					case "after":
						changed.After += "\n// changed"
					case "missing":
						changed.Successor = ""
					case "reverse":
						changed.Successor = prior
					case "unknown":
						changed.Successor += "\n// changed"
					case "duplicate":
						changed.Successor += "\n" + changed.Successor
					}
					if _, _, err := http2700PriorRecipe(changed, actual); err == nil {
						t.Fatal("HTTP continuation changed its historical/current tuple")
					}
				})
			}
			proveHTTP2700CopiedSource(t, pin, row, actual)
		})
	}
	proveHTTP2700Ledger(t, packet)
}

func http2700StatementMutant(t *testing.T, body string, assertion bool) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "control.go", "package proof\n"+body, 0)
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	ast.Inspect(file, func(node ast.Node) bool {
		if changed {
			return false
		}
		if assertion {
			if statement, ok := node.(*ast.IfStmt); ok {
				statement.Cond = &ast.UnaryExpr{Op: token.NOT, X: statement.Cond}
				changed = true
			}
		} else if call, ok := node.(*ast.CallExpr); ok && len(call.Args) > 0 {
			call.Args[0] = ast.NewIdent("foreignArgument")
			changed = true
		}
		return !changed
	})
	if !changed {
		t.Fatal("HTTP continuation control did not change a call/assertion")
	}
	return formattedNativeReadNode(file.Decls[0])
}

func proveHTTP2700SourcePins(t *testing.T, pin selectedreadrole.HTTP2700Continuation, original recipe, prior string) {
	t.Helper()
	priorRecipes, err := exec.Command("git", "-C", "../../..", "show", pin.PriorRecipeRef+":tools/fixture-codemod/pipeline-observations/recipes.json").Output()
	if err != nil {
		t.Fatal(err)
	}
	var historical []recipe
	if err := json.Unmarshal(priorRecipes, &historical); err != nil {
		t.Fatal(err)
	}
	if catchUp2542RecipeDigest(historical[pin.Index]) != catchUp2542RecipeDigest(original) {
		t.Fatal("HTTP continuation lost its exact signed prior recipe")
	}
	for _, source := range []struct{ ref, digest string }{{pin.PriorSource.Ref, pin.PriorSource.FileSHA256}, {pin.CurrentSourceRef, pin.CurrentFileSHA256}} {
		raw, err := exec.Command("git", "-C", "../../..", "show", source.ref+":"+original.File).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != source.digest {
			t.Fatalf("HTTP continuation lost its signed source: %s: %v", source.ref, err)
		}
		signature, err := exec.Command("git", "-C", "../../..", "show", "-s", "--format=%G?", source.ref).Output()
		if err != nil || strings.TrimSpace(string(signature)) != "G" {
			t.Fatalf("HTTP continuation source is not signed: %s: %v", source.ref, err)
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, original.File, raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		fn, err := uniqueRecipeFunction(file, original)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
		want := pin.Candidate
		wantRawSHA := pin.CurrentDeclarationSHA256
		if source.ref == pin.PriorSource.Ref {
			want = prior
			wantRawSHA = pin.PriorSource.DeclarationSHA256
		}
		actual, err := canonicalFunction(body)
		expected, canonicalErr := canonicalFunction(want)
		if err != nil || canonicalErr != nil || actual != expected || fmt.Sprintf("%x", sha256.Sum256([]byte(body))) != wantRawSHA {
			t.Fatal("HTTP continuation changed its physical declaration or inert layout correspondence")
		}
	}
	if fmt.Sprintf("%x", sha256.Sum256([]byte(original.Before))) != pin.BeforeSHA256 ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(original.After))) != pin.AfterSHA256 ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(prior))) != pin.PriorEndpointSHA256 ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Candidate))) != pin.CurrentDeclarationSHA256 {
		t.Fatal("HTTP continuation changed its complete immutable declarations")
	}
}

func proveHTTP2700Class(t *testing.T, pin selectedreadrole.HTTP2700Continuation, prior string) {
	t.Helper()
	switch pin.Class {
	case "EFFECT-CLASS-ONLY":
		got, removed := http2700EffectProjection(t, prior)
		want, err := canonicalFunction(pin.Candidate)
		if err != nil || got != want || removed != pin.RemovedPolicySites {
			t.Fatalf("HTTP continuation changed non-policy work: removed=%d/%d err=%v", removed, pin.RemovedPolicySites, err)
		}
	case "REPLAY-ADMISSION":
		line := "\t\t\t\tctx = admitAPITestAgentExecution(t, ctx, f.store.(storetest.AgentFixtureStore), runStartTestSourceArtifactFact())\n"
		got, err := canonicalFunction(strings.Replace(pin.Candidate, line, "", 1))
		want, priorErr := canonicalFunction(prior)
		if err != nil || priorErr != nil || got != want || strings.Count(pin.Candidate, line) != 1 {
			t.Fatal("replay continuation changed payload/routes/lineage/results beyond the admitted context")
		}
		for _, endpoint := range []string{"29668c84f85c62bef1fefdd58cb967e03d1e182a"} {
			raw, err := exec.Command("git", "-C", "../../..", "show", endpoint+":internal/apiv1/operator_event_replay_test.go").Output()
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != pin.CurrentFileSHA256 {
				t.Fatal("replay helper/retained-binding/pending-route source changed")
			}
		}
	case "PREVIOUSLY-ACCEPTED-MAILBOX":
		if strings.Count(pin.Candidate, proposedEffectJournalInstallation) != 1 || strings.Replace(pin.Candidate, proposedEffectJournalInstallation, "", 1) != prior {
			t.Fatal("mailbox continuation changed its one installation")
		}
	default:
		t.Fatal("unreviewed HTTP continuation class")
	}
}

func http2700EffectProjection(t *testing.T, body string) (string, int) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "policy.go", "package proof\n"+body, 0)
	if err != nil {
		t.Fatal(err)
	}
	removed := 0
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CallExpr:
			var args []ast.Expr
			for _, arg := range n.Args {
				if call, ok := arg.(*ast.CallExpr); ok {
					if fn, ok := call.Fun.(*ast.SelectorExpr); ok && fn.Sel.Name == "WithToolEffect" {
						removed++
						continue
					}
				}
				args = append(args, arg)
			}
			n.Args = args
		case *ast.BlockStmt:
			var statements []ast.Stmt
			for _, statement := range n.List {
				if assignment, ok := statement.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 {
					if field, ok := assignment.Lhs[0].(*ast.SelectorExpr); ok && http2700RetiredField(field.Sel.Name) {
						removed++
						continue
					}
				}
				statements = append(statements, statement)
			}
			n.List = statements
		case *ast.CompositeLit:
			var fields []ast.Expr
			for _, field := range n.Elts {
				if pair, ok := field.(*ast.KeyValueExpr); ok {
					if key, ok := pair.Key.(*ast.Ident); ok && http2700RetiredField(key.Name) {
						removed++
						continue
					}
				}
				fields = append(fields, field)
			}
			n.Elts = fields
		case *ast.BasicLit:
			if n.Kind != token.STRING {
				break
			}
			value, err := strconv.Unquote(n.Value)
			if err != nil {
				t.Fatal(err)
			}
			var lines []string
			for _, line := range strings.SplitAfter(value, "\n") {
				if strings.TrimSpace(line) == "effect_class: non_idempotent_write" {
					removed++
					continue
				}
				lines = append(lines, line)
			}
			if changed := strings.Join(lines, ""); changed != value {
				if strings.HasPrefix(n.Value, "`") {
					n.Value = "`" + changed + "`"
				} else {
					n.Value = strconv.Quote(changed)
				}
			}
		}
		return true
	})
	return formattedNativeReadNode(file.Decls[0]), removed
}

func http2700RetiredField(name string) bool {
	return name == "EffectClass" || name == "RetryMaxAttempts" || name == "ForkPolicy"
}

func proveHTTP2700CopiedSource(t *testing.T, pin selectedreadrole.HTTP2700Continuation, row recipe, actual string) {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, row.File)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join("../../..", row.File))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, source, 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, recognized, err := selectedreadrole.HTTP2700PriorRecipe(root, raw, actual); err != nil || !recognized {
		t.Fatalf("exact copied source refused: %v", err)
	}
	if len(pin.FileEdits) != 0 {
		prior, err := selectedreadrole.HTTP2700HistoricalSource(root, row.File, source)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != pin.PriorSource.FileSHA256 {
			t.Fatalf("exact Git-less copied source lost its finite history: %v", err)
		}
	}
	for _, changed := range [][]byte{append(bytes.Clone(source), []byte("\n// changed sibling\n")...), nil, source[:len(source)/2]} {
		if err := os.WriteFile(path, changed, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := selectedreadrole.HTTP2700PriorRecipe(root, raw, actual); err == nil {
			t.Fatal("warm copied source hid a changed/missing sibling")
		}
		if len(pin.FileEdits) != 0 {
			if _, err := selectedreadrole.HTTP2700HistoricalSource(root, row.File, source); err == nil {
				t.Fatal("warm historical view accepted stale caller bytes over changed copied source")
			}
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := selectedreadrole.HTTP2700PriorRecipe(root, raw, actual); err == nil {
		t.Fatal("missing actual source received a historical pair")
	}
}

func proveHTTP2700Ledger(t *testing.T, packet selectedreadrole.HTTP2700Packet) {
	t.Helper()
	prior, err := selectedreadrole.HTTP2700HistoricalRecipes("../../..", recipeBytes)
	if err != nil {
		t.Fatal(err)
	}
	got, err := selectedreadrole.CatchUp2542JSONDigest(prior)
	if err != nil || got != "eb7368804ebdf7e06c2cf9aa95f84cb05e62f4d569e32ed4bcac1dba7fe1d43e" {
		t.Fatalf("complete prior ledger changed: %s/%v", got, err)
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	index := packet.Rows[0].Index
	for name, candidate := range map[string][]json.RawMessage{
		"missing":   append(append([]json.RawMessage{}, rows[:index]...), rows[index+1:]...),
		"duplicate": append(append([]json.RawMessage{}, rows...), rows[index]),
		"foreign":   append([]json.RawMessage{json.RawMessage(`{"Family":"foreign"}`)}, rows[1:]...),
	} {
		t.Run("http2700/ledger_"+name, func(t *testing.T) {
			raw, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := selectedreadrole.HTTP2700HistoricalRecipes("../../..", raw)
			if err != nil {
				return
			}
			if !bytes.Equal(raw, observed) {
				t.Fatal("incomplete or foreign ledger received a historical inverse")
			}
		})
	}
}
