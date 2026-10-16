package runtimepersistence

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

var executableDeliverySQL = regexp.MustCompile(`(?is)\b(?:from|join|into|update|delete\s+from|on)\s+(?:event_deliveries|event_delivery_attempts)\b`)

var executableDeliverySQLOwners = map[string]string{
	"internal/store/internal/backend/delivery/adapter.go":                                            "private canonical executable-delivery lifecycle adapter",
	"internal/store/internal/backend/delivery/agent_claim_profile.go":                                "canonical immutable first-claim profile evidence under the delivery transaction",
	"internal/store/internal/backend/delivery/creating_predecessor.go":                               "canonical creating-turn outcome admission under the original delivery transaction",
	"internal/store/internal/backend/delivery/lifecycle.go":                                          "named delivery lifecycle owner resolving affected runs before mutation",
	"internal/store/internal/backend/delivery/queued_cancellation.go":                                "canonical exact queued and claimed agent-origin cancellation in the owning mutation",
	"internal/store/internal/backend/delivery/read_projections.go":                                   "private canonical bounded executable-delivery read projections",
	"internal/store/internal/backend/delivery/selected_successor_handoff.go":                         "selected-fork exact unfinished-delivery authority handoff",
	"internal/store/internal/backend/delivery/snapshots_batch.go":                                    "private canonical batched executable-delivery snapshot admission",
	"internal/store/internal/backend/delivery/test_agent_claim_profile_fault.go":                     "named exact first-claim capture CAS fault under the original selected transaction",
	"internal/store/internal/backend/runforkrevision/projection.go":                                  "private immutable fork-revision canonical projection",
	"internal/store/internal/adminpersistence/destructive_reset_cleanup.go":                          "named destructive-reset physical cleanup",
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_discard_owner.go": "selected-fork physical cleanup after typed terminalization",
	"internal/store/internal/backend/pipelinepersistence/standing_service.go":                        "standing-service pre-mutation execution-posture inspection",
	"internal/store/internal/backend/pipelinepersistence/test_creation_completion_fault.go":          "named exact native-delivery witness for the authorized completion-mark rollback fault (#2542 6176953400)",
	"internal/store/internal/backend/effectpersistence/canceled_turn_recovery.go":                    "read-only cancellation inventory excluding selected possession from normal recovery",
}

func TestRetiredGenericDeliveryReadersHaveNoProductionConsumers(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	retired := []string{"SnapshotsForRun", "SnapshotsForAgent", "EligibleAgentSnapshots"}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if err := checkoutsource.WalkDir(repoRoot, root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, name := range retired {
				if regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).Match(contents) {
					relative, relErr := filepath.Rel(repoRoot, path)
					if relErr != nil {
						return relErr
					}
					t.Errorf("%s consumes retired generic delivery reader %s", filepath.ToSlash(relative), name)
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
}

func TestExecutableDeliverySQLHasClosedOwners(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	found := map[string]int{}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if err := checkoutsource.WalkDir(repoRoot, root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			classifiedReads := classifiedPipelineDeliveryReads(relative, file)
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				raw, err := strconv.Unquote(literal.Value)
				if err != nil || !executableDeliverySQL.MatchString(raw) {
					return true
				}
				found[relative]++
				if _, allowed := executableDeliverySQLOwners[relative]; !allowed && !classifiedReads[literal] {
					t.Errorf("%s:%d owns executable-delivery SQL outside the closed lifecycle boundary", relative, fset.Position(literal.Pos()).Line)
				}
				return true
			})
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
	if found["internal/store/internal/backend/delivery/receiver_materialization.go"] != 0 {
		t.Fatal("receiver readiness must consume canonical construction authority, not an executable-delivery SQL interpreter")
	}
	for path, reason := range executableDeliverySQLOwners {
		if found[path] == 0 {
			t.Errorf("closed executable-delivery SQL owner %s (%s) has no classified SQL", path, reason)
		}
	}
	for path, want := range map[string]int{
		"internal/store/internal/backend/delivery/queued_cancellation.go":                       5,
		"internal/store/internal/backend/delivery/test_issue2564_evidence.go":                   0,
		"internal/store/internal/backend/effectpersistence/canceled_turn_recovery.go":           1,
		"internal/store/internal/backend/pipelinepersistence/test_creation_completion_fault.go": 1,
	} {
		if got := found[path]; got != want {
			t.Errorf("closed executable-delivery SQL owner %s has %d queries, want %d", path, got, want)
		}
	}
}

func classifiedPipelineDeliveryReads(path string, file *ast.File) map[*ast.BasicLit]bool {
	classified := map[*ast.BasicLit]bool{}
	if path != "internal/store/internal/backend/pipelinepersistence/deployment_observation.go" {
		return classified
	}
	// #2698/6205327685: exact read-only joins of pipeline receipts and delivery
	// facts, retaining their original SQL, ordering, failure behavior and cut.
	digests := map[string]string{
		"ReadDeploymentPreReceiptDeliveryCutStorageTx":    "70faa35732b3f5c9ef5683d6719ab0c69a5cb3ae3bb4ff2943fb6a835c9a3333",
		"ReadDeploymentPendingReceiverCutStorageTx":       "2f487864a056416dbd4a38bf547f77fa41d8f76fc5db79c25f92c75c7122e126",
		"ReadPinnedPortfolioDeliveryDiagnosticsStorageTx": "633692570e3484ee64ba7d9dcfa4ce1bc49e799b25a26d7de41a3dc967fc4c13",
		"ReadCanonicalRoutingSQLiteDiagnosticTx":          "148a5129aa61a7044063cd19d393816b9a256ef6311b13a8d9300cd12f4474a9",
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || digests[fn.Name.Name] == "" {
			continue
		}
		var out bytes.Buffer
		if format.Node(&out, token.NewFileSet(), fn) != nil || fmt.Sprintf("%x", sha256.Sum256(out.Bytes())) != digests[fn.Name.Name] {
			continue
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			if literal, ok := node.(*ast.BasicLit); ok {
				classified[literal] = true
			}
			return true
		})
	}
	return classified
}

// The former public-fixture exemptions are gone, including identical old SQL.
func TestUnrevisionedDeliveryFixtureSQLAllowanceIsExact(t *testing.T) {
	for _, query := range []string{
		"INSERT INTO event_deliveries (event_id) VALUES (?)",
		"UPDATE event_deliveries SET continuation_handoff_at=CURRENT_TIMESTAMP WHERE event_id=$1",
	} {
		if !executableDeliverySQL.MatchString(query) {
			t.Fatalf("retired fixture SQL escaped lifecycle detection: %s", query)
		}
		if _, allowed := executableDeliverySQLOwners["internal/store/storetest/event.go"]; allowed {
			t.Fatal("public event fixtures regained delivery SQL authority")
		}
	}
}

func TestReplayScopesAreNotExecutableDeliveries(t *testing.T) {
	for _, source := range []string{
		"internal/store/internal/backend/delivery/adapter.go",
		"internal/store/internal/backend/delivery/lifecycle.go",
	} {
		contents, err := os.ReadFile(filepath.Join(eventBoundaryRepositoryRoot(t), source))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(contents), "committed_replay_scopes") {
			t.Fatalf("%s conflates committed replay scope with executable delivery lifecycle", source)
		}
	}
}
