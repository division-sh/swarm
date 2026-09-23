package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var executableDeliverySQL = regexp.MustCompile(`(?is)\b(?:from|join|into|update|delete\s+from|on)\s+(?:event_deliveries|event_delivery_attempts|event_delivery_outcomes)\b`)

var executableDeliverySQLOwners = map[string]string{
	"internal/store/internal/backend/delivery/adapter.go":                                            "private canonical executable-delivery lifecycle adapter",
	"internal/store/internal/backend/delivery/lifecycle.go":                                          "named delivery lifecycle owner resolving affected runs before mutation",
	"internal/store/internal/backend/delivery/read_projections.go":                                   "private canonical bounded executable-delivery read projections",
	"internal/store/internal/backend/delivery/snapshots_batch.go":                                    "private canonical batched executable-delivery snapshot admission",
	"internal/store/internal/backend/runforkrevision/projection.go":                                  "private immutable fork-revision canonical projection",
	"internal/store/internal/adminpersistence/destructive_reset_cleanup.go":                          "named destructive-reset physical cleanup",
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_discard_owner.go": "selected-fork physical cleanup after typed terminalization",
	"internal/store/internal/backend/pipelinepersistence/standing_service.go":                        "standing-service pre-mutation execution-posture inspection",
	"internal/store/testsql/event.go":                                                                "named hostile rollback injection used only by tests",
}

const unrevisionedDeliveryFixturePath = "internal/store/storetest/event.go"

var unrevisionedDeliveryFixtureSQL = map[string]struct{ scope, query string }{
	"handoff": {
		scope: "commitUnrevisionedSemanticEventFixture",
		query: `UPDATE event_deliveries SET continuation_handoff_at = COALESCE(continuation_handoff_at, CURRENT_TIMESTAMP) WHERE event_id = $1`,
	},
	"postgres": {
		scope: "insertUnrevisionedDeliveryFixture",
		query: `INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
			agent_name_owner, agent_name_source, agent_route_presence,
			agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
			delivery_target_route, delivery_context, delivery_payload_projection, connect_execution_claim,
			receiver_materialization_plan, execution_authority_kind, authority_bundle_hash,
			execution_authority_id, execution_authority_generation,
			status, retry_count, max_retries, next_eligible_at, claim_version, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5, $6,
			$7, $8, $9, $10, $11, $12,
			$13::jsonb, $14::jsonb, $15::jsonb, $16::jsonb,
			$17::jsonb, $18, $19, $20, $21,
			'pending', 0, $22, $23, 0, $23, $23
		) ON CONFLICT (event_id, route_identity) DO NOTHING`,
	},
	"sqlite": {
		scope: "insertUnrevisionedDeliveryFixture",
		query: `INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
			agent_name_owner, agent_name_source, agent_route_presence,
			agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
			delivery_target_route, delivery_context, delivery_payload_projection, connect_execution_claim,
			receiver_materialization_plan, execution_authority_kind, authority_bundle_hash,
			execution_authority_id, execution_authority_generation,
			status, retry_count, max_retries, next_eligible_at, claim_version, created_at, updated_at
		) VALUES (
			?1, ?2, ?3, ?4, ?5, ?6,
			?7, ?8, ?9, ?10, ?11, ?12,
			?13, ?14, ?15, ?16,
			?17, ?18, ?19, ?20, ?21,
			'pending', 0, ?22, ?23, 0, ?23, ?23
		) ON CONFLICT(event_id, route_identity) DO NOTHING`,
	},
}

func classifiedUnrevisionedDeliveryFixtureSQL(path, scope, query string) string {
	if path != unrevisionedDeliveryFixturePath {
		return ""
	}
	for name, allowed := range unrevisionedDeliveryFixtureSQL {
		if scope == allowed.scope && strings.Join(strings.Fields(query), " ") == strings.Join(strings.Fields(allowed.query), " ") {
			return name
		}
	}
	return ""
}

func TestRetiredGenericDeliveryReadersHaveNoProductionConsumers(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	retired := []string{"SnapshotsForRun", "SnapshotsForAgent", "EligibleAgentSnapshots"}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
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
	fixtureFound := map[string]int{}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
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
				fixture := classifiedUnrevisionedDeliveryFixtureSQL(relative, eventBoundaryEnclosingScope(file, literal.Pos()), raw)
				if fixture != "" {
					fixtureFound[fixture]++
				}
				if _, allowed := executableDeliverySQLOwners[relative]; !allowed && !closedReceiverMaterializationSQL(relative, eventBoundaryEnclosingScope(file, literal.Pos()), raw) && fixture == "" {
					t.Errorf("%s:%d owns executable-delivery SQL outside the closed lifecycle boundary", relative, fset.Position(literal.Pos()).Line)
				}
				return true
			})
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
	if found["internal/store/internal/backend/delivery/receiver_materialization.go"] != 1 {
		t.Fatal("receiver dependency owner must have exactly one classified delivery-ID lookup")
	}
	for path, reason := range executableDeliverySQLOwners {
		if found[path] == 0 {
			t.Errorf("closed executable-delivery SQL owner %s (%s) has no classified SQL", path, reason)
		}
	}
	for name := range unrevisionedDeliveryFixtureSQL {
		if fixtureFound[name] != 1 {
			t.Errorf("test-only unrevisioned delivery fixture %s has %d exact SQL statements, want 1", name, fixtureFound[name])
		}
	}
}

func TestUnrevisionedDeliveryFixtureSQLAllowanceIsExact(t *testing.T) {
	for name, allowed := range unrevisionedDeliveryFixtureSQL {
		t.Run(name, func(t *testing.T) {
			if got := classifiedUnrevisionedDeliveryFixtureSQL(unrevisionedDeliveryFixturePath, allowed.scope, allowed.query); got != name {
				t.Fatalf("fixture SQL classification = %q, want %q", got, name)
			}
			for _, hostile := range []struct{ path, scope, query string }{
				{"internal/store/storetest/sibling.go", allowed.scope, allowed.query},
				{unrevisionedDeliveryFixturePath, "siblingFixture", allowed.query},
				{unrevisionedDeliveryFixturePath, allowed.scope, allowed.query + " RETURNING delivery_id"},
			} {
				if got := classifiedUnrevisionedDeliveryFixtureSQL(hostile.path, hostile.scope, hostile.query); got != "" {
					t.Fatalf("hostile fixture SQL classified as %q: %#v", got, hostile)
				}
			}
		})
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
func closedReceiverMaterializationSQL(path, scope, query string) bool {
	return path == "internal/store/internal/backend/delivery/receiver_materialization.go" &&
		scope == "Adapter.publicationRecords" &&
		strings.Join(strings.Fields(query), " ") == "SELECT delivery_id FROM event_deliveries WHERE event_id=$1 ORDER BY delivery_id"
}

func TestReceiverMaterializationSQLAllowanceIsExact(t *testing.T) {
	path := "internal/store/internal/backend/delivery/receiver_materialization.go"
	scope := "Adapter.publicationRecords"
	query := "SELECT delivery_id FROM event_deliveries WHERE event_id=$1 ORDER BY delivery_id"
	if !closedReceiverMaterializationSQL(path, scope, query) {
		t.Fatal("canonical lookup refused")
	}
	for _, tc := range []struct{ path, scope, query string }{
		{path, "Adapter.other", query},
		{path, scope, "SELECT * FROM event_deliveries WHERE event_id=$1"},
		{path, scope, "DELETE FROM event_deliveries WHERE event_id=$1"},
		{path, scope, "SELECT delivery_id FROM event_deliveries ORDER BY delivery_id"},
		{"internal/runtime/pipeline/other.go", scope, query},
	} {
		if closedReceiverMaterializationSQL(tc.path, tc.scope, tc.query) {
			t.Fatalf("unowned lookup admitted: %#v", tc)
		}
	}
}
