package runtimepersistence

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	privatedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

var executableDeliverySQL = regexp.MustCompile(`(?is)\b(?:from|join|into|update|delete\s+from|on)\s+(?:event_deliveries|event_delivery_attempts)\b`)

var executableDeliverySQLOwners = map[string]string{
	"internal/store/internal/backend/delivery/adapter.go":                                            "private canonical executable-delivery lifecycle adapter",
	"internal/store/internal/backend/delivery/creating_predecessor.go":                               "canonical creating-turn outcome admission under the original delivery transaction",
	"internal/store/internal/backend/delivery/lifecycle.go":                                          "named delivery lifecycle owner resolving affected runs before mutation",
	"internal/store/internal/backend/delivery/queued_cancellation.go":                                "canonical exact queued and claimed agent-origin cancellation in the owning mutation",
	"internal/store/internal/backend/delivery/read_projections.go":                                   "private canonical bounded executable-delivery read projections",
	"internal/store/internal/backend/delivery/selected_successor_handoff.go":                         "selected-fork exact unfinished-delivery authority handoff",
	"internal/store/internal/backend/delivery/snapshots_batch.go":                                    "private canonical batched executable-delivery snapshot admission",
	"internal/store/internal/backend/runforkrevision/projection.go":                                  "private immutable fork-revision canonical projection",
	"internal/store/internal/adminpersistence/destructive_reset_cleanup.go":                          "named destructive-reset physical cleanup",
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_discard_owner.go": "selected-fork physical cleanup after typed terminalization",
	"internal/store/internal/backend/pipelinepersistence/standing_service.go":                        "standing-service pre-mutation execution-posture inspection",
	"internal/store/internal/backend/effectpersistence/canceled_turn_recovery.go":                    "read-only cancellation inventory excluding selected possession from normal recovery",
	"internal/store/testsql/event.go":                                                                "named hostile rollback injection used only by tests",
	"internal/store/internal/backend/pipelinepersistence/test_selected_fan_out_rollback.go":          "exact read-only drafted/rolled-back publication cardinality proof",
	"internal/store/internal/backend/pipelinepersistence/test_selected_reply_return_rollback.go":     "exact read-only drafted/rolled-back accepted-return publication cardinality proof",
	"internal/store/internal/backend/runforkpersistence/run_fork_fan_out_materializer.go":            "exact read-only replay lineage and child-delivery membership, not lifecycle selection",
	"internal/store/internal/backend/runforkpersistence/test_receiver_execution_readback.go":         "exact read-only selected receiver claim and terminal receipt proof",
}

var selectedForkDeliveryMutationSQL = regexp.MustCompile(`(?is)\b(?:into|update|delete\s+from|on)\s+(?:event_deliveries|event_delivery_attempts)\b`)

func selectedForkDeliveryReadSQL(query string) bool {
	fields := strings.Fields(query)
	return len(fields) != 0 && strings.EqualFold(fields[0], "SELECT") && !selectedForkDeliveryMutationSQL.MatchString(query)
}

func TestSelectedForkDeliveryEvidenceReadersCannotMutate(t *testing.T) {
	if !selectedForkDeliveryReadSQL("SELECT\n COUNT(*) FROM event_deliveries") {
		t.Fatal("canonical multiline SELECT was refused")
	}
	for _, query := range []string{
		"UPDATE event_deliveries SET status='delivered'",
		"INSERT INTO event_delivery_attempts VALUES (1)",
		"DELETE FROM event_deliveries",
		"SELECT COUNT(*) FROM event_deliveries; DELETE FROM event_deliveries",
	} {
		if selectedForkDeliveryReadSQL(query) {
			t.Fatalf("selected evidence reader admitted mutation: %s", query)
		}
	}
}

func TestRetiredGenericDeliveryReadersHaveNoProductionConsumers(t *testing.T) {
	fact := reflect.TypeFor[privatedelivery.RunOperationalFact]()
	want := map[string]reflect.Type{
		"DeliveryID": reflect.TypeFor[string](), "EventID": reflect.TypeFor[string](), "RunID": reflect.TypeFor[string](), "SubscriberID": reflect.TypeFor[string](),
		"RouteIdentity": reflect.TypeFor[string](), "FlowInstance": reflect.TypeFor[string](), "ReasonCode": reflect.TypeFor[string](),
		"AgentIdentity": reflect.TypeFor[agentidentity.Identity](), "SubscriberClass": reflect.TypeFor[deliverylifecycle.SubscriberClass](),
		"Status": reflect.TypeFor[deliverylifecycle.Status](), "KnownCompletionAttempts": reflect.TypeFor[map[string]bool](),
	}
	if fact.NumField() != len(want) || fact.NumMethod() != 0 {
		t.Fatal("operational delivery evidence regained generic snapshot or executable custody")
	}
	for i := 0; i < fact.NumField(); i++ {
		field := fact.Field(i)
		if field.Type != want[field.Name] {
			t.Fatalf("operational delivery evidence exposes an unreviewed field: %s %s", field.Name, field.Type)
		}
	}
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
