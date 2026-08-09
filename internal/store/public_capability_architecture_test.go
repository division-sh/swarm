package store_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicReadModelsAreConsumerOwned(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	storeFacade := readArchitectureFile(t, root, "internal/store/store.go")
	for _, retired := range []string{
		"OperatorReadOptions", "OperatorEventFull", "OperatorEntityFull",
		"OperatorConversationSummary", "BundleCatalogDetail", "V1MailboxItem",
	} {
		if strings.Contains(storeFacade, retired) {
			t.Errorf("root store facade still owns public model %s", retired)
		}
	}
	for _, dir := range []string{"internal/operatorread", "internal/apiidempotency", "internal/bundlecatalog", "internal/mailbox"} {
		walkProductionGo(t, root, dir, func(path, source string) {
			if strings.Contains(source, `"github.com/division-sh/swarm/internal/store"`) {
				t.Errorf("consumer contract %s imports root store", path)
			}
		})
	}
}

func TestAPIMethodsDeclareExactCapabilities(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	walkProductionGo(t, root, "internal/apiv1", func(path, source string) {
		if strings.Contains(source, "OperatorReadOptions") {
			t.Errorf("broad API capability bag survives in %s", path)
		}
	})
	capabilities := readArchitectureFile(t, root, "internal/apiv1/operator_capabilities.go")
	for _, family := range []string{
		"RunReadHandlerOptions", "EntityHandlerOptions", "AgentConversationHandlerOptions",
		"ObservabilityHandlerOptions", "BundleCatalogHandlerOptions", "BundleDeleteHandlerOptions",
		"RuntimeNukeHandlerOptions", "EventPublicationOptions", "SubscriptionOptions",
	} {
		if !strings.Contains(capabilities, "type "+family+" struct") {
			t.Errorf("exact API capability family %s is missing", family)
		}
	}
}

func TestPublicCapabilityConsumersDoNotDiscoverRoles(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, dir := range []string{"internal/apiv1", "internal/builder", "internal/dashboard", "internal/serveapp"} {
		walkProductionGo(t, root, dir, func(path, source string) {
			for _, retired := range []string{
				".(AcknowledgedEventPublisher)", ".(EventRecipientPlanChecker)",
				".(EventReplayOwner)", ".(BundleCatalogRegisterStore)",
				".(decisioncard.ProposedEffectStore)", ".(MailboxNoticeAcknowledgmentStore)",
				".(RunForkExecutorSelector)", ".(runtimebus.RunLifecycleReadPersistence)",
			} {
				if strings.Contains(source, retired) {
					t.Errorf("required role %s is discovered dynamically in %s", retired, path)
				}
			}
		})
	}
}

func TestOperatorReadOwnersAreBounded(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	owner := readArchitectureFile(t, root, "internal/store/internal/operatorsurface/owner.go")
	for _, retired := range []string{"type OperatorPostgres struct", "type OperatorSQLite struct"} {
		if strings.Contains(owner, retired) {
			t.Errorf("broad operator owner survives: %s", retired)
		}
	}
	for _, bounded := range []string{
		"RunPostgres", "EntityPostgres", "AgentPostgres", "ConversationPostgres", "ObservabilityPostgres",
		"RunSQLite", "EntitySQLite", "AgentSQLite", "ConversationSQLite", "ObservabilitySQLite",
	} {
		if !strings.Contains(owner, "type "+bounded+" struct") {
			t.Errorf("bounded operator owner %s is missing", bounded)
		}
	}
}

func TestNonAPIReadConsumersUseCanonicalOperatorReadOwner(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, file := range []string{
		"internal/builder/api.go",
		"internal/builder/run_debug_stream.go",
		"internal/dashboard/server/conversations_sql.go",
		"internal/dashboard/server/observability_sql.go",
		"internal/serveapp/run_stalled_monitor.go",
		"internal/cliapp/forkchat_workspace_admission.go",
	} {
		source := readArchitectureFile(t, root, file)
		if strings.Contains(source, `"github.com/division-sh/swarm/internal/store"`) {
			t.Errorf("read consumer %s still imports the root store facade", file)
		}
	}
}

func TestDigestHasNoProductionPersistenceSurface(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, dir := range []string{"internal/digest", "internal/runtime", "internal/store"} {
		walkProductionGo(t, root, dir, func(path, source string) {
			for _, retired := range []string{"DigestPersistence", "InstanceDigestRow"} {
				if strings.Contains(source, retired) {
					t.Errorf("retired digest contract %s survives in %s", retired, path)
				}
			}
		})
	}
}

func TestAdministrativeLeasesAreOperationSpecific(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, dir := range []string{
		"internal/runtime/bundledelete",
		"internal/runtime/destructivereset",
		"internal/store/internal/adminpersistence",
	} {
		walkProductionGo(t, root, dir, func(path, source string) {
			for _, retired := range []string{
				"TryAcquire(", "BuildPlanWithLock", "DefaultLockKey",
				"destructiveResetCleanupExecutor", "destructiveResetCleanupQuery(",
			} {
				if strings.Contains(source, retired) {
					t.Errorf("generic administrative authority %s survives in %s", retired, path)
				}
			}
		})
	}
	locks := readArchitectureFile(t, root, "internal/store/internal/adminpersistence/destructive_reset_lock.go")
	for _, operation := range []string{"AcquireBundleDelete", "AcquireDestructiveReset"} {
		if !strings.Contains(locks, operation) {
			t.Errorf("operation-specific lease %s is missing", operation)
		}
	}
}

func TestAdjacent2149AuthorityFindingsClosed(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	registry := readArchitectureFile(t, root, "internal/store/testdata/persistence_authority_findings.tsv")
	for _, invalid := range []string{"adjacent-2149\t", "unclassified\t"} {
		if strings.Contains(registry, invalid) {
			t.Errorf("registry retains %q findings", strings.TrimSuffix(invalid, "\t"))
		}
	}
}

func readArchitectureFile(t *testing.T, root, path string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

func walkProductionGo(t *testing.T, root, relative string, visit func(path, source string)) {
	t.Helper()
	base := filepath.Join(root, filepath.FromSlash(relative))
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		visit(filepath.ToSlash(rel), string(raw))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", relative, err)
	}
}
