package testpostgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

func TestTemplateDigestUsesCanonicalGeneratedSchema(t *testing.T) {
	spec, err := loadPlatformSpec()
	if err != nil {
		t.Fatal(err)
	}
	plans, err := platformschema.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	first, err := templateDigest(plans, spec.Platform.Version, "role", "server", "version")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := yaml.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte("\n# unrelated non-schema spec comment\n")...)
	var reparsed runtimecontracts.PlatformSpecDocument
	source, err := yamlsource.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Decode(&reparsed); err != nil {
		t.Fatal(err)
	}
	reparsedPlans, err := platformschema.GeneratePlatformTableDDLs(reparsed)
	if err != nil {
		t.Fatal(err)
	}
	second, err := templateDigest(reparsedPlans, reparsed.Platform.Version, "role", "server", "version")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("non-schema spec bytes changed template digest: %q != %q", first, second)
	}
	changed := append([]platformschema.TableDDL(nil), plans...)
	changed[0] = plans[0]
	changed[0].Statements = append([]string(nil), plans[0].Statements...)
	changed[0].Statements[0] += "\nALTER TABLE runtime_store_metadata ADD COLUMN digest_probe text"
	third, err := templateDigest(changed, spec.Platform.Version, "role", "server", "version")
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("real generated schema change reused template digest")
	}
}

func TestManagerTemplateDropSQLHasNoAuthorityBypass(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "manager.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"dropSandbox": true, "dropTemplate": true}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee, ok := call.Fun.(*ast.Ident)
			if ok && callee.Name == "dropDatabase" && !allowed[function.Name.Name] {
				t.Errorf("production dropDatabase bypass in %s; database deletion must consume typed sandbox or exclusive template authority", function.Name.Name)
			}
			return true
		})
	}
	testFiles, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, testName := range testFiles {
		testFile, err := parser.ParseFile(token.NewFileSet(), testName, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range testFile.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					if callee.Name == "dropDatabase" && function.Name.Name != "dropTestDatabase" {
						t.Errorf("test dropDatabase bypass in %s:%s; destructive fixtures must use the test-owned boundary", testName, function.Name.Name)
					}
				case *ast.SelectorExpr:
					if callee.Sel.Name == "dropTemplate" && !isTestOwnedTemplateDrop(function) {
						t.Errorf("test dropTemplate bypass in %s:%s; destructive fixtures must use the test-owned boundary", testName, function.Name.Name)
					}
				}
				return true
			})
		}
	}
}

func isTestOwnedTemplateDrop(function *ast.FuncDecl) bool {
	if function.Name.Name != "drop" || function.Recv == nil || len(function.Recv.List) != 1 {
		return false
	}
	receiver, ok := function.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	name, ok := receiver.X.(*ast.Ident)
	return ok && name.Name == "testOwnedTemplate"
}

func TestManagerLifecycleSupportedRepresentations(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv(SourceEnv))
	if raw == "" {
		t.Skip(SourceEnv + " is not set")
	}
	base, err := ParseConnection(raw)
	if err != nil {
		t.Fatal(err)
	}
	params := base.Parameters()
	u := &url.URL{Scheme: "postgres", Host: params.Host + ":" + strconv.Itoa(int(params.Port)), Path: "/" + params.Database}
	u.User = url.UserPassword(params.User, params.Password)
	query := u.Query()
	query.Set("sslmode", params.SSLMode)
	u.RawQuery = query.Encode()
	keyword, err := base.String()
	if err != nil {
		t.Fatal(err)
	}

	for _, source := range []struct{ name, dsn string }{{"keyword", keyword}, {"url", u.String()}} {
		t.Run(source.name, func(t *testing.T) {
			connection, err := ParseConnection(source.dsn)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			manager, err := NewManager(ctx, connection)
			if err != nil {
				t.Fatal(err)
			}
			sandbox, err := manager.Acquire(ctx, true)
			if err != nil {
				t.Fatal(err)
			}
			var version string
			if err := sandbox.DB.QueryRowContext(ctx, `SELECT platform_version FROM runtime_store_metadata WHERE id=1`).Scan(&version); err != nil {
				t.Fatalf("canonical schema missing: %v", err)
			}
			if err := sandbox.Release(ctx); err != nil {
				t.Fatal(err)
			}
			assertDatabaseAbsent(t, connection, sandbox.Name)

			empty, err := manager.Acquire(ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			var table *string
			err = empty.DB.QueryRowContext(ctx, `SELECT to_regclass('public.runtime_store_metadata')::text`).Scan(&table)
			if err != nil || table != nil {
				t.Fatalf("empty sandbox runtime_store_metadata = %v, err=%v", table, err)
			}
			if err := empty.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagerReconcilesSandboxAfterLeaseOwnerDies(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	name := sandbox.Name
	_ = sandbox.DB.Close()
	_ = sandbox.leaseConn.Close()
	sandbox.leaseConn = nil
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertDatabaseAbsent(t, manager.admin, name)
}

func TestManagerReconcileLeavesActiveManagerSandboxUntouched(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sandbox.Release(context.Background())
	peer, err := NewManager(ctx, manager.admin)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertDatabaseExists(t, manager.admin, sandbox.Name)
}

func TestManagerLeavesUnprovableSandboxUntouched(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	name := sandboxNamePrefix + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	defer dropTestDatabase(context.Background(), manager, db, name)
	if err := manager.Reconcile(ctx); err == nil || !strings.Contains(err.Error(), "unprovable") {
		t.Fatalf("Reconcile() error = %v, want unprovable blocker", err)
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists); err != nil || !exists {
		t.Fatalf("sentinel exists=%v err=%v", exists, err)
	}
}

func TestManagerLeavesSignedUnstampedSandboxWithoutIntentUntouched(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := strings.ReplaceAll(uuid.NewString(), "-", "")
	name := manager.signedResourceName(sandboxNamePrefix, "sandbox", identity)
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	defer dropTestDatabase(context.Background(), manager, db, name)
	if err := manager.Reconcile(ctx); err == nil || !strings.Contains(err.Error(), "durable pre-create intent") {
		t.Fatalf("Reconcile() error = %v, want durable-intent blocker", err)
	}
	assertDatabaseExists(t, manager.admin, name)
}

func TestManagerPutIntentRequiresResourceAdvisoryLock(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Run("sandbox", func(t *testing.T) {
		identity := strings.ReplaceAll(uuid.NewString(), "-", "")
		name := manager.signedResourceName(sandboxNamePrefix, "sandbox", identity)
		intent := resourceIntent{Name: name, Kind: "sandbox", Identity: identity, LeaseKey: advisoryKey("sandbox:" + identity)}
		if err := manager.putIntent(ctx, intent); err == nil || !strings.Contains(err.Error(), "requires the resource advisory lock") {
			t.Fatalf("putIntent without lock error = %v, want teaching blocker", err)
		}
		if _, found, err := manager.intent(ctx, name); err != nil || found {
			t.Fatalf("intent found=%v err=%v, want absent after refused put", found, err)
		}
	})

	t.Run("template", func(t *testing.T) {
		identity := "nolock" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
		name := manager.signedResourceName(templateNamePrefix, "template", identity)
		intent := resourceIntent{Name: name, Kind: "template", Identity: identity}
		if err := manager.putIntent(ctx, intent); err == nil || !strings.Contains(err.Error(), "requires the resource advisory lock") {
			t.Fatalf("putIntent without lock error = %v, want teaching blocker", err)
		}
		if _, found, err := manager.intent(ctx, name); err != nil || found {
			t.Fatalf("intent found=%v err=%v, want absent after refused put", found, err)
		}
	})

	t.Run("succeedsWhileLockHeld", func(t *testing.T) {
		db, err := manager.admin.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		identity := strings.ReplaceAll(uuid.NewString(), "-", "")
		name := manager.signedResourceName(sandboxNamePrefix, "sandbox", identity)
		intent := resourceIntent{Name: name, Kind: "sandbox", Identity: identity, LeaseKey: advisoryKey("sandbox:" + identity)}
		creator, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := acquireAdvisoryLock(ctx, creator, resourceLockKey(intent), "teaching "+name); err != nil {
			t.Fatal(err)
		}
		defer releaseAdvisoryLock(creator, resourceLockKey(intent))
		if err := manager.putIntent(ctx, intent); err != nil {
			t.Fatalf("putIntent under resource advisory lock = %v, want success", err)
		}
		defer manager.deleteIntent(context.Background(), name)
		if _, found, err := manager.intent(ctx, name); err != nil || !found {
			t.Fatalf("intent found=%v err=%v, want present", found, err)
		}
	})
}

func TestManagerReclaimsSandboxInterruptedBetweenCreateAndMetadata(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := strings.ReplaceAll(uuid.NewString(), "-", "")
	name := manager.signedResourceName(sandboxNamePrefix, "sandbox", identity)
	intent := resourceIntent{Name: name, Kind: "sandbox", Identity: identity, LeaseKey: advisoryKey("sandbox:" + identity)}
	// The intent is written under the resource advisory lock, held through
	// createDatabase, then released before Reconcile — mirroring a manager
	// session dying between create and metadata, which is exactly the state
	// this test exercises. Holding the lock through create also prevents a
	// concurrent Reconcile from retiring the intent mid-create. #2196
	creator, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := acquireAdvisoryLock(ctx, creator, resourceLockKey(intent), "interrupted "+name); err != nil {
		t.Fatal(err)
	}
	defer releaseAdvisoryLock(creator, resourceLockKey(intent))
	if err := manager.putIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	releaseAdvisoryLock(creator, resourceLockKey(intent))
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertDatabaseAbsent(t, manager.admin, name)
	if _, found, err := manager.intent(ctx, name); err != nil || found {
		t.Fatalf("intent found=%v err=%v, want cleared", found, err)
	}
}

func TestManagerReclaimsTemplateInterruptedBetweenCreateAndMetadata(t *testing.T) {
	fixture := newTestOwnedTemplate(t, integrationManager(t), "interrupted")
	manager := fixture.manager
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := manager.templateID
	name := manager.templateName
	intent := resourceIntent{Name: name, Kind: "template", Identity: identity}
	// Same interrupted-between-create-and-metadata shape as the sandbox twin:
	// write the intent under the lock, hold through create, release (session
	// death), then let Reconcile reclaim. #2196
	creator, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := acquireAdvisoryLock(ctx, creator, resourceLockKey(intent), "interrupted "+name); err != nil {
		t.Fatal(err)
	}
	defer releaseAdvisoryLock(creator, resourceLockKey(intent))
	if err := manager.putIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	releaseAdvisoryLock(creator, resourceLockKey(intent))
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertDatabaseAbsent(t, manager.admin, name)
}

func TestManagerReconcilesAbruptCreateBeforeMetadataExit(t *testing.T) {
	manager := integrationManager(t)
	for _, kind := range []string{"sandbox", "template"} {
		t.Run(kind, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "resource-name")
			command := exec.Command(os.Args[0], "-test.run=^TestManagerCreateBeforeMetadataCrashHelper$")
			command.Env = append(os.Environ(), "SWARM_TEST_MANAGER_CRASH_KIND="+kind, "SWARM_TEST_MANAGER_CRASH_OUTPUT="+output)
			if raw, err := command.CombinedOutput(); err == nil {
				t.Fatalf("crash helper unexpectedly succeeded: %s", raw)
			}
			rawName, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			name := strings.TrimSpace(string(rawName))
			assertDatabaseExists(t, manager.admin, name)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			// The child exited holding the resource advisory lock; the Postgres
			// backend releases the session-scoped lock asynchronously on socket
			// EOF. Wait for the lock to be free (bounded deadline-poll, no
			// sleeps) so Reconcile's single try-lock cannot silently skip the
			// reclaimed resource. #2196
			if err := waitForAdvisoryLockFree(ctx, manager, name, kind); err != nil {
				t.Fatal(err)
			}
			if err := manager.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			assertDatabaseAbsent(t, manager.admin, name)
		})
	}
}

func TestManagerCreateBeforeMetadataCrashHelper(t *testing.T) {
	kind := os.Getenv("SWARM_TEST_MANAGER_CRASH_KIND")
	if kind == "" {
		t.Skip("subprocess helper")
	}
	output := os.Getenv("SWARM_TEST_MANAGER_CRASH_OUTPUT")
	connection, err := ConnectionFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	manager, err := NewManager(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	identity := kind + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	prefix := sandboxNamePrefix
	leaseKey := advisoryKey("sandbox:" + identity)
	if kind == "template" {
		prefix = templateNamePrefix
		leaseKey = 0
	}
	name := manager.signedResourceName(prefix, kind, identity)
	intent := resourceIntent{Name: name, Kind: kind, Identity: identity, LeaseKey: leaseKey}
	// Mirror production (Acquire/ensureTemplate): the advisory lock is taken
	// BEFORE the durable intent is written. The process then crashes via
	// os.Exit, which releases the session-scoped lock — the exact
	// interrupted-between-create-and-metadata state the parent test reclaims.
	// #2196
	lockConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := acquireAdvisoryLock(ctx, lockConn, resourceLockKey(intent), "crash-window "+name); err != nil {
		t.Fatal(err)
	}
	if err := manager.putIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, []byte(name+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	os.Exit(91)
}

func TestManagerRetainsStampedTemplateFromOlderSchemaDigest(t *testing.T) {
	fixture := newTestOwnedTemplate(t, integrationManager(t), "oldschema")
	manager := fixture.manager
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := manager.templateID
	name := manager.templateName
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	if err := setDatabaseMetadata(ctx, db, name, resourceMetadata{Version: 1, Kind: "template", Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertDatabaseExists(t, manager.admin, name)
}

func TestManagerRecoversIncompleteTemplateInSingleAcquire(t *testing.T) {
	canonicalManager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	canonicalSandbox, err := canonicalManager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := canonicalSandbox.Release(ctx); err != nil {
		t.Fatal(err)
	}

	fixture := newTestOwnedTemplate(t, canonicalManager, "recovery")
	manager := fixture.manager
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Crash-state fixture: the template database exists with an empty comment
	// (create completed, metadata never stamped) and a matching durable intent.
	// ensureTemplate must drop the incomplete template, delete its intent, and
	// recreate it in the SAME call — a single Acquire(withTemplate=true) must
	// succeed without the caller retrying.
	name := manager.templateName
	intent := resourceIntent{Name: name, Kind: "template", Identity: manager.templateID}
	creator, err := manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	defer creator.release()
	// The template is content-addressed and may already exist (stamped) from a
	// prior ensureTemplate on this server; clear it so the crash state below is
	// the only template. Holding the template advisory lock during setup keeps
	// concurrent managers' ensureTemplate out.
	if err := fixture.drop(ctx, db, creator); err != nil {
		t.Fatal(err)
	}
	if err := manager.putIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	defer manager.deleteIntent(context.Background(), name)
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	// Release the lock before Acquire: the fixture manager is dead, so its
	// session-scoped lock is gone — the exact recovery state ensureTemplate
	// must handle.
	creator.release()

	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatalf("single Acquire(withTemplate=true) after incomplete template = %v, want successful recovery and recreate", err)
	}
	defer sandbox.Release(context.Background())

	var comment string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(shobj_description(d.oid, 'pg_database'), '') FROM pg_database d WHERE d.datname=$1`, name).Scan(&comment); err != nil {
		t.Fatal(err)
	}
	metadata, parseErr := parseResourceMetadata(comment)
	if parseErr != nil || metadata.Kind != "template" || metadata.Identity != manager.templateID {
		t.Fatalf("recovered template metadata parseErr=%v metadata=%+v, want stamped template identity", parseErr, metadata)
	}
	assertDatabaseExists(t, canonicalManager.admin, canonicalManager.templateName)
	canonicalSandbox, err = canonicalManager.Acquire(ctx, true)
	if err != nil {
		t.Fatalf("canonical template was damaged by recovery fixture: %v", err)
	}
	if err := canonicalSandbox.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerRetiresMatchingIntentForValidTemplate(t *testing.T) {
	fixture := newTestOwnedTemplate(t, integrationManager(t), "validintent")
	manager := fixture.manager
	ensureOwnedTemplate(t, manager)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	possession, err := manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	intent := resourceIntent{Name: manager.templateName, Kind: "template", Identity: manager.templateID}
	if err := manager.putIntent(ctx, intent); err != nil {
		possession.release()
		t.Fatal(err)
	}
	possession.release()

	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatalf("reuse valid template with matching stale intent: %v", err)
	}
	defer sandbox.Release(context.Background())
	if _, found, err := manager.intent(ctx, manager.templateName); err != nil || found {
		t.Fatalf("valid template intent found=%v err=%v, want retired", found, err)
	}
}

func TestManagerReconstructsAbsentTemplateWithMatchingIntent(t *testing.T) {
	fixture := newTestOwnedTemplate(t, integrationManager(t), "absentintent")
	manager := fixture.manager
	fixture.remove(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	possession, err := manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	intent := resourceIntent{Name: manager.templateName, Kind: "template", Identity: manager.templateID}
	if err := manager.putIntent(ctx, intent); err != nil {
		possession.release()
		t.Fatal(err)
	}
	possession.release()

	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatalf("reconstruct absent template with matching stale intent: %v", err)
	}
	defer sandbox.Release(context.Background())
	assertDatabaseExists(t, manager.admin, manager.templateName)
	if _, found, err := manager.intent(ctx, manager.templateName); err != nil || found {
		t.Fatalf("reconstructed template intent found=%v err=%v, want retired", found, err)
	}
}

func TestManagerReplaysTemplateDropBeforeIntentRetirement(t *testing.T) {
	fixture := newTestOwnedTemplate(t, integrationManager(t), "droppedintent")
	manager := fixture.manager
	ensureOwnedTemplate(t, manager)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	possession, err := manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	intent := resourceIntent{Name: manager.templateName, Kind: "template", Identity: manager.templateID}
	if err := manager.putIntent(ctx, intent); err != nil {
		possession.release()
		t.Fatal(err)
	}
	if err := fixture.drop(ctx, db, possession); err != nil {
		possession.release()
		t.Fatal(err)
	}
	possession.release() // Simulate process death before deleteIntent.
	assertDatabaseAbsent(t, manager.admin, manager.templateName)

	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatalf("replay after template drop before intent retirement: %v", err)
	}
	defer sandbox.Release(context.Background())
	assertDatabaseExists(t, manager.admin, manager.templateName)
	if _, found, err := manager.intent(ctx, manager.templateName); err != nil || found {
		t.Fatalf("replayed template intent found=%v err=%v, want retired", found, err)
	}
}

func TestManagerRejectsMismatchedTemplateIntentForValidAndAbsentState(t *testing.T) {
	for _, state := range []string{"valid", "absent"} {
		t.Run(state, func(t *testing.T) {
			fixture := newTestOwnedTemplate(t, integrationManager(t), "mismatch"+state)
			manager := fixture.manager
			if state == "valid" {
				ensureOwnedTemplate(t, manager)
			} else {
				fixture.remove(t)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			controlDB, err := manager.control.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer controlDB.Close()
			if _, err := controlDB.ExecContext(ctx, `INSERT INTO `+quoteIdent(intentTableName)+`
				(resource_name, kind, identity, lease_key, template_name) VALUES ($1,'template','mismatched',0,'')`, manager.templateName); err != nil {
				t.Fatal(err)
			}
			if sandbox, err := manager.Acquire(ctx, true); err == nil || !strings.Contains(err.Error(), "durable intent mismatch") {
				if sandbox != nil {
					_ = sandbox.Release(ctx)
				}
				t.Fatalf("Acquire with mismatched %s template intent error = %v, want fail-closed mismatch", state, err)
			}
		})
	}
}

func TestManagerConcurrentFirstConstructionAndClones(t *testing.T) {
	base := integrationManager(t)
	fixture := newTestOwnedTemplate(t, base, "concurrent")
	manager := fixture.manager
	fixture.remove(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := make(chan struct{})
	done := make(chan error, 2)
	for range 2 {
		copy := *manager
		go func() {
			<-start
			sandbox, err := copy.Acquire(ctx, true)
			if err == nil {
				err = sandbox.Release(ctx)
			}
			done <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("concurrent first template construction/clone: %v", err)
		}
	}

	// Once valid, both package-equivalent managers must reach clone admission
	// together. An exclusive validation lock would deadlock this barrier and
	// expose accidental clone serialization.
	ready := make(chan struct{}, 2)
	resume := make(chan struct{})
	for range 2 {
		copy := *manager
		copy.beforeTemplateClone = func(context.Context, *sql.Conn) error {
			ready <- struct{}{}
			<-resume
			return nil
		}
		go func() {
			sandbox, err := copy.Acquire(ctx, true)
			if err == nil {
				err = sandbox.Release(ctx)
			}
			done <- err
		}()
	}
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			close(resume)
			t.Fatal("healthy template clones did not overlap: " + ctx.Err().Error())
		}
	}
	close(resume)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("overlapping healthy template clone: %v", err)
		}
	}
}

func TestManagerTemplateDeletionWaitsForActiveCloneProcess(t *testing.T) {
	base := integrationManager(t)
	fixture := newTestOwnedTemplate(t, base, "activeclone")
	manager := fixture.manager
	identity := manager.templateID
	ensureOwnedTemplate(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	resume := filepath.Join(dir, "resume")
	command, output := templateCloneHelperCommand(identity, "pause", ready, resume)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	if _, err := waitForFile(ctx, ready); err != nil {
		t.Fatal(err)
	}

	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := resourceLockKey(resourceIntent{Name: manager.templateName, Kind: "template", Identity: identity})
	probe, acquired, err := tryAdvisoryLock(ctx, db, key)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		releaseAdvisoryLock(probe, key)
		t.Fatal("exclusive template deletion possession acquired during active clone")
	}

	attempted := make(chan struct{})
	deleted := make(chan error, 1)
	go func() {
		close(attempted)
		possession, err := manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
		if err == nil {
			err = fixture.drop(ctx, db, possession)
			possession.release()
		}
		deleted <- err
	}()
	<-attempted
	select {
	case err := <-deleted:
		t.Fatalf("template deletion completed during active clone: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := os.WriteFile(resume, []byte("continue\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("clone helper failed: %v\n%s", err, output.String())
	}
	waited = true
	select {
	case err := <-deleted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertDatabaseAbsent(t, manager.admin, manager.templateName)
}

func TestManagerTemplateDeletionRequiresHeldExclusivePossession(t *testing.T) {
	base := integrationManager(t)
	fixture := newTestOwnedTemplate(t, base, "deleteauthority")
	manager := fixture.manager
	identity := manager.templateID
	ensureOwnedTemplate(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	shared, err := manager.acquireTemplatePossession(ctx, db, templateLockShared)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.drop(ctx, db, shared); err == nil || !strings.Contains(err.Error(), "exclusive") {
		t.Fatalf("drop with shared template possession error = %v, want exclusive-authority blocker", err)
	}
	shared.release()
	assertDatabaseExists(t, manager.admin, manager.templateName)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := resourceLockKey(resourceIntent{Name: manager.templateName, Kind: "template", Identity: identity})
	fabricated := &templatePossession{manager: manager, conn: conn, name: manager.templateName, key: key, mode: templateLockExclusive}
	if err := fixture.drop(ctx, db, fabricated); err == nil || !strings.Contains(err.Error(), "held exclusive") {
		t.Fatalf("drop with fabricated template possession error = %v, want held-authority blocker", err)
	}
	assertDatabaseExists(t, manager.admin, manager.templateName)
}

func TestManagerTemplateInitializationFailureCleansUpWithExclusivePossession(t *testing.T) {
	base := integrationManager(t)
	fixture := newTestOwnedTemplate(t, base, "failedinit")
	manager := fixture.manager
	manager.ddlPlans = append([]platformschema.TableDDL(nil), manager.ddlPlans...)
	manager.ddlPlans[0].Statements = []string{`SELECT missing_template_initialization_owner()`}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if sandbox, err := manager.Acquire(ctx, true); err == nil {
		_ = sandbox.Release(ctx)
		t.Fatal("template initialization with invalid canonical plan unexpectedly succeeded")
	}
	assertDatabaseAbsent(t, manager.admin, manager.templateName)
	if _, found, err := manager.intent(ctx, manager.templateName); err != nil || found {
		t.Fatalf("failed template intent found=%v err=%v, want absent after exclusive cleanup", found, err)
	}
}

func TestManagerCrashedClonerReleasesIdentityAndFencesBusyDDLProcess(t *testing.T) {
	base := integrationManager(t)
	fixture := newTestOwnedTemplate(t, base, "crashedclone")
	manager := fixture.manager
	identity := manager.templateID
	ensureOwnedTemplate(t, manager)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	command, output := templateCloneHelperCommand(identity, "busy", ready, "")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	rawPID, err := waitForFile(ctx, ready)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(rawPID))
	if err != nil {
		t.Fatalf("parse clone backend PID %q: %v", rawPID, err)
	}
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := waitForBackendActivity(ctx, db, pid, true); err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatalf("busy clone helper unexpectedly succeeded: %s", output.String())
	}
	waited = true

	exclusiveAcquired := make(chan struct{})
	deleted := make(chan error, 1)
	go func() {
		possession, err := manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
		if err == nil {
			close(exclusiveAcquired)
			err = fixture.drop(ctx, db, possession)
			possession.release()
		}
		deleted <- err
	}()
	select {
	case <-exclusiveAcquired:
	case err := <-deleted:
		t.Fatalf("acquire exclusive possession after client death: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if active, err := backendActivity(ctx, db, pid); err != nil {
		t.Fatal(err)
	} else if !active {
		t.Fatal("busy clone DDL backend ended before exact possession release was proven")
	}
	assertDatabaseExists(t, manager.admin, manager.templateName)
	select {
	case err := <-deleted:
		t.Fatalf("template deletion overlapped the still-busy clone DDL backend: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case err := <-deleted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := waitForBackendActivity(ctx, db, pid, false); err != nil {
		t.Fatal(err)
	}
	assertDatabaseAbsent(t, manager.admin, manager.templateName)
}

func TestManagerTemplateCloneProcessHelper(t *testing.T) {
	mode := os.Getenv("SWARM_TEST_TEMPLATE_CLONE_HELPER_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	manager := managerWithTemplateIdentity(integrationManager(t), os.Getenv("SWARM_TEST_TEMPLATE_CLONE_IDENTITY"))
	ready := os.Getenv("SWARM_TEST_TEMPLATE_CLONE_READY")
	resume := os.Getenv("SWARM_TEST_TEMPLATE_CLONE_RESUME")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	manager.beforeTemplateClone = func(ctx context.Context, conn *sql.Conn) error {
		var pid int
		if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return err
		}
		if err := os.WriteFile(ready, []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
			return err
		}
		switch mode {
		case "pause":
			_, err := waitForFile(ctx, resume)
			return err
		case "busy":
			_, err := conn.ExecContext(ctx, `SELECT pg_sleep(5)`)
			return err
		default:
			return fmt.Errorf("unknown template clone helper mode %q", mode)
		}
	}
	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := sandbox.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerDDLAdmissionSharesSandboxWorkAndFencesTemplateMutation(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	key := advisoryKey("global-ddl-admission")
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_lock_shared($1)`, key); err != nil {
		t.Fatal(err)
	}
	defer holder.ExecContext(context.Background(), `SELECT pg_advisory_unlock_shared($1)`, key)
	if err := manager.withDDLAdmission(ctx, db, "concurrent sandbox proof", func(*sql.Conn) error { return nil }); err != nil {
		t.Fatalf("shared sandbox admission blocked by shared holder: %v", err)
	}
	exclusiveCtx, exclusiveCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer exclusiveCancel()
	err = manager.withExclusiveDDLAdmission(exclusiveCtx, db, "template fence proof", func(*sql.Conn) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("exclusive admission error = %v, want shared-holder fence", err)
	}
}

func TestManagerIntentAuthorityInitializationUsesExclusiveDDLAdmission(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	controlDB, err := manager.control.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer controlDB.Close()
	holder, err := controlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	key := advisoryKey("global-ddl-admission")
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_lock_shared($1)`, key); err != nil {
		t.Fatal(err)
	}
	defer holder.ExecContext(context.Background(), `SELECT pg_advisory_unlock_shared($1)`, key)

	blockedCtx, blockedCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer blockedCancel()
	err = manager.ensureIntentAuthority(blockedCtx, controlDB)
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("intent authority initialization error = %v, want exclusive DDL fence", err)
	}
}

func TestManagerDDLAdmissionGivesQueuedExclusiveWriterPriority(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	mainKey := advisoryKey("global-ddl-admission")
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_lock_shared($1)`, mainKey); err != nil {
		t.Fatal(err)
	}
	defer holder.ExecContext(context.Background(), `SELECT pg_advisory_unlock_shared($1)`, mainKey)

	order := make(chan string, 2)
	exclusiveDone := make(chan error, 1)
	go func() {
		exclusiveDone <- manager.withExclusiveDDLAdmission(ctx, db, "queued template mutation", func(*sql.Conn) error {
			order <- "exclusive"
			return nil
		})
	}()
	waitForExclusiveAdmissionGate(t, ctx, db)
	sharedDone := make(chan error, 1)
	go func() {
		sharedDone <- manager.withDDLAdmission(ctx, db, "late sandbox mutation", func(*sql.Conn) error {
			order <- "shared"
			return nil
		})
	}()
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_unlock_shared($1)`, mainKey); err != nil {
		t.Fatal(err)
	}
	if first := <-order; first != "exclusive" {
		t.Fatalf("first admitted operation = %q, want queued exclusive writer", first)
	}
	if err := <-exclusiveDone; err != nil {
		t.Fatal(err)
	}
	if err := <-sharedDone; err != nil {
		t.Fatal(err)
	}
}

func waitForExclusiveAdmissionGate(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	key := advisoryKey("global-ddl-admission-gate")
	for {
		var acquired bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock_shared($1)`, key).Scan(&acquired); err != nil {
			t.Fatal(err)
		}
		if !acquired {
			return
		}
		if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock_shared($1)`, key); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestManagerReconcileRefreshesCreateWindowAfterTakingLease(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := strings.ReplaceAll(uuid.NewString(), "-", "")
	name := manager.signedResourceName(sandboxNamePrefix, "sandbox", identity)
	leaseKey := advisoryKey("sandbox:" + identity)
	intent := resourceIntent{Name: name, Kind: "sandbox", Identity: identity, LeaseKey: leaseKey}
	// The intent is written under the resource advisory lock (mirroring
	// production Acquire); the lock is released before reconcileDatabaseCandidate,
	// which takes the lease itself to refresh the stale CREATE-window snapshot.
	// #2196
	creator, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := acquireAdvisoryLock(ctx, creator, resourceLockKey(intent), "create-window "+name); err != nil {
		t.Fatal(err)
	}
	defer releaseAdvisoryLock(creator, resourceLockKey(intent))
	if err := manager.putIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	defer dropTestDatabase(context.Background(), manager, db, name)
	candidate := databaseCandidate{name: name, owner: manager.role, comment: ""}
	if err := setDatabaseMetadata(ctx, db, name, resourceMetadata{Version: 1, Kind: "sandbox", Identity: identity, LeaseKey: leaseKey}); err != nil {
		t.Fatal(err)
	}
	if err := manager.deleteIntent(ctx, name); err != nil {
		t.Fatal(err)
	}
	releaseAdvisoryLock(creator, resourceLockKey(intent))
	if err := manager.reconcileDatabaseCandidate(ctx, db, candidate); err != nil {
		t.Fatalf("stale CREATE-window snapshot produced a false blocker: %v", err)
	}
	assertDatabaseAbsent(t, manager.admin, name)
}

func TestManagerReconcileRefreshesIntentSnapshotAfterTakingLease(t *testing.T) {
	manager := integrationManager(t)
	for _, kind := range []string{"sandbox", "template"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			adminDB, err := manager.admin.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer adminDB.Close()
			identity := kind + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
			prefix := sandboxNamePrefix
			leaseKey := advisoryKey("sandbox:" + identity)
			if kind == "template" {
				prefix = templateNamePrefix
				leaseKey = 0
			}
			name := manager.signedResourceName(prefix, kind, identity)
			intent := resourceIntent{Name: name, Kind: kind, Identity: identity, LeaseKey: leaseKey}

			// Mirrors production (Acquire/ensureTemplate): the resource advisory
			// lock is held from BEFORE the durable intent is written through
			// createDatabase, so a concurrent manager's Reconcile (shared control
			// DB + advisory keyspace, see NewManager) can never retire the intent
			// mid-create and orphan the database. #2196.
			lockKey := resourceLockKey(intent)
			creator, err := adminDB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := acquireAdvisoryLock(ctx, creator, lockKey, "snapshot-race "+name); err != nil {
				t.Fatal(err)
			}
			defer releaseAdvisoryLock(creator, lockKey)
			if err := manager.putIntent(ctx, intent); err != nil {
				t.Fatal(err)
			}
			defer manager.deleteIntent(context.Background(), name)
			defer dropTestDatabase(context.Background(), manager, adminDB, name)

			snapshotReady := make(chan struct{})
			resume := make(chan struct{})
			resumed := false
			manager.afterCandidateSnapshot = func() {
				close(snapshotReady)
				<-resume
			}
			defer func() {
				if !resumed {
					close(resume)
				}
				manager.afterCandidateSnapshot = nil
			}()
			done := make(chan error, 1)
			go func() { done <- manager.Reconcile(ctx) }()
			select {
			case <-snapshotReady:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			if err := createDatabase(ctx, adminDB, name); err != nil {
				t.Fatal(err)
			}
			releaseAdvisoryLock(creator, lockKey)
			close(resume)
			resumed = true
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			assertDatabaseAbsent(t, manager.admin, name)
			if _, found, err := manager.intent(ctx, name); err != nil || found {
				t.Fatalf("intent found=%v err=%v, want retired after exact reconciliation", found, err)
			}
		})
	}
}

func TestManagerRetiresFailedCreateIntentOnlyAfterExactAbsence(t *testing.T) {
	manager := integrationManager(t)
	for _, kind := range []string{"sandbox", "template"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			db, err := manager.admin.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			identity := kind + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
			prefix := sandboxNamePrefix
			leaseKey := advisoryKey("sandbox:" + identity)
			if kind == "template" {
				prefix = templateNamePrefix
				leaseKey = 0
			}
			name := manager.signedResourceName(prefix, kind, identity)
			intent := resourceIntent{Name: name, Kind: kind, Identity: identity, LeaseKey: leaseKey}
			// Mirrors production (Acquire holds the lease for the resource
			// lifetime): the advisory lock is acquired BEFORE the durable intent
			// and held through the whole retained/retired assertions, so a
			// concurrent manager's Reconcile (shared control DB + advisory
			// keyspace, see NewManager) can never drop the database or retire
			// the intent underneath this test. #2196.
			lockKey := resourceLockKey(intent)
			creator, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseAdvisoryLock(creator, lockKey)
			if err := acquireAdvisoryLock(ctx, creator, lockKey, "retain-race "+name); err != nil {
				t.Fatal(err)
			}
			if err := manager.putIntent(ctx, intent); err != nil {
				t.Fatal(err)
			}
			defer manager.deleteIntent(context.Background(), name)
			defer dropTestDatabase(context.Background(), manager, db, name)
			if err := createDatabase(ctx, db, name); err != nil {
				t.Fatal(err)
			}
			if err := manager.retireIntentIfDatabaseAbsent(ctx, db, name); err == nil || !strings.Contains(err.Error(), "retained") {
				t.Fatalf("retire existing database intent error = %v, want retained blocker", err)
			}
			if _, found, err := manager.intent(ctx, name); err != nil || !found {
				t.Fatalf("existing database intent found=%v err=%v, want retained", found, err)
			}
			if err := dropTestDatabase(ctx, manager, db, name); err != nil {
				t.Fatal(err)
			}
			if err := manager.retireIntentIfDatabaseAbsent(ctx, db, name); err != nil {
				t.Fatal(err)
			}
			if _, found, err := manager.intent(ctx, name); err != nil || found {
				t.Fatalf("absent database intent found=%v err=%v, want retired", found, err)
			}
		})
	}
}

func integrationManager(t *testing.T) *Manager {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(SourceEnv))
	if raw == "" {
		t.Skip(SourceEnv + " is not set")
	}
	connection, err := ParseConnection(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	manager, err := NewManager(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func managerWithTemplateIdentity(manager *Manager, identity string) *Manager {
	copy := *manager
	copy.templateID = identity
	copy.templateName = copy.signedResourceName(templateNamePrefix, "template", identity)
	copy.beforeTemplateClone = nil
	return &copy
}

type testOwnedTemplate struct {
	manager       *Manager
	canonicalName string
}

func newTestOwnedTemplate(t *testing.T, canonical *Manager, purpose string) *testOwnedTemplate {
	t.Helper()
	identity := purpose + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	manager := managerWithTemplateIdentity(canonical, identity)
	if manager.templateName == canonical.templateName {
		t.Fatal("test-owned template fixture must not use the canonical manager identity")
	}
	fixture := &testOwnedTemplate{manager: manager, canonicalName: canonical.templateName}
	t.Cleanup(func() { fixture.cleanup(t) })
	return fixture
}

func (f *testOwnedTemplate) drop(ctx context.Context, db *sql.DB, possession *templatePossession) error {
	if f == nil || f.manager == nil || f.manager.templateName == f.canonicalName {
		return fmt.Errorf("test-owned template deletion refuses the canonical manager identity")
	}
	return f.manager.dropTemplate(ctx, db, possession)
}

func (f *testOwnedTemplate) remove(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := f.manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	possession, err := f.manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
	if err != nil {
		t.Fatal(err)
	}
	defer possession.release()
	if err := f.drop(ctx, db, possession); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.deleteIntent(ctx, f.manager.templateName); err != nil {
		t.Fatal(err)
	}
}

func (f *testOwnedTemplate) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := f.manager.admin.Open()
	if err != nil {
		t.Errorf("open template cleanup database: %v", err)
		return
	}
	defer db.Close()
	possession, err := f.manager.acquireTemplatePossession(ctx, db, templateLockExclusive)
	if err != nil {
		t.Errorf("acquire template cleanup possession: %v", err)
		return
	}
	defer possession.release()
	if err := f.drop(ctx, db, possession); err != nil {
		t.Errorf("drop test-owned template: %v", err)
	}
	if err := f.manager.deleteIntent(ctx, f.manager.templateName); err != nil {
		t.Errorf("delete test-owned template intent: %v", err)
	}
}

func dropTestDatabase(ctx context.Context, canonical *Manager, db databaseExecer, name string) error {
	if strings.HasPrefix(name, templateNamePrefix) {
		if _, ok := canonical.verifyResourceName(name, templateNamePrefix, "template"); !ok {
			return fmt.Errorf("test database cleanup refuses unsigned template %q", name)
		}
		if name == canonical.templateName {
			return fmt.Errorf("test database cleanup refuses canonical template %q", name)
		}
	}
	return dropDatabase(ctx, db, name)
}

func templateCloneHelperCommand(identity, mode, ready, resume string) (*exec.Cmd, *bytes.Buffer) {
	command := exec.Command(os.Args[0], "-test.run=^TestManagerTemplateCloneProcessHelper$")
	command.Env = append(os.Environ(),
		"SWARM_TEST_TEMPLATE_CLONE_HELPER_MODE="+mode,
		"SWARM_TEST_TEMPLATE_CLONE_IDENTITY="+identity,
		"SWARM_TEST_TEMPLATE_CLONE_READY="+ready,
		"SWARM_TEST_TEMPLATE_CLONE_RESUME="+resume,
	)
	output := &bytes.Buffer{}
	command.Stdout = output
	command.Stderr = output
	return command, output
}

func waitForFile(ctx context.Context, path string) (string, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			return string(raw), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for file %q: %w", path, ctx.Err())
		case <-ticker.C:
		}
	}
}

func backendActivity(ctx context.Context, db databaseRowQueryer, pid int) (bool, error) {
	var active bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active')`, pid).Scan(&active)
	return active, err
}

func waitForBackendActivity(ctx context.Context, db databaseRowQueryer, pid int, want bool) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		active, err := backendActivity(ctx, db, pid)
		if err != nil {
			return err
		}
		if active == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres backend %d active=%v, want %v before deadline", pid, active, want)
		case <-ticker.C:
		}
	}
}

func ensureOwnedTemplate(t *testing.T, manager *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := sandbox.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertDatabaseAbsent(t *testing.T, connection Connection, name string) {
	t.Helper()
	db, err := connection.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("database %q still exists", name)
	}
}

// waitForAdvisoryLockFree polls until the resource advisory lock is no longer
// granted by any session. It uses a deadline-poll on the deadline clock (no
// sleeps) per the deterministic-wait discipline, reuses the manager's
// advisory-lock introspection, and derives the key through resourceLockKey so
// the poll cannot drift from the guard or the reconciler.
func waitForAdvisoryLockFree(ctx context.Context, manager *Manager, name, kind string) error {
	prefix := sandboxNamePrefix
	if kind == "template" {
		prefix = templateNamePrefix
	}
	identity, ok := manager.verifyResourceName(name, prefix, kind)
	if !ok {
		return fmt.Errorf("verify %s resource name %q", kind, name)
	}
	leaseKey := int64(0)
	if kind == "sandbox" {
		leaseKey = advisoryKey("sandbox:" + identity)
	}
	lockKey := resourceLockKey(resourceIntent{Name: name, Kind: kind, Identity: identity, LeaseKey: leaseKey})
	db, err := manager.admin.Open()
	if err != nil {
		return err
	}
	defer db.Close()
	for {
		held, err := manager.advisoryLockHeld(ctx, db, lockKey)
		if err != nil {
			return err
		}
		if !held {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("resource advisory lock %d for %q never released before context deadline", lockKey, name)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func assertDatabaseExists(t *testing.T, connection Connection, name string) {
	t.Helper()
	db, err := connection.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("database %q is absent", name)
	}
}
