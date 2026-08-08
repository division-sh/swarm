package testpostgres

import (
	"context"
	"database/sql"
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
	defer dropDatabase(context.Background(), db, name)
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
	defer dropDatabase(context.Background(), db, name)
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
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := "interrupted" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	name := manager.signedResourceName(templateNamePrefix, "template", identity)
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
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := manager.admin.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	identity := "oldschema" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	name := manager.signedResourceName(templateNamePrefix, "template", identity)
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(context.Background(), db, name)
	if err := setDatabaseMetadata(ctx, db, name, resourceMetadata{Version: 1, Kind: "template", Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	assertDatabaseExists(t, manager.admin, name)
}

func TestManagerRecoversIncompleteTemplateInSingleAcquire(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
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
	lockKey := resourceLockKey(intent)
	creator, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := acquireAdvisoryLock(ctx, creator, lockKey, "recover-template "+name); err != nil {
		t.Fatal(err)
	}
	// The template is content-addressed and may already exist (stamped) from a
	// prior ensureTemplate on this server; clear it so the crash state below is
	// the only template. Holding the template advisory lock during setup keeps
	// concurrent managers' ensureTemplate out.
	if err := dropDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	if err := manager.putIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	defer manager.deleteIntent(context.Background(), name)
	if err := createDatabase(ctx, db, name); err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(context.Background(), db, name)
	// Release the lock before Acquire: the fixture manager is dead, so its
	// session-scoped lock is gone — the exact recovery state ensureTemplate
	// must handle.
	releaseAdvisoryLock(creator, lockKey)

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
	defer dropDatabase(context.Background(), db, name)
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
			defer dropDatabase(context.Background(), adminDB, name)

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
			defer dropDatabase(context.Background(), db, name)
			if err := createDatabase(ctx, db, name); err != nil {
				t.Fatal(err)
			}
			if err := manager.retireIntentIfDatabaseAbsent(ctx, db, name); err == nil || !strings.Contains(err.Error(), "retained") {
				t.Fatalf("retire existing database intent error = %v, want retained blocker", err)
			}
			if _, found, err := manager.intent(ctx, name); err != nil || !found {
				t.Fatalf("existing database intent found=%v err=%v, want retained", found, err)
			}
			if err := dropDatabase(ctx, db, name); err != nil {
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
