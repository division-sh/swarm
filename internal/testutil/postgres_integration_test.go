package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestPostgresDSNIntegrationMatrix(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("SWARM_TEST_POSTGRES_DSN"))
	if raw == "" {
		t.Skip("SWARM_TEST_POSTGRES_DSN is required for external DSN integration matrix")
	}
	base, err := parseTestPostgresDSN(raw)
	if err != nil {
		t.Fatalf("parse base DSN: %v", err)
	}
	ensureDefaultUserDatabase(t, base)

	for name, form := range externalPostgresDSNForms(t, base.config) {
		t.Run(name, func(t *testing.T) {
			exercisePostgresDSN(t, form)
		})
	}
}

func TestPostgresDSNNonPostgresRole(t *testing.T) {
	if os.Getenv("SWARM_TEST_POSTGRES_ROLE_PROOF") != "1" {
		t.Skip("set SWARM_TEST_POSTGRES_ROLE_PROOF=1 with a role-admin DSN to run role proof")
	}
	raw := strings.TrimSpace(os.Getenv("SWARM_TEST_POSTGRES_DSN"))
	if raw == "" {
		t.Fatal("SWARM_TEST_POSTGRES_DSN is required for role proof")
	}
	base, err := parseTestPostgresDSN(raw)
	if err != nil {
		t.Fatalf("parse base DSN: %v", err)
	}
	adminDB, err := base.open()
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer adminDB.Close()
	if err := waitForTestDatabase(context.Background(), adminDB, 10*time.Second); err != nil {
		t.Fatalf("admin readiness: %v", err)
	}

	role := fmt.Sprintf("swarm_test_role_%d_%d", os.Getpid(), time.Now().UnixNano())
	password := fmt.Sprintf("swarm-role-%d", time.Now().UnixNano())
	if _, err := adminDB.Exec("CREATE ROLE " + quoteIdent(role) + " LOGIN CREATEDB PASSWORD " + quoteLiteral(password)); err != nil {
		t.Fatalf("create proof role: %v", err)
	}
	t.Cleanup(func() {
		cleanupDB, openErr := base.open()
		if openErr != nil {
			return
		}
		defer cleanupDB.Close()
		_, _ = cleanupDB.Exec("DROP ROLE IF EXISTS " + quoteIdent(role))
	})

	roleConfig := base.config.Clone()
	roleConfig.User = role
	roleConfig.Password = password
	if roleConfig.Database == "" {
		roleConfig.Database = "postgres"
	}
	roleOwner, err := newTestPostgresDSN(roleConfig)
	if err != nil {
		t.Fatalf("role DSN: %v", err)
	}
	roleDSN, err := roleOwner.string()
	if err != nil {
		t.Fatalf("serialize role DSN: %v", err)
	}
	exercisePostgresDSN(t, roleDSN)
}

func TestOwnedPostgresSettings(t *testing.T) {
	if os.Getenv("SWARM_TEST_POSTGRES_ASSERT_OWNED_SETTINGS") != "1" {
		t.Skip("owned Postgres settings proof is opt-in")
	}
	_, db, cleanup := StartPostgres(t)
	defer cleanup()
	for setting, want := range map[string]string{
		"fsync":              "off",
		"synchronous_commit": "off",
		"full_page_writes":   "off",
	} {
		var got string
		if err := db.QueryRow("SHOW " + setting).Scan(&got); err != nil {
			t.Fatalf("SHOW %s: %v", setting, err)
		}
		if got != want {
			t.Fatalf("%s = %q, want %q", setting, got, want)
		}
	}
}

func exercisePostgresDSN(t *testing.T, raw string) {
	t.Helper()
	admin, err := parseTestPostgresDSN(raw)
	if err != nil {
		t.Fatalf("parse source: %v", err)
	}
	adminDB, err := admin.open()
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer adminDB.Close()
	if err := waitForTestDatabase(context.Background(), adminDB, 10*time.Second); err != nil {
		t.Fatalf("admin readiness: %v", err)
	}
	role, err := inspectTestPostgresSession(adminDB)
	if err != nil {
		t.Fatalf("inspect session: %v", err)
	}

	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	templateName := "mas_matrix_template_" + suffix
	cloneName := "mas_matrix_clone_" + suffix
	emptyName := "mas_matrix_empty_" + suffix
	childCleanupName := "mas_matrix_child_" + suffix
	for _, name := range []string{cloneName, emptyName, childCleanupName, templateName} {
		name := name
		t.Cleanup(func() {
			cleanupDB, openErr := admin.open()
			if openErr != nil {
				return
			}
			defer cleanupDB.Close()
			_ = dropIsolatedDatabase(cleanupDB, name)
		})
	}

	if err := createIsolatedDatabase(adminDB, templateName); err != nil {
		t.Fatalf("create template: %v", err)
	}
	templateDSN, err := admin.withDatabase(templateName)
	if err != nil {
		t.Fatalf("project template: %v", err)
	}
	templateDB, err := templateDSN.open()
	if err != nil {
		t.Fatalf("open template: %v", err)
	}
	if err := initializeDatabase(templateDB, role); err != nil {
		_ = templateDB.Close()
		t.Fatalf("initialize template: %v", err)
	}
	if err := templateDB.Close(); err != nil {
		t.Fatalf("close template: %v", err)
	}

	if err := createIsolatedDatabaseFromTemplate(adminDB, cloneName, templateName); err != nil {
		t.Fatalf("clone template: %v", err)
	}
	cloneDSN, err := admin.withDatabase(cloneName)
	if err != nil {
		t.Fatalf("project clone: %v", err)
	}
	serializedClone, err := cloneDSN.string()
	if err != nil {
		t.Fatalf("serialize clone: %v", err)
	}
	cloneDB, err := sql.Open("postgres", serializedClone)
	if err != nil {
		t.Fatalf("open returned clone DSN: %v", err)
	}
	assertStartPostgresSchema(t, cloneDB, mustPlatformVersion(t))
	if err := cloneDB.Close(); err != nil {
		t.Fatalf("close clone: %v", err)
	}

	if err := createIsolatedDatabase(adminDB, emptyName); err != nil {
		t.Fatalf("create empty database: %v", err)
	}
	emptyDSN, err := admin.withDatabase(emptyName)
	if err != nil {
		t.Fatalf("project empty database: %v", err)
	}
	serializedEmpty, err := emptyDSN.string()
	if err != nil {
		t.Fatalf("serialize empty database: %v", err)
	}
	emptyDB, err := sql.Open("postgres", serializedEmpty)
	if err != nil {
		t.Fatalf("open returned empty DSN: %v", err)
	}
	var hasSchemaVersion bool
	if err := emptyDB.QueryRow(`SELECT to_regclass('public.schema_version') IS NOT NULL`).Scan(&hasSchemaVersion); err != nil {
		_ = emptyDB.Close()
		t.Fatalf("inspect empty database: %v", err)
	}
	if err := emptyDB.Close(); err != nil {
		t.Fatalf("close empty database: %v", err)
	}
	if hasSchemaVersion {
		t.Fatal("StartEmptyPostgres projection unexpectedly contained canonical schema")
	}

	if err := createIsolatedDatabase(adminDB, childCleanupName); err != nil {
		t.Fatalf("create child-cleanup database: %v", err)
	}
	proveCleanupChildTransport(t, admin, childCleanupName)
	assertDatabaseAbsent(t, adminDB, childCleanupName)

	for _, name := range []string{cloneName, emptyName, templateName} {
		if err := dropIsolatedDatabase(adminDB, name); err != nil {
			t.Fatalf("drop %s: %v", name, err)
		}
		assertDatabaseAbsent(t, adminDB, name)
	}
}

func externalPostgresDSNForms(t *testing.T, cfg pq.Config) map[string]string {
	t.Helper()
	if len(cfg.Multi) > 0 {
		t.Fatal("integration representation matrix requires a single-host base DSN")
	}
	host := cfg.Host
	if host == "" && cfg.Hostaddr.IsValid() {
		host = cfg.Hostaddr.String()
	}
	if host == "" || strings.HasPrefix(host, "/") || strings.HasPrefix(host, "@") {
		t.Fatal("integration representation matrix requires a TCP host")
	}
	database := cfg.Database
	if database == "" {
		database = cfg.User
	}
	keyword := func(includeDatabase bool) string {
		parts := []string{
			"host=" + quotePostgresKeywordValue(host),
			"port=" + quotePostgresKeywordValue(strconv.Itoa(int(cfg.Port))),
			"user=" + quotePostgresKeywordValue(cfg.User),
			"password=" + quotePostgresKeywordValue(cfg.Password),
			"sslmode=" + quotePostgresKeywordValue(string(cfg.SSLMode)),
		}
		if includeDatabase {
			parts = append(parts, "dbname="+quotePostgresKeywordValue(database))
		}
		return strings.Join(parts, " ")
	}
	postgresURL := func(includeDatabase bool) string {
		u := &url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(cfg.User, cfg.Password),
			Host:   net.JoinHostPort(host, strconv.Itoa(int(cfg.Port))),
		}
		if includeDatabase {
			u.Path = "/" + database
		}
		query := u.Query()
		query.Set("sslmode", string(cfg.SSLMode))
		u.RawQuery = query.Encode()
		return u.String()
	}
	return map[string]string{
		"keyword explicit database": keyword(true),
		"URL explicit database":     postgresURL(true),
		"keyword default database":  keyword(false),
		"URL default database":      postgresURL(false),
	}
}

func ensureDefaultUserDatabase(t *testing.T, admin testPostgresDSN) {
	t.Helper()
	if admin.config.User == "" {
		t.Fatal("base Postgres user is required")
	}
	db, err := admin.open()
	if err != nil {
		t.Fatalf("open admin for default database: %v", err)
	}
	defer db.Close()
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, admin.config.User).Scan(&exists); err != nil {
		t.Fatalf("check default database: %v", err)
	}
	if exists {
		return
	}
	if err := createIsolatedDatabase(db, admin.config.User); err != nil {
		t.Fatalf("create default user database %q: %v", admin.config.User, err)
	}
	t.Cleanup(func() {
		cleanupDB, openErr := admin.open()
		if openErr != nil {
			return
		}
		defer cleanupDB.Close()
		_ = dropIsolatedDatabase(cleanupDB, admin.config.User)
	})
}

func proveCleanupChildTransport(t *testing.T, admin testPostgresDSN, database string) {
	t.Helper()
	adminDSN, err := admin.string()
	if err != nil {
		t.Fatalf("serialize cleanup admin: %v", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^$")
	cmd.Env = append(withoutPostgresConnectionEnv(os.Environ()),
		"SWARM_TEST_POSTGRES_TEMPLATE_CLEANUP=1",
		"SWARM_TEST_POSTGRES_TEMPLATE_PARENT_PID="+strconv.Itoa(int(^uint32(0)>>1)),
		"SWARM_TEST_POSTGRES_TEMPLATE_ADMIN_DSN="+adminDSN,
		"SWARM_TEST_POSTGRES_TEMPLATE_NAME="+database,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cleanup child: %v output=%s", err, strings.TrimSpace(string(output)))
	}
}

func assertDatabaseAbsent(t *testing.T, adminDB *sql.DB, database string) {
	t.Helper()
	var exists bool
	if err := adminDB.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, database).Scan(&exists); err != nil {
		t.Fatalf("check database %q: %v", database, err)
	}
	if exists {
		t.Fatalf("database %q still exists", database)
	}
}

func mustPlatformVersion(t *testing.T) string {
	t.Helper()
	spec, err := loadPlatformSpec()
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	return spec.Platform.Version
}

func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
