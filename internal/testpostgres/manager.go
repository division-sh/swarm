package testpostgres

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const (
	resourceMetadataPrefix = "swarm-test-postgres:v1:"
	templateNamePrefix     = "mas_template_v1_"
	sandboxNamePrefix      = "mas_test_v1_"
)

type Manager struct {
	admin        Connection
	role         string
	ownershipKey []byte
	templateID   string
	templateName string
	spec         runtimecontracts.PlatformSpecDocument
	ddlPlans     []platformschema.TableDDL
}

type Sandbox struct {
	Name       string
	Connection Connection
	DB         *sql.DB

	manager    *Manager
	leaseConn  *sql.Conn
	leaseKey   int64
	once       sync.Once
	releaseErr error
}

type resourceMetadata struct {
	Version  int    `json:"version"`
	Kind     string `json:"kind"`
	Identity string `json:"identity"`
	LeaseKey int64  `json:"lease_key,omitempty"`
	Template string `json:"template,omitempty"`
}

func NewManager(ctx context.Context, admin Connection) (*Manager, error) {
	db, err := admin.Open()
	if err != nil {
		return nil, fmt.Errorf("open postgres test admin connection: %w", err)
	}
	defer db.Close()
	if err := waitForDatabase(ctx, db, 30*time.Second); err != nil {
		return nil, fmt.Errorf("postgres test admin connection is not ready: %w", err)
	}
	role, serverID, err := inspectSession(ctx, db)
	if err != nil {
		return nil, err
	}
	spec, raw, err := loadPlatformSpec()
	if err != nil {
		return nil, err
	}
	plans, err := platformschema.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return nil, fmt.Errorf("generate platform table DDL: %w", err)
	}
	var serverVersion string
	if err := db.QueryRowContext(ctx, `SHOW server_version_num`).Scan(&serverVersion); err != nil {
		return nil, fmt.Errorf("read postgres server version: %w", err)
	}
	hash := sha256.New()
	_, _ = hash.Write(raw)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(role))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(serverID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(serverVersion))
	digest := hex.EncodeToString(hash.Sum(nil))[:24]
	keyDigest := sha256.Sum256([]byte("swarm-test-postgres:v1:ownership\x00" + serverID + "\x00" + role))
	m := &Manager{
		admin: admin, role: role, ownershipKey: keyDigest[:], templateID: digest,
		spec: spec, ddlPlans: plans,
	}
	m.templateName = m.signedResourceName(templateNamePrefix, "template", digest)
	if err := m.Reconcile(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

func ManagerFromEnvironment(ctx context.Context) (*Manager, error) {
	connection, err := ConnectionFromEnvironment()
	if err != nil {
		return nil, err
	}
	manager, err := NewManager(ctx, connection)
	if err != nil {
		return nil, fmt.Errorf("%s is set but unusable; no Docker fallback is available; see internal/testutil/POSTGRES.md: %w", SourceEnv, err)
	}
	return manager, nil
}

func (m *Manager) Acquire(ctx context.Context, withTemplate bool) (*Sandbox, error) {
	adminDB, err := m.admin.Open()
	if err != nil {
		return nil, fmt.Errorf("open postgres admin: %w", err)
	}
	defer adminDB.Close()

	if withTemplate {
		if err := m.ensureTemplate(ctx, adminDB); err != nil {
			return nil, err
		}
	}

	identity := uuid.NewString()
	identity = strings.ReplaceAll(identity, "-", "")
	name := m.signedResourceName(sandboxNamePrefix, "sandbox", identity)
	leaseKey := advisoryKey("sandbox:" + identity)
	leaseConn, err := adminDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open sandbox lease connection: %w", err)
	}
	if err := acquireAdvisoryLock(ctx, leaseConn, leaseKey, "sandbox "+name); err != nil {
		_ = leaseConn.Close()
		return nil, err
	}

	cleanupLease := true
	defer func() {
		if cleanupLease {
			_, _ = leaseConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, leaseKey)
			_ = leaseConn.Close()
		}
	}()

	if withTemplate {
		err = m.withDDLAdmission(ctx, adminDB, "clone sandbox "+name, func(conn *sql.Conn) error {
			return createDatabaseFromTemplate(ctx, conn, name, m.templateName)
		})
	} else {
		err = m.withDDLAdmission(ctx, adminDB, "create empty sandbox "+name, func(conn *sql.Conn) error {
			return createDatabase(ctx, conn, name)
		})
	}
	if err != nil {
		return nil, fmt.Errorf("create postgres sandbox %q: %w", name, err)
	}
	metadata := resourceMetadata{Version: 1, Kind: "sandbox", Identity: identity, LeaseKey: leaseKey}
	if withTemplate {
		metadata.Template = m.templateName
	}
	if err := setDatabaseMetadata(ctx, adminDB, name, metadata); err != nil {
		_ = m.dropSandbox(context.Background(), adminDB, name)
		return nil, err
	}

	projected, err := m.admin.WithDatabase(name)
	if err != nil {
		_ = m.dropSandbox(context.Background(), adminDB, name)
		return nil, fmt.Errorf("project postgres sandbox %q: %w", name, err)
	}
	db, err := projected.Open()
	if err != nil {
		_ = m.dropSandbox(context.Background(), adminDB, name)
		return nil, fmt.Errorf("open postgres sandbox %q: %w", name, err)
	}
	if err := waitForDatabase(ctx, db, 30*time.Second); err != nil {
		_ = db.Close()
		_ = m.dropSandbox(context.Background(), adminDB, name)
		return nil, fmt.Errorf("wait for postgres sandbox %q: %w", name, err)
	}
	cleanupLease = false
	return &Sandbox{Name: name, Connection: projected, DB: db, manager: m, leaseConn: leaseConn, leaseKey: leaseKey}, nil
}

func (s *Sandbox) Release(ctx context.Context) error {
	s.once.Do(func() {
		if s.DB != nil {
			s.releaseErr = s.DB.Close()
		}
		adminDB, err := s.manager.admin.Open()
		if err == nil {
			err = s.manager.dropSandbox(ctx, adminDB, s.Name)
			_ = adminDB.Close()
		}
		if s.releaseErr == nil && err != nil {
			s.releaseErr = err
		}
		if s.leaseConn != nil {
			_, unlockErr := s.leaseConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, s.leaseKey)
			closeErr := s.leaseConn.Close()
			if s.releaseErr == nil {
				if unlockErr != nil {
					s.releaseErr = unlockErr
				} else if closeErr != nil {
					s.releaseErr = closeErr
				}
			}
		}
	})
	return s.releaseErr
}

func (m *Manager) Reconcile(ctx context.Context) error {
	db, err := m.admin.Open()
	if err != nil {
		return fmt.Errorf("open postgres admin for reconciliation: %w", err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
		SELECT d.datname, COALESCE(shobj_description(d.oid, 'pg_database'), ''), r.rolname
		FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba
		WHERE d.datname LIKE 'mas_test_v1_%'
		ORDER BY d.datname`)
	if err != nil {
		return fmt.Errorf("list postgres test sandboxes: %w", err)
	}
	type candidate struct{ name, comment, owner string }
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.name, &c.comment, &c.owner); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan postgres test sandbox: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, c := range candidates {
		if c.owner != m.role {
			return fmt.Errorf("unprovable postgres test sandbox %q left untouched: owner %q does not match authenticated role %q", c.name, c.owner, m.role)
		}
		identity, signed := m.verifyResourceName(c.name, sandboxNamePrefix, "sandbox")
		if !signed {
			return fmt.Errorf("unprovable postgres test sandbox %q left untouched: invalid ownership signature", c.name)
		}
		metadata, err := parseResourceMetadata(c.comment)
		if err != nil {
			if strings.TrimSpace(c.comment) != "" {
				return fmt.Errorf("unprovable postgres test sandbox %q left untouched: %v", c.name, err)
			}
			metadata = resourceMetadata{Version: 1, Kind: "sandbox", Identity: identity, LeaseKey: advisoryKey("sandbox:" + identity)}
		}
		if metadata.Kind != "sandbox" || metadata.Identity != identity || metadata.LeaseKey != advisoryKey("sandbox:"+identity) {
			return fmt.Errorf("unprovable postgres test sandbox %q left untouched: metadata mismatch", c.name)
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		var acquired bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, metadata.LeaseKey).Scan(&acquired); err != nil {
			_ = conn.Close()
			return err
		}
		if !acquired {
			_ = conn.Close()
			continue
		}
		if err := m.dropSandbox(ctx, db, c.name); err != nil {
			_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, metadata.LeaseKey)
			_ = conn.Close()
			return fmt.Errorf("reconcile stale postgres sandbox %q: %w", c.name, err)
		}
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, metadata.LeaseKey)
		_ = conn.Close()
	}
	return nil
}

func (m *Manager) ensureTemplate(ctx context.Context, adminDB *sql.DB) error {
	lockConn, err := adminDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer lockConn.Close()
	lockKey := advisoryKey("template:" + m.templateName)
	if err := acquireAdvisoryLock(ctx, lockConn, lockKey, "template "+m.templateName); err != nil {
		return err
	}
	defer func() { _, _ = lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey) }()

	var comment, owner string
	err = adminDB.QueryRowContext(ctx, `
		SELECT COALESCE(shobj_description(d.oid, 'pg_database'), ''), r.rolname
		FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba
		WHERE d.datname=$1`, m.templateName).Scan(&comment, &owner)
	if err == nil {
		if owner != m.role {
			return fmt.Errorf("template database %q owner %q does not match authenticated role %q; left untouched", m.templateName, owner, m.role)
		}
		metadata, parseErr := parseResourceMetadata(comment)
		if parseErr != nil {
			if strings.TrimSpace(comment) != "" {
				return fmt.Errorf("template database %q has invalid Swarm metadata; left untouched", m.templateName)
			}
			if _, signed := m.verifyResourceName(m.templateName, templateNamePrefix, "template"); !signed {
				return fmt.Errorf("template database %q lacks valid ownership proof; left untouched", m.templateName)
			}
			if err := m.dropSandbox(ctx, adminDB, m.templateName); err != nil {
				return fmt.Errorf("recover incomplete signed template %q: %w", m.templateName, err)
			}
		} else if metadata.Kind != "template" || metadata.Identity != m.templateID {
			return fmt.Errorf("template database %q metadata mismatch; left untouched", m.templateName)
		} else {
			return nil
		}
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("inspect postgres template %q: %w", m.templateName, err)
	}
	if err := m.withDDLAdmission(ctx, adminDB, "create template "+m.templateName, func(conn *sql.Conn) error {
		return createDatabase(ctx, conn, m.templateName)
	}); err != nil {
		return fmt.Errorf("create postgres template %q: %w", m.templateName, err)
	}
	projected, err := m.admin.WithDatabase(m.templateName)
	if err != nil {
		_ = m.dropSandbox(context.Background(), adminDB, m.templateName)
		return err
	}
	templateDB, err := projected.Open()
	if err != nil {
		_ = m.dropSandbox(context.Background(), adminDB, m.templateName)
		return err
	}
	if err := initializeDatabase(ctx, templateDB, m.role, m.spec, m.ddlPlans); err != nil {
		_ = templateDB.Close()
		_ = m.dropSandbox(context.Background(), adminDB, m.templateName)
		return err
	}
	if err := templateDB.Close(); err != nil {
		_ = m.dropSandbox(context.Background(), adminDB, m.templateName)
		return err
	}
	metadata := resourceMetadata{Version: 1, Kind: "template", Identity: m.templateID}
	if err := setDatabaseMetadata(ctx, adminDB, m.templateName, metadata); err != nil {
		_ = m.dropSandbox(context.Background(), adminDB, m.templateName)
		return err
	}
	return nil
}

func initializeDatabase(ctx context.Context, db *sql.DB, role string, spec runtimecontracts.PlatformSpecDocument, plans []platformschema.TableDDL) error {
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS public`,
		`GRANT ALL ON SCHEMA public TO ` + quoteIdent(role),
		`GRANT ALL ON SCHEMA public TO public`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize postgres template statement %q: %w", stmt, err)
		}
	}
	if err := platformschema.EnsurePostgresTables(ctx, db, plans, nil); err != nil {
		return fmt.Errorf("bootstrap platform tables: %w", err)
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO schema_version (id, platform_version, applied_at)
		VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET platform_version=EXCLUDED.platform_version, applied_at=EXCLUDED.applied_at`, spec.Platform.Version)
	return err
}

func inspectSession(ctx context.Context, db *sql.DB) (string, string, error) {
	var role, serverID string
	var gss bool
	err := db.QueryRowContext(ctx, `
		SELECT current_user,
		       COALESCE((SELECT gss_authenticated FROM pg_catalog.pg_stat_gssapi WHERE pid=pg_backend_pid()), false),
		       (SELECT system_identifier::text FROM pg_control_system())`).Scan(&role, &gss, &serverID)
	if err != nil {
		return "", "", fmt.Errorf("verify postgres test authentication and server identity: %w", err)
	}
	if strings.TrimSpace(role) == "" || strings.TrimSpace(serverID) == "" || gss {
		return "", "", fmt.Errorf("postgres test connection must expose a stable server identity and use a non-GSS authenticated role")
	}
	return role, serverID, nil
}

func acquireAdvisoryLock(ctx context.Context, conn *sql.Conn, key int64, resource string) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ok bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok); err != nil {
			return fmt.Errorf("acquire postgres admission for %s: %w", resource, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("acquire postgres admission for %s: %w", resource, ctx.Err())
		case <-deadline.C:
			blockers := advisoryBlockers(context.Background(), conn, key)
			return fmt.Errorf("postgres admission blocked for %s (advisory key %d): %s", resource, key, blockers)
		case <-ticker.C:
		}
	}
}

func advisoryBlockers(ctx context.Context, conn *sql.Conn, key int64) string {
	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := conn.QueryContext(queryCtx, `
		SELECT a.pid, COALESCE(a.application_name,''), COALESCE(a.state,''), COALESCE(a.datname,''), left(COALESCE(a.query,''), 160)
		FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
		WHERE l.locktype='advisory' AND l.granted
		  AND ((l.classid::bigint << 32) | l.objid::bigint)=$1
		ORDER BY a.pid`, key)
	if err != nil {
		return "unable to inspect pg_stat_activity/pg_locks: " + err.Error()
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var pid int
		var app, state, database, query string
		if err := rows.Scan(&pid, &app, &state, &database, &query); err != nil {
			return "unable to scan pg_stat_activity/pg_locks: " + err.Error()
		}
		values = append(values, fmt.Sprintf("pid=%d app=%q state=%q database=%q query=%q", pid, app, state, database, query))
	}
	if len(values) == 0 {
		return "no granted advisory-lock holder was visible"
	}
	return strings.Join(values, "; ")
}

func (m *Manager) withDDLAdmission(ctx context.Context, db *sql.DB, resource string, fn func(*sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open DDL admission connection for %s: %w", resource, err)
	}
	defer conn.Close()
	key := advisoryKey("global-ddl-admission")
	if err := acquireAdvisoryLock(ctx, conn, key, resource); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, key) }()
	if _, err := conn.ExecContext(ctx, `SET lock_timeout='15s'`); err != nil {
		return fmt.Errorf("set postgres DDL lock timeout for %s: %w", resource, err)
	}
	if _, err := conn.ExecContext(ctx, `SET statement_timeout='30s'`); err != nil {
		return fmt.Errorf("set postgres DDL statement timeout for %s: %w", resource, err)
	}
	return fn(conn)
}

func (m *Manager) dropSandbox(ctx context.Context, db *sql.DB, name string) error {
	return m.withDDLAdmission(ctx, db, "drop database "+name, func(conn *sql.Conn) error {
		return dropDatabase(ctx, conn, name)
	})
}

type databaseExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func createDatabase(ctx context.Context, db databaseExecer, name string) error {
	_, err := db.ExecContext(ctx, `CREATE DATABASE `+quoteIdent(name))
	return err
}

func createDatabaseFromTemplate(ctx context.Context, db databaseExecer, name, template string) error {
	_, err := db.ExecContext(ctx, `CREATE DATABASE `+quoteIdent(name)+` WITH TEMPLATE `+quoteIdent(template))
	return err
}

func dropDatabase(ctx context.Context, db databaseExecer, name string) error {
	if _, err := db.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()`, name); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(name))
	return err
}

func setDatabaseMetadata(ctx context.Context, db *sql.DB, name string, metadata resourceMetadata) error {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `COMMENT ON DATABASE `+quoteIdent(name)+` IS `+quoteLiteral(resourceMetadataPrefix+string(raw)))
	return err
}

func parseResourceMetadata(comment string) (resourceMetadata, error) {
	if !strings.HasPrefix(comment, resourceMetadataPrefix) {
		return resourceMetadata{}, fmt.Errorf("missing %q metadata prefix", resourceMetadataPrefix)
	}
	var metadata resourceMetadata
	if err := json.Unmarshal([]byte(strings.TrimPrefix(comment, resourceMetadataPrefix)), &metadata); err != nil {
		return resourceMetadata{}, err
	}
	if metadata.Version != 1 || metadata.Identity == "" {
		return resourceMetadata{}, fmt.Errorf("invalid resource metadata")
	}
	return metadata, nil
}

func waitForDatabase(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func loadPlatformSpec() (runtimecontracts.PlatformSpecDocument, []byte, error) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	path := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtimecontracts.PlatformSpecDocument{}, nil, fmt.Errorf("read platform spec: %w", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return runtimecontracts.PlatformSpecDocument{}, nil, fmt.Errorf("unmarshal platform spec: %w", err)
	}
	return spec, raw, nil
}

func advisoryKey(value string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("swarm-test-postgres:v1:" + value))
	return int64(h.Sum64())
}

func (m *Manager) signedResourceName(prefix, kind, identity string) string {
	mac := hmac.New(sha256.New, m.ownershipKey)
	_, _ = mac.Write([]byte(kind + "\x00" + identity))
	signature := hex.EncodeToString(mac.Sum(nil))[:12]
	return prefix + identity + "_" + signature
}

func (m *Manager) verifyResourceName(name, prefix, kind string) (string, bool) {
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(name, prefix)
	index := strings.LastIndex(rest, "_")
	if index <= 0 {
		return "", false
	}
	identity := rest[:index]
	return identity, hmac.Equal([]byte(name), []byte(m.signedResourceName(prefix, kind, identity)))
}

func quoteIdent(value string) string   { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
func quoteLiteral(value string) string { return `'` + strings.ReplaceAll(value, `'`, `''`) + `'` }
