package testpostgres

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

const (
	resourceMetadataPrefix = "swarm-test-postgres:v1:"
	controlNamePrefix      = "mas_control_v1_"
	templateNamePrefix     = "mas_template_v1_"
	sandboxNamePrefix      = "mas_test_v1_"
	poolNamePrefix         = "mas_pool_v1_"
	leaseRolePrefix        = "mas_lease_v1_"
	dmlRolePrefix          = "mas_dml_v1_"
	intentTableName        = "swarm_test_resource_intents_v1"
)

type Manager struct {
	admin        Connection
	role         string
	ownershipKey []byte
	controlName  string
	control      Connection
	templateID   string
	templateName string
	spec         runtimecontracts.PlatformSpecDocument
	ddlPlans     []platformschema.TableDDL
	dmlRole      string
	rowPool      rowStatePool

	afterCandidateSnapshot func()
	beforeDMLRoleCommit    func(role string) error
	beforeLeaseRoleCommit  func(role string) error
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
	Role     string `json:"role,omitempty"`
	Lease    string `json:"lease,omitempty"`
}

type resourceIntent struct {
	Name     string
	Kind     string
	Identity string
	LeaseKey int64
	Template string
}

type databaseCandidate struct {
	name    string
	comment string
	owner   string
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
	spec, err := loadPlatformSpec()
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
	digest, err := templateDigest(plans, spec.Platform.Version, role, serverID, serverVersion)
	if err != nil {
		return nil, err
	}
	keyDigest := sha256.Sum256([]byte("swarm-test-postgres:v1:ownership\x00" + serverID + "\x00" + role))
	m := &Manager{
		admin: admin, role: role, ownershipKey: keyDigest[:], templateID: digest,
		spec: spec, ddlPlans: plans,
	}
	m.dmlRole = m.signedResourceName(dmlRolePrefix, "dml-role", controlIDForKey(keyDigest[:]))
	if err := m.preflightCapabilities(ctx, db); err != nil {
		return nil, err
	}
	if err := m.ensureDMLRole(ctx, db); err != nil {
		return nil, err
	}
	controlID := controlIDForKey(keyDigest[:])
	m.controlName = m.signedResourceName(controlNamePrefix, "control", controlID)
	m.control, err = admin.WithDatabase(m.controlName)
	if err != nil {
		return nil, fmt.Errorf("project postgres test control database: %w", err)
	}
	m.templateName = m.signedResourceName(templateNamePrefix, "template", digest)
	if err := m.ensureControlDatabase(ctx, db); err != nil {
		return nil, err
	}
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
	intent := resourceIntent{Name: name, Kind: "sandbox", Identity: identity, LeaseKey: leaseKey}
	if withTemplate {
		intent.Template = m.templateName
	}
	if err := m.putIntent(ctx, intent); err != nil {
		return nil, err
	}

	if withTemplate {
		err = m.withDDLAdmission(ctx, adminDB, "clone sandbox "+name, func(conn *sql.Conn) error {
			if err := createDatabaseFromTemplate(ctx, conn, name, m.templateName); err != nil {
				return err
			}
			return hardenManagedDatabase(ctx, conn, name)
		})
	} else {
		err = m.withDDLAdmission(ctx, adminDB, "create empty sandbox "+name, func(conn *sql.Conn) error {
			if err := createDatabase(ctx, conn, name); err != nil {
				return err
			}
			return hardenManagedDatabase(ctx, conn, name)
		})
	}
	if err != nil {
		cleanupErr := m.retireIntentIfDatabaseAbsent(context.Background(), adminDB, name)
		return nil, errors.Join(fmt.Errorf("create postgres sandbox %q: %w", name, err), cleanupErr)
	}
	metadata := resourceMetadata{Version: 1, Kind: "sandbox", Identity: identity, LeaseKey: leaseKey}
	if withTemplate {
		metadata.Template = m.templateName
	}
	if err := setDatabaseMetadata(ctx, adminDB, name, metadata); err != nil {
		cleanupErr := m.dropIntendedDatabase(context.Background(), adminDB, name)
		return nil, errors.Join(err, cleanupErr)
	}
	if err := m.deleteIntent(ctx, name); err != nil {
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

func (m *Manager) ensureControlDatabase(ctx context.Context, adminDB *sql.DB) error {
	exists, err := m.validateControlDatabase(ctx, adminDB)
	if err != nil {
		return err
	}
	if !exists {
		err = m.withExclusiveDDLAdmission(ctx, adminDB, "initialize test control database", func(conn *sql.Conn) error {
			exists, err := m.validateControlDatabase(ctx, conn)
			if err != nil || exists {
				return err
			}
			if err := createDatabase(ctx, conn, m.controlName); err != nil {
				return err
			}
			return hardenManagedDatabase(ctx, conn, m.controlName)
		})
	}
	if err != nil {
		return fmt.Errorf("initialize postgres test control database: %w", err)
	}
	if err := hardenManagedDatabase(ctx, adminDB, m.controlName); err != nil {
		return err
	}
	controlDB, err := m.control.Open()
	if err != nil {
		return fmt.Errorf("open postgres test control database: %w", err)
	}
	defer controlDB.Close()
	if err := waitForDatabase(ctx, controlDB, 30*time.Second); err != nil {
		return fmt.Errorf("wait for postgres test control database: %w", err)
	}
	if err := m.ensureIntentAuthority(ctx, controlDB); err != nil {
		return fmt.Errorf("create postgres test resource-intent authority: %w", err)
	}
	return nil
}

func (m *Manager) ensureIntentAuthority(ctx context.Context, controlDB *sql.DB) error {
	return m.withExclusiveDDLAdmission(ctx, controlDB, "initialize resource-intent authority", func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+quoteIdent(intentTableName)+` (
			resource_name text PRIMARY KEY,
			kind text NOT NULL,
			identity text NOT NULL,
			lease_key bigint NOT NULL DEFAULT 0,
			template_name text NOT NULL DEFAULT '',
			created_at timestamptz NOT NULL DEFAULT now()
		)`)
		return err
	})
}

type databaseRowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (m *Manager) validateControlDatabase(ctx context.Context, db databaseRowQueryer) (bool, error) {
	var owner string
	err := db.QueryRowContext(ctx, `
		SELECT r.rolname FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba
		WHERE d.datname=$1`, m.controlName).Scan(&owner)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if owner != m.role {
		return false, fmt.Errorf("control database %q owner %q does not match authenticated role %q; left untouched", m.controlName, owner, m.role)
	}
	if _, signed := m.verifyResourceName(m.controlName, controlNamePrefix, "control"); !signed {
		return false, fmt.Errorf("control database %q lacks valid ownership proof; left untouched", m.controlName)
	}
	return true, nil
}

func (m *Manager) putIntent(ctx context.Context, intent resourceIntent) error {
	if !m.intentMatchesName(intent) {
		return fmt.Errorf("refuse invalid postgres test resource intent for %q", intent.Name)
	}
	db, err := m.control.Open()
	if err != nil {
		return fmt.Errorf("open postgres test control authority: %w", err)
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `INSERT INTO `+quoteIdent(intentTableName)+`
		(resource_name, kind, identity, lease_key, template_name) VALUES ($1,$2,$3,$4,$5)`,
		intent.Name, intent.Kind, intent.Identity, intent.LeaseKey, intent.Template)
	if err != nil {
		return fmt.Errorf("record postgres test resource intent %q: %w", intent.Name, err)
	}
	return nil
}

func (m *Manager) intent(ctx context.Context, name string) (resourceIntent, bool, error) {
	db, err := m.control.Open()
	if err != nil {
		return resourceIntent{}, false, err
	}
	defer db.Close()
	var intent resourceIntent
	err = db.QueryRowContext(ctx, `SELECT resource_name, kind, identity, lease_key, template_name FROM `+quoteIdent(intentTableName)+` WHERE resource_name=$1`, name).
		Scan(&intent.Name, &intent.Kind, &intent.Identity, &intent.LeaseKey, &intent.Template)
	if err == sql.ErrNoRows {
		return resourceIntent{}, false, nil
	}
	if err != nil {
		return resourceIntent{}, false, err
	}
	return intent, true, nil
}

func (m *Manager) intents(ctx context.Context) ([]resourceIntent, error) {
	db, err := m.control.Open()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT resource_name, kind, identity, lease_key, template_name FROM `+quoteIdent(intentTableName)+` ORDER BY resource_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var intents []resourceIntent
	for rows.Next() {
		var intent resourceIntent
		if err := rows.Scan(&intent.Name, &intent.Kind, &intent.Identity, &intent.LeaseKey, &intent.Template); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

func (m *Manager) deleteIntent(ctx context.Context, name string) error {
	db, err := m.control.Open()
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `DELETE FROM `+quoteIdent(intentTableName)+` WHERE resource_name=$1`, name)
	return err
}

func (m *Manager) retireIntentIfDatabaseAbsent(ctx context.Context, db databaseRowQueryer, name string) error {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists); err != nil {
		return fmt.Errorf("verify exact absence before retiring postgres test resource intent %q: %w", name, err)
	}
	if exists {
		return fmt.Errorf("postgres test resource intent %q retained because the database exists", name)
	}
	if err := m.deleteIntent(ctx, name); err != nil {
		return fmt.Errorf("retire absent postgres test resource intent %q: %w", name, err)
	}
	return nil
}

func (m *Manager) dropIntendedDatabase(ctx context.Context, db *sql.DB, name string) error {
	if err := m.dropSandbox(ctx, db, name); err != nil {
		return fmt.Errorf("drop intended postgres test database %q: %w", name, err)
	}
	return m.retireIntentIfDatabaseAbsent(ctx, db, name)
}

func (m *Manager) intentMatchesName(intent resourceIntent) bool {
	if intent.Name == "" || intent.Identity == "" {
		return false
	}
	switch intent.Kind {
	case "sandbox":
		identity, ok := m.verifyResourceName(intent.Name, sandboxNamePrefix, "sandbox")
		if !ok || identity != intent.Identity || intent.LeaseKey != advisoryKey("sandbox:"+identity) {
			return false
		}
		if intent.Template == "" {
			return true
		}
		_, templateOK := m.verifyResourceName(intent.Template, templateNamePrefix, "template")
		return templateOK
	case "template":
		identity, ok := m.verifyResourceName(intent.Name, templateNamePrefix, "template")
		return ok && identity == intent.Identity && intent.LeaseKey == 0 && intent.Template == ""
	case "pool":
		identity, ok := m.verifyResourceName(intent.Name, poolNamePrefix, "pool")
		return ok && identity == intent.Identity && intent.LeaseKey == advisoryKey("pool:"+identity) && intent.Template == m.templateName
	default:
		return false
	}
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
		WHERE d.datname LIKE 'mas_test_v1_%' OR d.datname LIKE 'mas_template_v1_%' OR d.datname LIKE 'mas_pool_v1_%'
		ORDER BY d.datname`)
	if err != nil {
		return fmt.Errorf("list postgres test resources: %w", err)
	}
	var candidates []databaseCandidate
	seen := make(map[string]bool)
	for rows.Next() {
		var c databaseCandidate
		if err := rows.Scan(&c.name, &c.comment, &c.owner); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan postgres test resource: %w", err)
		}
		candidates = append(candidates, c)
		seen[c.name] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if m.afterCandidateSnapshot != nil {
		m.afterCandidateSnapshot()
	}
	for _, c := range candidates {
		if err := m.reconcileDatabaseCandidate(ctx, db, c); err != nil {
			return err
		}
	}
	intents, err := m.intents(ctx)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		if seen[intent.Name] {
			continue
		}
		if !m.intentMatchesName(intent) {
			return fmt.Errorf("invalid postgres test resource intent %q left untouched", intent.Name)
		}
		lockKey := intent.LeaseKey
		if intent.Kind == "template" {
			lockKey = advisoryKey("template:" + intent.Name)
		}
		lockConn, acquired, err := tryAdvisoryLock(ctx, db, lockKey)
		if err != nil {
			return err
		}
		if !acquired {
			continue
		}
		candidate, exists, refreshErr := databaseCandidateByName(ctx, db, intent.Name)
		if refreshErr != nil {
			err = refreshErr
		} else if exists {
			err = m.reconcileDatabaseCandidateLocked(ctx, db, candidate)
		} else {
			err = m.deleteIntent(ctx, intent.Name)
		}
		releaseAdvisoryLock(lockConn, lockKey)
		if err != nil {
			return err
		}
	}
	return m.reconcileLeaseRoles(ctx, db)
}

func (m *Manager) reconcileDatabaseCandidate(ctx context.Context, db *sql.DB, candidate databaseCandidate) error {
	kind, prefix := "sandbox", sandboxNamePrefix
	if strings.HasPrefix(candidate.name, templateNamePrefix) {
		kind, prefix = "template", templateNamePrefix
	} else if strings.HasPrefix(candidate.name, poolNamePrefix) {
		kind, prefix = "pool", poolNamePrefix
	}
	identity, signed := m.verifyResourceName(candidate.name, prefix, kind)
	if !signed {
		return fmt.Errorf("unprovable postgres test %s %q left untouched: invalid ownership signature", kind, candidate.name)
	}
	lockKey := advisoryKey("template:" + candidate.name)
	if kind == "sandbox" {
		lockKey = advisoryKey("sandbox:" + identity)
	} else if kind == "pool" {
		lockKey = advisoryKey("pool:" + identity)
	}
	lockConn, acquired, err := tryAdvisoryLock(ctx, db, lockKey)
	if err != nil || !acquired {
		return err
	}
	defer releaseAdvisoryLock(lockConn, lockKey)

	refreshed, exists, err := databaseCandidateByName(ctx, db, candidate.name)
	if err != nil || !exists {
		return err
	}
	return m.reconcileDatabaseCandidateLocked(ctx, db, refreshed)
}

func databaseCandidateByName(ctx context.Context, db databaseRowQueryer, name string) (databaseCandidate, bool, error) {
	var candidate databaseCandidate
	err := db.QueryRowContext(ctx, `
		SELECT d.datname, COALESCE(shobj_description(d.oid, 'pg_database'), ''), r.rolname
		FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba
		WHERE d.datname=$1`, name).Scan(&candidate.name, &candidate.comment, &candidate.owner)
	if err == sql.ErrNoRows {
		return databaseCandidate{}, false, nil
	}
	if err != nil {
		return databaseCandidate{}, false, err
	}
	return candidate, true, nil
}

func (m *Manager) reconcileDatabaseCandidateLocked(ctx context.Context, db *sql.DB, candidate databaseCandidate) error {
	if candidate.owner != m.role {
		return fmt.Errorf("unprovable postgres test resource %q left untouched: owner %q does not match authenticated role %q", candidate.name, candidate.owner, m.role)
	}
	kind, prefix := "sandbox", sandboxNamePrefix
	if strings.HasPrefix(candidate.name, templateNamePrefix) {
		kind, prefix = "template", templateNamePrefix
	} else if strings.HasPrefix(candidate.name, poolNamePrefix) {
		kind, prefix = "pool", poolNamePrefix
	}
	identity, signed := m.verifyResourceName(candidate.name, prefix, kind)
	if !signed {
		return fmt.Errorf("unprovable postgres test %s %q left untouched: invalid ownership signature", kind, candidate.name)
	}
	metadata, metadataErr := parseResourceMetadata(candidate.comment)
	intent, hasIntent, err := m.intent(ctx, candidate.name)
	if err != nil {
		return fmt.Errorf("read postgres test resource intent %q: %w", candidate.name, err)
	}
	if metadataErr != nil {
		if strings.TrimSpace(candidate.comment) != "" || !hasIntent || !m.intentMatchesName(intent) || intent.Kind != kind || intent.Identity != identity {
			return fmt.Errorf("unprovable postgres test %s %q left untouched: valid stamped metadata or matching durable pre-create intent is required", kind, candidate.name)
		}
	} else {
		if metadata.Kind != kind || metadata.Identity != identity || !m.validResourceMetadata(metadata) {
			return fmt.Errorf("unprovable postgres test %s %q left untouched: metadata mismatch", kind, candidate.name)
		}
		if hasIntent {
			if !m.intentMatchesName(intent) || intent.Kind != kind || intent.Identity != identity {
				return fmt.Errorf("unprovable postgres test %s %q left untouched: durable intent mismatch", kind, candidate.name)
			}
			if err := m.deleteIntent(ctx, candidate.name); err != nil {
				return err
			}
		}
		if kind == "template" {
			// Content-addressed templates are immutable retained caches. A newer
			// schema digest creates a sibling without mutating or deleting this one.
			return nil
		}
	}
	if metadataErr == nil && metadata.Role != "" {
		if err := m.retireLeaseRole(ctx, db, candidate.name, metadata.Role, metadata.Lease, metadata.LeaseKey); err != nil {
			return fmt.Errorf("reconcile stale postgres lease role %q: %w", metadata.Role, err)
		}
	}
	if err := m.dropSandbox(ctx, db, candidate.name); err != nil {
		return fmt.Errorf("reconcile stale postgres test %s %q: %w", kind, candidate.name, err)
	}
	return m.deleteIntent(ctx, candidate.name)
}

func (m *Manager) validResourceMetadata(metadata resourceMetadata) bool {
	switch metadata.Kind {
	case "sandbox":
		if metadata.LeaseKey != advisoryKey("sandbox:"+metadata.Identity) {
			return false
		}
		if metadata.Template == "" {
			return true
		}
		_, ok := m.verifyResourceName(metadata.Template, templateNamePrefix, "template")
		return ok
	case "template":
		return metadata.LeaseKey == 0 && metadata.Template == ""
	case "pool":
		if metadata.LeaseKey != advisoryKey("pool:"+metadata.Identity) || metadata.Template != m.templateName {
			return false
		}
		if (metadata.Role == "") != (metadata.Lease == "") {
			return false
		}
		_, ok := m.verifyResourceName(m.signedResourceName(poolNamePrefix, "pool", metadata.Identity), poolNamePrefix, "pool")
		return ok
	default:
		return false
	}
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
			intent, found, intentErr := m.intent(ctx, m.templateName)
			if intentErr != nil {
				return intentErr
			}
			if strings.TrimSpace(comment) != "" || !found || !m.intentMatchesName(intent) || intent.Kind != "template" || intent.Identity != m.templateID {
				return fmt.Errorf("template database %q lacks valid stamped metadata or matching durable pre-create intent; left untouched", m.templateName)
			}
			if err := m.dropSandbox(ctx, adminDB, m.templateName); err != nil {
				return fmt.Errorf("recover incomplete intended template %q: %w", m.templateName, err)
			}
			if err := m.deleteIntent(ctx, m.templateName); err != nil {
				return err
			}
		} else if metadata.Kind != "template" || metadata.Identity != m.templateID {
			return fmt.Errorf("template database %q metadata mismatch; left untouched", m.templateName)
		} else {
			if err := m.deleteIntent(ctx, m.templateName); err != nil {
				return err
			}
			return nil
		}
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("inspect postgres template %q: %w", m.templateName, err)
	}
	if err := m.putIntent(ctx, resourceIntent{Name: m.templateName, Kind: "template", Identity: m.templateID}); err != nil {
		return err
	}
	if err := m.withExclusiveDDLAdmission(ctx, adminDB, "create template "+m.templateName, func(conn *sql.Conn) error {
		if err := createDatabase(ctx, conn, m.templateName); err != nil {
			return err
		}
		return hardenManagedDatabase(ctx, conn, m.templateName)
	}); err != nil {
		cleanupErr := m.retireIntentIfDatabaseAbsent(context.Background(), adminDB, m.templateName)
		return errors.Join(fmt.Errorf("create postgres template %q: %w", m.templateName, err), cleanupErr)
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
	if err := initializeDatabase(ctx, templateDB, m.role, m.dmlRole, m.spec, m.ddlPlans); err != nil {
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
		cleanupErr := m.dropIntendedDatabase(context.Background(), adminDB, m.templateName)
		return errors.Join(err, cleanupErr)
	}
	return m.deleteIntent(ctx, m.templateName)
}

func initializeDatabase(ctx context.Context, db *sql.DB, role, dmlRole string, spec runtimecontracts.PlatformSpecDocument, plans []platformschema.TableDDL) error {
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS public`,
		`GRANT ALL ON SCHEMA public TO ` + quoteIdent(role),
		`GRANT ALL ON SCHEMA public TO public`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize postgres template statement %q: %w", stmt, err)
		}
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin platform bootstrap: %w", err)
	}
	if err := platformschema.BootstrapFreshPostgres(ctx, tx, plans, "test-harness", spec.Platform.Version, time.Now().UTC()); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("bootstrap platform tables: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit platform bootstrap: %w", err)
	}
	for _, stmt := range []string{
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		`GRANT USAGE ON SCHEMA public TO ` + quoteIdent(dmlRole),
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ` + quoteIdent(dmlRole),
		`GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO ` + quoteIdent(dmlRole),
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize postgres template lease privilege %q: %w", stmt, err)
		}
	}
	return nil
}

func controlIDForKey(key []byte) string {
	return hex.EncodeToString(key)[:24]
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
	return acquireAdvisoryLockQuery(ctx, conn, key, resource, `SELECT pg_try_advisory_lock($1)`)
}

func acquireDDLAdmission(ctx context.Context, conn *sql.Conn, key int64, resource string, shared bool) error {
	query := `SELECT pg_try_advisory_lock($1)`
	if shared {
		query = `SELECT pg_try_advisory_lock_shared($1)`
	}
	return acquireAdvisoryLockQuery(ctx, conn, key, resource, query)
}

func acquireAdvisoryLockQuery(ctx context.Context, conn *sql.Conn, key int64, resource, query string) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ok bool
		if err := conn.QueryRowContext(ctx, query, key).Scan(&ok); err != nil {
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

func tryAdvisoryLock(ctx context.Context, db *sql.DB, key int64) (*sql.Conn, bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !acquired {
		_ = conn.Close()
		return nil, false, nil
	}
	return conn, true, nil
}

func releaseAdvisoryLock(conn *sql.Conn, key int64) {
	if conn == nil {
		return
	}
	_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, key)
	_ = conn.Close()
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
	return m.withDDLAdmissionMode(ctx, db, resource, true, fn)
}

func (m *Manager) withExclusiveDDLAdmission(ctx context.Context, db *sql.DB, resource string, fn func(*sql.Conn) error) error {
	return m.withDDLAdmissionMode(ctx, db, resource, false, fn)
}

func (m *Manager) withDDLAdmissionMode(ctx context.Context, db *sql.DB, resource string, shared bool, fn func(*sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open DDL admission connection for %s: %w", resource, err)
	}
	defer conn.Close()
	gateKey := advisoryKey("global-ddl-admission-gate")
	if err := acquireDDLAdmission(ctx, conn, gateKey, resource+" admission gate", shared); err != nil {
		return err
	}
	gateHeld := true
	defer func() {
		if gateHeld {
			releaseDDLAdmission(conn, gateKey, shared)
		}
	}()
	key := advisoryKey("global-ddl-admission")
	if err := acquireDDLAdmission(ctx, conn, key, resource, shared); err != nil {
		return err
	}
	defer releaseDDLAdmission(conn, key, shared)
	// Once the operation owns the main lock, the gate can admit the next waiter.
	// An exclusive waiter holds the gate while draining existing shared work, so
	// a stream of new sandbox DDL cannot starve template/control mutation.
	releaseDDLAdmission(conn, gateKey, shared)
	gateHeld = false
	if _, err := conn.ExecContext(ctx, `SET lock_timeout='15s'`); err != nil {
		return fmt.Errorf("set postgres DDL lock timeout for %s: %w", resource, err)
	}
	if _, err := conn.ExecContext(ctx, `SET statement_timeout='30s'`); err != nil {
		return fmt.Errorf("set postgres DDL statement timeout for %s: %w", resource, err)
	}
	return fn(conn)
}

func releaseDDLAdmission(conn *sql.Conn, key int64, shared bool) {
	query := `SELECT pg_advisory_unlock($1)`
	if shared {
		query = `SELECT pg_advisory_unlock_shared($1)`
	}
	_, _ = conn.ExecContext(context.Background(), query, key)
}

func (m *Manager) dropSandbox(ctx context.Context, db *sql.DB, name string) error {
	admit := m.withDDLAdmission
	if strings.HasPrefix(name, templateNamePrefix) || strings.HasPrefix(name, controlNamePrefix) {
		admit = m.withExclusiveDDLAdmission
	}
	return admit(ctx, db, "drop database "+name, func(conn *sql.Conn) error {
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

func hardenManagedDatabase(ctx context.Context, db databaseExecer, name string) error {
	_, err := db.ExecContext(ctx, `REVOKE CONNECT, TEMPORARY ON DATABASE `+quoteIdent(name)+` FROM PUBLIC`)
	if err != nil {
		return fmt.Errorf("revoke public authority on managed postgres database %q: %w", name, err)
	}
	return nil
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

func loadPlatformSpec() (runtimecontracts.PlatformSpecDocument, error) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	path := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		if cause, ok := yamlsource.ParseCause(err); ok {
			return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", cause)
		}
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("read platform spec: %w", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", err)
	}
	return spec, nil
}

func templateDigest(plans []platformschema.TableDDL, platformVersion, role, serverID, serverVersion string) (string, error) {
	canonical, err := json.Marshal(plans)
	if err != nil {
		return "", fmt.Errorf("encode canonical platform schema: %w", err)
	}
	hash := sha256.New()
	for _, value := range [][]byte{
		[]byte(resourceMetadataPrefix), []byte("lease-dml-v1"), canonical, []byte(platformVersion),
		[]byte(role), []byte(serverID), []byte(serverVersion),
	} {
		_, _ = hash.Write(value)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))[:24], nil
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
