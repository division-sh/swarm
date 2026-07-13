package testpostgres

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const maxRowStateSlotsPerProcess = 4

type rowStatePool struct {
	mu        sync.Mutex
	available []*rowStateSlot
	total     int
}

type rowStateSlot struct {
	name        string
	identity    string
	leaseKey    int64
	leaseConn   *sql.Conn
	fingerprint string
}

type RowStateLease struct {
	Name       string
	Connection Connection
	DB         *sql.DB

	manager *Manager
	slot    *rowStateSlot
	role    string
	once    sync.Once
	err     error
}

type leaseRoleMetadata struct {
	Version  int    `json:"version"`
	Manager  string `json:"manager"`
	Slot     string `json:"slot"`
	Identity string `json:"identity"`
	LeaseKey int64  `json:"lease_key"`
	MAC      string `json:"mac"`
}

func (m *Manager) AcquireRowState(ctx context.Context) (*RowStateLease, error) {
	slot, err := m.takeRowStateSlot(ctx)
	if err != nil {
		return nil, err
	}
	lease, err := m.beginRowStateLease(ctx, slot)
	if err != nil {
		m.retireRowStateSlot(context.Background(), slot)
		return nil, err
	}
	return lease, nil
}

func (m *Manager) takeRowStateSlot(ctx context.Context) (*rowStateSlot, error) {
	for {
		m.rowPool.mu.Lock()
		last := len(m.rowPool.available) - 1
		if last >= 0 {
			slot := m.rowPool.available[last]
			m.rowPool.available = m.rowPool.available[:last]
			m.rowPool.mu.Unlock()
			return slot, nil
		}
		if m.rowPool.total < maxRowStateSlotsPerProcess {
			m.rowPool.total++
			m.rowPool.mu.Unlock()
			slot, err := m.createRowStateSlot(ctx)
			if err != nil {
				m.rowPool.mu.Lock()
				m.rowPool.total--
				m.rowPool.mu.Unlock()
				return nil, err
			}
			return slot, nil
		}
		m.rowPool.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for reusable postgres row-state slot: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (m *Manager) createRowStateSlot(ctx context.Context) (*rowStateSlot, error) {
	adminDB, err := m.admin.Open()
	if err != nil {
		return nil, fmt.Errorf("open postgres admin for row-state slot: %w", err)
	}
	defer adminDB.Close()
	if err := m.ensureTemplate(ctx, adminDB); err != nil {
		return nil, err
	}
	identity := strings.ReplaceAll(uuid.NewString(), "-", "")
	name := m.signedResourceName(poolNamePrefix, "pool", identity)
	leaseKey := advisoryKey("pool:" + identity)
	leaseConn, err := adminDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open row-state slot lease connection: %w", err)
	}
	if err := acquireAdvisoryLock(ctx, leaseConn, leaseKey, "row-state slot "+name); err != nil {
		_ = leaseConn.Close()
		return nil, err
	}
	cleanupLock := true
	defer func() {
		if cleanupLock {
			releaseAdvisoryLock(leaseConn, leaseKey)
		}
	}()
	intent := resourceIntent{Name: name, Kind: "pool", Identity: identity, LeaseKey: leaseKey, Template: m.templateName}
	if err := m.putIntent(ctx, intent); err != nil {
		return nil, err
	}
	if err := m.withDDLAdmission(ctx, adminDB, "clone row-state slot "+name, func(conn *sql.Conn) error {
		if err := createDatabaseFromTemplate(ctx, conn, name, m.templateName); err != nil {
			return err
		}
		return hardenManagedDatabase(ctx, conn, name)
	}); err != nil {
		cleanupErr := m.retireIntentIfDatabaseAbsent(context.Background(), adminDB, name)
		return nil, errors.Join(fmt.Errorf("create postgres row-state slot %q: %w", name, err), cleanupErr)
	}
	metadata := resourceMetadata{Version: 1, Kind: "pool", Identity: identity, LeaseKey: leaseKey, Template: m.templateName}
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
		return nil, err
	}
	db, err := projected.Open()
	if err != nil {
		return nil, err
	}
	fingerprint, err := postgresSchemaFingerprint(ctx, db)
	_ = db.Close()
	if err != nil {
		return nil, err
	}
	cleanupLock = false
	return &rowStateSlot{name: name, identity: identity, leaseKey: leaseKey, leaseConn: leaseConn, fingerprint: fingerprint}, nil
}

func (m *Manager) beginRowStateLease(ctx context.Context, slot *rowStateSlot) (*RowStateLease, error) {
	identity := strings.ReplaceAll(uuid.NewString(), "-", "")
	role := leaseRolePrefix + identity
	password, err := randomLeasePassword()
	if err != nil {
		return nil, err
	}
	adminDB, err := m.admin.Open()
	if err != nil {
		return nil, err
	}
	defer adminDB.Close()
	tx, err := adminDB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin postgres row-state lease role transaction: %w", err)
	}
	rollback := func(cause error) error {
		return errors.Join(cause, tx.Rollback())
	}
	metadata := m.newLeaseRoleMetadata(role, slot.name, identity, slot.leaseKey)
	rawMetadata, err := json.Marshal(metadata)
	if err != nil {
		return nil, rollback(err)
	}
	statements := []string{
		`CREATE ROLE ` + quoteIdent(role) + ` LOGIN PASSWORD ` + quoteLiteral(password),
		`COMMENT ON ROLE ` + quoteIdent(role) + ` IS ` + quoteLiteral(resourceMetadataPrefix+string(rawMetadata)),
		`GRANT ` + quoteIdent(m.dmlRole) + ` TO ` + quoteIdent(role),
		`GRANT CONNECT ON DATABASE ` + quoteIdent(slot.name) + ` TO ` + quoteIdent(role),
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return nil, rollback(fmt.Errorf("establish postgres row-state lease authority: %w", err))
		}
	}
	databaseMetadata := resourceMetadata{Version: 1, Kind: "pool", Identity: slot.identity, LeaseKey: slot.leaseKey, Template: m.templateName, Role: role, Lease: identity}
	rawDatabaseMetadata, err := json.Marshal(databaseMetadata)
	if err != nil {
		return nil, rollback(err)
	}
	if _, err := tx.ExecContext(ctx, `COMMENT ON DATABASE `+quoteIdent(slot.name)+` IS `+quoteLiteral(resourceMetadataPrefix+string(rawDatabaseMetadata))); err != nil {
		return nil, rollback(fmt.Errorf("bind postgres row-state lease metadata: %w", err))
	}
	if m.beforeLeaseRoleCommit != nil {
		if err := m.beforeLeaseRoleCommit(role); err != nil {
			return nil, rollback(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit postgres row-state lease authority: %w", err)
	}
	connection, err := m.admin.WithIdentity(slot.name, role, password)
	if err != nil {
		_ = m.retireLeaseRole(context.Background(), adminDB, slot.name, role, identity, slot.leaseKey)
		return nil, err
	}
	db, err := connection.Open()
	if err != nil {
		_ = m.retireLeaseRole(context.Background(), adminDB, slot.name, role, identity, slot.leaseKey)
		return nil, err
	}
	if err := waitForDatabase(ctx, db, 30*time.Second); err != nil {
		_ = db.Close()
		_ = m.retireLeaseRole(context.Background(), adminDB, slot.name, role, identity, slot.leaseKey)
		return nil, err
	}
	return &RowStateLease{Name: slot.name, Connection: connection, DB: db, manager: m, slot: slot, role: role}, nil
}

func (l *RowStateLease) Release(ctx context.Context) error {
	l.once.Do(func() {
		if l.DB != nil {
			l.err = l.DB.Close()
		}
		adminDB, err := l.manager.admin.Open()
		roleErr := err
		if err == nil {
			roleErr = l.manager.retireLeaseRole(ctx, adminDB, l.slot.name, l.role, strings.TrimPrefix(l.role, leaseRolePrefix), l.slot.leaseKey)
		}
		err = roleErr
		if roleErr == nil {
			err = l.manager.resetRowStateSlot(ctx, l.slot)
		}
		if adminDB != nil {
			_ = adminDB.Close()
		}
		if l.err == nil {
			l.err = err
		}
		if l.err != nil {
			if roleErr != nil {
				l.manager.quarantineRowStateSlot(l.slot)
			} else {
				l.manager.retireRowStateSlot(context.Background(), l.slot)
			}
			return
		}
		l.manager.rowPool.mu.Lock()
		l.manager.rowPool.available = append(l.manager.rowPool.available, l.slot)
		l.manager.rowPool.mu.Unlock()
	})
	return l.err
}

func (m *Manager) resetRowStateSlot(ctx context.Context, slot *rowStateSlot) error {
	adminDB, err := m.admin.Open()
	if err != nil {
		return err
	}
	defer adminDB.Close()
	connection, err := m.admin.WithDatabase(slot.name)
	if err != nil {
		return err
	}
	db, err := connection.Open()
	if err != nil {
		return err
	}
	defer db.Close()
	tables := make([]string, 0, len(m.ddlPlans))
	for _, plan := range m.ddlPlans {
		tables = append(tables, quoteIdent(plan.TableName))
	}
	if len(tables) > 0 {
		if _, err := db.ExecContext(ctx, `TRUNCATE TABLE `+strings.Join(tables, ", ")+` RESTART IDENTITY CASCADE`); err != nil {
			return fmt.Errorf("reset postgres row-state slot %q: %w", slot.name, err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO runtime_store_metadata (id, swarm_version, platform_version, created_at) VALUES (1, $1, $2, $3)`, "test-harness", m.spec.Platform.Version, time.Now().UTC()); err != nil {
		return fmt.Errorf("restore postgres row-state canonical seed: %w", err)
	}
	fingerprint, err := postgresSchemaFingerprint(ctx, db)
	if err != nil {
		return err
	}
	if fingerprint != slot.fingerprint {
		return fmt.Errorf("row-state slot %q schema/object shape changed; retire instead of reuse", slot.name)
	}
	if err := db.Close(); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Second)
	for {
		var otherSessions int
		if err := adminDB.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=$1`, slot.name).Scan(&otherSessions); err != nil {
			return fmt.Errorf("verify row-state slot sessions: %w", err)
		}
		if otherSessions == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("row-state slot %q retains %d active or dormant sessions", slot.name, otherSessions)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	metadata := resourceMetadata{Version: 1, Kind: "pool", Identity: slot.identity, LeaseKey: slot.leaseKey, Template: m.templateName}
	return setDatabaseMetadata(ctx, adminDB, slot.name, metadata)
}

func (m *Manager) retireRowStateSlot(ctx context.Context, slot *rowStateSlot) {
	adminDB, err := m.admin.Open()
	if err == nil {
		_ = m.dropSandbox(ctx, adminDB, slot.name)
		_ = adminDB.Close()
	}
	releaseAdvisoryLock(slot.leaseConn, slot.leaseKey)
	m.rowPool.mu.Lock()
	m.rowPool.total--
	m.rowPool.mu.Unlock()
}

func (m *Manager) quarantineRowStateSlot(slot *rowStateSlot) {
	releaseAdvisoryLock(slot.leaseConn, slot.leaseKey)
	m.rowPool.mu.Lock()
	m.rowPool.total--
	m.rowPool.mu.Unlock()
}

func (m *Manager) preflightCapabilities(ctx context.Context, db *sql.DB) error {
	var version int
	var superuser, createDB, createRole bool
	if err := db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname=current_user`).Scan(&version, &superuser, &createDB, &createRole); err != nil {
		return fmt.Errorf("inspect postgres test manager capabilities: %w", err)
	}
	if version < 160000 || (!superuser && (!createDB || !createRole)) {
		return fmt.Errorf("postgres test manager requires PostgreSQL 16+ and CREATEDB + CREATEROLE (or superuser); see internal/testutil/POSTGRES.md")
	}
	return m.withExclusiveDDLAdmission(ctx, db, "test role capability preflight", func(conn *sql.Conn) error {
		return m.probeConnectIsolation(ctx, conn)
	})
}

func (m *Manager) probeConnectIsolation(ctx context.Context, conn *sql.Conn) error {
	probe := leaseRolePrefix + "probe_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE ROLE `+quoteIdent(probe)+` NOLOGIN`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("probe postgres test role authority: %w; see internal/testutil/POSTGRES.md", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT datname FROM pg_database WHERE datallowconn AND has_database_privilege($1, datname, 'CONNECT') ORDER BY datname`, probe)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	var connectable []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			return err
		}
		connectable = append(connectable, name)
	}
	_ = rows.Close()
	if err := tx.Rollback(); err != nil {
		return err
	}
	if len(connectable) > 0 {
		return fmt.Errorf("postgres test host grants untrusted roles CONNECT to %s; use a dedicated host and revoke PUBLIC CONNECT as documented in internal/testutil/POSTGRES.md", strings.Join(connectable, ", "))
	}
	return nil
}

func (m *Manager) ensureDMLRole(ctx context.Context, db *sql.DB) error {
	lockConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer lockConn.Close()
	lockKey := advisoryKey("dml-role:" + m.dmlRole)
	if err := acquireAdvisoryLock(ctx, lockConn, lockKey, "DML role "+m.dmlRole); err != nil {
		return err
	}
	defer func() { _, _ = lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey) }()
	var comment string
	err = lockConn.QueryRowContext(ctx, `SELECT COALESCE(shobj_description(oid, 'pg_authid'), '') FROM pg_roles WHERE rolname=$1`, m.dmlRole).Scan(&comment)
	if err == nil {
		if comment != m.signedRoleComment(m.dmlRole, "dml-role") {
			return fmt.Errorf("postgres test DML role %q has invalid ownership metadata; left untouched", m.dmlRole)
		}
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	tx, err := lockConn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin postgres test DML role transaction: %w", err)
	}
	rollback := func(cause error) error {
		return errors.Join(cause, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `CREATE ROLE `+quoteIdent(m.dmlRole)+` NOLOGIN`); err != nil {
		return rollback(fmt.Errorf("create postgres test DML role: %w", err))
	}
	if _, err := tx.ExecContext(ctx, `COMMENT ON ROLE `+quoteIdent(m.dmlRole)+` IS `+quoteLiteral(m.signedRoleComment(m.dmlRole, "dml-role"))); err != nil {
		return rollback(fmt.Errorf("stamp postgres test DML role: %w", err))
	}
	if m.beforeDMLRoleCommit != nil {
		if err := m.beforeDMLRoleCommit(m.dmlRole); err != nil {
			return rollback(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit postgres test DML role: %w", err)
	}
	return nil
}

func (m *Manager) retireLeaseRole(ctx context.Context, db *sql.DB, slot, role, identity string, leaseKey int64) error {
	var comment string
	err := db.QueryRowContext(ctx, `SELECT COALESCE(shobj_description(oid, 'pg_authid'), '') FROM pg_roles WHERE rolname=$1`, role).Scan(&comment)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if !m.validLeaseRoleComment(comment, role, slot, identity, leaseKey) {
		return fmt.Errorf("role metadata is invalid; role left untouched")
	}
	statements := []string{`ALTER ROLE ` + quoteIdent(role) + ` NOLOGIN`}
	var slotExists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, slot).Scan(&slotExists); err != nil {
		return err
	}
	if slotExists {
		statements = append(statements, `REVOKE CONNECT ON DATABASE `+quoteIdent(slot)+` FROM `+quoteIdent(role))
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename=$1 AND datname=$2 AND pid<>pg_backend_pid()`, role, slot); err != nil {
		return err
	}
	sessionDeadline := time.Now().Add(time.Second)
	for {
		var sessions int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND datname=$2`, role, slot).Scan(&sessions); err != nil {
			return err
		}
		if sessions == 0 {
			break
		}
		if time.Now().After(sessionDeadline) {
			return fmt.Errorf("lease role %q retains %d sessions", role, sessions)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	for _, stmt := range []string{
		`REVOKE ` + quoteIdent(m.dmlRole) + ` FROM ` + quoteIdent(role),
		`DROP ROLE ` + quoteIdent(role),
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	var roleExists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, role).Scan(&roleExists); err != nil {
		return err
	}
	if roleExists {
		return fmt.Errorf("lease role %q still exists after drop", role)
	}
	return nil
}

func (m *Manager) reconcileLeaseRoles(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT rolname, COALESCE(shobj_description(oid, 'pg_authid'), '') FROM pg_roles WHERE rolname LIKE $1 ORDER BY rolname`, leaseRolePrefix+"%")
	if err != nil {
		return fmt.Errorf("list postgres test lease roles: %w", err)
	}
	type candidate struct{ role, comment string }
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.role, &item.comment); err != nil {
			_ = rows.Close()
			return err
		}
		candidates = append(candidates, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range candidates {
		metadata, err := m.parseLeaseRoleComment(item.comment, item.role)
		if err != nil {
			return fmt.Errorf("unprovable postgres test lease role %q left untouched: %w", item.role, err)
		}
		lockConn, acquired, err := tryAdvisoryLock(ctx, db, metadata.LeaseKey)
		if err != nil {
			return err
		}
		if !acquired {
			continue
		}
		err = m.retireLeaseRole(ctx, db, metadata.Slot, item.role, metadata.Identity, metadata.LeaseKey)
		if err == nil {
			var slotExists bool
			if queryErr := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, metadata.Slot).Scan(&slotExists); queryErr != nil {
				err = queryErr
			} else if slotExists {
				err = m.dropSandbox(ctx, db, metadata.Slot)
			}
		}
		releaseAdvisoryLock(lockConn, metadata.LeaseKey)
		if err != nil {
			return fmt.Errorf("reconcile postgres test lease role %q: %w", item.role, err)
		}
	}
	return nil
}

func (m *Manager) newLeaseRoleMetadata(role, slot, identity string, leaseKey int64) leaseRoleMetadata {
	metadata := leaseRoleMetadata{Version: 1, Manager: m.role, Slot: slot, Identity: identity, LeaseKey: leaseKey}
	metadata.MAC = m.leaseRoleMAC(role, metadata)
	return metadata
}

func (m *Manager) validLeaseRoleComment(comment, role, slot, identity string, leaseKey int64) bool {
	if !strings.HasPrefix(comment, resourceMetadataPrefix) {
		return false
	}
	var metadata leaseRoleMetadata
	if json.Unmarshal([]byte(strings.TrimPrefix(comment, resourceMetadataPrefix)), &metadata) != nil {
		return false
	}
	want := m.newLeaseRoleMetadata(role, slot, identity, leaseKey)
	return metadata == want
}

func (m *Manager) parseLeaseRoleComment(comment, role string) (leaseRoleMetadata, error) {
	if !strings.HasPrefix(comment, resourceMetadataPrefix) {
		return leaseRoleMetadata{}, fmt.Errorf("missing signed metadata")
	}
	var metadata leaseRoleMetadata
	if err := json.Unmarshal([]byte(strings.TrimPrefix(comment, resourceMetadataPrefix)), &metadata); err != nil {
		return leaseRoleMetadata{}, err
	}
	if metadata.Version != 1 || metadata.Manager != m.role || role != leaseRolePrefix+metadata.Identity {
		return leaseRoleMetadata{}, fmt.Errorf("metadata identity mismatch")
	}
	slotIdentity, ok := m.verifyResourceName(metadata.Slot, poolNamePrefix, "pool")
	if !ok || metadata.LeaseKey != advisoryKey("pool:"+slotIdentity) {
		return leaseRoleMetadata{}, fmt.Errorf("slot ownership mismatch")
	}
	if !m.validLeaseRoleComment(comment, role, metadata.Slot, metadata.Identity, metadata.LeaseKey) {
		return leaseRoleMetadata{}, fmt.Errorf("metadata signature mismatch")
	}
	return metadata, nil
}

func (m *Manager) leaseRoleMAC(role string, metadata leaseRoleMetadata) string {
	value := strings.Join([]string{role, metadata.Manager, metadata.Slot, metadata.Identity, fmt.Sprint(metadata.LeaseKey)}, "\x00")
	mac := hmac.New(sha256.New, m.ownershipKey)
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func (m *Manager) signedRoleComment(role, kind string) string {
	mac := hmac.New(sha256.New, m.ownershipKey)
	_, _ = mac.Write([]byte(kind + "\x00" + role))
	return resourceMetadataPrefix + hex.EncodeToString(mac.Sum(nil))
}

func randomLeasePassword() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate postgres lease password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func postgresSchemaFingerprint(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT n.nspname, c.relkind::text, c.relname,
		       COALESCE(pg_get_viewdef(c.oid, true), ''),
		       COALESCE(string_agg(a.attname || ':' || format_type(a.atttypid,a.atttypmod) || ':' || a.attnotnull::text, ',' ORDER BY a.attnum) FILTER (WHERE a.attnum > 0 AND NOT a.attisdropped), '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid=c.relnamespace
		LEFT JOIN pg_attribute a ON a.attrelid=c.oid
		WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f','i')
		GROUP BY n.nspname,c.relkind,c.relname,c.oid
		ORDER BY c.relkind,c.relname`)
	if err != nil {
		return "", fmt.Errorf("inspect postgres row-state schema shape: %w", err)
	}
	defer rows.Close()
	var objects []string
	for rows.Next() {
		var schema, kind, name, definition, columns string
		if err := rows.Scan(&schema, &kind, &name, &definition, &columns); err != nil {
			return "", err
		}
		objects = append(objects, strings.Join([]string{schema, kind, name, definition, columns}, "\x00"))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	sort.Strings(objects)
	digest := sha256.Sum256([]byte(strings.Join(objects, "\x01")))
	return hex.EncodeToString(digest[:]), nil
}
