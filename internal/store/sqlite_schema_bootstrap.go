package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func (s *SQLiteSchemaStore) BootstrapSchema(ctx context.Context, request SchemaBootstrapRequest) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite store is required for schema bootstrap")
	}
	if err := request.validate(); err != nil {
		return err
	}
	expected, err := expectedSchemaShape(request.PlatformPlans, SchemaDialectSQLite)
	if err != nil {
		return err
	}
	bootstrapMu := sqliteSchemaBootstrapMutex(s.path)
	bootstrapMu.Lock()
	defer bootstrapMu.Unlock()
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open serialized sqlite schema connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("serialize sqlite schema bootstrap: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	report, err := inspectSQLiteCompatibility(ctx, conn, expected)
	if err != nil {
		return err
	}
	diagnostic := schemaCompatibilityDiagnostic{Backend: SchemaDialectSQLite, Target: s.path, Current: request.Origin, Origin: report.Origin}
	switch report.State {
	case schemaStateFresh:
		if err := executeSQLitePlans(ctx, conn, request.PlatformPlans); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO runtime_store_metadata (id, swarm_version, platform_version, created_at) VALUES (1, ?, ?, ?)`, request.Origin.SwarmVersion, request.Origin.PlatformVersion, request.Origin.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("stamp fresh sqlite store origin: %w", err)
		}
	case schemaStateCompatible:
	case schemaStateIncompatible:
		return diagnostic.failure(report.Drift)
	default:
		return fmt.Errorf("unknown sqlite schema compatibility state %q", report.State)
	}
	if err := ensureSQLiteStatePlans(ctx, conn, request.StatePlans, diagnostic); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("commit sqlite schema bootstrap: %w", err)
	}
	committed = true
	if _, err := s.DB.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		return fmt.Errorf("enable sqlite WAL after schema acceptance: %w", err)
	}
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}

func inspectSQLiteCompatibility(ctx context.Context, q schemaQueryer, expected schemaShape) (schemaCompatibilityReport, error) {
	tables, err := sqliteUserTables(ctx, q)
	if err != nil {
		return schemaCompatibilityReport{}, err
	}
	if len(tables) == 0 {
		return schemaCompatibilityReport{State: schemaStateFresh}, nil
	}
	var origin *RuntimeStoreOrigin
	var drift []string
	if _, ok := tables[RuntimeStoreMetadataTable]; !ok {
		drift = append(drift, "non-empty SQLite store has no runtime_store_metadata origin stamp")
	} else {
		origin, err = readRuntimeStoreOrigin(ctx, q)
		if err != nil {
			drift = append(drift, "runtime_store_metadata origin row is malformed: "+err.Error())
		} else if origin == nil {
			drift = append(drift, "runtime_store_metadata does not contain the required id=1 origin row")
		}
	}
	actual, err := loadSQLiteSchemaShape(ctx, q, expected)
	if err != nil {
		return schemaCompatibilityReport{}, err
	}
	drift = append(drift, compareSchemaShapes(expected, actual)...)
	state := schemaStateCompatible
	if len(drift) > 0 {
		state = schemaStateIncompatible
	}
	return schemaCompatibilityReport{State: state, Origin: origin, Drift: drift}, nil
}

func sqliteUserTables(ctx context.Context, q schemaQueryer) (map[string]struct{}, error) {
	rows, err := q.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type IN ('table', 'view', 'index', 'trigger') AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return nil, fmt.Errorf("inspect sqlite tables: %w", err)
	}
	defer rows.Close()
	tables := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables[strings.TrimSpace(name)] = struct{}{}
	}
	return tables, rows.Err()
}

func loadSQLiteSchemaShape(ctx context.Context, q schemaQueryer, expected schemaShape) (schemaShape, error) {
	actual := schemaShape{Tables: map[string]schemaTableShape{}}
	rows, err := q.QueryContext(ctx, `SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, name`)
	if err != nil {
		return schemaShape{}, fmt.Errorf("inspect sqlite schema: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var objectType, name, tableName, statement string
		if err := rows.Scan(&objectType, &name, &tableName, &statement); err != nil {
			return schemaShape{}, err
		}
		if _, required := expected.Tables[tableName]; !required {
			continue
		}
		if err := addStatementToShape(&actual, statement); err != nil {
			return schemaShape{}, fmt.Errorf("inspect sqlite %s %s: %w", objectType, name, err)
		}
	}
	return actual, rows.Err()
}

func executeSQLitePlans(ctx context.Context, conn *sql.Conn, plans []SchemaTableDDL) error {
	for _, plan := range plans {
		statements, err := SQLiteStatementsForPlan(plan)
		if err != nil {
			return err
		}
		for _, statement := range statements {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("create sqlite %s table %s: %w", plan.SchemaKind, plan.TableName, err)
			}
		}
	}
	return nil
}

func ensureSQLiteStatePlans(ctx context.Context, conn *sql.Conn, plans []SchemaTableDDL, diagnostic schemaCompatibilityDiagnostic) error {
	for _, plan := range plans {
		expected, err := expectedSchemaShape([]SchemaTableDDL{plan}, SchemaDialectSQLite)
		if err != nil {
			return err
		}
		tables, err := sqliteUserTables(ctx, conn)
		if err != nil {
			return err
		}
		if _, exists := tables[plan.TableName]; !exists {
			if err := executeSQLitePlans(ctx, conn, []SchemaTableDDL{plan}); err != nil {
				return err
			}
			continue
		}
		actual, err := loadSQLiteSchemaShape(ctx, conn, expected)
		if err != nil {
			return err
		}
		if drift := compareSchemaShapes(expected, actual); len(drift) > 0 {
			return diagnostic.failure(generatedStateDrift(plan.TableName, drift))
		}
	}
	return nil
}
