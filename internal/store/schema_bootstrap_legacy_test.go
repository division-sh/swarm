package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// EnsureSchemaTables exists only in the store package test binary so legacy
// fixture tests can be migrated independently without restoring the production
// schema API.
func (s *PostgresStore) EnsureSchemaTables(ctx context.Context, plans []SchemaTableDDL) error {
	if schemaPlansIncludePlatform(plans) {
		if err := ensurePostgresCanonicalFailureSchema(ctx, s.DB); err != nil {
			return err
		}
		if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
			return err
		}
	}
	if _, err := s.DB.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, plan := range plans {
		for _, statement := range plan.Statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				if mapped := s.outdatedSchemaErrorForLegacyTest(ctx, plan, err); mapped != nil {
					return mapped
				}
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if schemaPlansIncludePlatform(plans) {
		if err := ensurePostgresCanonicalFailureSchema(ctx, s.DB); err != nil {
			return err
		}
		if err := s.ensureSchemaCompatibilityColumns(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteSchemaStore) EnsureSchemaTables(ctx context.Context, plans []SchemaTableDDL) error {
	if schemaPlansIncludePlatform(plans) {
		if err := ensureSQLiteCanonicalFailureSchema(ctx, s.DB); err != nil {
			return err
		}
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var agentStatements []string
	for _, plan := range plans {
		statements, err := SQLiteStatementsForPlan(plan)
		if err != nil {
			return err
		}
		if plan.TableName == "agents" {
			agentStatements = append(agentStatements, statements...)
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if schemaPlansIncludePlatform(plans) {
		if err := ensureSQLiteCanonicalFailureSchema(ctx, s.DB); err != nil {
			return err
		}
	}
	if len(agentStatements) > 0 {
		if err := s.ensureSQLiteAgentLLMBackendProfiles(ctx, agentStatements); err != nil {
			return err
		}
		if err := s.ensureSQLiteAgentModelAliases(ctx); err != nil {
			return err
		}
		if err := s.ensureSQLiteAgentLifecycleColumns(ctx); err != nil {
			return err
		}
	}
	if err := s.ensureSQLiteMailboxDeferredUntil(ctx); err != nil {
		return err
	}
	return s.ensureSQLiteReplyContextColumns(ctx)
}

func (s *SQLiteRuntimeStore) EnsureSchemaTables(ctx context.Context, plans []SchemaTableDDL) error {
	return s.SQLiteSchemaStore.EnsureSchemaTables(ctx, plans)
}

func schemaPlansIncludePlatform(plans []SchemaTableDDL) bool {
	for _, plan := range plans {
		if plan.SchemaKind == "platform_spec" {
			return true
		}
	}
	return false
}

func (s *PostgresStore) outdatedSchemaErrorForLegacyTest(ctx context.Context, plan SchemaTableDDL, cause error) error {
	tableName := strings.TrimSpace(plan.TableName)
	expectedColumns := schemaDDLPlanColumnNames(plan)
	if tableName == "" || len(expectedColumns) == 0 {
		return nil
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil || !catalog.hasTable(tableName) {
		return nil
	}
	var missing []string
	for _, column := range expectedColumns {
		if !catalog.hasColumns(tableName, column) {
			missing = append(missing, column)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return &OutdatedSchemaError{SchemaKind: plan.SchemaKind, TableName: tableName, MissingColumns: missing, Cause: fmt.Errorf("%w", cause)}
}

func testSchemaBootstrapRequest(plans []SchemaTableDDL) SchemaBootstrapRequest {
	request := SchemaBootstrapRequest{Origin: RuntimeStoreOrigin{SwarmVersion: "test", PlatformVersion: "test", CreatedAt: time.Now().UTC()}}
	for _, plan := range plans {
		if plan.SchemaKind == "platform_spec" {
			request.PlatformPlans = append(request.PlatformPlans, plan)
		} else if plan.SchemaKind == "state_schema" {
			request.StatePlans = append(request.StatePlans, plan)
		}
	}
	return request
}
