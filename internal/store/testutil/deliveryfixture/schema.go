package deliveryfixture

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// CreateSQLiteDeadLetterSchema completes partial delivery fixtures using the
// same table, constraints and indexes as a canonical SQLite store.
func CreateSQLiteDeadLetterSchema(ctx context.Context, db *sql.DB) error {
	statements, err := sqliteDeadLetterStatements()
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create canonical SQLite dead-letter fixture schema: %w", err)
		}
	}
	return nil
}

var sqliteDeadLetterStatements = sync.OnceValues(func() ([]string, error) {
	source, err := yamlsource.Load(platform.PlatformSpecYAML())
	if err != nil {
		return nil, err
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
		return nil, err
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return nil, err
	}
	for _, plan := range plans {
		if plan.TableName == "dead_letters" {
			return schemastore.SQLiteStatementsForPlan(plan)
		}
	}
	return nil, fmt.Errorf("canonical dead_letters schema is required")
})
