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
	return createSQLiteFixtureTables(ctx, db, []string{"dead_letters"})
}

// CreateSQLiteRunAdmissionSchema supplies the canonical lifecycle/control and
// standing relations now read by delivery claim admission in partial fixtures.
func CreateSQLiteRunAdmissionSchema(ctx context.Context, db *sql.DB) error {
	return createSQLiteFixtureTables(ctx, db, []string{"run_control_state", "standing_services", "standing_service_generations"})
}

func createSQLiteFixtureTables(ctx context.Context, db *sql.DB, names []string) error {
	tables, err := sqliteFixtureStatements()
	if err != nil {
		return err
	}
	for _, name := range names {
		for _, statement := range tables[name] {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("create canonical SQLite %s fixture schema: %w", name, err)
			}
		}
	}
	return nil
}

var sqliteFixtureStatements = sync.OnceValues(func() (map[string][]string, error) {
	source, err := yamlsource.Load(platform.PlatformSpecYAML())
	if err != nil {
		return nil, err
	}
	var spec runtimecontracts.PlatformSpecDocument
	spec, err = runtimecontracts.AdmitPlatformSpecValue(source.Document("platform-spec.yaml").Root())
	if err != nil {
		return nil, err
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return nil, err
	}
	tables := map[string][]string{}
	for _, plan := range plans {
		switch plan.TableName {
		case "dead_letters", "run_control_state", "standing_services", "standing_service_generations":
			statements, err := schemastore.SQLiteStatementsForPlan(plan)
			if err != nil {
				return nil, err
			}
			tables[plan.TableName] = statements
		}
	}
	if len(tables) != 4 {
		return nil, fmt.Errorf("canonical SQLite delivery fixture requires all four table plans")
	}
	return tables, nil
})
