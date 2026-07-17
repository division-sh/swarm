package platformschema

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func BootstrapFreshPostgres(ctx context.Context, tx *sql.Tx, plans []TableDDL, swarmVersion, platformVersion string, createdAt time.Time) error {
	if tx == nil {
		return fmt.Errorf("postgres bootstrap transaction is required")
	}
	if _, err := tx.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return fmt.Errorf("create pgcrypto for fresh postgres store: %w", err)
	}
	for _, plan := range plans {
		for _, statement := range plan.Statements {
			if _, err := tx.ExecContext(ctx, strings.TrimSpace(statement)); err != nil {
				return fmt.Errorf("create postgres %s table %s: %w", plan.SchemaKind, plan.TableName, err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_store_metadata (id, swarm_version, platform_version, created_at) VALUES (1, $1, $2, $3)`, swarmVersion, platformVersion, createdAt.UTC()); err != nil {
		return fmt.Errorf("stamp fresh postgres store origin: %w", err)
	}
	return nil
}

type TableDDL struct {
	TableName   string
	SchemaKind  string
	ColumnCount int
	Statements  []string
}

var (
	createTableName = regexp.MustCompile(`(?is)^create\s+table(?:\s+if\s+not\s+exists)?\s+"?([a-z_][a-z0-9_]*)"?`)
	inlineIndexLine = regexp.MustCompile(`(?i)^(unique\s+)?index\s+([a-z_][a-z0-9_]*)\s*\((.+?)\)\s*(where\s+.+)?$`)
)

func GeneratePlatformTableDDLs(spec runtimecontracts.PlatformSpecDocument) ([]TableDDL, error) {
	if len(spec.PlatformTables.Tables) == 0 {
		return nil, fmt.Errorf("platform spec platform_tables.tables.*.ddl is required")
	}
	tableNames := make([]string, 0, len(spec.PlatformTables.Tables))
	for tableName := range spec.PlatformTables.Tables {
		tableNames = append(tableNames, strings.TrimSpace(tableName))
	}
	sort.Slice(tableNames, func(i, j int) bool {
		left := platformTableOrder(tableNames[i])
		right := platformTableOrder(tableNames[j])
		if left != right {
			return left < right
		}
		return tableNames[i] < tableNames[j]
	})
	plans := make([]TableDDL, 0, len(tableNames))
	for _, declaredName := range tableNames {
		tableSpec := spec.PlatformTables.Tables[declaredName]
		rawDDL := strings.TrimSpace(tableSpec.DDL)
		if rawDDL == "" {
			return nil, fmt.Errorf("platform spec table %s ddl is required", declaredName)
		}
		statements, err := normalizePlatformDDLStatements(rawDDL)
		if err != nil {
			return nil, fmt.Errorf("platform spec table %s: %w", declaredName, err)
		}
		tableName := declaredName
		for _, statement := range statements {
			if parsedTable := ExtractTableName(statement); parsedTable != "" {
				tableName = parsedTable
				break
			}
		}
		plans = append(plans, TableDDL{
			TableName:   tableName,
			SchemaKind:  "platform_spec",
			ColumnCount: columnCount(statements),
			Statements:  statements,
		})
	}
	return plans, nil
}

func Flatten(plans []TableDDL) []string {
	if len(plans) == 0 {
		return nil
	}
	out := make([]string, 0, len(plans)*2)
	for _, plan := range plans {
		for _, statement := range plan.Statements {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			out = append(out, statement)
		}
	}
	return out
}

func IncludesPlatformTables(plans []TableDDL) bool {
	for _, plan := range plans {
		if strings.TrimSpace(plan.SchemaKind) == "platform_spec" {
			return true
		}
	}
	return false
}

func ExtractTableName(statement string) string {
	statement = strings.TrimSpace(statement)
	matches := createTableName.FindStringSubmatch(statement)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func PlanColumnNames(plan TableDDL) []string {
	for _, statement := range plan.Statements {
		if ExtractTableName(statement) == "" {
			continue
		}
		return createTableColumnNames(statement)
	}
	return nil
}

func platformTableOrder(name string) int {
	switch strings.TrimSpace(name) {
	case "runtime_store_metadata":
		return 0
	case "bundles":
		return 3
	case "runs":
		return 5
	case "standing_services":
		return 8
	case "standing_service_generations":
		return 9
	case "standing_service_journal":
		return 9
	case "author_activity_order":
		return 6
	case "author_activity_occurrences":
		return 7
	case "events":
		return 10
	case "run_fork_revision_heads":
		return 11
	case "run_fork_revisions":
		return 12
	case "run_fork_fact_revisions":
		return 13
	case "agent_directive_operations":
		return 14
	case "reply_contexts":
		return 15
	case "activity_attempts":
		return 16
	case "run_fork_selected_contract_bindings":
		return 17
	case "run_fork_selected_contract_runtime_executions":
		return 18
	case "run_fork_selected_contract_executions":
		return 19
	case "run_fork_selected_contract_branch_divergences":
		return 20
	case "run_fork_selected_contract_route_recoveries":
		return 21
	case "inbound_publications":
		return 22
	case "inbound_publication_events":
		return 23
	case "dead_letters":
		return 24
	case "agents":
		return 30
	case "flow_instances":
		return 40
	case "entity_state":
		return 50
	case "conversation_forks":
		return 55
	case "conversation_fork_snapshots":
		return 56
	case "conversation_fork_turns":
		return 57
	case "agent_sessions":
		return 60
	case "runtime_startup_authority_facts":
		return 61
	case "managed_agent_capability_surfaces":
		return 62
	case "runtime_external_effect_operations":
		return 63
	case "runtime_external_effect_attempts":
		return 64
	case "agent_turns":
		return 65
	case "conversation_fork_turn_completions":
		return 68
	case "routing_rules":
		return 70
	case "event_deliveries":
		return 80
	case "run_fork_delivery_event_replays":
		return 85
	case "event_receipts":
		return 90
	case "entity_mutations":
		return 95
	case "mailbox":
		return 100
	case "decision_cards":
		return 101
	case "proposed_effect_continuations", "human_task_continuations":
		return 102
	case "decision_card_input_drafts":
		return 103
	case "decision_card_changes":
		return 104
	case "decision_card_route_obligations":
		return 105
	case "api_idempotency":
		return 106
	case "runtime_ingress_state":
		return 108
	case "run_control_state":
		return 109
	case "spend_ledger":
		return 110
	case "budget_admission_scopes":
		return 111
	case "runtime_effect_budget_reservations":
		return 112
	case "timers":
		return 120
	default:
		return 1000
	}
}

func normalizePlatformDDLStatements(rawDDL string) ([]string, error) {
	chunks := strings.Split(rawDDL, ";")
	statements := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		statement := strings.TrimSpace(chunk)
		if statement == "" {
			continue
		}
		switch {
		case strings.HasPrefix(strings.ToUpper(statement), "CREATE TABLE "):
			tableStatement, indexStatements, err := normalizeCreateTable(statement)
			if err != nil {
				return nil, err
			}
			statements = append(statements, tableStatement)
			statements = append(statements, indexStatements...)
			continue
		case strings.HasPrefix(strings.ToUpper(statement), "CREATE INDEX "):
			statement = ensureIfNotExists(statement, "CREATE INDEX")
		case strings.HasPrefix(strings.ToUpper(statement), "CREATE UNIQUE INDEX "):
			statement = ensureIfNotExists(statement, "CREATE UNIQUE INDEX")
		default:
			return nil, fmt.Errorf("unsupported platform DDL statement %q", statement)
		}
		statements = append(statements, statement)
	}
	if len(statements) == 0 {
		return nil, fmt.Errorf("no executable platform DDL statements found")
	}
	return statements, nil
}

func normalizeCreateTable(statement string) (string, []string, error) {
	statement = ensureIfNotExists(statement, "CREATE TABLE")
	tableName := ExtractTableName(statement)
	if tableName == "" {
		return "", nil, fmt.Errorf("unable to extract table name from %q", statement)
	}
	start := strings.Index(statement, "(")
	end := strings.LastIndex(statement, ")")
	if start < 0 || end <= start {
		return statement, nil, nil
	}
	body := statement[start+1 : end]
	lines := strings.Split(body, "\n")
	kept := make([]string, 0, len(lines))
	indexStatements := make([]string, 0, 2)
	for _, rawLine := range lines {
		trimmed := strings.TrimSpace(strings.TrimSuffix(rawLine, ","))
		if trimmed == "" {
			continue
		}
		if matches := inlineIndexLine.FindStringSubmatch(trimmed); len(matches) >= 3 {
			uniquePrefix := strings.TrimSpace(matches[1])
			indexName := strings.TrimSpace(matches[2])
			indexCols := strings.TrimSpace(matches[3])
			whereClause := ""
			if len(matches) >= 5 {
				whereClause = strings.TrimSpace(matches[4])
			}
			statement := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)", quoteIdent(indexName), quoteIdent(tableName), indexCols)
			if uniquePrefix != "" {
				statement = fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(%s)", quoteIdent(indexName), quoteIdent(tableName), indexCols)
			}
			if whereClause != "" {
				statement += " " + whereClause
			}
			indexStatements = append(indexStatements, statement)
			continue
		}
		kept = append(kept, trimmed)
	}
	normalizedTable := fmt.Sprintf("%s (\n    %s\n)", statement[:start], strings.Join(kept, ",\n    "))
	return normalizedTable, indexStatements, nil
}

func ensureIfNotExists(statement, prefix string) string {
	statement = strings.TrimSpace(statement)
	upper := strings.ToUpper(statement)
	if strings.HasPrefix(upper, prefix+" IF NOT EXISTS ") {
		return statement
	}
	return prefix + " IF NOT EXISTS " + strings.TrimSpace(statement[len(prefix):])
}

func columnCount(statements []string) int {
	for _, statement := range statements {
		tableName := ExtractTableName(statement)
		if tableName == "" {
			continue
		}
		start := strings.Index(statement, "(")
		end := strings.LastIndex(statement, ")")
		if start < 0 || end <= start {
			return 0
		}
		count := 0
		for _, line := range strings.Split(statement[start+1:end], "\n") {
			line = strings.TrimSpace(strings.TrimSuffix(line, ","))
			if line == "" || strings.HasPrefix(strings.ToUpper(line), "PRIMARY KEY") {
				continue
			}
			count++
		}
		return count
	}
	return 0
}

func createTableColumnNames(statement string) []string {
	start := strings.Index(statement, "(")
	end := strings.LastIndex(statement, ")")
	if start < 0 || end <= start {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, line := range strings.Split(statement[start+1:end], "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, ","))
		if line == "" || createTableLineIsConstraint(line) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		columnName := strings.Trim(fields[0], `"`)
		if columnName == "" {
			continue
		}
		if _, exists := seen[columnName]; exists {
			continue
		}
		seen[columnName] = struct{}{}
		out = append(out, columnName)
	}
	return out
}

func createTableLineIsConstraint(line string) bool {
	fields := strings.Fields(strings.ToUpper(strings.TrimSpace(line)))
	if len(fields) == 0 {
		return false
	}
	switch fields[0] {
	case "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT", "EXCLUDE":
		return true
	default:
		return false
	}
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}
