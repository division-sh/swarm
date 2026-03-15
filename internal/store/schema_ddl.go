package store

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

type SchemaTableDDL struct {
	TableName   string
	SchemaKind  string
	ColumnCount int
	Statements  []string
}

var (
	schemaDDLIdentifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	schemaDDLNumericPattern    = regexp.MustCompile(`^numeric\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)
	schemaDDLCreateTableName   = regexp.MustCompile(`(?is)^create\s+table(?:\s+if\s+not\s+exists)?\s+"?([a-z_][a-z0-9_]*)"?`)
	schemaDDLInlineIndexLine   = regexp.MustCompile(`(?i)^index\s+([a-z_][a-z0-9_]*)\s*\((.+?)\)\s*(where\s+.+)?$`)
)

func SchemaFieldTypeToDDL(schemaType string) (string, error) {
	schemaType = strings.TrimSpace(schemaType)
	if schemaType == "" {
		return "", fmt.Errorf("schema type is required")
	}
	normalized := strings.ToLower(schemaType)
	switch normalized {
	case "text", "string":
		return "TEXT", nil
	case "integer":
		return "BIGINT", nil
	case "boolean":
		return "BOOLEAN", nil
	case "jsonb":
		return "JSONB", nil
	case "timestamp":
		return "TIMESTAMPTZ", nil
	case "uuid":
		return "UUID", nil
	}
	if matches := schemaDDLNumericPattern.FindStringSubmatch(normalized); len(matches) == 3 {
		return fmt.Sprintf("NUMERIC(%s,%s)", matches[1], matches[2]), nil
	}
	return "", fmt.Errorf("%w %q", ErrUnknownSchemaType, schemaType)
}

func GeneratePlatformTableDDLs(spec runtimecontracts.PlatformSpecDocument) ([]SchemaTableDDL, error) {
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
	plans := make([]SchemaTableDDL, 0, len(tableNames))
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
			if parsedTable := schemaDDLExtractTableName(statement); parsedTable != "" {
				tableName = parsedTable
				break
			}
		}
		plans = append(plans, SchemaTableDDL{
			TableName:   tableName,
			SchemaKind:  "platform_spec",
			ColumnCount: schemaDDLColumnCount(statements),
			Statements:  statements,
		})
	}
	return plans, nil
}

func GenerateEntityTableDDLs(schema runtimecontracts.EntitySchema) ([]SchemaTableDDL, error) {
	if schema.Empty() {
		return nil, nil
	}
	groups := append([]runtimecontracts.EntitySchemaGroup{}, schema.Groups...)
	sort.Slice(groups, func(i, j int) bool {
		return strings.TrimSpace(groups[i].Name) < strings.TrimSpace(groups[j].Name)
	})
	plans := make([]SchemaTableDDL, 0, len(groups))
	seenTables := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		tableName, err := validateSchemaDDLIdentifier(group.Name, "entity schema group")
		if err != nil {
			return nil, err
		}
		if _, exists := seenTables[tableName]; exists {
			return nil, fmt.Errorf("entity schema group %q declares duplicate table %q", strings.TrimSpace(group.Name), tableName)
		}
		seenTables[tableName] = struct{}{}

		columnDefs := []string{
			`"entity_id" UUID PRIMARY KEY`,
			`"created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
			`"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		}
		seenColumns := map[string]struct{}{
			"entity_id":  {},
			"created_at": {},
			"updated_at": {},
		}
		indexStatements := make([]string, 0, len(group.Fields))
		for _, field := range group.Fields {
			columnName, err := validateSchemaDDLIdentifier(field.Name, fmt.Sprintf("entity schema group %s field", tableName))
			if err != nil {
				return nil, err
			}
			if _, exists := seenColumns[columnName]; exists {
				return nil, fmt.Errorf("entity schema group %s declares duplicate column %s", tableName, columnName)
			}
			seenColumns[columnName] = struct{}{}
			columnType, err := SchemaFieldTypeToDDL(field.Type)
			if err != nil {
				return nil, fmt.Errorf("entity schema group %s field %s: %w", tableName, columnName, err)
			}
			columnDef := fmt.Sprintf("%s %s", quoteIdent(columnName), columnType)
			if !field.Nullable || field.Primary {
				columnDef += " NOT NULL"
			}
			if field.Primary {
				columnDef += " UNIQUE"
			}
			columnDefs = append(columnDefs, columnDef)
			if field.Indexed {
				indexName := schemaDDLIndexName("idx", tableName, columnName)
				indexStatements = append(indexStatements,
					fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)", quoteIdent(indexName), quoteIdent(tableName), quoteIdent(columnName)),
				)
			}
		}
		statements := []string{
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)", quoteIdent(tableName), strings.Join(columnDefs, ",\n    ")),
		}
		statements = append(statements, indexStatements...)
		plans = append(plans, SchemaTableDDL{
			TableName:   tableName,
			SchemaKind:  "entity_schema",
			ColumnCount: len(columnDefs),
			Statements:  statements,
		})
	}
	return plans, nil
}

func GenerateNodeStateTableDDLs(nodes map[string]runtimecontracts.SystemNodeContract) ([]SchemaTableDDL, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	plans := make([]SchemaTableDDL, 0, len(nodeIDs))
	tableOwners := map[string]string{}
	for _, nodeID := range nodeIDs {
		node := nodes[nodeID]
		if strings.TrimSpace(node.StateTable) == "" {
			continue
		}
		tableName, err := validateSchemaDDLIdentifier(node.StateTable, fmt.Sprintf("node %s state_table", strings.TrimSpace(nodeID)))
		if err != nil {
			return nil, err
		}
		if owner, exists := tableOwners[tableName]; exists {
			return nil, fmt.Errorf("state table %s declared by multiple nodes: %s, %s", tableName, owner, strings.TrimSpace(nodeID))
		}
		tableOwners[tableName] = strings.TrimSpace(nodeID)

		columnDefs := []string{
			`"entity_id" UUID NOT NULL`,
			`"node_id" TEXT NOT NULL`,
			`"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		}
		seenColumns := map[string]struct{}{
			"entity_id":  {},
			"node_id":    {},
			"updated_at": {},
		}
		for _, field := range node.StateSchema.Fields {
			columnName, err := validateSchemaDDLIdentifier(field.Name, fmt.Sprintf("node %s state_schema field", strings.TrimSpace(nodeID)))
			if err != nil {
				return nil, err
			}
			if _, exists := seenColumns[columnName]; exists {
				return nil, fmt.Errorf("node %s state table %s declares duplicate column %s", strings.TrimSpace(nodeID), tableName, columnName)
			}
			seenColumns[columnName] = struct{}{}
			columnType, err := SchemaFieldTypeToDDL(field.Type)
			if err != nil {
				return nil, fmt.Errorf("node %s state table %s field %s: %w", strings.TrimSpace(nodeID), tableName, columnName, err)
			}
			columnDefs = append(columnDefs, fmt.Sprintf("%s %s", quoteIdent(columnName), columnType))
		}
		columnDefs = append(columnDefs, `PRIMARY KEY ("entity_id", "node_id")`)
		plans = append(plans, SchemaTableDDL{
			TableName:   tableName,
			SchemaKind:  "state_schema",
			ColumnCount: len(columnDefs) - 1,
			Statements: []string{
				fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)", quoteIdent(tableName), strings.Join(columnDefs, ",\n    ")),
			},
		})
	}
	return plans, nil
}

func FlattenSchemaTableDDLs(plans []SchemaTableDDL) []string {
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

func validateSchemaDDLIdentifier(name, context string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%s identifier is required", strings.TrimSpace(context))
	}
	if !schemaDDLIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("%s identifier %q must match %s", strings.TrimSpace(context), name, schemaDDLIdentifierPattern.String())
	}
	return name, nil
}

func schemaDDLIndexName(parts ...string) string {
	raw := strings.Join(parts, "_")
	name := sanitizeSchemaIdent(raw)
	if name == "" {
		return "idx_generated"
	}
	return name
}

func platformTableOrder(name string) int {
	switch strings.TrimSpace(name) {
	case "schema_version":
		return 0
	case "events":
		return 10
	case "dead_letters":
		return 20
	case "agents":
		return 30
	case "flow_instances":
		return 40
	case "entity_state":
		return 50
	case "agent_sessions":
		return 60
	case "routing_rules":
		return 70
	case "event_deliveries":
		return 80
	case "event_receipts":
		return 90
	case "mailbox":
		return 100
	case "spend_ledger":
		return 110
	case "timers":
		return 120
	default:
		return 1000
	}
}

func QuoteIdent(v string) string {
	return quoteIdent(v)
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
			tableStatement, indexStatements, err := schemaDDLNormalizeCreateTable(statement)
			if err != nil {
				return nil, err
			}
			statements = append(statements, tableStatement)
			statements = append(statements, indexStatements...)
			continue
		case strings.HasPrefix(strings.ToUpper(statement), "CREATE INDEX "):
			statement = schemaDDLEnsureIfNotExists(statement, "CREATE INDEX")
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

func schemaDDLNormalizeCreateTable(statement string) (string, []string, error) {
	statement = schemaDDLEnsureIfNotExists(statement, "CREATE TABLE")
	tableName := schemaDDLExtractTableName(statement)
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
		if matches := schemaDDLInlineIndexLine.FindStringSubmatch(trimmed); len(matches) >= 3 {
			indexName := strings.TrimSpace(matches[1])
			indexCols := strings.TrimSpace(matches[2])
			whereClause := ""
			if len(matches) >= 4 {
				whereClause = strings.TrimSpace(matches[3])
			}
			statement := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)", quoteIdent(indexName), quoteIdent(tableName), indexCols)
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

func schemaDDLEnsureIfNotExists(statement, prefix string) string {
	statement = strings.TrimSpace(statement)
	upper := strings.ToUpper(statement)
	if strings.HasPrefix(upper, prefix+" IF NOT EXISTS ") {
		return statement
	}
	return prefix + " IF NOT EXISTS " + strings.TrimSpace(statement[len(prefix):])
}

func schemaDDLExtractTableName(statement string) string {
	statement = strings.TrimSpace(statement)
	matches := schemaDDLCreateTableName.FindStringSubmatch(statement)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func schemaDDLColumnCount(statements []string) int {
	for _, statement := range statements {
		tableName := schemaDDLExtractTableName(statement)
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
