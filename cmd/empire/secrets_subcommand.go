package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"empireai/internal/config"
	"gopkg.in/yaml.v3"
)

func runSecretsSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire secrets <set|list|rotate> ...")
	}
	switch args[0] {
	case "set":
		return runSecretsSetSubcommand(args[1:])
	case "list":
		return runSecretsListSubcommand(args[1:])
	case "rotate":
		return runSecretsRotateSubcommand(args[1:])
	default:
		return fmt.Errorf("unknown secrets subcommand: %s", args[0])
	}
}

func runSecretsSetSubcommand(args []string) error {
	fs := flag.NewFlagSet("secrets set", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 3 {
		return fmt.Errorf("usage: empire secrets set <vertical-id|slug> <key.path> <value>")
	}
	target := strings.TrimSpace(fs.Args()[0])
	keyPath := strings.TrimSpace(fs.Args()[1])
	value := strings.TrimSpace(strings.Join(fs.Args()[2:], " "))
	if keyPath == "" || value == "" {
		return fmt.Errorf("key and value are required")
	}
	path := splitCredentialPath(keyPath)
	if len(path) == 0 {
		return fmt.Errorf("invalid key path")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("secrets set requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	var currentRaw []byte
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(credentials, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&currentRaw); err != nil {
		return fmt.Errorf("load existing credentials: %w", err)
	}
	current := map[string]any{}
	_ = json.Unmarshal(currentRaw, &current)
	storedValue, err := maybeEncryptCredentialValue(ctx, stores.SQLDB, value)
	if err != nil {
		return err
	}
	setNestedYAML(current, path, storedValue)
	nextRaw, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("encode updated credentials: %w", err)
	}
	if _, err := stores.SQLDB.ExecContext(ctx, `
		UPDATE verticals
		SET credentials = $2::jsonb,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, string(nextRaw)); err != nil {
		return fmt.Errorf("set credential value: %w", err)
	}
	fmt.Printf("secret set vertical=%s key=%s\n", verticalID, keyPath)
	return nil
}

func maybeEncryptCredentialValue(ctx context.Context, db *sql.DB, plain string) (string, error) {
	plain = strings.TrimSpace(plain)
	if plain == "" || db == nil {
		return plain, nil
	}
	key := strings.TrimSpace(os.Getenv("EMPIREAI_CREDENTIALS_KEY"))
	if key == "" {
		return plain, nil
	}
	var encoded string
	if err := db.QueryRowContext(ctx, `
		SELECT encode(pgp_sym_encrypt($1::text, $2::text), 'base64')
	`, plain, key).Scan(&encoded); err != nil {
		return "", fmt.Errorf("encrypt credential value: %w", err)
	}
	return "enc::" + strings.TrimSpace(encoded), nil
}

func runSecretsListSubcommand(args []string) error {
	fs := flag.NewFlagSet("secrets list", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("usage: empire secrets list <vertical-id|slug>")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("secrets list requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, strings.TrimSpace(fs.Args()[0]))
	if err != nil {
		return err
	}
	var raw []byte
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(credentials, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&raw); err != nil {
		return fmt.Errorf("load credentials: %w", err)
	}
	keys := flattenCredentialKeys(raw)
	if len(keys) == 0 {
		fmt.Println("no secret keys configured")
		return nil
	}
	for _, k := range keys {
		fmt.Printf("- %s\n", k)
	}
	return nil
}

func runSecretsRotateSubcommand(args []string) error {
	fs := flag.NewFlagSet("secrets rotate", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	value := fs.String("value", "", "New secret value")
	key := fs.String("key", "", "Optional key path override")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 2 {
		return fmt.Errorf("usage: empire secrets rotate <vertical-id|slug> <provider> --value <new-value> [--key provider.token]")
	}
	if strings.TrimSpace(*value) == "" {
		return fmt.Errorf("--value is required")
	}
	target := strings.TrimSpace(fs.Args()[0])
	provider := strings.TrimSpace(fs.Args()[1])
	keyPath := strings.TrimSpace(*key)
	if keyPath == "" {
		keyPath = provider + ".token"
	}
	return runSecretsSetSubcommand([]string{
		"-config", *cfgPath,
		"-store", *storeMode,
		fmt.Sprintf("-migrate=%v", *migrate),
		"-migration-file", *migrationFile,
		target, keyPath, *value,
	})
}

func splitCredentialPath(path string) []string {
	parts := strings.Split(path, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func flattenCredentialKeys(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil
	}
	keys := make([]string, 0, 16)
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		switch node := v.(type) {
		case map[string]any:
			for k, vv := range node {
				next := k
				if prefix != "" {
					next = prefix + "." + k
				}
				walk(next, vv)
			}
		default:
			if prefix != "" {
				keys = append(keys, prefix)
			}
		}
	}
	walk("", root)
	return keys
}

func runConfigSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire config <set|get> ...")
	}
	switch args[0] {
	case "set":
		fs := flag.NewFlagSet("config set", flag.ContinueOnError)
		file := fs.String("file", "configs/empire.yaml", "Config file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 2 {
			return fmt.Errorf("usage: empire config set <key.path> <value>")
		}
		keyPath := strings.TrimSpace(fs.Args()[0])
		value := strings.TrimSpace(strings.Join(fs.Args()[1:], " "))
		doc, err := readYAMLDocument(*file)
		if err != nil {
			return err
		}
		setNestedYAML(doc, splitCredentialPath(keyPath), parseConfigValue(value))
		return writeYAMLDocument(*file, doc)
	case "get":
		fs := flag.NewFlagSet("config get", flag.ContinueOnError)
		file := fs.String("file", "configs/empire.yaml", "Config file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire config get <key.path>")
		}
		keyPath := strings.TrimSpace(fs.Args()[0])
		doc, err := readYAMLDocument(*file)
		if err != nil {
			return err
		}
		val, ok := getNestedYAML(doc, splitCredentialPath(keyPath))
		if !ok {
			return fmt.Errorf("config key not found: %s", keyPath)
		}
		out, _ := json.MarshalIndent(val, "", "  ")
		fmt.Println(string(out))
		return nil
	default:
		return fmt.Errorf("unknown config subcommand: %s", args[0])
	}
}

func readYAMLDocument(path string) (map[string]any, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if doc == nil {
		doc = map[string]any{}
	}
	return doc, nil
}

func writeYAMLDocument(path string, doc map[string]any) error {
	b, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal yaml: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}
	fmt.Printf("config updated %s\n", path)
	return nil
}

func setNestedYAML(root map[string]any, path []string, value any) {
	if len(path) == 0 {
		return
	}
	cur := root
	for i := 0; i < len(path)-1; i++ {
		k := path[i]
		next, ok := cur[k]
		if !ok {
			n := map[string]any{}
			cur[k] = n
			cur = n
			continue
		}
		m, ok := next.(map[string]any)
		if !ok {
			m = map[string]any{}
			cur[k] = m
		}
		cur = m
	}
	cur[path[len(path)-1]] = value
}

func getNestedYAML(root map[string]any, path []string) (any, bool) {
	if len(path) == 0 {
		return root, true
	}
	var cur any = root
	for _, k := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := m[k]
		if !ok {
			return nil, false
		}
		cur = next
	}
	return cur, true
}

func parseConfigValue(raw string) any {
	v := strings.TrimSpace(raw)
	switch strings.ToLower(v) {
	case "true", "enabled", "on":
		return true
	case "false", "disabled", "off":
		return false
	}
	if i, err := strconv.Atoi(v); err == nil {
		return i
	}
	return v
}
