package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

type contractToolSchemaEntry struct {
	Category    string         `yaml:"category"`
	Description string         `yaml:"description"`
	InputSchema map[string]any `yaml:"input_schema"`
}

var (
	contractToolSchemasOnce sync.Once
	contractToolSchemas     map[string]contractToolSchemaEntry
	contractToolSchemasErr  error
)

func loadContractToolSchemas() (map[string]contractToolSchemaEntry, error) {
	contractToolSchemasOnce.Do(func() {
		path := filepath.Join(runtimeRepoRoot(), "contracts", "tool-schemas.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			contractToolSchemasErr = fmt.Errorf("read %s: %w", path, err)
			return
		}
		parsed := map[string]contractToolSchemaEntry{}
		if err := yaml.Unmarshal(raw, &parsed); err != nil {
			contractToolSchemasErr = fmt.Errorf("parse %s: %w", path, err)
			return
		}
		contractToolSchemas = parsed
	})
	if contractToolSchemasErr != nil {
		return nil, contractToolSchemasErr
	}
	return contractToolSchemas, nil
}

func contractToolDefinitions() ([]ToolDefinition, error) {
	entries, err := loadContractToolSchemas()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	defs := make([]ToolDefinition, 0, len(names))
	for _, name := range names {
		entry := entries[name]
		defs = append(defs, ToolDefinition{
			Name:        name,
			Description: strings.TrimSpace(entry.Description),
			Schema:      deepCloneJSONValue(entry.InputSchema),
		})
	}
	return defs, nil
}

func runtimeRepoRoot() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func deepCloneJSONValue(v any) any {
	if v == nil {
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}
