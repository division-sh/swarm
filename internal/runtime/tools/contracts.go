package tools

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	llm "empireai/internal/runtime/llm"
	"gopkg.in/yaml.v3"
)

type ContractSchemaEntry struct {
	Category    string         `yaml:"category"`
	Description string         `yaml:"description"`
	InputSchema map[string]any `yaml:"input_schema"`
}

var (
	contractSchemasOnce sync.Once
	contractSchemas     map[string]ContractSchemaEntry
	contractSchemasErr  error
)

func LoadContractSchemas() (map[string]ContractSchemaEntry, error) {
	contractSchemasOnce.Do(func() {
		path := filepath.Join(repoRoot(), "contracts", "tool-schemas.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			contractSchemasErr = fmt.Errorf("read %s: %w", path, err)
			return
		}
		parsed := map[string]ContractSchemaEntry{}
		if err := yaml.Unmarshal(raw, &parsed); err != nil {
			contractSchemasErr = fmt.Errorf("parse %s: %w", path, err)
			return
		}
		contractSchemas = parsed
	})
	if contractSchemasErr != nil {
		return nil, contractSchemasErr
	}
	return contractSchemas, nil
}

func ContractDefinitions() ([]llm.ToolDefinition, error) {
	entries, err := LoadContractSchemas()
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

	defs := make([]llm.ToolDefinition, 0, len(names))
	for _, name := range names {
		entry := entries[name]
		defs = append(defs, llm.ToolDefinition{
			Name:        name,
			Description: strings.TrimSpace(entry.Description),
			Schema:      deepCloneJSONValue(entry.InputSchema),
		})
	}
	return defs, nil
}

func ObjectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func repoRoot() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
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
