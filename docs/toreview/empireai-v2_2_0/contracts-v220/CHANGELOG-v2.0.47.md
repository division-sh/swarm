# EmpireAI v2.0.47 — Prompt Template Variables

**Version:** 2.0.47
**Previous:** 2.0.46
**Date:** 2026-03-05

## Summary

Introduces `prompt-variables.yaml` — a single source of truth for all values that appear in multiple contract files or prompts. Eliminates the #1 recurring audit finding: threshold/enum drift between files.

42 variables covering: numeric thresholds (19), enum lists (5), capability tiers (3), scoring dimensions (11), structural constants (4). Prompt files will use `{{variable_name}}` syntax. The runtime substitutes values from this file before sending prompts to the LLM.

## Implementation Instructions

### 1. Build the template renderer

In the prompt loader (where you read `.md` files from `contracts/prompts/`), add a substitution step:

```go
func renderPrompt(promptText string, vars map[string]interface{}) string {
    for key, val := range vars {
        placeholder := "{{" + key + "}}"
        switch v := val.(type) {
        case string:
            promptText = strings.ReplaceAll(promptText, placeholder, v)
        case int:
            promptText = strings.ReplaceAll(promptText, placeholder, strconv.Itoa(v))
        case []interface{}:
            // Render as comma-separated or newline list
            items := make([]string, len(v))
            for i, item := range v {
                items[i] = fmt.Sprintf("%v", item)
            }
            promptText = strings.ReplaceAll(promptText, placeholder, strings.Join(items, ", "))
        }
    }
    return promptText
}
```

Load `prompt-variables.yaml` once at startup. Apply `renderPrompt()` after reading each `.md` file, before sending to the LLM.

### 2. Add a validation test

```go
func TestPromptVariablesComplete(t *testing.T) {
    // Read all prompt files
    // Find every {{...}} token
    // Assert each has a matching key in prompt-variables.yaml
    // Assert no unresolved {{...}} tokens in rendered output
}
```

### 3. Migration is incremental

Current prompts with hardcoded values still work — the renderer is a no-op if there are no `{{}}` tokens. I will convert prompts to use variables one at a time. No breaking change.

### 4. What NOT to template

Event names, payload field names, tool schemas — these are enforced by MCP/EventSchemaRegistry at runtime. The template system covers behavioral instructions (thresholds, enum lists, capability descriptions) that the schema can't enforce.

## Touches

contracts:
  - prompt-variables.yaml: NEW — 42 variables covering all duplicated values
  - All contract headers bumped to 2.0.47

spec:
  - Version bumped to 2.0.47

## Post-Update Verification

```
cd contracts/prompts && sha256sum -c ../prompt-manifest.sha256
```

Prompt files unchanged in this version — only the variables file is new.
