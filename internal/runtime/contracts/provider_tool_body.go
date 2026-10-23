package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// AdmitProviderToolDeclarations consumes the captured connector role, keeping
// ordinary tool grammar in its existing lexical and execution owners.
func AdmitProviderToolDeclarations(body packartifact.ProviderBody) (map[string]ToolSchemaEntry, error) {
	if !body.Valid() || body.Role() != packmodel.BodyConnector {
		return nil, fmt.Errorf("provider tools require an admitted connector.yaml BODY")
	}
	tools, err := body.SourceValue().Lookup("tools")
	if err != nil {
		return nil, err
	}
	entries, err := projectToolDeclarationsValue(tools.Value)
	if err != nil {
		return nil, err
	}
	declarations, err := tools.Value.Mapping()
	if err != nil {
		return nil, err
	}
	out := make(map[string]ToolSchemaEntry, len(entries))
	for _, declaration := range declarations {
		name, err := providerBodyLocalName(declaration.Value, body.ProviderKey(), declaration.Name, "tool")
		if err != nil {
			return nil, err
		}
		entry, err := qualifyProviderToolCredentials(entries[declaration.Name], declaration.Value, body.ProviderKey())
		if err != nil {
			return nil, err
		}
		out[name] = entry
	}
	return out, nil
}

// QualifyProviderLocalName is a pure syntax projection for local tool and
// credential names. The captured BODY, not this function, supplies identity.
func QualifyProviderLocalName(key, local string) (string, error) {
	if err := runtimepaths.ValidateFlowSegment(key); err != nil {
		return "", fmt.Errorf("provider name requires an exact portable directory key: %w", err)
	}
	if !toolPathNamePattern.MatchString(local) {
		return "", fmt.Errorf("name %q must use a short local token such as send_message or bot_token; directory %q supplies the namespace", local, key)
	}
	return key + "." + local, nil
}

func providerBodyLocalName(value yamlsource.Value, key, local, kind string) (string, error) {
	qualified, err := QualifyProviderLocalName(key, local)
	if err != nil {
		example := "bot_token"
		if kind == "tool" {
			example = "send_message"
		}
		return "", nodeValueError(value, fmt.Errorf("provider %s %q must use a short local name such as %q; directory %q supplies the namespace", kind, local, example, key))
	}
	return qualified, nil
}

func qualifyProviderToolCredentials(entry ToolSchemaEntry, source yamlsource.Value, key string) (ToolSchemaEntry, error) {
	copyValue := *entry.value
	roles := map[string]string{}
	static, err := source.Lookup("credentials")
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	if static.Presence != yamlsource.PresenceMissing {
		copyValue.credentials, roles, err = qualifyProviderStaticRoles(static.Value, key)
		if err != nil {
			return ToolSchemaEntry{}, err
		}
	}
	if entry.value.hasManagedCredential {
		copyValue.managedCredential, err = qualifyProviderManagedRole(entry.value.managedCredential, source, key)
		if err != nil {
			return ToolSchemaEntry{}, err
		}
	}
	if entry.value.hasHTTP {
		copyValue.http, err = qualifyProviderHTTP(entry.value.http, roles)
		if err != nil {
			return ToolSchemaEntry{}, nodeValueError(source, err)
		}
	}
	out := entry
	out.value = &copyValue
	return out, out.validate()
}

func qualifyProviderStaticRoles(source yamlsource.Value, key string) ([]toolCredentialKey, map[string]string, error) {
	rows, err := source.Sequence()
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, len(rows))
	roles := make(map[string]string, len(rows))
	for i, row := range rows {
		local, err := agentValueText(row)
		if err != nil {
			return nil, nil, err
		}
		qualified, err := providerBodyLocalName(row, key, local, "credential role")
		if err != nil {
			return nil, nil, err
		}
		keys[i], roles[local] = qualified, qualified
	}
	admitted, err := admitToolCredentialKeys(keys)
	return admitted, roles, err
}

func qualifyProviderManagedRole(managed ToolManagedCredential, source yamlsource.Value, key string) (ToolManagedCredential, error) {
	field, err := source.Lookup("managed_credential")
	if err != nil {
		return ToolManagedCredential{}, err
	}
	role, err := field.Value.Lookup("key")
	if err != nil {
		return ToolManagedCredential{}, err
	}
	local, err := agentValueText(role.Value)
	if err != nil {
		return ToolManagedCredential{}, err
	}
	qualified, err := providerBodyLocalName(role.Value, key, local, "managed credential role")
	if err != nil {
		return ToolManagedCredential{}, err
	}
	ref := managed.syntax()
	ref.Key = qualified
	return admitManagedCredential(ref)
}

func qualifyProviderHTTP(execution ToolHTTPExecution, roles map[string]string) (ToolHTTPExecution, error) {
	copyValue := *execution.value
	var err error
	if copyValue.url, err = qualifyProviderTemplate(copyValue.url, roles); err != nil {
		return ToolHTTPExecution{}, fmt.Errorf("http.url: %w", err)
	}
	copyValue.headers = make(map[string]toolTemplate, len(execution.value.headers))
	for name, template := range execution.value.headers {
		qualified, err := qualifyProviderTemplate(template, roles)
		if err != nil {
			return ToolHTTPExecution{}, fmt.Errorf("http.headers[%s]: %w", name, err)
		}
		copyValue.headers[name] = qualified
	}
	if copyValue.hasBody {
		copyValue.body, err = qualifyProviderTemplateValue(copyValue.body, roles)
		if err != nil {
			return ToolHTTPExecution{}, fmt.Errorf("http.body: %w", err)
		}
	}
	return ToolHTTPExecution{value: &copyValue}, nil
}

func qualifyProviderTemplate(template toolTemplate, roles map[string]string) (toolTemplate, error) {
	if len(template.parts) == 0 {
		return template, nil
	}
	var out strings.Builder
	for _, part := range template.parts {
		out.WriteString(part.literal)
		if len(part.path.segments) == 0 {
			continue
		}
		if part.path.segments[0].name != "credentials" {
			out.WriteString(template.syntax[part.start:part.end])
			continue
		}
		if len(part.path.segments) != 2 || part.path.segments[1].kind != toolPathProperty {
			return toolTemplate{}, fmt.Errorf("credential template %q must reference one declared short credential role", part.path.syntax)
		}
		qualified, present := roles[part.path.segments[1].name]
		if !present {
			return toolTemplate{}, fmt.Errorf("credential template %q must reference a declared short credential role", part.path.syntax)
		}
		out.WriteString("{{credentials." + qualified + "}}")
	}
	return compileToolTemplate(out.String(), "input", "credentials")
}

func qualifyProviderTemplateValue(value compiledToolTemplateValue, roles map[string]string) (compiledToolTemplateValue, error) {
	if value.template != nil {
		qualified, err := qualifyProviderTemplate(*value.template, roles)
		if err != nil {
			return compiledToolTemplateValue{}, err
		}
		value.template = &qualified
	}
	if value.object != nil {
		object := make(map[string]compiledToolTemplateValue, len(value.object))
		for name, member := range value.object {
			qualified, err := qualifyProviderTemplateValue(member, roles)
			if err != nil {
				return compiledToolTemplateValue{}, err
			}
			object[name] = qualified
		}
		value.object = object
	}
	if value.array != nil {
		array := make([]compiledToolTemplateValue, len(value.array))
		for i, member := range value.array {
			qualified, err := qualifyProviderTemplateValue(member, roles)
			if err != nil {
				return compiledToolTemplateValue{}, err
			}
			array[i] = qualified
		}
		value.array = array
	}
	return value, nil
}
