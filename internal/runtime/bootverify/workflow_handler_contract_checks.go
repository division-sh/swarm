package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkHandlerFieldCompliance(c *checkerContext) []Finding { return c.handlerFieldCompliance() }

func (c *checkerContext) handlerFieldCompliance() []Finding {
	if c.handlerLoaded {
		return c.handlerFindings
	}
	c.handlerLoaded = true
	nodes := c.source.NodeEntries()
	for _, transition := range c.source.WorkflowTransitions() {
		id := strings.TrimSpace(transition.ID)
		if id == "" {
			continue
		}
		for _, actionID := range transition.Actions {
			actionID = strings.TrimSpace(actionID)
			if actionID == "" {
				continue
			}
			action, ok := c.source.ActionInstructionByID(actionID)
			if !ok {
				continue
			}
			if action.Executable() || isSupportedWorkflowHandlerActionID(firstNonEmptyString(action.Builtin, action.Key.String())) {
				continue
			}
			c.handlerFindings = append(c.handlerFindings, Finding{
				CheckID:  "handler_field_compliance",
				Severity: "error",
				Message:  fmt.Sprintf("transition %s action %s has no executable runtime implementation", id, actionID),
				Location: id,
			})
		}
		for _, guardID := range transition.Guards {
			guardID = strings.TrimSpace(guardID)
			if guardID == "" {
				continue
			}
			guard, ok := c.source.GuardInstructionByID(guardID)
			if !ok || guard.Executable() {
				continue
			}
			c.handlerFindings = append(c.handlerFindings, Finding{
				CheckID:  "handler_field_compliance",
				Severity: "error",
				Message:  fmt.Sprintf("transition %s guard %s has no executable runtime implementation", id, guardID),
				Location: id,
			})
		}
	}
	for nodeID, node := range nodes {
		nodeID = strings.TrimSpace(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if actionID := strings.TrimSpace(handler.Action.ID); actionID != "" {
				if normalizeWorkflowBuiltinActionID(actionID) == "create_flow_instance" {
					templateID := strings.TrimSpace(handler.Action.Template)
					if templateID == "" {
						c.handlerFindings = append(c.handlerFindings, Finding{
							CheckID:  "handler_field_compliance",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s create_flow_instance is missing template", nodeID, eventType),
							Location: nodeID,
						})
					} else if !flowSchemaIsTemplate(c.source, templateID) {
						c.handlerFindings = append(c.handlerFindings, Finding{
							CheckID:  "handler_field_compliance",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s create_flow_instance template %s is not mode: template", nodeID, eventType, templateID),
							Location: nodeID,
						})
					}
					if strings.TrimSpace(handler.Action.InstanceIDFrom) == "" {
						c.handlerFindings = append(c.handlerFindings, Finding{
							CheckID:  "handler_field_compliance",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s create_flow_instance is missing instance_id_from", nodeID, eventType),
							Location: nodeID,
						})
					}
					if handler.Action.ConfigFrom == nil || len(handler.Action.ConfigFrom.ConfigEntries()) == 0 {
						c.handlerFindings = append(c.handlerFindings, Finding{
							CheckID:  "handler_field_compliance",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s create_flow_instance is missing config_from", nodeID, eventType),
							Location: nodeID,
						})
					}
				}
				if normalizeWorkflowBuiltinActionID(actionID) == "record_evidence" && strings.TrimSpace(handler.EvidenceTarget) == "" {
					c.handlerFindings = append(c.handlerFindings, Finding{
						CheckID:  "handler_field_compliance",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s record_evidence is missing evidence_target", nodeID, eventType),
						Location: nodeID,
					})
				}
				if normalizeWorkflowBuiltinActionID(actionID) == "mailbox_write" {
					if handler.Action.Mailbox == nil {
						c.handlerFindings = append(c.handlerFindings, Finding{
							CheckID:  "handler_field_compliance",
							Severity: "error",
							Message:  fmt.Sprintf("node %s handler %s mailbox_write is missing mailbox", nodeID, eventType),
							Location: nodeID,
						})
					} else {
						if handler.Action.Mailbox.ItemType.IsZero() {
							c.handlerFindings = append(c.handlerFindings, Finding{
								CheckID:  "handler_field_compliance",
								Severity: "error",
								Message:  fmt.Sprintf("node %s handler %s mailbox_write is missing mailbox.item_type", nodeID, eventType),
								Location: nodeID,
							})
						}
						if handler.Action.Mailbox.Summary.IsZero() {
							c.handlerFindings = append(c.handlerFindings, Finding{
								CheckID:  "handler_field_compliance",
								Severity: "error",
								Message:  fmt.Sprintf("node %s handler %s mailbox_write is missing mailbox.summary", nodeID, eventType),
								Location: nodeID,
							})
						}
					}
				} else if handler.Action.Mailbox != nil {
					c.handlerFindings = append(c.handlerFindings, Finding{
						CheckID:  "handler_field_compliance",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s mailbox declaration requires action mailbox_write", nodeID, eventType),
						Location: nodeID,
					})
				}
				if normalizeWorkflowBuiltinActionID(actionID) == "artifact_repo_commit" {
					c.handlerFindings = append(c.handlerFindings, validateArtifactRepoActionSpec(c.source, nodeFlowID(c.source, nodeID), nodeID, eventType, handler.Action)...)
				} else if handler.Action.ArtifactRepo != nil {
					c.handlerFindings = append(c.handlerFindings, Finding{
						CheckID:  "handler_field_compliance",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s artifact_repo declaration requires action artifact_repo_commit", nodeID, eventType),
						Location: nodeID,
					})
				}
				if !handlerActionExecutable(c.source, actionID) {
					c.handlerFindings = append(c.handlerFindings, Finding{
						CheckID:  "handler_field_compliance",
						Severity: "error",
						Message:  fmt.Sprintf("node %s handler %s action %s is not executable", nodeID, eventType, actionID),
						Location: nodeID,
					})
				}
			}
		}
	}
	c.handlerFindings = append(c.handlerFindings, runtimeHandledEventsMissingExecutors(c.source)...)
	return c.handlerFindings
}

func supportedWorkflowRuntimeExecutorIDs(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	for nodeID, entry := range source.NodeEntries() {
		if strings.TrimSpace(nodeID) == "" {
			continue
		}
		if len(source.NodeEventHandlers(nodeID)) > 0 || len(entry.EventHandlers) > 0 {
			out[nodeID] = struct{}{}
		}
	}
	return out
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizeWorkflowBuiltinActionID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func isSupportedWorkflowActionBuiltin(id string) bool {
	return runtimecontracts.IsSupportedHandlerActionID(normalizeWorkflowBuiltinActionID(id))
}

func isSupportedWorkflowHandlerActionID(id string) bool {
	return runtimecontracts.IsSupportedHandlerActionID(normalizeWorkflowBuiltinActionID(id))
}

func handlerActionExecutable(source semanticview.Source, actionID string) bool {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return true
	}
	if isSupportedWorkflowHandlerActionID(actionID) {
		return true
	}
	entry, ok := source.ActionInstructionByID(actionID)
	return ok && entry.Executable()
}

func validateArtifactRepoActionSpec(source semanticview.Source, flowID, nodeID, eventType string, action runtimecontracts.ActionSpec) []Finding {
	spec := action.ArtifactRepo
	if spec == nil {
		return []Finding{artifactRepoFinding(nodeID, eventType, "artifact_repo_commit is missing artifact_repo")}
	}
	findings := []Finding{}
	provider := strings.TrimSpace(spec.Provider)
	if provider == "" {
		findings = append(findings, artifactRepoFinding(nodeID, eventType, "artifact_repo_commit is missing artifact_repo.provider"))
	} else if provider != "local_git" {
		findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo_commit provider %s is unsupported", provider)))
	}
	for label, expr := range map[string]runtimecontracts.ExpressionValue{
		"repo_id":    spec.RepoID,
		"request_id": spec.RequestID,
	} {
		if expr.IsZero() {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo_commit is missing artifact_repo.%s", label)))
		}
	}
	for label, expr := range map[string]runtimecontracts.ExpressionValue{
		"namespace":     spec.Namespace,
		"partition_key": spec.PartitionKey,
	} {
		if value, ok := artifactRepoLiteralString(expr); ok {
			if err := validateArtifactRepoSegment(value); err != nil {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s %q is invalid: %v", label, value, err)))
			}
		}
	}
	if value, ok := artifactRepoLiteralString(spec.DisplaySlug); ok {
		if err := validateArtifactRepoDisplaySlug(value); err != nil {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.display_slug %q is invalid: %v", value, err)))
		}
	}
	for key, expr := range spec.Provenance {
		if err := validateArtifactRepoProvenanceKey(key); err != nil {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.provenance key %q is invalid: %v", key, err)))
		}
		if expr.IsZero() {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.provenance.%s is missing value", strings.TrimSpace(key))))
		}
	}
	if len(spec.AllowedPaths) == 0 {
		findings = append(findings, artifactRepoFinding(nodeID, eventType, "artifact_repo_commit requires at least one artifact_repo.allowed_paths entry"))
	}
	seenAllowed := map[string]struct{}{}
	for _, raw := range spec.AllowedPaths {
		cleaned, err := validateArtifactRepoDeclaredPath(raw)
		if err != nil {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.allowed_paths %q is invalid: %v", raw, err)))
			continue
		}
		if _, ok := seenAllowed[cleaned]; ok {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.allowed_paths contains duplicate canonical path %s", cleaned)))
		}
		seenAllowed[cleaned] = struct{}{}
	}
	if len(spec.Files) == 0 {
		findings = append(findings, artifactRepoFinding(nodeID, eventType, "artifact_repo_commit requires at least one artifact_repo.files entry"))
	}
	seenFiles := map[string]struct{}{}
	for i, file := range spec.Files {
		if file.Path.IsZero() {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d] is missing path", i)))
		} else if pathValue, ok := artifactRepoLiteralString(file.Path); ok {
			cleaned, err := validateArtifactRepoDeclaredPath(pathValue)
			if err != nil {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].path %q is invalid: %v", i, pathValue, err)))
			} else {
				if _, allowed := seenAllowed[cleaned]; len(seenAllowed) > 0 && !allowed {
					findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].path %s is not in artifact_repo.allowed_paths", i, cleaned)))
				}
				if _, exists := seenFiles[cleaned]; exists {
					findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files contains duplicate canonical path %s", cleaned)))
				}
				seenFiles[cleaned] = struct{}{}
			}
		}
		if file.Content.IsZero() {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d] is missing content", i)))
		}
		switch contentType := strings.TrimSpace(file.ContentType); contentType {
		case "yaml", "markdown", "text":
			schemaType := strings.TrimSpace(file.Schema.Type)
			if contentType == "yaml" {
				if schemaType == "" {
					findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].schema.type is required for yaml content", i)))
				} else if schemaType != "object" {
					findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].schema.type %q is unsupported", i, schemaType)))
				}
			} else if schemaType != "" || len(file.Schema.RequiredFields) > 0 {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].schema is only supported for yaml content", i)))
			}
		default:
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].content_type %q is unsupported", i, contentType)))
		}
		seenRequired := map[string]struct{}{}
		for _, field := range file.Schema.RequiredFields {
			field = strings.TrimSpace(field)
			if field == "" {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].schema.required_fields contains an empty field", i)))
				continue
			}
			if _, exists := seenRequired[field]; exists {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].schema.required_fields contains duplicate field %s", i, field)))
			}
			seenRequired[field] = struct{}{}
		}
		if file.MaxBytes < 0 {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.files[%d].max_bytes must be non-negative", i)))
		}
	}
	for _, field := range []struct {
		label string
		value string
	}{
		{"output.repo_url", spec.Output.RepoURL},
		{"output.current_ref", spec.Output.CurrentRef},
		{"output.file_manifest", spec.Output.FileManifest},
		{"output.status", spec.Output.Status},
		{"output.failure_reason", spec.Output.FailureReason},
		{"output.last_request_id", spec.Output.LastRequestID},
		{"output.last_source_event_id", spec.Output.LastSourceEventID},
	} {
		if strings.TrimSpace(field.value) == "" {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo_commit is missing artifact_repo.%s", field.label)))
		}
	}
	if spec.Limits.MaxYAMLBytes < 0 || spec.Limits.MaxMarkdownBytes < 0 || spec.Limits.MaxTextBytes < 0 || spec.Limits.MaxRepoBytes < 0 {
		findings = append(findings, artifactRepoFinding(nodeID, eventType, "artifact_repo.limits values must be non-negative"))
	}
	findings = append(findings, validateArtifactRepoResultEventSpec(source, flowID, nodeID, eventType, "success", spec.SuccessEvent, spec.SuccessPayload, spec)...)
	findings = append(findings, validateArtifactRepoResultEventSpec(source, flowID, nodeID, eventType, "failure", spec.FailureEvent, spec.FailurePayload, spec)...)
	return findings
}

func validateArtifactRepoResultEventSpec(source semanticview.Source, flowID, nodeID, eventType, label, resultEvent string, payload map[string]runtimecontracts.ExpressionValue, spec *runtimecontracts.ArtifactRepoSpec) []Finding {
	findings := []Finding{}
	resultEvent = strings.TrimSpace(resultEvent)
	if resultEvent == "" && len(payload) > 0 {
		findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_payload requires artifact_repo.%s_event", label, label)))
	}
	var (
		entry       runtimecontracts.EventCatalogEntry
		entryFound  bool
		covered     = artifactRepoResultCoveredPayloadFields(label, payload, spec)
		runtimeKeys = artifactRepoResultRuntimePayloadFields(label, spec)
	)
	if resultEvent != "" {
		if strings.Contains(resultEvent, "*") {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_event must be a concrete event, got %s", label, resultEvent)))
		}
		entry, entryFound = artifactRepoResultEventEntry(source, flowID, resultEvent)
		if !entryFound {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_event %s does not resolve to a declared event", label, resultEvent)))
		}
	}
	for target, expr := range payload {
		target = strings.TrimSpace(target)
		if target == "" {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_payload contains an empty target field", label)))
			continue
		}
		if artifactRepoResultPayloadFieldReserved(target) {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_payload must not override runtime-owned field %s", label, target)))
		}
		if expr.IsZero() {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_payload.%s is missing value", label, target)))
		}
	}
	if resultEvent != "" && entryFound {
		if len(entry.Payload.Properties) == 0 {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_event %s must declare a payload schema for artifact_repo result fields", label, resultEvent)))
		}
		for field := range runtimeKeys {
			if _, ok := entry.Payload.Properties[field]; !ok {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_event %s schema is missing runtime-owned field %s", label, resultEvent, field)))
			}
		}
		findings = append(findings, validateArtifactRepoResultRuntimePayloadFieldTypes(source, flowID, nodeID, eventType, label, resultEvent, runtimeKeys)...)
		for target := range payload {
			target = strings.TrimSpace(target)
			if target == "" {
				continue
			}
			if _, ok := entry.Payload.Properties[target]; !ok {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_payload.%s is not declared by %s payload schema", label, target, resultEvent)))
			}
		}
		for _, field := range entry.Required {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if _, ok := covered[field]; !ok {
				findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_event %s requires payload field %s not provided by runtime-owned fields or artifact_repo.%s_payload", label, resultEvent, field, label)))
			}
		}
	}
	return findings
}

func validateArtifactRepoResultRuntimePayloadFieldTypes(source semanticview.Source, flowID, nodeID, eventType, label, resultEvent string, runtimeKeys map[string]struct{}) []Finding {
	resolution := semanticview.ResolveEventSchema(source, flowID, resultEvent)
	if !resolution.HasSchema {
		return nil
	}
	if err := resolution.UnresolvedTypeError(); err != nil {
		return []Finding{artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_event %s payload schema is not fully resolvable: %v", label, resultEvent, err))}
	}
	properties := artifactRepoJSONSchemaProperties(resolution.Schema.Schema)
	if len(properties) == 0 {
		return nil
	}
	findings := []Finding{}
	for field := range runtimeKeys {
		expected := artifactRepoResultRuntimePayloadFieldKind(field)
		if expected == "" {
			continue
		}
		prop, ok := properties[field]
		if !ok {
			continue
		}
		if !artifactRepoJSONSchemaAllowsKind(prop, expected) {
			findings = append(findings, artifactRepoFinding(nodeID, eventType, fmt.Sprintf("artifact_repo.%s_event %s runtime-owned field %s must be %s-compatible, got %s", label, resultEvent, field, expected, artifactRepoJSONSchemaKindSummary(prop))))
		}
	}
	return findings
}

func artifactRepoResultRuntimePayloadFieldKind(field string) string {
	switch strings.TrimSpace(field) {
	case "file_manifest", "provenance":
		return "object"
	case "repo_id", "namespace", "partition_key", "display_slug", "request_id", "source_event_id", "repo_url", "current_ref", "failure_reason":
		return "string"
	default:
		return ""
	}
}

func artifactRepoJSONSchemaProperties(schema map[string]any) map[string]any {
	properties, _ := schema["properties"].(map[string]any)
	return properties
}

func artifactRepoJSONSchemaAllowsKind(raw any, expected string) bool {
	schema, ok := raw.(map[string]any)
	if !ok {
		return false
	}
	for _, typ := range artifactRepoJSONSchemaTypes(schema["type"]) {
		if typ == expected {
			return true
		}
	}
	if expected == "object" {
		if _, ok := schema["properties"].(map[string]any); ok {
			return true
		}
	}
	return false
}

func artifactRepoJSONSchemaKindSummary(raw any) string {
	schema, ok := raw.(map[string]any)
	if !ok {
		return fmt.Sprintf("%T", raw)
	}
	types := artifactRepoJSONSchemaTypes(schema["type"])
	if len(types) > 0 {
		return strings.Join(types, "|")
	}
	if _, ok := schema["properties"].(map[string]any); ok {
		return "object"
	}
	return "unknown"
}

func artifactRepoJSONSchemaTypes(raw any) []string {
	switch typed := raw.(type) {
	case string:
		typ := strings.TrimSpace(typed)
		if typ == "" {
			return nil
		}
		return []string{typ}
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			typ := strings.TrimSpace(fmt.Sprint(item))
			if typ != "" {
				out = append(out, typ)
			}
		}
		return out
	default:
		return nil
	}
}

func artifactRepoResultEventEntry(source semanticview.Source, flowID, resultEvent string) (runtimecontracts.EventCatalogEntry, bool) {
	if source == nil {
		return runtimecontracts.EventCatalogEntry{}, false
	}
	resultEvent = strings.TrimSpace(resultEvent)
	if resultEvent == "" {
		return runtimecontracts.EventCatalogEntry{}, false
	}
	if entry, _, ok := source.ResolveFlowEventCatalogEntry(flowID, resultEvent); ok {
		return entry, true
	}
	if entry, ok := source.EventEntry(resultEvent); ok {
		return entry, true
	}
	if entry, ok := source.ResolvedEventCatalog()[resultEvent]; ok {
		return entry, true
	}
	return runtimecontracts.EventCatalogEntry{}, false
}

func artifactRepoResultCoveredPayloadFields(label string, payload map[string]runtimecontracts.ExpressionValue, spec *runtimecontracts.ArtifactRepoSpec) map[string]struct{} {
	covered := artifactRepoResultRuntimePayloadFields(label, spec)
	for target := range payload {
		target = strings.TrimSpace(target)
		if target != "" {
			covered[target] = struct{}{}
		}
	}
	return covered
}

func artifactRepoResultRuntimePayloadFields(label string, spec *runtimecontracts.ArtifactRepoSpec) map[string]struct{} {
	fields := map[string]struct{}{
		"repo_id":         {},
		"namespace":       {},
		"request_id":      {},
		"source_event_id": {},
		"provenance":      {},
	}
	if spec != nil && !spec.PartitionKey.IsZero() {
		fields["partition_key"] = struct{}{}
	}
	if spec != nil && !spec.DisplaySlug.IsZero() {
		fields["display_slug"] = struct{}{}
	}
	switch strings.TrimSpace(label) {
	case "success":
		fields["repo_url"] = struct{}{}
		fields["current_ref"] = struct{}{}
		fields["file_manifest"] = struct{}{}
	case "failure":
		fields["failure_reason"] = struct{}{}
	}
	return fields
}

func artifactRepoResultPayloadFieldReserved(field string) bool {
	switch strings.TrimSpace(field) {
	case "repo_id", "namespace", "partition_key", "display_slug", "request_id", "source_event_id", "repo_url", "current_ref", "file_manifest", "failure_reason", "provenance":
		return true
	default:
		return false
	}
}

func artifactRepoFinding(nodeID, eventType, message string) Finding {
	return Finding{
		CheckID:  "handler_field_compliance",
		Severity: "error",
		Message:  fmt.Sprintf("node %s handler %s %s", nodeID, eventType, message),
		Location: nodeID,
	}
}

func artifactRepoLiteralString(expr runtimecontracts.ExpressionValue) (string, bool) {
	if expr.Kind != runtimecontracts.ExpressionKindLiteral {
		return "", false
	}
	value := strings.TrimSpace(fmt.Sprint(expr.Literal))
	return value, value != ""
}

func validateArtifactRepoSegment(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fmt.Errorf("value is required")
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("only letters, digits, dash, underscore, and dot are allowed")
	}
	if value == "." || value == ".." || strings.Contains(value, "..") {
		return fmt.Errorf("path traversal markers are not allowed")
	}
	return nil
}

func validateArtifactRepoProvenanceKey(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fmt.Errorf("key is required")
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("only letters, digits, dash, underscore, and dot are allowed")
	}
	return nil
}

func validateArtifactRepoDisplaySlug(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	if strings.Contains(value, "\x00") || strings.ContainsAny(value, `/\`) {
		return fmt.Errorf("path separators are not allowed")
	}
	if value == "." || value == ".." || strings.Contains(value, "..") {
		return fmt.Errorf("path traversal markers are not allowed")
	}
	hasAlphaNumeric := false
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			hasAlphaNumeric = true
			break
		}
	}
	if !hasAlphaNumeric {
		return fmt.Errorf("must contain at least one letter or digit")
	}
	return nil
}

func validateArtifactRepoDeclaredPath(raw string) (string, error) {
	value := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if value == "" {
		return "", fmt.Errorf("path is required")
	}
	if strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	parts := []string{}
	for _, part := range strings.Split(value, "/") {
		part = strings.TrimSpace(part)
		switch part {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("path traversal is not allowed")
		default:
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("path is required")
	}
	return strings.Join(parts, "/"), nil
}
