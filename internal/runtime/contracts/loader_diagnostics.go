package contracts

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type LoaderDiagnosticLocation struct {
	File     string `json:"file,omitempty"`
	YAMLPath string `json:"yaml_path,omitempty"`
	Line     int    `json:"line,omitempty"`
	Column   int    `json:"column,omitempty"`
}

func (l LoaderDiagnosticLocation) String() string {
	file := strings.TrimSpace(l.File)
	path := strings.TrimSpace(l.YAMLPath)
	switch {
	case file != "" && path != "":
		return file + ":" + path
	case file != "":
		return file
	case path != "":
		return path
	default:
		return ""
	}
}

type LoaderDiagnostic struct {
	Code         string                   `json:"code"`
	Location     LoaderDiagnosticLocation `json:"location,omitempty"`
	Problem      string                   `json:"problem"`
	Remediation  string                   `json:"remediation,omitempty"`
	ValidOptions []string                 `json:"valid_options,omitempty"`
	RawCause     string                   `json:"raw_cause,omitempty"`

	cause error
}

func (d *LoaderDiagnostic) Error() string {
	if d == nil {
		return ""
	}
	if problem := strings.TrimSpace(d.Problem); problem != "" {
		return problem
	}
	return strings.TrimSpace(d.RawCause)
}

func (d *LoaderDiagnostic) Unwrap() error {
	if d == nil {
		return nil
	}
	return d.cause
}

func (d *LoaderDiagnostic) withLocation(location LoaderDiagnosticLocation) *LoaderDiagnostic {
	if d == nil {
		return nil
	}
	out := *d
	if strings.TrimSpace(out.Location.File) == "" {
		out.Location.File = strings.TrimSpace(location.File)
	}
	if strings.TrimSpace(out.Location.YAMLPath) == "" {
		out.Location.YAMLPath = strings.TrimSpace(location.YAMLPath)
	}
	if out.Location.Line == 0 {
		out.Location.Line = location.Line
	}
	if out.Location.Column == 0 {
		out.Location.Column = location.Column
	}
	return &out
}

func AsLoaderDiagnostic(err error) (*LoaderDiagnostic, bool) {
	if err == nil {
		return nil, false
	}
	var diagnostic *LoaderDiagnostic
	if errors.As(err, &diagnostic) && diagnostic != nil {
		return diagnostic, true
	}
	return nil, false
}

func NewContractsPathRequiredDiagnostic() *LoaderDiagnostic {
	return &LoaderDiagnostic{
		Code:        "contract_loader.contracts_path_required",
		Problem:     "a contracts directory is required.",
		Remediation: "Pass a contracts directory with --contracts, or run from a project containing package.yaml.",
	}
}

func NewMissingPackageDiagnostic(path string) *LoaderDiagnostic {
	target := strings.TrimSpace(path)
	if target == "" {
		target = "."
	}
	return &LoaderDiagnostic{
		Code:        "contract_loader.package_manifest_missing",
		Problem:     fmt.Sprintf("no Swarm package manifest was found under %s.", target),
		Remediation: "Create package.yaml and schema.yaml at the contracts root. Minimal package.yaml: `name: <flow-name>`; `version: 0.1.0`.",
		Location: LoaderDiagnosticLocation{
			File: target,
		},
	}
}

func NewUndefinedFieldDiagnostic(context, key string, allowed map[string]struct{}) *LoaderDiagnostic {
	context = strings.TrimSpace(context)
	key = strings.TrimSpace(key)
	if context == "" {
		context = "contract"
	}
	options := sortedLoaderFieldOptions(allowed)
	remediation := fmt.Sprintf("Use one of the supported %s fields.", context)
	if context == "handler" && key == "mailbox_write" {
		remediation = "Use the supported action field, for example `action: {id: mailbox_write, mailbox: {...}}`."
	}
	return &LoaderDiagnostic{
		Code:         "contract_loader.undefined_field",
		Problem:      fmt.Sprintf("%s field %q is not supported.", context, key),
		Remediation:  remediation,
		ValidOptions: options,
		Location: LoaderDiagnosticLocation{
			YAMLPath: context,
		},
	}
}

func NewRetiredConnectDeliveryDiagnostic() *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.retired_connect_delivery",
		"package.yaml.connect.delivery",
		"connect.delivery is retired.",
		"Remove delivery. For delivery: one, run 'swarm migrate-connect-delivery-one --contracts <bundle-root>'. A connect row declares one inter-flow edge; use multiple rows for static fan-out, and declare instance selection or cardinality on the receiver input resolution.",
		nil,
	)
}

func NewRetiredConnectReplyDiagnostic() *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.retired_connect_reply",
		"package.yaml.connect.reply",
		"connect.reply is retired.",
		"Remove reply and declare receiver input resolution mode reply with replies_to; request and response remain separate connect edges.",
		nil,
	)
}

func NewExpectedShapeDiagnostic(code, yamlPath, problem, remediation string, cause error) *LoaderDiagnostic {
	return &LoaderDiagnostic{
		Code:        strings.TrimSpace(code),
		Problem:     strings.TrimSpace(problem),
		Remediation: strings.TrimSpace(remediation),
		RawCause:    rawCauseString(cause),
		cause:       cause,
		Location: LoaderDiagnosticLocation{
			YAMLPath: strings.TrimSpace(yamlPath),
		},
	}
}

func NewYAMLParseDiagnostic(cause error) *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.yaml_parse",
		"",
		"contract YAML could not be parsed.",
		"Fix the YAML syntax, then run the command again.",
		cause,
	)
}

func NewPackageDocumentMappingDiagnostic(cause error) *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.package_manifest_mapping",
		"package.yaml",
		"package.yaml must be a mapping.",
		"Use a package.yaml mapping with fields like name, version, flows, and packages.",
		cause,
	)
}

func NewSchemaDocumentMappingDiagnostic(cause error) *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.schema_mapping",
		"schema.yaml",
		"schema.yaml must be a mapping.",
		"Use a schema.yaml mapping with fields like name, states, pins, and entity.",
		cause,
	)
}

func NewOutputEventPinNameRequiredDiagnostic(cause error) *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.output_event_pin_name_required",
		"schema.yaml.pins.outputs.events",
		"output event pins must name the pin or use a scalar event name.",
		"Use `events: [item.processed]` or `events: [{name: item_processed, event: item.processed}]`.",
		cause,
	)
}

func wrapLoaderDiagnosticFile(err error, file string) error {
	if err == nil {
		return nil
	}
	if diagnostic, ok := AsLoaderDiagnostic(err); ok {
		return diagnostic.withLocation(LoaderDiagnosticLocation{File: file})
	}
	if diagnostic, ok := diagnoseLegacyLoaderError(err); ok {
		return diagnostic.withLocation(LoaderDiagnosticLocation{File: file})
	}
	return fmt.Errorf("parse %s: %w", file, err)
}

func loaderDiagnosticForPackageDecode(err error) error {
	if err == nil {
		return nil
	}
	if diagnostic, ok := AsLoaderDiagnostic(err); ok {
		return diagnostic
	}
	raw := err.Error()
	if strings.Contains(raw, "ProjectFlowRef") {
		return NewExpectedShapeDiagnostic(
			"contract_loader.package_flows_shape",
			"package.yaml.flows",
			"package.yaml flows entries must be mappings with id, flow, and optional mode.",
			"Use entries like `flows: [{id: child, flow: child, mode: child}]`.",
			err,
		)
	}
	if strings.Contains(raw, "ProjectPackageRef") {
		return NewExpectedShapeDiagnostic(
			"contract_loader.package_imports_shape",
			"package.yaml.packages",
			"package.yaml package entries must be mappings with id and path or package.",
			"Use entries like `packages: [{id: shared, path: flows/shared}]`.",
			err,
		)
	}
	return err
}

func diagnoseLegacyLoaderError(err error) (*LoaderDiagnostic, bool) {
	if err == nil {
		return nil, false
	}
	raw := strings.TrimSpace(err.Error())
	if raw == "" {
		return nil, false
	}
	if diagnostic, ok := diagnoseLegacyUndefinedField(err, raw); ok {
		return diagnostic, true
	}
	if isYAMLParseError(raw) {
		return NewYAMLParseDiagnostic(err), true
	}
	if strings.Contains(raw, "package.yaml must be a mapping") {
		return NewPackageDocumentMappingDiagnostic(err), true
	}
	if strings.Contains(raw, "flow schema document must be a mapping") {
		return NewSchemaDocumentMappingDiagnostic(err), true
	}
	if strings.Contains(raw, "input event pin name is required") {
		return NewExpectedShapeDiagnostic(
			"contract_loader.input_event_pin_name_required",
			"schema.yaml.pins.inputs.events",
			"input event pins must name the pin or use a scalar event name.",
			"Use `events: [item.received]` or `events: [{name: item_received, event: item.received, source: external}]`.",
			err,
		), true
	}
	if strings.Contains(raw, "output event pin name is required") {
		return NewOutputEventPinNameRequiredDiagnostic(err), true
	}
	if strings.Contains(raw, "ProjectFlowRef") {
		return NewExpectedShapeDiagnostic(
			"contract_loader.package_flows_shape",
			"package.yaml.flows",
			"package.yaml flows entries must be mappings with id, flow, and optional mode.",
			"Use entries like `flows: [{id: child, flow: child, mode: child}]`.",
			err,
		), true
	}
	if isKnownContractLoaderShapeError(raw) {
		return NewExpectedShapeDiagnostic(
			"contract_loader.yaml_shape",
			"",
			"contract YAML has a value with the wrong shape.",
			"Use the authored YAML shape described by the contract field and run the command again.",
			err,
		), true
	}
	return nil, false
}

func isYAMLParseError(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	return strings.HasPrefix(raw, "yaml: line ") ||
		strings.Contains(raw, ": did not find expected ") ||
		strings.Contains(raw, ": could not find expected ") ||
		strings.Contains(raw, ": found unexpected ") ||
		strings.Contains(raw, ": mapping values are not allowed ")
}

func isKnownContractLoaderShapeError(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	return strings.Contains(raw, "yaml: unmarshal errors:") ||
		strings.Contains(raw, "cannot unmarshal !!") ||
		strings.Contains(raw, "into contracts.")
}

var legacyUndefinedFieldPattern = regexp.MustCompile(`UNDEFINED-FIELD:\s+(.+?)\s+(?:field|option)\s+"([^"]+)"\s+not in platform spec`)

func diagnoseLegacyUndefinedField(err error, raw string) (*LoaderDiagnostic, bool) {
	match := legacyUndefinedFieldPattern.FindStringSubmatch(raw)
	if len(match) != 3 {
		return nil, false
	}
	context := strings.TrimSpace(match[1])
	key := strings.TrimSpace(match[2])
	diagnostic := NewUndefinedFieldDiagnostic(context, key, loaderFieldOptionsForContext(context))
	diagnostic.RawCause = rawCauseString(err)
	diagnostic.cause = err
	return diagnostic, true
}

func loaderFieldOptionsForContext(context string) map[string]struct{} {
	switch strings.TrimSpace(context) {
	case "package.yaml":
		return projectPackageDocumentFields
	case "schema":
		return flowSchemaDocumentFields
	case "stage":
		return stageDeclarationFieldOptions
	case "node":
		return systemNodeContractFields
	case "handler":
		return handlerFieldOptions
	case "action":
		return actionFieldOptions
	case "input event pin":
		return inputEventPinFieldOptions
	case "output event pin":
		return outputEventPinFieldOptions
	case "input event pin carry":
		return inputEventPinCarryFieldOptions
	case "input event pin resolution":
		return inputEventPinResolutionFieldOptions
	case "input event pin resolution.instance_key":
		return inputEventPinResolutionInstanceKeyFieldOptions
	case "input event pin address":
		return inputEventPinAddressFieldOptions
	case "rule":
		return ruleFieldOptions
	case "template instance":
		return templateInstanceFieldOptions
	case "compute":
		return computeFieldOptions
	case "guard.on_fail":
		return guardOnFailFieldOptions
	case "guard.on_fail.escalate":
		return guardOnFailEscalateFieldOptions
	case "accumulate":
		return accumulateFieldOptions
	case "fan_out":
		return fanOutFieldOptions
	case "emit":
		return emitFieldOptions
	case "on_success":
		return onSuccessFieldOptions
	case "activity":
		return activityFieldOptions
	case "activity.approval":
		return activityApprovalFieldOptions
	case "emit.target":
		return emitTargetFieldOptions
	case "mailbox":
		return mailboxFieldOptions
	case "artifact_repo":
		return artifactRepoFieldOptions
	case "artifact_repo.files":
		return artifactRepoFilesFieldOptions
	case "artifact_repo.files.schema":
		return artifactRepoFilesSchemaFieldOptions
	case "artifact_repo.output":
		return artifactRepoOutputFieldOptions
	case "artifact_repo.limits":
		return artifactRepoLimitsFieldOptions
	case "select_entity", "select_or_create_entity":
		return entitySelectionFieldOptions
	case "agent":
		return agentRegistryEntryFieldOptions
	case "requires":
		return flowPackageRequiresFieldOptions
	case "bind":
		return flowPackageBindFieldOptions
	case "connector_packs":
		return connectorPackFieldOptions
	case "connector_packs.imports":
		return connectorPackImportFieldOptions
	case "requires.policy":
		return flowPackageRequiresPolicyFieldOptions
	case "connect":
		return flowPackageConnectFieldOptions
	case "connect using":
		return flowPackageConnectUsingFieldOptions
	case "connect using instance":
		return flowPackageConnectInstanceFieldOptions
	case "connect map":
		return flowPackageConnectMapFieldOptions
	case "type catalog":
		return typeCatalogFieldOptions
	case "type metadata":
		return typeMetadataFieldOptions
	case "entity metadata":
		return entityMetadataFieldOptions
	case "length":
		return schemaLengthRefinementFieldOptions
	case "range":
		return schemaRangeRefinementFieldOptions
	default:
		return nil
	}
}

func sortedLoaderFieldOptions(fields map[string]struct{}) []string {
	if len(fields) == 0 {
		return nil
	}
	out := make([]string, 0, len(fields))
	for key := range fields {
		key = strings.TrimSpace(key)
		if key != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func rawCauseString(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}
