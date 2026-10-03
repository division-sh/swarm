package contracts

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
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
		if d.Code == "contract_loader.undefined_field" {
			if location := d.Location.String(); location != "" {
				problem = location + ": " + problem
			}
			if d.Location.Line > 0 {
				problem += fmt.Sprintf(" (line %d, column %d)", d.Location.Line, d.Location.Column)
			}
			if len(d.ValidOptions) > 0 {
				problem += " Valid fields: " + strings.Join(d.ValidOptions, ", ") + "."
			}
			if d.Remediation != "" {
				problem += " " + d.Remediation
			}
		}
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

func NewSourceArtifactRequiredDiagnostic() *LoaderDiagnostic {
	return &LoaderDiagnostic{
		Code:        "contract_loader.source_artifact_required",
		Problem:     "an admitted source artifact is required.",
		Remediation: "Pass an explicit source directory to a source-aware command.",
	}
}

func NewUndefinedFieldDiagnostic(context, key string, allowed map[string]struct{}, source ...yamlsource.MappingField) *LoaderDiagnostic {
	context = strings.TrimSpace(context)
	key = strings.TrimSpace(key)
	if context == "" {
		context = "contract"
	}
	options := sortedLoaderFieldOptions(allowed)
	remediation := fmt.Sprintf("Use one of the supported %s fields.", context)
	if nearest := NearestFieldOption(key, options); nearest != "" {
		remediation = fmt.Sprintf("Did you mean %q? %s", nearest, remediation)
	}
	diagnostic := &LoaderDiagnostic{
		Code:         "contract_loader.undefined_field",
		Problem:      fmt.Sprintf("%s field %q is not supported.", context, key),
		Remediation:  remediation,
		ValidOptions: options,
		Location: LoaderDiagnosticLocation{
			YAMLPath: context,
		},
	}
	if len(source) != 0 {
		field := source[0]
		location := field.IntroductionLocation()
		diagnostic.Location = LoaderDiagnosticLocation{File: location.File, YAMLPath: field.Value.SemanticPath(), Line: location.Line, Column: location.Column}
	}
	return diagnostic
}

// NearestFieldOption suggests only a close member of the current vocabulary.
// Sorting makes equal-distance choices independent of map iteration order.
func NearestFieldOption(key string, options []string) string {
	options = append([]string(nil), options...)
	sort.Strings(options)
	best, limit := "", 3
	for _, option := range options {
		a, b := []rune(key), []rune(option)
		if len(a)-len(b) >= limit || len(b)-len(a) >= limit {
			continue
		}
		previous := make([]int, len(b)+1)
		for j := range previous {
			previous[j] = j
		}
		for i, left := range a {
			current := make([]int, len(b)+1)
			current[0] = i + 1
			for j, right := range b {
				cost := 0
				if left != right {
					cost = 1
				}
				current[j+1] = min(current[j]+1, previous[j+1]+1, previous[j]+cost)
			}
			previous = current
		}
		if distance := previous[len(b)]; distance < limit {
			best, limit = option, distance
		}
	}
	return best
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

func NewSchemaDocumentMappingDiagnostic(cause error) *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.schema_mapping",
		"schema.yaml",
		"schema.yaml must be a mapping.",
		"Use a non-empty schema.yaml mapping with fields like name, stages, and pins; entity ownership is flow-local.",
		cause,
	)
}

func NewOutputEventPinOptionsRequiredDiagnostic(cause error) *LoaderDiagnostic {
	return NewExpectedShapeDiagnostic(
		"contract_loader.output_event_pin_options_required",
		"schema.yaml.pins.outputs.events",
		"output event pin mappings require a non-default sink.",
		"Use `events: [item.processed]` unless the validation-only `sink: harness` option is required.",
		cause,
	)
}

func NewOptionalDeclarationFileEmptyDiagnostic(fileName string) *LoaderDiagnostic {
	fileName = strings.TrimSpace(fileName)
	return NewExpectedShapeDiagnostic(
		"contract_loader.optional_declaration_file_empty",
		fileName,
		fmt.Sprintf("%s declares nothing - delete the file (absent means empty).", fileName),
		"Delete the file. Optional workflow declaration files exist only when they contain at least one declaration.",
		nil,
	)
}

func NewOptionalDeclarationFileShapeDiagnostic(fileName string, presence yamlsource.Presence) *LoaderDiagnostic {
	fileName = strings.TrimSpace(fileName)
	return NewExpectedShapeDiagnostic(
		"contract_loader.optional_declaration_file_shape",
		fileName,
		fmt.Sprintf("%s must contain a declaration mapping, found %s.", fileName, presence),
		"Use a mapping keyed by declaration name, or delete the optional file when it has no declarations.",
		nil,
	)
}

func NewDeclarationNameInvalidDiagnostic(context, name string, location yamlsource.Location) *LoaderDiagnostic {
	return &LoaderDiagnostic{
		Code:        "contract_loader.declaration_name_invalid",
		Problem:     fmt.Sprintf("%s name %q at %s must be non-empty and have no surrounding whitespace.", strings.TrimSpace(context), name, location),
		Remediation: "Use one exact non-empty declaration name.",
		Location: LoaderDiagnosticLocation{
			File:     location.File,
			YAMLPath: strings.TrimSpace(context),
			Line:     location.Line,
			Column:   location.Column,
		},
	}
}

func NewDeclarationNameCollisionDiagnostic(context, name string, first, second yamlsource.Location) *LoaderDiagnostic {
	return &LoaderDiagnostic{
		Code:        "contract_loader.declaration_name_collision",
		Problem:     fmt.Sprintf("%s names at %s and %s collide as %q.", strings.TrimSpace(context), first, second, name),
		Remediation: "Use distinct exact declaration names.",
		Location: LoaderDiagnosticLocation{
			File:     second.File,
			YAMLPath: strings.TrimSpace(context),
			Line:     second.Line,
			Column:   second.Column,
		},
	}
}

func wrapLoaderDiagnosticFile(err error, file string) error {
	if err == nil {
		return nil
	}
	if diagnostic, ok := AsLoaderDiagnostic(err); ok {
		return diagnostic.withLocation(LoaderDiagnosticLocation{File: file})
	}
	if diagnostic, ok := diagnoseLoaderShapeError(err); ok {
		return diagnostic.withLocation(LoaderDiagnosticLocation{File: file})
	}
	return fmt.Errorf("parse %s: %w", file, err)
}

func diagnoseLoaderShapeError(err error) (*LoaderDiagnostic, bool) {
	if err == nil {
		return nil, false
	}
	raw := strings.TrimSpace(err.Error())
	if raw == "" {
		return nil, false
	}
	if isYAMLParseError(raw) {
		return NewYAMLParseDiagnostic(err), true
	}
	if strings.Contains(raw, "flow schema document must be a mapping") {
		return NewSchemaDocumentMappingDiagnostic(err), true
	}
	if strings.Contains(raw, "input event pin mapping requires a non-default source, resolution or initialize") {
		return NewExpectedShapeDiagnostic(
			"contract_loader.input_event_pin_options_required",
			"schema.yaml.pins.inputs.events",
			"input event pin mappings require a non-default source, resolution or initialize.",
			"Use `events: [item.received]` unless `source`, retained `resolution` or `initialize` is required.",
			err,
		), true
	}
	if strings.Contains(raw, "output event pin mapping requires a non-default sink") {
		return NewOutputEventPinOptionsRequiredDiagnostic(err), true
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
		strings.Contains(raw, "YAML input has no document") ||
		strings.Contains(raw, "YAML input has multiple documents") ||
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
