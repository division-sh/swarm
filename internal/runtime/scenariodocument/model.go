package scenariodocument

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// Document retains admitted values and lexical provenance, never caller-owned maps.
type Document struct {
	value  semanticvalue.Value
	source yamlsource.Value
}

// CLI is a defensive execution projection, not another admission authority.
type CLI struct {
	Name    string
	Seed    string
	Vars    map[string]any
	Setup   Setup
	Steps   []Step
	Expect  Expect
	Invalid *Invalid
	Derive  *Derive
}

type Setup struct{ Entities []SetupEntity }
type SetupEntity struct {
	Alias        string
	EntityType   string
	Flow         any
	CurrentState any
	StateSet     bool
	Fields       any
	FieldsSet    bool
	Gates        any
	GatesSet     bool
}

type Step struct {
	Action             string
	PublishEvent       string
	Payload            any
	Match              map[string]any
	Verdict            any
	Fields             any
	Until              any
	IdempotencyKey     any
	Emitter            any
	SourceEventID      any
	Target             any
	TargetFlowInstance any
	TargetEntityID     any
	GeneratePayload    bool
}

type Expect struct {
	Events        EventExpect
	NoDeadLetters *bool
	Entities      []EntityExpect
}
type EventExpect struct {
	Include []string
	Exact   []string
	Ordered []string
}
type EntityExpect struct {
	Ref          string
	EntityType   string
	Count        *int
	CurrentState any
	StateSet     bool
	Fields       any
	FieldsSet    bool
	Gates        any
	GatesSet     bool
}
type Invalid struct {
	Base  map[string]any
	Cases []InvalidCase
}
type InvalidCase struct {
	Name string
	Set  map[string]any
}
type Derive struct {
	Name               string
	FlowID             string
	Input              string
	Set                map[string]any
	ConnectorResponses map[string]any
}

func (d Document) SourceValue() yamlsource.Value { return d.source }
func (d Document) Projection() (CLI, error)      { return projectDocument(d.value) }

func (e Expect) Empty() bool {
	return len(e.Events.Include) == 0 && e.Events.Exact == nil && len(e.Events.Ordered) == 0 && e.NoDeadLetters == nil && len(e.Entities) == 0
}
func (e EntityExpect) HasDetailAssertion() bool { return e.StateSet || e.FieldsSet || e.GatesSet }

func expression(value semanticvalue.Value) bool {
	text, ok := value.String()
	return ok && IsExpression(text)
}

func IsExpression(text string) bool {
	return len(text) >= 3 && strings.HasPrefix(text, "${") && strings.HasSuffix(text, "}")
}

func mapping(value semanticvalue.Value, label string, allowed ...string) (map[string]semanticvalue.Value, error) {
	fields, ok := value.ObjectMap()
	if !ok {
		return nil, fmt.Errorf("%s must be a mapping", label)
	}
	for _, member := range value.Members() {
		if !contains(allowed, member.Name) {
			return nil, fmt.Errorf("unsupported %s field %q", label, member.Name)
		}
	}
	return fields, nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func text(value semanticvalue.Value, label string, nonempty bool) (string, error) {
	result, ok := value.String()
	if !ok {
		return "", fmt.Errorf("%s must be text", label)
	}
	result = strings.TrimSpace(result)
	if nonempty && result == "" {
		return "", fmt.Errorf("%s must be non-empty", label)
	}
	return result, nil
}

func requiredText(fields map[string]semanticvalue.Value, key, label string) (string, error) {
	value, ok := fields[key]
	if !ok {
		return "", fmt.Errorf("%s is required", label)
	}
	return text(value, label, true)
}

func optionalText(fields map[string]semanticvalue.Value, key, label string, nonempty bool) (string, error) {
	value, ok := fields[key]
	if !ok {
		return "", nil
	}
	return text(value, label, nonempty)
}

func literal(value semanticvalue.Value) any {
	projected, err := workflowexpr.ProjectSemanticValue(value)
	if err != nil {
		// The retained value and every number already passed semantic admission.
		panic(err)
	}
	return projected
}

func object(value semanticvalue.Value, label string) (map[string]any, error) {
	if value.Kind() != semanticvalue.KindObject {
		return nil, fmt.Errorf("%s must be a mapping", label)
	}
	return literal(value).(map[string]any), nil
}

func objectOrExpression(value semanticvalue.Value, label string) (any, error) {
	if expression(value) {
		return literal(value), nil
	}
	return object(value, label)
}

func sequence(value semanticvalue.Value, label string, nonempty bool) ([]semanticvalue.Value, error) {
	if value.Kind() != semanticvalue.KindArray || nonempty && value.Len() == 0 {
		return nil, fmt.Errorf("%s must be a %slist", label, map[bool]string{true: "non-empty ", false: ""}[nonempty])
	}
	items := make([]semanticvalue.Value, value.Len())
	for i := range items {
		items[i], _ = value.At(i)
	}
	return items, nil
}
