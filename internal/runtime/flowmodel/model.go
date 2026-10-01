package flowmodel

type PolicyDocument struct {
	Values map[string]PolicyValue `yaml:",inline"`
}

type PolicyValue struct {
	Value any
}

func (v PolicyValue) MarshalYAML() (any, error)         { return v.Value, nil }
func (p PolicyCriteriaParam) MarshalYAML() (any, error) { return p.Value, nil }

type PolicyCriteriaSet struct {
	Classes map[string]PolicyCriteriaClass `yaml:"classes"`
	Rules   []PolicyCriteriaRule           `yaml:"rules"`
}

type PolicyCriteriaClass struct {
	Disposition string `yaml:"disposition"`
}

type PolicyCriteriaRule struct {
	ID     string                         `yaml:"id"`
	Class  string                         `yaml:"class"`
	Text   string                         `yaml:"text"`
	Params map[string]PolicyCriteriaParam `yaml:"params"`
}

type PolicyCriteriaParam struct {
	Value any
}

type PolicyValidationSet struct {
	Classes map[string]PolicyValidationClass `yaml:"classes"`
	Inputs  map[string]string                `yaml:"inputs"`
	Rules   []PolicyValidationRule           `yaml:"rules"`
}

type PolicyValidationClass struct {
	Disposition string `yaml:"disposition"`
}

type PolicyValidationRule struct {
	ID           string                         `yaml:"id"`
	Class        string                         `yaml:"class"`
	Text         string                         `yaml:"text"`
	Params       map[string]PolicyCriteriaParam `yaml:"params"`
	PinCandidate *bool                          `yaml:"pin_candidate"`
	Check        PolicyValidationCheck          `yaml:"check"`
}

type PolicyValidationCheck struct {
	Equal *PolicyValidationEqualCheck `yaml:"equal"`
}

type PolicyValidationEqualCheck struct {
	Left  string `yaml:"left"`
	Right string `yaml:"right"`
}

type Tree[T any] struct {
	Root   *T
	ByPath map[string]*T
	ByID   map[string]*T
}

type URIRegistry struct {
	Scheme string
	Nodes  map[string]URIRef
	Agents map[string]URIRef
	Events map[string]URIRef
	ByURI  map[string]URIRef
}

type URIRef struct {
	Kind     string
	FlowID   string
	LocalID  string
	Path     string
	Absolute string
	Full     string
}
