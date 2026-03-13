package pipeline

type PriceRange struct {
	Min      float64 `json:"min,omitempty"`
	Max      float64 `json:"max,omitempty"`
	Currency string  `json:"currency,omitempty"`
}

type ParsedDirective struct {
	Raw             string      `json:"raw,omitempty"`
	Mode            string      `json:"mode,omitempty"`
	ExplicitMode    bool        `json:"explicit_mode,omitempty"`
	CorpusPath      string      `json:"corpus_path,omitempty"`
	Geography       string      `json:"geography,omitempty"`
	Country         string      `json:"country,omitempty"`
	Region          string      `json:"region,omitempty"`
	TaxonomyFocus   []string    `json:"taxonomy_focus,omitempty"`
	TaxonomySkip    []string    `json:"taxonomy_skip,omitempty"`
	ICPConstraints  []string    `json:"icp_constraints,omitempty"`
	PriceRange      *PriceRange `json:"price_range,omitempty"`
	AvoidSectors    []string    `json:"avoid_sectors,omitempty"`
	TechConstraints []string    `json:"tech_constraints,omitempty"`
	BudgetCap       float64     `json:"budget_cap,omitempty"`
	KnownPatterns   []string    `json:"known_patterns,omitempty"`
	DomainPortfolio []string    `json:"domain_portfolio,omitempty"`
	Intent          string      `json:"intent,omitempty"`
	ScanContext     string      `json:"scan_context,omitempty"`
}

type DirectiveParser struct{}

func (DirectiveParser) Parse(text string) ParsedDirective {
	policy := workflowModuleScanPolicy(defaultWorkflowModule())
	if policy == nil {
		return ParsedDirective{}
	}
	return policy.ParseDirective(text)
}

func ParseDirectiveGeography(text string) (name, country, region string) {
	policy := workflowModuleScanPolicy(defaultWorkflowModule())
	if policy == nil {
		return "", "", ""
	}
	return policy.ParseDirectiveGeography(text)
}

func SanitizeGeographyPhrase(text string) string {
	policy := workflowModuleScanPolicy(defaultWorkflowModule())
	if policy == nil {
		return ""
	}
	return policy.SanitizeGeographyPhrase(text)
}

func IsComplexDirectiveText(text string) bool {
	policy := workflowModuleScanPolicy(defaultWorkflowModule())
	if policy == nil {
		return false
	}
	return policy.IsComplexDirectiveText(text)
}

func ExtractCorpusPathFromStrategicContext(strategic map[string]any) string {
	policy := workflowModuleScanPolicy(defaultWorkflowModule())
	if policy == nil {
		return ""
	}
	return policy.ExtractCorpusPathFromStrategicContext(strategic)
}
