package pipeline

import (
	"regexp"
	"strconv"
	"strings"
)

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
	raw := strings.TrimSpace(text)
	mode, explicit := ParseDirectiveMode(raw)
	geoName, country, region := ParseDirectiveGeography(raw)
	out := ParsedDirective{
		Raw:             raw,
		Mode:            mode,
		ExplicitMode:    explicit,
		CorpusPath:      extractDirectiveCorpusPath(raw),
		Geography:       geoName,
		Country:         country,
		Region:          region,
		TaxonomyFocus:   dedupeList(extractListClause(raw, "focus on")),
		TaxonomySkip:    dedupeList(extractListClause(raw, "skip")),
		ICPConstraints:  dedupeList(extractListClause(raw, "icp")),
		AvoidSectors:    dedupeList(extractListClause(raw, "avoid")),
		TechConstraints: dedupeList(extractListClause(raw, "tech")),
		KnownPatterns:   dedupeList(extractListClause(raw, "pattern")),
		DomainPortfolio: dedupeList(extractListClause(raw, "domain")),
		ScanContext:     raw,
	}
	out.PriceRange = extractPriceRange(raw)
	out.BudgetCap = extractBudgetCap(raw)
	if IsComplexDirectiveText(raw) {
		out.Intent = "complex"
	} else {
		out.Intent = "deterministic"
	}
	return out
}

func extractListClause(text, keyword string) []string {
	text = strings.ToLower(strings.TrimSpace(text))
	keyword = strings.ToLower(strings.TrimSpace(keyword))
	if text == "" || keyword == "" {
		return nil
	}
	idx := strings.Index(text, keyword)
	if idx < 0 {
		return nil
	}
	clause := strings.TrimSpace(text[idx+len(keyword):])
	for _, stop := range []string{";", ".", " with ", " in ", " and "} {
		if j := strings.Index(clause, stop); j >= 0 {
			clause = strings.TrimSpace(clause[:j])
			break
		}
	}
	if clause == "" {
		return nil
	}
	rawItems := strings.FieldsFunc(clause, func(r rune) bool {
		return r == ',' || r == '|' || r == '/'
	})
	out := make([]string, 0, len(rawItems))
	for _, item := range rawItems {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

var (
	budgetNumberPattern = regexp.MustCompile(`(?i)\bbudget[^0-9$]*\$?\s*([0-9][0-9,\.]*)`)
	priceRangePattern   = regexp.MustCompile(`(?i)\b(?:price|pricing|ticket)\b[^0-9$]*\$?\s*([0-9][0-9,\.]*)\s*(?:-|to)\s*\$?\s*([0-9][0-9,\.]*)`)
	corpusPathPattern   = regexp.MustCompile(`(?i)\bcorpus_path\s*[:=]\s*([^\s,;]+)`)
	corpusFilePattern   = regexp.MustCompile(`(?i)\b(?:corpus|jsonl)\s*(?:at|from|path|file)?\s*[:=]?\s*([^\s,;]+\.jsonl)\b`)
)

func extractBudgetCap(text string) float64 {
	m := budgetNumberPattern.FindStringSubmatch(text)
	if len(m) < 2 {
		return 0
	}
	return parseNumberToken(m[1])
}

func extractPriceRange(text string) *PriceRange {
	m := priceRangePattern.FindStringSubmatch(text)
	if len(m) < 3 {
		return nil
	}
	min := parseNumberToken(m[1])
	max := parseNumberToken(m[2])
	if min <= 0 && max <= 0 {
		return nil
	}
	return &PriceRange{Min: min, Max: max, Currency: "USD"}
}

func extractDirectiveCorpusPath(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	if m := corpusPathPattern.FindStringSubmatch(text); len(m) >= 2 {
		return strings.Trim(strings.TrimSpace(m[1]), `"'`)
	}
	if m := corpusFilePattern.FindStringSubmatch(text); len(m) >= 2 {
		return strings.Trim(strings.TrimSpace(m[1]), `"'`)
	}
	return ""
}

func parseNumberToken(raw string) float64 {
	raw = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(raw, ",", ""), "$", ""))
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return v
}

func dedupeList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, item := range in {
		v := strings.TrimSpace(item)
		if v == "" {
			continue
		}
		k := strings.ToLower(v)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return out
}
