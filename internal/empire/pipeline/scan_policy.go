package empire

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (module) ExpandScanAssignments(mode string, payload map[string]any, assigned map[string]any, batchSize int) ([]map[string]any, error) {
	mode = normalizeEmpireScanMode(mode)
	if mode != "corpus" {
		return []map[string]any{assigned}, nil
	}
	corpusPath := strings.TrimSpace(asString(payload["corpus_path"]))
	assigned["corpus_path"] = corpusPath
	batches, err := readJSONLFile(corpusPath, batchSize)
	if err != nil {
		assigned["corpus_signals"] = []map[string]any{}
		return []map[string]any{assigned}, err
	}
	if len(batches) == 0 {
		assigned["corpus_signals"] = []map[string]any{}
		return []map[string]any{assigned}, nil
	}
	out := make([]map[string]any, 0, len(batches))
	for _, batch := range batches {
		perBatch := cloneMap(assigned)
		perBatch["corpus_signals"] = batch
		out = append(out, perBatch)
	}
	return out, nil
}

func (module) ReadJSONLBatches(path string, batchSize int) ([][]map[string]any, error) {
	return readJSONLFile(path, batchSize)
}

func (module) ParseDirective(text string) runtimepipeline.ParsedDirective {
	raw := strings.TrimSpace(text)
	mode, explicit := runtimepipeline.ParseDirectiveMode(raw)
	geoName, country, region := ParseDirectiveGeography(raw)
	out := runtimepipeline.ParsedDirective{
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

func (module) ParseDirectiveGeography(text string) (name, country, region string) {
	return ParseDirectiveGeography(text)
}

func (module) SanitizeGeographyPhrase(text string) string {
	return sanitizeGeographyPhrase(text)
}

func (module) IsComplexDirectiveText(text string) bool {
	return IsComplexDirectiveText(text)
}

func (module) ResolveDirectiveCorpusPath(mode string, parsed runtimepipeline.ParsedDirective, payload map[string]any) (string, error) {
	corpusPath := strings.TrimSpace(asString(payload["corpus_path"]))
	if corpusPath == "" {
		corpusPath = strings.TrimSpace(parsed.CorpusPath)
	}
	if normalizeEmpireScanMode(mode) == "corpus" && corpusPath == "" {
		return "", fmt.Errorf("corpus_path is required for corpus mode")
	}
	return corpusPath, nil
}

func normalizeEmpireScanMode(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return ""
	}
	mode = strings.ReplaceAll(mode, "-", "_")
	mode = strings.Join(strings.Fields(mode), "_")
	switch mode {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return mode
	case "local_underserved", "local", "local_service", "services":
		return "local_services"
	case "discovery", "scan", "default", "automation", "micro", "saas":
		return "saas_gap"
	case "trend", "trend_scan", "trend_opportunity", "adjacent_opportunity":
		return "saas_trend"
	case "corpus_mode", "signal_corpus":
		return "corpus"
	default:
		return ""
	}
}

func (module) ExtractCorpusPathFromStrategicContext(strategic map[string]any) string {
	return ExtractCorpusPathFromStrategicContext(strategic)
}

func readJSONLFile(path string, batchSize int) ([][]map[string]any, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("corpus_path is required for corpus mode")
	}
	if batchSize <= 0 {
		batchSize = 25
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maxCapacity := maxInt(batchSize*8*1024, 1024*1024)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	batches := make([][]map[string]any, 0)
	current := make([]map[string]any, 0, batchSize)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		entry, ok := parseJSONLine(line)
		if !ok {
			continue
		}
		current = append(current, entry)
		if batchSize > 0 && len(current) >= batchSize {
			batches = append(batches, current)
			current = make([]map[string]any, 0, batchSize)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches, nil
}

func parseJSONLine(line string) (map[string]any, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, false
	}
	row := map[string]any{}
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return nil, false
	}
	return row, true
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

func extractPriceRange(text string) *runtimepipeline.PriceRange {
	m := priceRangePattern.FindStringSubmatch(text)
	if len(m) < 3 {
		return nil
	}
	min := parseNumberToken(m[1])
	max := parseNumberToken(m[2])
	if min <= 0 && max <= 0 {
		return nil
	}
	return &runtimepipeline.PriceRange{Min: min, Max: max, Currency: "USD"}
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

func ExtractCorpusPathFromStrategicContext(strategic map[string]any) string {
	if len(strategic) == 0 {
		return ""
	}
	if path := strings.TrimSpace(asString(strategic["corpus_path"])); path != "" {
		return path
	}
	if directive, ok := strategic["directive"].(map[string]any); ok && len(directive) > 0 {
		if params, ok := directive["parameters"].(map[string]any); ok && len(params) > 0 {
			if path := strings.TrimSpace(asString(params["corpus_path"])); path != "" {
				return path
			}
		}
	}
	parsed, ok := strategic["parsed"].(map[string]any)
	if !ok {
		return ""
	}
	return strings.TrimSpace(asString(parsed["corpus_path"]))
}

func IsComplexDirectiveText(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	if t == "" {
		return false
	}
	complexHints := []string{"latam", "across", "countries", "country with", "internet penetration", "focus on", "exclude", "excluding", "greater than", "less than", ">", "<", "compared to"}
	for _, hint := range complexHints {
		if strings.Contains(t, hint) {
			return true
		}
	}
	return false
}

var (
	directiveInPattern = regexp.MustCompile(`(?i)\bin\s+([a-z][a-z\s-]{1,})`)
	directiveGeoAlias  = map[string]string{
		"paraguay":                 "Paraguay",
		"argentina":                "Argentina",
		"brazil":                   "Brazil",
		"mexico":                   "Mexico",
		"chile":                    "Chile",
		"peru":                     "Peru",
		"colombia":                 "Colombia",
		"uruguay":                  "Uruguay",
		"us":                       "United States",
		"usa":                      "United States",
		"united states":            "United States",
		"united states of america": "United States",
	}
)

func ParseDirectiveGeography(text string) (name, country, region string) {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return "unspecified", "unspecified", ""
	}
	if token := strings.TrimSpace(strings.SplitN(raw, ",", 2)[0]); token != "" {
		if label, ok := canonicalDirectiveGeography(token); ok {
			return label, label, ""
		}
	}
	lower := strings.ToLower(raw)
	for needle, label := range directiveGeoAlias {
		if containsDirectiveAlias(lower, needle) {
			return label, label, ""
		}
	}
	m := directiveInPattern.FindStringSubmatch(raw)
	if len(m) == 2 {
		part := sanitizeGeographyPhrase(m[1])
		if part != "" {
			if label, ok := canonicalDirectiveGeography(part); ok {
				return label, label, ""
			}
			return part, part, ""
		}
	}
	return "unspecified", "unspecified", ""
}

func containsDirectiveAlias(haystack, alias string) bool {
	haystack = strings.ToLower(strings.TrimSpace(haystack))
	alias = strings.ToLower(strings.TrimSpace(alias))
	if haystack == "" || alias == "" {
		return false
	}
	pattern := `\b` + regexp.QuoteMeta(alias) + `\b`
	return regexp.MustCompile(pattern).MatchString(haystack)
}

func canonicalDirectiveGeography(v string) (string, bool) {
	norm := strings.ToLower(strings.TrimSpace(v))
	norm = strings.Trim(norm, `"'`)
	norm = strings.ReplaceAll(norm, ".", "")
	norm = strings.ReplaceAll(norm, "_", " ")
	norm = strings.Join(strings.Fields(norm), " ")
	if norm == "" {
		return "", false
	}
	label, ok := directiveGeoAlias[norm]
	return label, ok
}

func sanitizeGeographyPhrase(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	lower := strings.ToLower(v)
	for _, cut := range []string{" for ", " with ", " using ", " where ", ".", ","} {
		if idx := strings.Index(lower, cut); idx >= 0 {
			v = strings.TrimSpace(v[:idx])
			lower = strings.ToLower(v)
		}
	}
	if v == "" {
		return ""
	}
	parts := strings.Fields(strings.ToLower(v))
	for i := range parts {
		if len(parts[i]) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, " ")
}
