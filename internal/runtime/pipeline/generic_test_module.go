package pipeline

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/semanticview"
)

type genericTestModule struct {
	once           *sync.Once
	contractBundle *runtimecontracts.WorkflowContractBundle
	workflow       *WorkflowDefinition
	workflowNodes  []WorkflowNode
	guardRegistry  GuardRegistry
	actionRegistry ActionRegistry
	loadErr        error
}

func NewGenericTestWorkflowModule() WorkflowModule {
	return &genericTestModule{}
}

func (m *genericTestModule) init() {
	if m.once == nil {
		m.once = &sync.Once{}
	}
	m.once.Do(func() {
		repoRoot := WorkflowRepoRoot()
		contractsDir := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-mas-bundle")
		platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
		m.contractBundle, m.loadErr = runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsDir, platformSpec)
		if m.loadErr != nil {
			return
		}
		m.workflow, m.loadErr = LoadWorkflowDefinition(semanticview.Wrap(m.contractBundle))
		if m.loadErr != nil {
			return
		}
		m.workflowNodes, m.loadErr = LoadWorkflowNodes(semanticview.Wrap(m.contractBundle))
		if m.loadErr != nil {
			return
		}
		source := semanticview.Wrap(m.contractBundle)
		m.guardRegistry = NewContractGuardRegistry(source)
		m.actionRegistry = NewContractActionRegistry(source)
	})
	if m.loadErr != nil {
		panic(m.loadErr)
	}
}

func (m *genericTestModule) SemanticSource() semanticview.Source {
	m.init()
	return semanticview.Wrap(m.contractBundle)
}

func (m *genericTestModule) WorkflowDefinition() *WorkflowDefinition {
	m.init()
	return m.workflow
}

func (m *genericTestModule) WorkflowNodes() []WorkflowNode {
	m.init()
	out := make([]WorkflowNode, 0, len(m.workflowNodes))
	for _, node := range m.workflowNodes {
		out = append(out, node)
	}
	return out
}

func (m *genericTestModule) GuardRegistry() GuardRegistry {
	m.init()
	return m.guardRegistry
}

func (m *genericTestModule) ActionRegistry() ActionRegistry {
	m.init()
	return m.actionRegistry
}

func (*genericTestModule) DiscoveryPolicy() DiscoveryPolicy { return genericTestModule{} }

func (*genericTestModule) ScanPolicy() ScanPolicy { return genericTestModule{} }

func (*genericTestModule) ScoringPolicy() ScoringPolicy { return genericTestModule{} }

func (*genericTestModule) PayloadFactory() PayloadFactory { return genericTestModule{} }

func (genericTestModule) EvaluateDiscoveryPreFilter(_ map[string]any, rawSignal float64) (bool, float64, string) {
	return true, rawSignal, ""
}

func (genericTestModule) BuildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	out := cloneMap(payload)
	out["raw_signal"] = rawSignal
	out["adjusted_signal"] = adjustedSignal
	out["reason"] = strings.TrimSpace(reason)
	out["mode"] = strings.TrimSpace(mode)
	return out
}

func (genericTestModule) NormalizeOpportunityPattern(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "-", "_")
	raw = strings.ReplaceAll(raw, " ", "_")
	return raw
}

func (genericTestModule) BuildDiscoveryCandidatesForReport(scanMode string, payload map[string]any) []DiscoveryCandidate {
	return []DiscoveryCandidate{{
		Mode:    normalizeCampaignScanMode(scanMode),
		Signal:  genericTestSignalStrength(payload),
		Payload: cloneMap(payload),
	}}
}

func (genericTestModule) ExpandScanAssignments(mode string, payload map[string]any, assigned ScanAssignedPayload, batchSize int) ([]ScanAssignedPayload, error) {
	mode = normalizeCampaignScanMode(mode)
	if mode != "corpus" {
		return []ScanAssignedPayload{assigned}, nil
	}
	corpusPath, err := genericTestResolveCorpusPath(payload, assigned)
	if err != nil {
		return nil, err
	}
	batches, err := genericTestReadJSONLBatches(corpusPath, batchSize)
	if err != nil {
		return nil, err
	}
	out := make([]ScanAssignedPayload, 0, len(batches))
	for i, batch := range batches {
		next := assigned
		next.CorpusPath = corpusPath
		next.CorpusSignals = batch
		next.PlannedShards = len(batches)
		next.RequestedAt = fmt.Sprintf("shard-%d", i)
		out = append(out, next)
	}
	if len(out) == 0 {
		return []ScanAssignedPayload{assigned}, nil
	}
	return out, nil
}

func (genericTestModule) ReadJSONLBatches(path string, batchSize int) ([][]map[string]any, error) {
	return genericTestReadJSONLBatches(path, batchSize)
}

func (genericTestModule) ParseDirective(text string) ParsedDirective {
	mode, explicit := parseDirectiveModeCompat(text)
	name, country, region := genericTestParseDirectiveGeography(text)
	corpusPath := genericTestCorpusPathFromText(text)
	return ParsedDirective{
		Raw:          strings.TrimSpace(text),
		Mode:         mode,
		ExplicitMode: explicit,
		CorpusPath:   corpusPath,
		Geography:    name,
		Country:      country,
		Region:       region,
		ScanContext:  strings.TrimSpace(text),
	}
}

func (genericTestModule) ParseDirectiveGeography(text string) (name, country, region string) {
	return genericTestParseDirectiveGeography(text)
}

func (genericTestModule) SanitizeGeographyPhrase(text string) string {
	return strings.TrimSpace(strings.Trim(strings.ReplaceAll(text, ",", " "), " "))
}

func (genericTestModule) IsComplexDirectiveText(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	return len(strings.Fields(text)) >= 8 || strings.Contains(text, " with ") || strings.Contains(text, "focus on")
}

func (m genericTestModule) ResolveDirectiveCorpusPath(mode string, parsed ParsedDirective, payload map[string]any) (string, error) {
	if normalizeCampaignScanMode(mode) != "corpus" {
		return "", nil
	}
	if strings.TrimSpace(parsed.CorpusPath) != "" {
		return strings.TrimSpace(parsed.CorpusPath), nil
	}
	if path, err := genericTestResolveCorpusPath(payload, ScanAssignedPayload{}); err == nil {
		return path, nil
	}
	return "", fmt.Errorf("corpus_path is required for corpus mode")
}

func (genericTestModule) ExtractCorpusPathFromStrategicContext(strategic map[string]any) string {
	if strategic == nil {
		return ""
	}
	if path := strings.TrimSpace(asString(strategic["corpus_path"])); path != "" {
		return path
	}
	if directive, ok := strategic["directive"].(map[string]any); ok {
		if params, ok := directive["parameters"].(map[string]any); ok {
			return strings.TrimSpace(asString(params["corpus_path"]))
		}
	}
	return ""
}

func (genericTestModule) ExpectedScoringDimensions(rubric string) []string {
	switch strings.TrimSpace(strings.ToLower(rubric)) {
	case "signals", "saas":
		return []string{"market_size", "urgency", "feasibility"}
	default:
		return []string{"value", "feasibility", "adoption"}
	}
}

func (m genericTestModule) SelectScoringRubric(mode string) string {
	if normalizeCampaignScanMode(mode) == "corpus" {
		return "signals"
	}
	return "default"
}

func (m genericTestModule) ComputeComposite(in ScoringAccumulatorInput) ScoringComposite {
	total := 0
	count := 0
	for _, dim := range in.Received {
		total += dim.Score
		count++
	}
	score := 0.0
	if count > 0 {
		score = float64(total) / float64(count)
	}
	result := "rejected"
	switch {
	case score >= 75:
		result = "shortlisted"
	case score >= 50:
		result = "marginal"
	}
	return ScoringComposite{
		Result:         result,
		Reason:         "generic_test_policy",
		CompositeScore: math.Round(score*100) / 100,
		ViabilityScore: math.Round(score*100) / 100,
		MarketScore:    math.Round(score*100) / 100,
		Dimensions:     in.Received,
		Rubric:         strings.TrimSpace(in.Rubric),
		Partial:        in.Partial,
	}
}

func (genericTestModule) BuildDiscoveryContextPayload(raw map[string]any) map[string]any {
	return cloneMap(raw)
}

func (genericTestModule) ResolveScoringAnalysisRecipient(recipients []string, excludedAgent string) string {
	excludedAgent = strings.TrimSpace(excludedAgent)
	for _, recipient := range recipients {
		recipient = strings.TrimSpace(recipient)
		if recipient == "" || recipient == excludedAgent {
			continue
		}
		return recipient
	}
	if len(recipients) == 0 {
		return ""
	}
	return strings.TrimSpace(recipients[0])
}

func (genericTestModule) NormalizeGeographicScope(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func (genericTestModule) ScoringRestoreDeltaBucket() string {
	return "generic-scoring-restore"
}

func (genericTestModule) EncodeScoringRestoreDelta(acc *ScoringAccumulator) map[string]any {
	return EncodeScoringRestoreDelta(acc)
}

func (genericTestModule) ApplyScoringRestoreDelta(acc *ScoringAccumulator, delta map[string]any) {
	ApplyScoringRestoreDelta(acc, delta)
}

func (genericTestModule) BuildScanAssignedPayload(scanID, campaignID, mode, geography string, source map[string]any, plannedShards int) ScanAssignedPayload {
	return ScanAssignedPayload{
		ScanID:          scanID,
		CampaignID:      campaignID,
		Mode:            mode,
		Geography:       geography,
		CampaignContext: cloneMap(source),
		CorpusPath:      strings.TrimSpace(asString(source["corpus_path"])),
		PlannedShards:   plannedShards,
	}
}

func (genericTestModule) BuildSynthesisNeededPayload(scanID, campaignID, mode, geography string, raw map[string]any) SynthesisNeededPayload {
	return SynthesisNeededPayload{
		ScanID:      scanID,
		CampaignID:  campaignID,
		Mode:        mode,
		Geography:   geography,
		Category:    strings.TrimSpace(asString(raw["category"])),
		Subcategory: strings.TrimSpace(asString(raw["subcategory"])),
		RawReport:   cloneMap(raw),
	}
}

func (genericTestModule) BuildDedupAmbiguousPayload(scanID, dedupEventID string, similarity float64, candidateName, geography string, signal float64, existingID, existingName string) DedupAmbiguousPayload {
	return DedupAmbiguousPayload{
		ScanID:       scanID,
		DedupEventID: dedupEventID,
		Similarity:   similarity,
		NewCandidate: DedupCandidatePayload{Name: candidateName, Geography: geography, SignalStrength: signal},
		ExistingVertical: DedupCandidatePayload{
			ID:             existingID,
			Name:           existingName,
			Geography:      geography,
			SignalStrength: signal,
		},
	}
}

func (genericTestModule) BuildVerticalDiscoveredPayload(verticalID, name, geography, mode, scanID, campaignID string, signal float64, discoverySource string, rawSignals map[string]any) VerticalDiscoveredPayload {
	return VerticalDiscoveredPayload{
		VerticalID:       verticalID,
		VerticalName:     name,
		Name:             name,
		Geography:        geography,
		Mode:             mode,
		ScanID:           scanID,
		CampaignID:       campaignID,
		SignalStrength:   signal,
		DiscoverySource:  discoverySource,
		RawSignals:       cloneMap(rawSignals),
		DiscoveryContext: cloneMap(rawSignals),
	}
}

func (genericTestModule) BuildScanCompletedPayload(in ScanCompletedBuildInput) ScanCompletedPayload {
	return ScanCompletedPayload{
		ScanID:          in.ScanID,
		CampaignID:      in.CampaignID,
		Mode:            in.Mode,
		Geography:       in.Geography,
		ReportsReceived: in.ReportsReceived,
		Expected:        in.Expected,
		Complete:        in.Complete,
		Discovered:      in.Discovered,
		Skipped:         in.Skipped,
		PendingDedup:    in.PendingDedup,
		TimedOut:        in.TimedOut,
		ShardsTotal:     in.ShardsTotal,
		ShardsCompleted: in.ShardsCompleted,
		ShardsFailed:    in.ShardsFailed,
	}
}

func (genericTestModule) BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric string, dimensions []string, discoveryContext map[string]any) ScoringRequestedPayload {
	return ScoringRequestedPayload{
		VerticalID:          verticalID,
		VerticalName:        verticalName,
		Geography:           geography,
		Mode:                mode,
		Rubric:              rubric,
		DimensionsRequested: append([]string(nil), dimensions...),
		DiscoveryContext:    cloneMap(discoveryContext),
	}
}

func (genericTestModule) BuildScoringContestedPayload(verticalID, dimension string, contest ContestedDimension, rubric, mode string) ScoringContestedPayload {
	return ScoringContestedPayload{
		VerticalID: verticalID,
		Dimension:  dimension,
		Scores:     append([]int(nil), contest.Scores...),
		Evidence:   append([]string(nil), contest.Evidence...),
		Spread:     contest.Spread,
		Rubric:     rubric,
		Mode:       mode,
	}
}

func (genericTestModule) BuildVerticalScoredPayload(verticalID string, result ScoringComposite, verticalName, geography, mode string) VerticalScoredPayload {
	return VerticalScoredPayload{
		VerticalID:     verticalID,
		Result:         result.Result,
		Reason:         result.Reason,
		CompositeScore: result.CompositeScore,
		ViabilityScore: result.ViabilityScore,
		MarketScore:    result.MarketScore,
		Dimensions:     result.Dimensions,
		Rubric:         result.Rubric,
		Partial:        result.Partial,
		Mode:           mode,
		VerticalName:   verticalName,
		Geography:      geography,
	}
}

func (genericTestModule) BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) VerticalShortlistedPayload {
	return VerticalShortlistedPayload{
		VerticalID:     verticalID,
		CompositeScore: composite,
		ViabilityScore: viability,
		ScoringPayload: cloneMap(scoringPayload),
	}
}

func (genericTestModule) BuildVerticalMarginalPayload(verticalID string, result ScoringComposite) VerticalMarginalPayload {
	return VerticalMarginalPayload{
		VerticalID:        verticalID,
		CompositeScore:    result.CompositeScore,
		ViabilityScore:    result.ViabilityScore,
		Dimensions:        result.Dimensions,
		PromotionEligible: result.CompositeScore >= 50,
	}
}

func (genericTestModule) BuildVerticalRejectedPayload(verticalID string, result ScoringComposite) VerticalRejectedPayload {
	return VerticalRejectedPayload{
		VerticalID: verticalID,
		Reason:     result.Reason,
	}
}

func (genericTestModule) BuildBrandRequestedPayload(verticalID, name, geography string, scoring, brief map[string]any) BrandRequestedPayload {
	return BrandRequestedPayload{
		VerticalID:    verticalID,
		VerticalName:  name,
		Name:          name,
		Geography:     geography,
		Scoring:       cloneMap(scoring),
		BusinessBrief: cloneMap(brief),
	}
}

func (genericTestModule) BuildValidationPackageReadyPayload(verticalID, name, geography string, snap ValidationContextSnapshot) ValidationPackageReadyPayload {
	return ValidationPackageReadyPayload{
		VerticalID:   verticalID,
		VerticalName: name,
		Geography:    geography,
		Research:     cloneMap(snap.Research),
		Spec:         cloneMap(snap.Spec),
		CTONotes:     cloneMap(snap.CTONotes),
		Brand:        cloneMap(snap.Brand),
		Scoring:      cloneMap(snap.Scoring),
		SpecVersion:  snap.SpecVersion,
	}
}

func (genericTestModule) BuildSpecValidationRequestedPayload(verticalID string, spec map[string]any) SpecValidationRequestedPayload {
	return SpecValidationRequestedPayload{
		VerticalID:  verticalID,
		SpecContent: cloneMap(spec),
		SpecTier:    "generic",
	}
}

func (genericTestModule) BuildCTOSpecReviewRequestedPayload(verticalID, name, geography string, specValidation map[string]any, snap ValidationContextSnapshot) CTOSpecReviewRequestedPayload {
	return CTOSpecReviewRequestedPayload{
		VerticalID:     verticalID,
		VerticalName:   name,
		Geography:      geography,
		SpecValidation: cloneMap(specValidation),
		SpecVersion:    snap.SpecVersion,
		Research:       cloneMap(snap.Research),
		Spec:           cloneMap(snap.Spec),
		Scoring:        cloneMap(snap.Scoring),
	}
}

func (genericTestModule) BuildSpecRevisionRequestedPayload(verticalID, source, name, geography string, feedback map[string]any, snap ValidationContextSnapshot) SpecRevisionRequestedPayload {
	return SpecRevisionRequestedPayload{
		VerticalID:   verticalID,
		Source:       source,
		VerticalName: name,
		Geography:    geography,
		Feedback:     cloneMap(feedback),
		Research:     cloneMap(snap.Research),
		Spec:         cloneMap(snap.Spec),
		Scoring:      cloneMap(snap.Scoring),
	}
}

func (genericTestModule) BuildValidationMoreDataPayload(verticalID, name, geography string, request map[string]any, snap ValidationContextSnapshot) ValidationMoreDataNeededPayload {
	return ValidationMoreDataNeededPayload{
		VerticalID:   verticalID,
		VerticalName: name,
		Geography:    geography,
		Request:      cloneMap(request),
		Research:     cloneMap(snap.Research),
		Spec:         cloneMap(snap.Spec),
		Scoring:      cloneMap(snap.Scoring),
	}
}

func (genericTestModule) BuildBrandRevisionNeededPayload(verticalID, name, geography string, feedback, brand map[string]any) BrandRevisionNeededPayload {
	return BrandRevisionNeededPayload{
		VerticalID:   verticalID,
		VerticalName: name,
		Geography:    geography,
		Feedback:     cloneMap(feedback),
		Brand:        cloneMap(brand),
	}
}

func (genericTestModule) BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent string, reason map[string]any) VerticalKilledPayload {
	return VerticalKilledPayload{
		VerticalID:   verticalID,
		VerticalName: name,
		Geography:    geography,
		SourceEvent:  sourceEvent,
		Priority:     "normal",
		Reason:       cloneMap(reason),
	}
}

func (genericTestModule) BuildValidationStartedPayload(verticalID, name, geography string, scoring map[string]any) ValidationStartedPayload {
	return ValidationStartedPayload{
		VerticalID:   verticalID,
		VerticalName: name,
		Name:         name,
		Geography:    geography,
		Scoring:      cloneMap(scoring),
	}
}

func genericTestSignalStrength(payload map[string]any) float64 {
	switch v := payload["signal_strength"].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	default:
		return 50
	}
}

func genericTestResolveCorpusPath(payload map[string]any, assigned ScanAssignedPayload) (string, error) {
	if path := strings.TrimSpace(assigned.CorpusPath); path != "" {
		return path, nil
	}
	if path := strings.TrimSpace(asString(payload["corpus_path"])); path != "" {
		return path, nil
	}
	if nested, ok := payload["campaign_context"].(map[string]any); ok {
		if path := strings.TrimSpace(asString(nested["corpus_path"])); path != "" {
			return path, nil
		}
	}
	return "", fmt.Errorf("corpus_path is required for corpus mode")
}

func genericTestReadJSONLBatches(path string, batchSize int) ([][]map[string]any, error) {
	if batchSize <= 0 {
		batchSize = 25
	}
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	batches := make([][]map[string]any, 0, 4)
	current := make([]map[string]any, 0, batchSize)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, err
		}
		current = append(current, row)
		if len(current) == batchSize {
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

func genericTestParseDirectiveGeography(text string) (name, country, region string) {
	normalized := strings.ToLower(strings.TrimSpace(text))
	switch {
	case strings.Contains(normalized, " paraguay"):
		return "Paraguay", "Paraguay", "latam"
	case strings.Contains(normalized, " argentina"):
		return "Argentina", "Argentina", "latam"
	case strings.Contains(normalized, " us"), strings.HasPrefix(normalized, "us,"), strings.Contains(normalized, " united states"):
		return "United States", "United States", "na"
	default:
		return "", "", ""
	}
}

func genericTestCorpusPathFromText(text string) string {
	for _, token := range strings.Fields(strings.TrimSpace(text)) {
		if strings.HasPrefix(token, "corpus_path=") {
			return strings.TrimPrefix(token, "corpus_path=")
		}
	}
	return ""
}
