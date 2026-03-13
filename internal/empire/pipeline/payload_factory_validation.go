package empire

import (
	"strings"

	"empireai/internal/runtime/sharedjson"
)

func BuildBrandRequestedPayload(verticalID, name, geography string, scoring, brief map[string]any) BrandRequestedPayload {
	if scoring == nil {
		scoring = map[string]any{}
	}
	if brief == nil {
		brief = map[string]any{}
	}
	return BrandRequestedPayload{
		VerticalID:    strings.TrimSpace(verticalID),
		VerticalName:  strings.TrimSpace(name),
		Name:          strings.TrimSpace(name),
		Geography:     strings.TrimSpace(geography),
		Scoring:       scoring,
		BusinessBrief: brief,
	}
}

func BuildValidationPackageReadyPayload(verticalID, name, geography string, snap ValidationContextSnapshot) ValidationPackageReadyPayload {
	if snap.Research == nil {
		snap.Research = map[string]any{}
	}
	if snap.Spec == nil {
		snap.Spec = map[string]any{}
	}
	if snap.CTONotes == nil {
		snap.CTONotes = map[string]any{}
	}
	if snap.Brand == nil {
		snap.Brand = map[string]any{}
	}
	if snap.Scoring == nil {
		snap.Scoring = map[string]any{}
	}
	return ValidationPackageReadyPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: strings.TrimSpace(name),
		Geography:    strings.TrimSpace(geography),
		Research:     snap.Research,
		Spec:         snap.Spec,
		CTONotes:     snap.CTONotes,
		Brand:        snap.Brand,
		Scoring:      snap.Scoring,
		SpecVersion:  snap.SpecVersion,
	}
}

func BuildSpecValidationRequestedPayload(verticalID string, spec map[string]any) SpecValidationRequestedPayload {
	if spec == nil {
		spec = map[string]any{}
	}
	specTier := strings.TrimSpace(sharedjson.AsString(spec["spec_tier"]))
	if specTier == "" {
		specTier = strings.TrimSpace(sharedjson.AsString(spec["spec_type"]))
	}
	if specTier == "" {
		specTier = "vertical_spec"
	}
	return SpecValidationRequestedPayload{
		VerticalID:  strings.TrimSpace(verticalID),
		SpecContent: spec,
		SpecTier:    specTier,
	}
}

func BuildCTOSpecReviewRequestedPayload(verticalID, name, geography string, specValidation map[string]any, snap ValidationContextSnapshot) CTOSpecReviewRequestedPayload {
	if specValidation == nil {
		specValidation = map[string]any{}
	}
	specVersion := asInt(specValidation["spec_version"])
	if specVersion == 0 {
		specVersion = snap.SpecVersion
	}
	businessBrief := parsePayloadMap(nil)
	if snap.Research != nil {
		if brief, ok := snap.Research["business_brief"].(map[string]any); ok && brief != nil {
			businessBrief = brief
		} else {
			businessBrief = snap.Research
		}
	}
	return CTOSpecReviewRequestedPayload{
		VerticalID:    strings.TrimSpace(verticalID),
		MvPSpec:       summarizeContractPayload(firstNonEmptyMap(specValidation, snap.Spec)),
		BusinessBrief: businessBrief,
		VerticalContext: map[string]any{
			"vertical_name": strings.TrimSpace(name),
			"geography":     strings.TrimSpace(geography),
			"scoring":       snap.Scoring,
		},
		VerticalName:   strings.TrimSpace(name),
		Geography:      strings.TrimSpace(geography),
		SpecValidation: specValidation,
		SpecVersion:    specVersion,
		Research:       snap.Research,
		Spec:           snap.Spec,
		Scoring:        snap.Scoring,
	}
}

func BuildSpecRevisionRequestedPayload(verticalID, source, name, geography string, feedback map[string]any, snap ValidationContextSnapshot) SpecRevisionRequestedPayload {
	if feedback == nil {
		feedback = map[string]any{}
	}
	return SpecRevisionRequestedPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		CTOFeedback:  summarizeContractPayload(feedback),
		VerticalName: strings.TrimSpace(name),
		Geography:    strings.TrimSpace(geography),
		Source:       strings.TrimSpace(source),
		Feedback:     feedback,
		Research:     snap.Research,
		Spec:         snap.Spec,
		Scoring:      snap.Scoring,
	}
}

func BuildValidationMoreDataPayload(verticalID, name, geography string, request map[string]any, snap ValidationContextSnapshot) ValidationMoreDataNeededPayload {
	if request == nil {
		request = map[string]any{}
	}
	return ValidationMoreDataNeededPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		Questions:    summarizeContractPayload(request),
		VerticalName: strings.TrimSpace(name),
		Geography:    strings.TrimSpace(geography),
		Request:      request,
		Research:     snap.Research,
		Spec:         snap.Spec,
		Scoring:      snap.Scoring,
	}
}

func BuildBrandRevisionNeededPayload(verticalID, name, geography string, feedback, brand map[string]any) BrandRevisionNeededPayload {
	if feedback == nil {
		feedback = map[string]any{}
	}
	if brand == nil {
		brand = map[string]any{}
	}
	return BrandRevisionNeededPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: strings.TrimSpace(name),
		Geography:    strings.TrimSpace(geography),
		Feedback:     feedback,
		Brand:        brand,
	}
}

func BuildVerticalKilledPayload(verticalID, name, geography, sourceEvent string, reason map[string]any) VerticalKilledPayload {
	if reason == nil {
		reason = map[string]any{}
	}
	return VerticalKilledPayload{
		VerticalID:   strings.TrimSpace(verticalID),
		VerticalName: strings.TrimSpace(name),
		Geography:    strings.TrimSpace(geography),
		SourceEvent:  strings.TrimSpace(sourceEvent),
		Priority:     "high",
		Reason:       reason,
	}
}

func BuildValidationStartedPayload(verticalID, name, geography string, scoring map[string]any) ValidationStartedPayload {
	if scoring == nil {
		scoring = map[string]any{}
	}
	out := ValidationStartedPayload{
		VerticalID: strings.TrimSpace(verticalID),
		Scoring:    scoring,
	}
	if strings.TrimSpace(name) != "" {
		out.VerticalName = strings.TrimSpace(name)
		out.Name = strings.TrimSpace(name)
	}
	if strings.TrimSpace(geography) != "" {
		out.Geography = strings.TrimSpace(geography)
	}
	out.ScoringContext = summarizeContractPayload(scoring)
	return out
}
