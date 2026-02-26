package models

import (
	"encoding/json"
	"time"
)

type BrandCandidate struct {
	Name             string          `json:"name"`
	Tagline          string          `json:"tagline,omitempty"`
	DomainSuggestion string          `json:"domain_suggestion,omitempty"`
	Availability     json.RawMessage `json:"availability,omitempty"`
	StyleGuide       json.RawMessage `json:"style_guide,omitempty"`
	Score            float64         `json:"score,omitempty"`
}

type BrandPackage struct {
	VerticalID     string           `json:"vertical_id"`
	Recommended    string           `json:"recommended,omitempty"`
	Candidates     []BrandCandidate `json:"candidates,omitempty"`
	RevisionReason string           `json:"revision_reason,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
}
