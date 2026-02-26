package models

import (
	"encoding/json"
	"time"
)

type Geography struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Country    string          `json:"country"`
	Region     string          `json:"region,omitempty"`
	Timezone   string          `json:"timezone,omitempty"`
	Currency   string          `json:"currency,omitempty"`
	Language   string          `json:"language,omitempty"`
	TaxRatePct float64         `json:"tax_rate_pct,omitempty"`
	RawSignals json.RawMessage `json:"raw_signals,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}
