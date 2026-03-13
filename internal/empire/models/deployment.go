package models

import "time"

type Deployment struct {
	ID          string    `json:"id"`
	VerticalID  string    `json:"vertical_id"`
	Environment string    `json:"environment"`
	Version     int       `json:"version"`
	Status      string    `json:"status"`
	Health      string    `json:"health_status,omitempty"`
	URL         string    `json:"url,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
