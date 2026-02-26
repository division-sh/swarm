package models

import "time"

type VerticalMetrics struct {
	ID               string    `json:"id"`
	VerticalID       string    `json:"vertical_id"`
	PeriodStart      time.Time `json:"period_start"`
	PeriodEnd        time.Time `json:"period_end"`
	UsersTotal       int       `json:"users_total"`
	UsersNew         int       `json:"users_new"`
	UsersChurned     int       `json:"users_churned"`
	MRRCents         int       `json:"mrr_cents"`
	SupportTickets   int       `json:"support_tickets"`
	BugsReported     int       `json:"bugs_reported"`
	BugsFixed        int       `json:"bugs_fixed"`
	FeaturesShipped  int       `json:"features_shipped"`
	OutreachSent     int       `json:"outreach_sent"`
	OutreachResponse int       `json:"outreach_responses"`
	CSATAvg          float64   `json:"csat_avg"`
	APICostCents     int       `json:"api_cost_cents"`
	InfraCostCents   int       `json:"infra_cost_cents"`
	CreatedAt        time.Time `json:"created_at"`
}
