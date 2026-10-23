package packmodel

import "github.com/division-sh/swarm/internal/runtime/core/manifesthash"

const (
	TypeTrigger        = "trigger"
	TypeConnector      = "connector"
	TypeChannel        = "channel"
	ProvenancePlatform = "platform"
	ProvenanceExternal = "external"
	ProvenanceProject  = "project"
)

type Capabilities struct {
	Can    CanCapabilities `yaml:"can" json:"can"`
	Cannot []string        `yaml:"cannot" json:"cannot"`
}

type CanCapabilities struct {
	ReceiveHTTPSRoute       string   `yaml:"receive_https_route,omitempty" json:"receive_https_route,omitempty"`
	ReceiveSessionEvents    string   `yaml:"receive_session_events,omitempty" json:"receive_session_events,omitempty"`
	VerifySecret            string   `yaml:"verify_secret,omitempty" json:"verify_secret,omitempty"`
	EmitEvents              []string `yaml:"emit_events,omitempty" json:"emit_events,omitempty"`
	PersistDedupeMarkers    bool     `yaml:"persist_dedupe_markers,omitempty" json:"persist_dedupe_markers,omitempty"`
	CallProviderActions     []string `yaml:"call_provider_actions,omitempty" json:"call_provider_actions,omitempty"`
	LowerThroughActivity    bool     `yaml:"lower_through_activity,omitempty" json:"lower_through_activity,omitempty"`
	JournalActivityAttempts bool     `yaml:"journal_activity_attempts,omitempty" json:"journal_activity_attempts,omitempty"`
}

// Requires is a readback derived from bodies, never an authored manifest list.
type Requires struct {
	Secrets            []string `json:"secrets"`
	ManagedCredentials []string `json:"managed_credentials"`
}

func ManifestHash(body []byte) string { return manifesthash.FromBytes(body).String() }
