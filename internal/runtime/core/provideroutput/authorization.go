package provideroutput

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type Kind string

const (
	KindRaw        Kind = "raw"
	KindNormalized Kind = "normalized"
)

var manifestHashPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Authorization is the admitted verified-pack provenance required to grant a
// normalized provider output target-free input routing authority.
type Authorization struct {
	provider     string
	event        string
	packID       string
	packVersion  string
	manifestHash string
	generation   triggergeneration.Generation
	valid        bool
}

func NewAuthorization(provider, event, packID, packVersion, manifestHash string, generation triggergeneration.Generation) (Authorization, error) {
	values := []*string{&provider, &event, &packID, &packVersion, &manifestHash}
	for _, value := range values {
		*value = strings.TrimSpace(*value)
		if *value == "" {
			return Authorization{}, fmt.Errorf("provider output authorization fields are required")
		}
	}
	if !manifestHashPattern.MatchString(manifestHash) {
		return Authorization{}, fmt.Errorf("provider output authorization manifest hash must use canonical sha256:<lowercase-hex>")
	}
	if !generation.Valid() {
		return Authorization{}, fmt.Errorf("provider output authorization catalog generation is required")
	}
	return Authorization{
		provider: provider, event: event, packID: packID, packVersion: packVersion,
		manifestHash: manifestHash, generation: generation, valid: true,
	}, nil
}

func MustAuthorization(provider, event, packID, packVersion, manifestHash string, generation triggergeneration.Generation) Authorization {
	authorization, err := NewAuthorization(provider, event, packID, packVersion, manifestHash, generation)
	if err != nil {
		panic(err)
	}
	return authorization
}

func ParseAuthorization(provider, event, packID, packVersion, manifestHash, generationID string) (Authorization, error) {
	generation, err := triggergeneration.Parse(strings.TrimSpace(generationID))
	if err != nil {
		return Authorization{}, fmt.Errorf("provider output authorization catalog generation: %w", err)
	}
	return NewAuthorization(provider, event, packID, packVersion, manifestHash, generation)
}

func (a Authorization) Provider() string                         { return a.provider }
func (a Authorization) Event() string                            { return a.event }
func (a Authorization) PackID() string                           { return a.packID }
func (a Authorization) PackVersion() string                      { return a.packVersion }
func (a Authorization) ManifestHash() string                     { return a.manifestHash }
func (a Authorization) Generation() triggergeneration.Generation { return a.generation }

func (a Authorization) Valid() bool {
	return a.valid && a.provider != "" && a.event != "" && a.packID != "" && a.packVersion != "" &&
		a.manifestHash != "" && a.generation.Valid()
}

func (a Authorization) Empty() bool {
	return !a.valid && a.provider == "" && a.event == "" && a.packID == "" && a.packVersion == "" &&
		a.manifestHash == "" && !a.generation.Valid()
}

func (a Authorization) Matches(other Authorization) bool {
	return a.Valid() && other.Valid() &&
		a.provider == other.provider &&
		a.event == other.event &&
		a.packID == other.packID &&
		a.packVersion == other.packVersion &&
		a.manifestHash == other.manifestHash &&
		a.generation.Equal(other.generation)
}

func (a Authorization) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Provider     string `json:"Provider"`
		Event        string `json:"Event"`
		PackID       string `json:"PackID"`
		PackVersion  string `json:"PackVersion"`
		ManifestHash string `json:"ManifestHash"`
		GenerationID string `json:"GenerationID"`
	}{
		Provider: a.provider, Event: a.event, PackID: a.packID, PackVersion: a.packVersion,
		ManifestHash: a.manifestHash, GenerationID: a.generation.Diagnostic(),
	})
}

func (a *Authorization) UnmarshalJSON(raw []byte) error {
	if a == nil {
		return fmt.Errorf("provider output authorization destination is nil")
	}
	var wire struct {
		Provider     string `json:"Provider"`
		Event        string `json:"Event"`
		PackID       string `json:"PackID"`
		PackVersion  string `json:"PackVersion"`
		ManifestHash string `json:"ManifestHash"`
		GenerationID string `json:"GenerationID"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	if wire.Provider == "" && wire.Event == "" && wire.PackID == "" && wire.PackVersion == "" && wire.ManifestHash == "" && wire.GenerationID == "" {
		*a = Authorization{}
		return nil
	}
	admitted, err := ParseAuthorization(wire.Provider, wire.Event, wire.PackID, wire.PackVersion, wire.ManifestHash, wire.GenerationID)
	if err != nil {
		return err
	}
	*a = admitted
	return nil
}
