package provideroutput

import "strings"

type Kind string

const (
	KindRaw        Kind = "raw"
	KindNormalized Kind = "normalized"
)

// Authorization is the verified-pack provenance required to grant a
// normalized provider output target-free input routing authority.
type Authorization struct {
	Provider     string
	Event        string
	PackID       string
	PackVersion  string
	ManifestHash string
	GenerationID string
}

func (a Authorization) Normalized() Authorization {
	return Authorization{
		Provider:     strings.TrimSpace(a.Provider),
		Event:        strings.TrimSpace(a.Event),
		PackID:       strings.TrimSpace(a.PackID),
		PackVersion:  strings.TrimSpace(a.PackVersion),
		ManifestHash: strings.TrimSpace(a.ManifestHash),
		GenerationID: strings.TrimSpace(a.GenerationID),
	}
}

func (a Authorization) Valid() bool {
	a = a.Normalized()
	return a.Provider != "" && a.Event != "" && a.PackID != "" && a.PackVersion != "" && a.ManifestHash != "" && a.GenerationID != ""
}

func (a Authorization) Empty() bool {
	return a.Normalized() == (Authorization{})
}

func (a Authorization) Matches(other Authorization) bool {
	a = a.Normalized()
	other = other.Normalized()
	return a.Valid() && other.Valid() && a == other
}
