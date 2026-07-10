package providertriggers

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
)

// StampPackEnvelope is the only trigger-pack envelope authoring path. The
// body-specific owner derives summaries; the universal owner stamps body bytes.
func StampPackEnvelope(envelopeBody, manifestBody []byte) (packs.Envelope, []byte, error) {
	envelope, err := packs.ParseEnvelope(envelopeBody)
	if err != nil {
		return packs.Envelope{}, nil, fmt.Errorf("parse trigger pack envelope: %w", err)
	}
	if strings.TrimSpace(envelope.Type) != packs.TypeTrigger {
		return packs.Envelope{}, nil, fmt.Errorf("pack %q type must be %q for trigger stamping", envelope.ID, packs.TypeTrigger)
	}
	manifest, err := parseManifestStrict(manifestBody)
	if err != nil {
		return packs.Envelope{}, nil, fmt.Errorf("parse trigger manifest for pack %q: %w", envelope.ID, err)
	}
	if err := manifest.Validate(); err != nil {
		return packs.Envelope{}, nil, fmt.Errorf("validate trigger manifest for pack %q: %w", envelope.ID, err)
	}
	envelope.Capabilities = DerivedCapabilities(manifest)
	envelope.Requires = DerivedRequires(manifest)
	return packs.StampEnvelope(envelope, manifestBody)
}
