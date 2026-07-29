package triggergeneration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Generation is the immutable identity of one admitted provider-trigger
// catalog. Text exists only at wire, durable, and diagnostic boundaries.
type Generation struct {
	digest [sha256.Size]byte
	valid  bool
}

func FromCanonicalBytes(body []byte) Generation {
	return Generation{digest: sha256.Sum256(body), valid: true}
}

func Parse(encoded string) (Generation, error) {
	if encoded != strings.TrimSpace(encoded) || len(encoded) != sha256.Size*2 || encoded != strings.ToLower(encoded) {
		return Generation{}, fmt.Errorf("trigger catalog generation must use canonical lowercase sha256 hexadecimal")
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return Generation{}, fmt.Errorf("trigger catalog generation is invalid: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return Generation{digest: digest, valid: true}, nil
}

func (g Generation) Valid() bool {
	return g.valid
}

func (g Generation) Equal(other Generation) bool {
	return g.valid && other.valid && g.digest == other.digest
}

func (g Generation) Diagnostic() string {
	if !g.valid {
		return ""
	}
	return hex.EncodeToString(g.digest[:])
}

func (g Generation) MatchesDiagnostic(encoded string) bool {
	other, err := Parse(encoded)
	return err == nil && g.Equal(other)
}

func (g Generation) MarshalJSON() ([]byte, error) {
	if !g.valid {
		return nil, fmt.Errorf("trigger catalog generation is missing")
	}
	return json.Marshal(g.Diagnostic())
}

func (g *Generation) UnmarshalJSON(raw []byte) error {
	if g == nil {
		return fmt.Errorf("trigger catalog generation destination is nil")
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return fmt.Errorf("decode trigger catalog generation: %w", err)
	}
	decoded, err := Parse(encoded)
	if err != nil {
		return err
	}
	*g = decoded
	return nil
}
