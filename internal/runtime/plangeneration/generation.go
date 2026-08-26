package plangeneration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

const encodedPrefix = "sha256:"

// Generation is the immutable identity of one complete compiled plan.
// Text exists only at the durable JSON/storage codec boundary.
type Generation struct {
	digest [sha256.Size]byte
	valid  bool
}

func FromCanonicalValue(value any) (Generation, error) {
	encoded, err := canonicaljson.Hash(value)
	if err != nil {
		return Generation{}, err
	}
	return decode(encoded)
}

// Parse decodes the canonical durable representation of a plan generation.
func Parse(encoded string) (Generation, error) {
	return decode(encoded)
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
	return encodedPrefix + hex.EncodeToString(g.digest[:])
}

func (g Generation) MarshalJSON() ([]byte, error) {
	if !g.valid {
		return nil, fmt.Errorf("plan generation is missing")
	}
	return json.Marshal(g.Diagnostic())
}

func (g *Generation) UnmarshalJSON(raw []byte) error {
	if g == nil {
		return fmt.Errorf("plan generation destination is nil")
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return fmt.Errorf("decode plan generation: %w", err)
	}
	decoded, err := decode(encoded)
	if err != nil {
		return err
	}
	*g = decoded
	return nil
}

func decode(encoded string) (Generation, error) {
	if encoded != strings.TrimSpace(encoded) || !strings.HasPrefix(encoded, encodedPrefix) {
		return Generation{}, fmt.Errorf("plan generation must use canonical sha256 encoding")
	}
	raw := strings.TrimPrefix(encoded, encodedPrefix)
	if len(raw) != sha256.Size*2 {
		return Generation{}, fmt.Errorf("plan generation sha256 digest has invalid length")
	}
	if raw != strings.ToLower(raw) {
		return Generation{}, fmt.Errorf("plan generation sha256 digest must use lowercase hexadecimal")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return Generation{}, fmt.Errorf("plan generation sha256 digest is invalid: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return Generation{digest: digest, valid: true}, nil
}
