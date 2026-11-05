package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
)

var manifestHashPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ManifestHash is a canonical immutable pack-manifest digest.
type ManifestHash struct {
	digest string
}

func ParseManifestHash(raw string) (ManifestHash, error) {
	if !manifestHashPattern.MatchString(raw) {
		return ManifestHash{}, fmt.Errorf("manifest hash must use canonical sha256:<64 lowercase hex>")
	}
	return ManifestHash{digest: raw}, nil
}

func MustParseManifestHash(raw string) ManifestHash {
	hash, err := ParseManifestHash(raw)
	if err != nil {
		panic(err)
	}
	return hash
}

func ManifestHashFromBytes(body []byte) ManifestHash {
	sum := sha256.Sum256(body)
	return ManifestHash{digest: "sha256:" + hex.EncodeToString(sum[:])}
}

func (h ManifestHash) String() string {
	return h.digest
}

func (h ManifestHash) Valid() bool {
	return manifestHashPattern.MatchString(h.digest)
}

func (h ManifestHash) Equal(other ManifestHash) bool {
	return h.Valid() && other.Valid() && h.digest == other.digest
}
