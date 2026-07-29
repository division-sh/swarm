package manifesthash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
)

var canonicalPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Hash is a canonical immutable pack-manifest digest.
type Hash struct {
	value string
}

func Parse(raw string) (Hash, error) {
	if !canonicalPattern.MatchString(raw) {
		return Hash{}, fmt.Errorf("manifest hash must use canonical sha256:<64 lowercase hex>")
	}
	return Hash{value: raw}, nil
}

func MustParse(raw string) Hash {
	hash, err := Parse(raw)
	if err != nil {
		panic(err)
	}
	return hash
}

func FromBytes(body []byte) Hash {
	sum := sha256.Sum256(body)
	return Hash{value: "sha256:" + hex.EncodeToString(sum[:])}
}

func (h Hash) String() string {
	return h.value
}

func (h Hash) Valid() bool {
	return canonicalPattern.MatchString(h.value)
}

func (h Hash) Equal(other Hash) bool {
	return h.Valid() && other.Valid() && h.value == other.value
}
