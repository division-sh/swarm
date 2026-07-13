package canonicaljson

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

// Bytes returns the canonical JSON encoding used for persisted semantic
// identity. encoding/json orders object keys; decoding with UseNumber keeps
// numeric tokens stable instead of routing them through float64.
func Bytes(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return Canonicalize(raw)
}

// Decode preserves JSON number tokens in interface-valued fields. Semantic
// values that cross persistence boundaries must not be routed through float64.
func Decode(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON content")
	}
	return nil
}

func Canonicalize(raw []byte) ([]byte, error) {
	var value any
	if err := Decode(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func Hash(value any) (string, error) {
	raw, err := Bytes(value)
	if err != nil {
		return "", err
	}
	return HashBytes(raw), nil
}

func HashRaw(raw []byte) (string, error) {
	canonical, err := Canonicalize(raw)
	if err != nil {
		return "", err
	}
	return HashBytes(canonical), nil
}

func HashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
