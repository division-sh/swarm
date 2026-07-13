package canonicaljson

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
)

const MaxSafeInteger = 9007199254740991

var jsonNumberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// Bytes returns the canonical JSON encoding used for persisted semantic
// identity. Values are admitted through the same I-JSON-safe semantic owner
// used by transport and persistence decoding.
func Bytes(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return Canonicalize(raw)
}

// Decode admits exactly one semantic JSON value, rejects duplicate keys and
// unsupported numbers, and writes only normalized JSON values to destination.
func Decode(raw []byte, destination any) error {
	value, err := decodeValue(raw)
	if err != nil {
		return err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(canonical, destination)
}

func decodeValue(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeTokenValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON content")
		}
		return nil, fmt.Errorf("trailing JSON content: %w", err)
	}
	return value, nil
}

func decodeTokenValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch typed := token.(type) {
	case nil, bool, string:
		return typed, nil
	case json.Number:
		return NormalizeNumber(typed)
	case json.Delim:
		switch typed {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, fmt.Errorf("JSON object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, fmt.Errorf("duplicate JSON object key %q", key)
				}
				value, err := decodeTokenValue(decoder)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, fmt.Errorf("unterminated JSON object")
			}
			return object, nil
		case '[':
			array := []any{}
			for decoder.More() {
				value, err := decodeTokenValue(decoder)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, fmt.Errorf("unterminated JSON array")
			}
			return array, nil
		}
	}
	return nil, fmt.Errorf("unsupported JSON token %v", token)
}

// NormalizeNumber applies the v1 semantic JSON numeric contract. Integers
// outside the I-JSON safe range, non-finite values, negative zero, and
// unsupported lexical spellings fail closed.
func NormalizeNumber(value any) (float64, error) {
	var number float64
	switch typed := value.(type) {
	case int:
		return normalizeInteger(int64(typed))
	case int8:
		return normalizeInteger(int64(typed))
	case int16:
		return normalizeInteger(int64(typed))
	case int32:
		return normalizeInteger(int64(typed))
	case int64:
		return normalizeInteger(typed)
	case uint:
		return normalizeUnsignedInteger(uint64(typed))
	case uint8:
		return normalizeUnsignedInteger(uint64(typed))
	case uint16:
		return normalizeUnsignedInteger(uint64(typed))
	case uint32:
		return normalizeUnsignedInteger(uint64(typed))
	case uint64:
		return normalizeUnsignedInteger(typed)
	case float32:
		parsed, err := strconv.ParseFloat(strconv.FormatFloat(float64(typed), 'g', -1, 32), 64)
		if err != nil {
			return 0, fmt.Errorf("unsupported JSON number %v: %w", typed, err)
		}
		number = parsed
	case float64:
		number = typed
	case json.Number:
		if !jsonNumberPattern.MatchString(typed.String()) {
			return 0, fmt.Errorf("unsupported JSON number spelling %q", typed)
		}
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			return 0, fmt.Errorf("unsupported JSON number %q: %w", typed, err)
		}
		number = parsed
	default:
		return 0, fmt.Errorf("value %T is not a JSON number", value)
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, fmt.Errorf("non-finite JSON number")
	}
	if number == 0 && math.Signbit(number) {
		return 0, fmt.Errorf("negative zero is not supported")
	}
	if math.Trunc(number) == number && math.Abs(number) > MaxSafeInteger {
		return 0, fmt.Errorf("integer-valued JSON number is outside I-JSON safe range")
	}
	return number, nil
}

func normalizeInteger(value int64) (float64, error) {
	if value < -MaxSafeInteger || value > MaxSafeInteger {
		return 0, fmt.Errorf("integer JSON number is outside I-JSON safe range")
	}
	return float64(value), nil
}

func normalizeUnsignedInteger(value uint64) (float64, error) {
	if value > MaxSafeInteger {
		return 0, fmt.Errorf("integer JSON number is outside I-JSON safe range")
	}
	return float64(value), nil
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
