package canonicaljson

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

const MaxSafeInteger = semanticvalue.MaxSafeInteger

var jsonNumberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// AdmissionError identifies syntactically valid JSON that violates the
// platform's semantic value contract.
type AdmissionError struct {
	err error
}

func (e *AdmissionError) Error() string { return e.err.Error() }
func (e *AdmissionError) Unwrap() error { return e.err }

func IsAdmissionError(err error) bool {
	var target *AdmissionError
	return errors.As(err, &target)
}

func admissionErrorf(format string, args ...any) error {
	return &AdmissionError{err: fmt.Errorf(format, args...)}
}

// Decode admits exactly one JSON value into the closed semantic value model.
func Decode(raw []byte) (semanticvalue.Value, error) {
	if !utf8.Valid(raw) {
		return semanticvalue.Value{}, fmt.Errorf("semantic JSON is not valid UTF-8")
	}
	if err := validateStringLexemes(raw); err != nil {
		return semanticvalue.Value{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeTokenValue(decoder)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return semanticvalue.Value{}, fmt.Errorf("trailing JSON content")
		}
		return semanticvalue.Value{}, fmt.Errorf("trailing JSON content: %w", err)
	}
	return value, nil
}

// DecodeInto is a centralized typed adapter. The admitted Value remains the
// semantic owner; destination is only a transport projection.
func DecodeInto(raw []byte, destination any) error {
	value, err := Decode(raw)
	if err != nil {
		return err
	}
	return ValueInto(value, destination)
}

func ValueInto(value semanticvalue.Value, destination any) error {
	canonical, err := Encode(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(canonical, destination)
}

// FromGo converts a typed programmatic value through its declared JSON DTO
// projection and admits the result into the semantic model.
func FromGo(value any) (semanticvalue.Value, error) {
	if admitted, ok := value.(semanticvalue.Value); ok {
		return admitted, nil
	}
	if err := validateGoStrings(reflect.ValueOf(value), map[visit]struct{}{}); err != nil {
		return semanticvalue.Value{}, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return semanticvalue.Value{}, err
	}
	return Decode(raw)
}

type visit struct {
	typeID reflect.Type
	ptr    uintptr
}

func validateGoStrings(value reflect.Value, seen map[visit]struct{}) error {
	if !value.IsValid() {
		return nil
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return fmt.Errorf("programmatic semantic string is not valid UTF-8")
		}
	case reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		key := visit{typeID: value.Type(), ptr: value.Pointer()}
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		return validateGoStrings(value.Elem(), seen)
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		key := visit{typeID: value.Type(), ptr: value.Pointer()}
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		iter := value.MapRange()
		for iter.Next() {
			if err := validateGoStrings(iter.Key(), seen); err != nil {
				return err
			}
			if err := validateGoStrings(iter.Value(), seen); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if value.IsNil() {
			return nil
		}
		key := visit{typeID: value.Type(), ptr: value.Pointer()}
		if value.Pointer() != 0 {
			if _, ok := seen[key]; ok {
				return nil
			}
			seen[key] = struct{}{}
		}
		for i := 0; i < value.Len(); i++ {
			if err := validateGoStrings(value.Index(i), seen); err != nil {
				return err
			}
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := validateGoStrings(value.Index(i), seen); err != nil {
				return err
			}
		}
	case reflect.Struct:
		typeOf := value.Type()
		for i := 0; i < value.NumField(); i++ {
			if typeOf.Field(i).PkgPath != "" {
				continue
			}
			if err := validateGoStrings(value.Field(i), seen); err != nil {
				return err
			}
		}
	}
	return nil
}

// Bytes converts a typed DTO or admitted Value to canonical semantic JSON.
func Bytes(value any) ([]byte, error) {
	admitted, err := FromGo(value)
	if err != nil {
		return nil, err
	}
	return Encode(admitted)
}

func Encode(value semanticvalue.Value) ([]byte, error) {
	return appendValue(nil, value)
}

func appendValue(dst []byte, value semanticvalue.Value) ([]byte, error) {
	switch value.Kind() {
	case semanticvalue.KindNull:
		return append(dst, "null"...), nil
	case semanticvalue.KindBool:
		boolean, _ := value.Bool()
		return strconv.AppendBool(dst, boolean), nil
	case semanticvalue.KindNumber:
		number, _ := value.Number()
		raw, err := json.Marshal(number)
		if err != nil {
			return nil, err
		}
		return append(dst, raw...), nil
	case semanticvalue.KindString:
		text, _ := value.String()
		raw, err := json.Marshal(text)
		if err != nil {
			return nil, err
		}
		return append(dst, raw...), nil
	case semanticvalue.KindArray:
		dst = append(dst, '[')
		for i := 0; i < value.Len(); i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			item, _ := value.At(i)
			var err error
			dst, err = appendValue(dst, item)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case semanticvalue.KindObject:
		dst = append(dst, '{')
		for i, member := range value.Members() {
			if i > 0 {
				dst = append(dst, ',')
			}
			raw, err := json.Marshal(member.Name)
			if err != nil {
				return nil, err
			}
			dst = append(dst, raw...)
			dst = append(dst, ':')
			dst, err = appendValue(dst, member.Value)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	default:
		return nil, fmt.Errorf("unsupported semantic value kind %d", value.Kind())
	}
}

func decodeTokenValue(decoder *json.Decoder) (semanticvalue.Value, error) {
	token, err := decoder.Token()
	if err != nil {
		return semanticvalue.Value{}, err
	}
	switch typed := token.(type) {
	case nil:
		return semanticvalue.Null(), nil
	case bool:
		return semanticvalue.Bool(typed), nil
	case string:
		return semanticvalue.String(typed)
	case json.Number:
		return decodeNumber(typed.String())
	case json.Delim:
		switch typed {
		case '{':
			entries := []semanticvalue.ObjectEntry{}
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return semanticvalue.Value{}, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return semanticvalue.Value{}, fmt.Errorf("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return semanticvalue.Value{}, admissionErrorf("duplicate JSON object key %q", key)
				}
				seen[key] = struct{}{}
				value, err := decodeTokenValue(decoder)
				if err != nil {
					return semanticvalue.Value{}, err
				}
				entries = append(entries, semanticvalue.ObjectEntry{Name: key, Value: value})
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return semanticvalue.Value{}, fmt.Errorf("unterminated JSON object")
			}
			return semanticvalue.Object(entries)
		case '[':
			values := []semanticvalue.Value{}
			for decoder.More() {
				value, err := decodeTokenValue(decoder)
				if err != nil {
					return semanticvalue.Value{}, err
				}
				values = append(values, value)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return semanticvalue.Value{}, fmt.Errorf("unterminated JSON array")
			}
			return semanticvalue.Array(values), nil
		}
	}
	return semanticvalue.Value{}, fmt.Errorf("unsupported JSON token %v", token)
}

func decodeNumber(raw string) (semanticvalue.Value, error) {
	if !jsonNumberPattern.MatchString(raw) {
		return semanticvalue.Value{}, admissionErrorf("unsupported JSON number spelling %q", raw)
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if math.IsInf(parsed, 0) {
		return semanticvalue.Value{}, admissionErrorf("JSON number %q overflows binary64", raw)
	}
	if parsed == 0 && !lexicallyZero(raw) {
		return semanticvalue.Value{}, admissionErrorf("JSON number %q underflows binary64 to zero", raw)
	}
	if err != nil && parsed == 0 {
		return semanticvalue.Value{}, admissionErrorf("unsupported JSON number %q: %v", raw, err)
	}
	value, valueErr := semanticvalue.Number(parsed)
	if valueErr != nil {
		return semanticvalue.Value{}, admissionErrorf("unsupported JSON number %q: %v", raw, valueErr)
	}
	return value, nil
}

func lexicallyZero(raw string) bool {
	for _, char := range raw {
		if char == 'e' || char == 'E' {
			break
		}
		switch char {
		case '-', '+', '.':
			continue
		case '0':
			continue
		default:
			if char >= '1' && char <= '9' {
				return false
			}
		}
	}
	return true
}

// NormalizeNumber is a compatibility adapter for typed validation code in the
// approved child. It returns the closed model's binary64 representation.
func NormalizeNumber(value any) (float64, error) {
	var raw string
	switch typed := value.(type) {
	case int:
		raw = strconv.FormatInt(int64(typed), 10)
	case int8:
		raw = strconv.FormatInt(int64(typed), 10)
	case int16:
		raw = strconv.FormatInt(int64(typed), 10)
	case int32:
		raw = strconv.FormatInt(int64(typed), 10)
	case int64:
		raw = strconv.FormatInt(typed, 10)
	case uint:
		raw = strconv.FormatUint(uint64(typed), 10)
	case uint8:
		raw = strconv.FormatUint(uint64(typed), 10)
	case uint16:
		raw = strconv.FormatUint(uint64(typed), 10)
	case uint32:
		raw = strconv.FormatUint(uint64(typed), 10)
	case uint64:
		raw = strconv.FormatUint(typed, 10)
	case float32:
		raw = strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		value, err := semanticvalue.Number(typed)
		if err != nil {
			return 0, err
		}
		number, _ := value.Number()
		return number, nil
	case json.Number:
		raw = typed.String()
	case semanticvalue.Value:
		number, ok := typed.Number()
		if !ok {
			return 0, fmt.Errorf("semantic value is not a number")
		}
		return number, nil
	default:
		return 0, fmt.Errorf("value %T is not a semantic number", value)
	}
	admitted, err := decodeNumber(raw)
	if err != nil {
		return 0, err
	}
	number, _ := admitted.Number()
	return number, nil
}

func Canonicalize(raw []byte) ([]byte, error) {
	value, err := Decode(raw)
	if err != nil {
		return nil, err
	}
	return Encode(value)
}

func Hash(value any) (string, error) {
	admitted, err := FromGo(value)
	if err != nil {
		return "", err
	}
	return HashValue(admitted)
}

func HashValue(value semanticvalue.Value) (string, error) {
	raw, err := Encode(value)
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

func validateStringLexemes(raw []byte) error {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		for i++; i < len(raw); i++ {
			switch raw[i] {
			case '"':
				goto nextString
			case '\\':
				i++
				if i >= len(raw) {
					return fmt.Errorf("unterminated JSON string escape")
				}
				if raw[i] != 'u' {
					continue
				}
				first, next, err := decodeUnicodeEscape(raw, i)
				if err != nil {
					return err
				}
				i = next - 1
				if first >= 0xD800 && first <= 0xDBFF {
					if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
						return fmt.Errorf("unpaired high surrogate in JSON string")
					}
					second, after, err := decodeUnicodeEscape(raw, i+2)
					if err != nil || second < 0xDC00 || second > 0xDFFF || utf16.DecodeRune(rune(first), rune(second)) == utf8.RuneError {
						return fmt.Errorf("invalid surrogate pair in JSON string")
					}
					i = after - 1
				} else if first >= 0xDC00 && first <= 0xDFFF {
					return fmt.Errorf("unpaired low surrogate in JSON string")
				}
			default:
				if raw[i] < 0x20 {
					return fmt.Errorf("unescaped control character in JSON string")
				}
			}
		}
		return fmt.Errorf("unterminated JSON string")
	nextString:
	}
	return nil
}

func decodeUnicodeEscape(raw []byte, uIndex int) (uint16, int, error) {
	if uIndex+5 > len(raw) {
		return 0, 0, fmt.Errorf("short Unicode escape in JSON string")
	}
	value, err := strconv.ParseUint(string(raw[uIndex+1:uIndex+5]), 16, 16)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Unicode escape in JSON string")
	}
	return uint16(value), uIndex + 5, nil
}
