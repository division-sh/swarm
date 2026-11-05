package operatorchannel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Only this private file codec accepts the two canonical rc.1 account members.
// Unknown members survive normalization for the final strict record decode.
func decodeProofDocumentV2(raw []byte) (proofDocument, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := consumeProofJSONValue(decoder); err != nil {
		return proofDocument{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return proofDocument{}, fmt.Errorf("proof document has trailing JSON content")
	}
	var stored struct {
		Version int                        `json:"version"`
		Entries map[string]json.RawMessage `json:"entries"`
	}
	if err := decodeClosedProofJSON(raw, &stored); err != nil {
		return proofDocument{}, err
	}
	doc := proofDocument{Version: stored.Version}
	if stored.Entries != nil {
		doc.Entries = make(map[string]proofRecord, len(stored.Entries))
	}
	for key, rawRecord := range stored.Entries {
		normalized, err := normalizeProofRecordV2(rawRecord)
		if err != nil {
			return proofDocument{}, fmt.Errorf("proof entry %q: %w", key, err)
		}
		var record proofRecord
		if err := decodeClosedProofJSON(normalized, &record); err != nil {
			return proofDocument{}, err
		}
		doc.Entries[key] = record
	}
	return doc, nil
}

func decodeClosedProofJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func normalizeProofRecordV2(raw []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if err := normalizeProofAccountMember(fields, "external_account_reference", "external_account"); err != nil {
		return nil, err
	}
	if authority, present := fields["provider_authority"]; present {
		normalized, err := normalizeProofAuthorityV2(authority)
		if err != nil {
			return nil, err
		}
		fields["provider_authority"] = normalized
	}
	return json.Marshal(fields)
}

func normalizeProofAuthorityV2(raw []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if rawSession, present := fields["session_account"]; present {
		var session map[string]json.RawMessage
		if err := json.Unmarshal(rawSession, &session); err != nil {
			return nil, err
		}
		if err := normalizeProofAccountMember(session, "account_reference", "operator_account"); err != nil {
			return nil, err
		}
		normalized, err := json.Marshal(session)
		if err != nil {
			return nil, err
		}
		fields["session_account"] = normalized
	}
	return json.Marshal(fields)
}

func normalizeProofAccountMember(fields map[string]json.RawMessage, retired, current string) error {
	for name := range fields {
		if name != retired && strings.EqualFold(name, retired) || name != current && strings.EqualFold(name, current) {
			return fmt.Errorf("json: unknown field %q", name)
		}
	}
	oldValue, oldPresent := fields[retired]
	value, present := fields[current]
	if oldPresent && present {
		return fmt.Errorf("proof account members %q and %q cannot coexist", retired, current)
	}
	if oldPresent {
		value, present = oldValue, true
	}
	if present {
		var text *string
		if err := json.Unmarshal(value, &text); err != nil || text == nil {
			return fmt.Errorf("proof account member %q must be a non-null string", current)
		}
	}
	if oldPresent {
		fields[current] = oldValue
		delete(fields, retired)
	}
	return nil
}

// Token traversal detects duplicates before RawMessage maps can discard them.
// UseNumber above keeps unrelated int64 proof revisions lossless.
func consumeProofJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		if delimiter == '{' {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := token.(string)
			if !ok {
				return fmt.Errorf("proof object member must be a string")
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("duplicate proof JSON member %q", name)
			}
			seen[name] = struct{}{}
		}
		if err := consumeProofJSONValue(decoder); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}
