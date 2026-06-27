package builder

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func methodUnavailable(message string) *RPCError {
	return &RPCError{Code: -32004, Message: strings.TrimSpace(message)}
}

func internalError(err error) *RPCError {
	if err == nil {
		err = errors.New("internal error")
	}
	return &RPCError{Code: -32000, Message: strings.TrimSpace(err.Error())}
}

func errUnavailable(message string) error { return errors.New(strings.TrimSpace(message)) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func asStringSlice(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		if stringsSlice, ok := v.([]string); ok {
			out := make([]string, 0, len(stringsSlice))
			for _, item := range stringsSlice {
				if item = strings.TrimSpace(item); item != "" {
					out = append(out, item)
				}
			}
			return out
		}
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if value := strings.TrimSpace(asString(item)); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func credentialRecords(items []runtimecredentials.Descriptor) []CredentialRecord {
	out := make([]CredentialRecord, 0, len(items))
	for _, item := range items {
		out = append(out, credentialRecord(item))
	}
	return out
}

func credentialRecord(item runtimecredentials.Descriptor) CredentialRecord {
	record := CredentialRecord{
		Key:      item.Key,
		Present:  item.Present,
		Source:   item.Source,
		Writable: item.Writable,
		Shadowed: item.Shadowed,
	}
	if item.UpdatedAt != nil && !item.UpdatedAt.IsZero() {
		record.UpdatedAt = item.UpdatedAt.UTC().Format(time.RFC3339)
	}
	for _, ref := range item.RequiredBy {
		record.RequiredBy = append(record.RequiredBy, CredentialRequirement{
			Kind: ref.Kind,
			Name: ref.Name,
		})
	}
	return record
}

func strconvQuote(value string) string {
	raw, err := json.Marshal(strings.TrimSpace(value))
	if err != nil {
		return `""`
	}
	return string(raw)
}
