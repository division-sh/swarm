package mailboxpersistence

import (
	"fmt"
	"strings"
	"time"
)

func sqliteNullUUID(raw string) any {
	return sqliteNullString(raw)
}

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if value.IsZero() {
			return time.Time{}, false, nil
		}
		return value.UTC(), true, nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid sqlite time %q", raw)
}
