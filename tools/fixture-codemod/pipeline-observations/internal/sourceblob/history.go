package sourceblob

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type historyRow map[string]any

// The three reviewed old rows retain their entire immutable history. Only their
// current endpoints advance; the one previously unrecorded verify root appends.
func ComposeHistory(prior []byte, pins []Pin, recipes bool) ([]byte, error) {
	var rows []historyRow
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	if len(pins) != len(Rules) {
		return nil, fmt.Errorf("source blob history inventory changed")
	}
	for index, pin := range pins {
		if err := validatePinRule(pin); err != nil {
			return nil, err
		}
		if pin.File != Rules[index].File || pin.Function != Rules[index].Function {
			return nil, fmt.Errorf("source blob history foreign or duplicate pin")
		}
		var err error
		rows, err = composePin(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
	}
	return encodeHistoryRows(prior, rows)
}

func composePin(rows []historyRow, pin Pin, recipes bool) ([]historyRow, error) {
	match := -1
	for index, row := range rows {
		if row["File"] == pin.File && row["Function"] == pin.Function {
			if match != -1 {
				return nil, fmt.Errorf("source blob history ambiguous declaration: %s", pin.Function)
			}
			match = index
		}
	}
	existing := pin.Function != Rules[1].Function
	if (match != -1) != existing {
		return nil, fmt.Errorf("source blob history missing or foreign row: %s", pin.Function)
	}
	if match == -1 {
		row := historyRow{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if recipes {
			row["Family"] = "persisted-source-blob-family145"
		}
		return append(rows, row), nil
	}
	row := rows[match]
	current := row["After"]
	if recipes && row["Successor"] != nil {
		current = row["Successor"]
	}
	if current != pin.Before || row["Removed"] != nil {
		return nil, fmt.Errorf("source blob exact old endpoint changed: %s", pin.Function)
	}
	key := "After"
	if recipes {
		key = "Successor"
	}
	row[key] = pin.After
	return rows, nil
}

func encodeHistoryRows(prior []byte, rows []historyRow) ([]byte, error) {
	var original []json.RawMessage
	if err := json.Unmarshal(prior, &original); err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, len(rows))
	for index, row := range rows {
		if index < len(original) {
			var old historyRow
			if err := json.Unmarshal(original[index], &old); err != nil {
				return nil, err
			}
			if reflect.DeepEqual(old, row) {
				out[index] = original[index]
				continue
			}
		}
		raw, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		out[index] = raw
	}
	return json.MarshalIndent(out, "", "  ")
}

// RestoreHistory is a comparison copy: complete old rows replace only exact
// reviewed current records; only the one named new verify root are removed.
func RestoreHistory(current, prior []byte, pins []Pin, recipes bool) ([]byte, error) {
	expected, err := ComposeHistory(prior, pins, recipes)
	if err != nil {
		return nil, err
	}
	var rows, old, next []historyRow
	for _, input := range []struct {
		raw  []byte
		rows *[]historyRow
	}{{current, &rows}, {prior, &old}, {expected, &next}} {
		if err := json.Unmarshal(input.raw, input.rows); err != nil {
			return nil, err
		}
	}
	oldByKey, nextByKey := historyIndex(old), historyIndex(next)
	seen := map[string]bool{}
	var restored []historyRow
	for _, row := range rows {
		key := historyKey(row)
		if historyPin(pins, row) {
			if seen[key] || !reflect.DeepEqual(row, nextByKey[key]) {
				return nil, fmt.Errorf("source blob history endpoint or metadata changed: %s", key)
			}
			seen[key] = true
			row = oldByKey[key]
		}
		if row != nil {
			restored = append(restored, row)
		}
	}
	if len(seen) != len(pins) {
		return nil, fmt.Errorf("source blob history missing endpoint")
	}
	return json.Marshal(restored)
}

func historyIndex(rows []historyRow) map[string]historyRow {
	index := map[string]historyRow{}
	for _, row := range rows {
		index[historyKey(row)] = row
	}
	return index
}

func historyKey(row historyRow) string { return fmt.Sprint(row["File"], "/", row["Function"]) }

func historyPin(pins []Pin, row historyRow) bool {
	for _, pin := range pins {
		if row["File"] == pin.File && row["Function"] == pin.Function {
			return true
		}
	}
	return false
}
