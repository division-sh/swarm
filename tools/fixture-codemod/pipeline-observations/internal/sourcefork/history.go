package sourcefork

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

type historyRow map[string]any

// ComposeHistory advances only the six reviewed current endpoints. Historical
// fields and relative order are retained, including the timer's prior Successor.
func ComposeHistory(prior []byte, pins []Pin, recipes bool) ([]byte, error) {
	var rows []historyRow
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	if len(pins) != len(Rules) {
		return nil, fmt.Errorf("source fork history inventory changed")
	}
	for index, pin := range pins {
		if pin.File != Rules[index].File || pin.Function != Rules[index].Function {
			return nil, fmt.Errorf("source fork history foreign or duplicate pin")
		}
		var err error
		rows, err = composePin(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
	}
	return encodeHistoryRows(prior, rows)
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

func composePin(rows []historyRow, pin Pin, recipes bool) ([]historyRow, error) {
	match := -1
	for index, row := range rows {
		if row["File"] == pin.File && row["Function"] == pin.Function {
			if match != -1 {
				return nil, fmt.Errorf("source fork history ambiguous declaration: %s", pin.Function)
			}
			match = index
		}
	}
	wantExisting := pin.Function == Rules[0].Function || pin.Function == Rules[4].Function || recipes && pin.Function == Rules[1].Function
	if (match != -1) != wantExisting {
		return nil, fmt.Errorf("source fork history missing or foreign row: %s", pin.Function)
	}
	if match == -1 {
		row := historyRow{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if recipes {
			row["Family"] = "source-fork-cardinality-family145"
		}
		return append(rows, row), nil
	}
	row := rows[match]
	current, ok := row["After"].(string)
	if recipes && row["Successor"] != nil {
		current, ok = row["Successor"].(string)
	}
	before, err := requestkey.Normalize(pin.Before)
	canonical, canonicalErr := requestkey.Normalize(current)
	formatOnly := pin.Function == Rules[1].Function && canonical == before
	if !ok || err != nil || canonicalErr != nil || current != pin.Before && !formatOnly || row["Removed"] != nil {
		return nil, fmt.Errorf("source fork history predecessor differs: %s", pin.Function)
	}
	key := "After"
	if recipes {
		key = "Successor"
	}
	row[key] = pin.After
	return rows, nil
}

// RestoreHistory is a comparison-only inverse. It restores complete old rows
// only from exact reviewed endpoints, and removes only the named new records.
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
				return nil, fmt.Errorf("source fork history endpoint or metadata changed: %s", key)
			}
			seen[key] = true
			row = oldByKey[key]
		}
		if row != nil {
			restored = append(restored, row)
		}
	}
	if len(seen) != len(pins) {
		return nil, fmt.Errorf("source fork history missing endpoint")
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
