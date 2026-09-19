package eventrecord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

// Frozen comparator and decoder from before the physical parsing experiment.
func jsonEqualBefore(left, right []byte) bool {
	l, err := decodeJSONBefore(left)
	if err != nil {
		return bytes.Equal(bytes.TrimSpace(left), bytes.TrimSpace(right))
	}
	r, err := decodeJSONBefore(right)
	if err != nil {
		return false
	}
	return equalJSONValueBefore(l, r)
}

func decodeJSONBefore(raw []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	var trailing any
	if err := d.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return v, nil
}

func equalJSONValueBefore(left, right any) bool {
	switch l := left.(type) {
	case json.Number:
		r, ok := right.(json.Number)
		if !ok {
			return false
		}
		ln, lok := new(big.Rat).SetString(l.String())
		rn, rok := new(big.Rat).SetString(r.String())
		return lok && rok && ln.Cmp(rn) == 0
	case []any:
		r, ok := right.([]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for i := range l {
			if !equalJSONValueBefore(l[i], r[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		r, ok := right.(map[string]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for k, v := range l {
			other, exists := r[k]
			if !exists || !equalJSONValueBefore(v, other) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

func assertJSONEqualPhysicalParity(t testing.TB, left, right []byte) {
	t.Helper()
	leftCopy, rightCopy := bytes.Clone(left), bytes.Clone(right)
	want := jsonEqualBefore(left, right)
	if got := jsonEqual(left, right); got != want {
		t.Fatalf("production(%q,%q)=%v, frozen=%v", left, right, got, want)
	}
	for _, raw := range [][]byte{left, right} {
		before, oldErr := decodeJSONBefore(raw)
		after, newErr := decodeJSON(raw)
		if (oldErr == nil) != (newErr == nil) || oldErr == nil && !reflect.DeepEqual(before, after) {
			t.Fatalf("decode(%q): before=%#v/%v after=%#v/%v", raw, before, oldErr, after, newErr)
		}
	}
	if !bytes.Equal(left, leftCopy) || !bytes.Equal(right, rightCopy) {
		t.Fatal("comparison modified input bytes")
	}
}

func TestJSONEqualPhysicalDifferential(t *testing.T) {
	corpus := []string{
		``, ` `, "\t\r\n", `null`, `true`, `false`, `[]`, `{}`, `[null]`, `[true,false]`,
		`""`, `"123-45e67"`, `"null"`, `"a"`, `"\u0061"`, `"\u003c&\u003e"`, `"<&>"`,
		`"\\"`, `"\"12"`, `"\\\"123"`, `"\u0022-1"`, `"\ud800"`, `"\udfff"`, `"\ufffd"`,
		"\"\xff\"", "\"\xc0\xaf\"", "\"a\x00b\"", "\ufeffnull", "\u00a0null", "null\u00a0", "\u2003{}\u2003",
		`{"a":null}`, `{"b":null}`, `{"a":[]}`, `{"a":{}}`, `{"a":true}`, `{"A":true}`,
		`{"a":"x","a":"y"}`, `{"a":"y"}`, `{"a":null,"a":"y"}`, `{"a":"y","a":null}`,
		`{"a":"x","\u0061":"y"}`, `{"\ud800":"x"}`, "{\"\xff\":\"x\"}",
		`{"long":[null,{"b":"2","a":"1"}],"x":false}`, `{"x": false,"long": [null,{"a":"1","b":"2"}]}`,
		`null null`, `{}[]`, `[]x`, `true false`, `null/*x*/`, `NaN`, `Infinity`, `{"a":}`, `{"a":null,}`,
		`"unterminated`, `"dangling\`, `"\x31"`, `"\u00"`, `"\"0`, `"abc"0`, `['x']`,
		`0`, `-0`, `0.0`, `1`, `1.0`, `1e0`, `1e2`, `100`, `-1`, `0.1`, `0.10`,
		`9007199254740992`, `9007199254740993`, `-9007199254740993`, `1e-30`, `0.000000000000000000000000000001`,
		`01`, `+1`, `1.`, `1e`, `-`, `{"n":1,"n":2}`, `{"n":2}`, `{"n":"2"}`, `[1,null,"3"]`,
	}
	for i, left := range corpus {
		for j, right := range corpus {
			t.Run(fmt.Sprintf("pair_%d_%d", i, j), func(t *testing.T) {
				assertJSONEqualPhysicalParity(t, []byte(left), []byte(right))
			})
		}
	}
	for _, raw := range []string{
		`1e1000001`, `1e-1000001`, `0e1000001`, `-0e-1000001`, `1e9999999999999999999999`,
		`{"n":1e1000001}`, `{"n":1e1000001,"n":"last"}`, `{"n":"last","n":1e1000001}`,
		strings.Repeat("[", 10000) + `null` + strings.Repeat("]", 10000),
		strings.Repeat("[", 10001) + `null` + strings.Repeat("]", 10001),
	} {
		assertJSONEqualPhysicalParity(t, []byte(raw), []byte(raw))
		assertJSONEqualPhysicalParity(t, []byte(raw), []byte(`null`))
		assertJSONEqualPhysicalParity(t, []byte(`null`), []byte(raw))
	}
	truncated := `{"quoted\\\"1":"\ud800","a":[null,true,{"n":1e2}],"a":{"v":"-0"}}`
	for i := 0; i <= len(truncated); i++ {
		raw := []byte(truncated[:i])
		assertJSONEqualPhysicalParity(t, raw, raw)
		assertJSONEqualPhysicalParity(t, raw, append(bytes.Clone(raw), ' '))
	}
	for _, plans := range []int{0, 1, 18} {
		_, settlement, _ := admittedSerializationFixture(t, plans, "consumer/quoted\"123\\end")
		raw, err := settlement.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		formatted := equalityFormattedJSON(t, raw)
		assertJSONEqualPhysicalParity(t, raw, formatted)
		assertJSONEqualPhysicalParity(t, formatted, raw)
		assertJSONEqualPhysicalParity(t, raw, bytes.ReplaceAll(formatted, []byte("consumer"), []byte("different")))
	}
}

func TestJSONEqualPhysicalNumericAndMalformedContracts(t *testing.T) {
	for _, tc := range []struct {
		left, right string
		want        bool
	}{
		{`1e1000001`, `1e1000001`, false},
		{`0e1000001`, `0`, true},
		{`1e`, `1e`, true},
		{"\u00a0null", `null`, true},
		{`null`, "\u00a0null", false},
		{`9007199254740992`, `9007199254740993`, false},
		{`{"a":1e1000001,"a":"x"}`, `{"a":"x"}`, true},
	} {
		if got := jsonEqualBefore([]byte(tc.left), []byte(tc.right)); got != tc.want {
			t.Fatalf("frozen contract(%q,%q)=%v want=%v", tc.left, tc.right, got, tc.want)
		}
		assertJSONEqualPhysicalParity(t, []byte(tc.left), []byte(tc.right))
	}
}

func TestRecordEqualPhysicalJSONFields(t *testing.T) {
	fields := []struct {
		name string
		set  func(*Record, []byte)
	}{
		{"source", func(r *Record, raw []byte) { r.SourceRoute = raw }},
		{"target", func(r *Record, raw []byte) { r.TargetRoute = raw }},
		{"targets", func(r *Record, raw []byte) { r.TargetSet = raw }},
		{"settlement", func(r *Record, raw []byte) { r.RouteSettlement = raw }},
		{"origin", func(r *Record, raw []byte) { r.InheritedFanOutOrigin = raw }},
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			for _, pair := range [][2]string{
				{`{"b":"2","a":[null,true]}`, `{"a":[null,true],"b":"2"}`},
				{`{"a":null}`, `{"b":null}`}, {`1`, `1e0`}, {`1e1000001`, `1e1000001`},
				{`[1,2]`, `[2,1]`}, {`null`, `[]`}, {`1e`, `1e`},
				{"\u00a0null", `null`}, {`null`, "\u00a0null"},
			} {
				var left, right Record
				field.set(&left, []byte(pair[0]))
				field.set(&right, []byte(pair[1]))
				beforeLeft, beforeRight := left.Clone(), right.Clone()
				want := jsonEqualBefore([]byte(pair[0]), []byte(pair[1]))
				if got := left.Equal(right); got != want {
					t.Fatalf("Record.Equal(%q,%q)=%v want=%v", pair[0], pair[1], got, want)
				}
				if !reflect.DeepEqual(left, beforeLeft) || !reflect.DeepEqual(right, beforeRight) {
					t.Fatal("Record.Equal changed a record")
				}
			}
		})
	}
}

func TestJSONNumberDetectionQuotedParity(t *testing.T) {
	for count := 0; count <= 32; count++ {
		quoted, err := json.Marshal(strings.Repeat("\\", count) + `"-123\u0022`)
		if err != nil {
			t.Fatal(err)
		}
		withoutNumber := []byte(`{"-123":` + string(quoted) + `}`)
		withNumber := []byte(`{"-123":` + string(quoted) + `,"n":9007199254740993}`)
		if jsonMayContainNumber(withoutNumber) || !jsonMayContainNumber(withNumber) {
			t.Fatalf("number classification changed with %d backslashes", count)
		}
		assertJSONEqualPhysicalParity(t, withoutNumber, withoutNumber)
		assertJSONEqualPhysicalParity(t, withNumber, withNumber)
	}
}

func FuzzJSONEqualPhysicalDifferential(f *testing.F) {
	for _, pair := range [][2]string{
		{`{"a":"b","c":[null,true]}`, `{"c":[null,true],"a":"b"}`},
		{`{"x":"123"}`, `{"x":123}`}, {`1`, `1.0`}, {`1e`, `1e`},
		{`"\\\"1"`, `"\u0022"`}, {"\u00a0null", `null`},
		{`{"a":true,"a":false}`, `{"a":false}`},
	} {
		f.Add([]byte(pair[0]), []byte(pair[1]))
	}
	f.Fuzz(func(t *testing.T, left, right []byte) {
		if len(left) > 512 || len(right) > 512 {
			t.Skip("bounded physical parsing corpus")
		}
		assertJSONEqualPhysicalParity(t, left, right)
		quoted, err := json.Marshal(string(left) + string(right))
		if err != nil {
			t.Fatal(err)
		}
		numeric := []byte(`{"quoted":` + string(quoted) + `,"n":9007199254740993}`)
		assertJSONEqualPhysicalParity(t, numeric, numeric)
	})
}

// Reordered/spaced object keys model JSONB formatting without requiring a DB.
func equalityFormattedJSON(tb testing.TB, raw []byte) []byte {
	tb.Helper()
	value, err := decodeJSONBefore(raw)
	if err != nil {
		tb.Fatal(err)
	}
	formatted, err := json.MarshalIndent(value, "", " ")
	if err != nil {
		tb.Fatal(err)
	}
	return formatted
}

var jsonEqualPhysicalSink bool

func BenchmarkJSONEqualPhysical(b *testing.B) {
	for _, plans := range []int{0, 1, 18} {
		_, settlement, _ := admittedSerializationFixture(b, plans, "consumer/"+strings.Repeat("value<&>123", 8))
		raw, err := settlement.MarshalJSON()
		if err != nil {
			b.Fatal(err)
		}
		formatted := equalityFormattedJSON(b, raw)
		b.Run(fmt.Sprintf("plans%d", plans), func(b *testing.B) {
			benchmarkJSONEqualPair(b, raw, formatted)
		})
	}
	b.Run("numbers", func(b *testing.B) {
		benchmarkJSONEqualPair(b, []byte(`{"n":[1,2,3,4,5,6,7,8,9,10]}`), []byte(`{"n":[1.0,2e0,3,4,5,6,7,8,9,10]}`))
	})
}

func benchmarkJSONEqualPair(b *testing.B, left, right []byte) {
	for _, variant := range []struct {
		name  string
		equal func([]byte, []byte) bool
	}{{"before", jsonEqualBefore}, {"after", jsonEqual}} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				jsonEqualPhysicalSink = variant.equal(left, right)
			}
			if !jsonEqualPhysicalSink {
				b.Fatal("fixture must compare equal")
			}
		})
	}
}
