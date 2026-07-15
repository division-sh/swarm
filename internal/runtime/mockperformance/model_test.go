package mockperformance

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestPerformanceAuthoredGrammarIsPythonOnly(t *testing.T) {
	typ := reflect.TypeOf(Performance{})
	var authored []string
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name != "" && name != "-" {
			authored = append(authored, name)
		}
	}
	sort.Strings(authored)
	want := []string{"kind", "module"}
	if !reflect.DeepEqual(authored, want) {
		t.Fatalf("authored mock grammar = %#v, want Python-only fields %#v", authored, want)
	}
}
