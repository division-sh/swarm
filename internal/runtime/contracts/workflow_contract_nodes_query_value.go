package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

var nodeQueryFields = map[string]struct{}{
	"source": {}, "entities": {}, "filter": {}, "group_by": {},
	"count": {}, "select": {}, "store_as": {},
}

func projectNodeQueryValue(value yamlsource.Value) (*QuerySpec, error) {
	fields, err := nodeValueFields(value, "query", nodeQueryFields, map[string]string{
		"operation": "query.operation is not executed; use source/entities and the declared query selectors",
	})
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("query at %s must not be empty", value.Location())
	}
	var out QuerySpec
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"source", &out.Source}, {"entities", &out.Entities},
		{"filter", &out.Filter}, {"group_by", &out.GroupBy},
		{"store_as", &out.StoreAs},
	} {
		field, present := fields[entry.key]
		if !present {
			continue
		}
		text, err := nodeValueText(field, "query."+entry.key)
		if err != nil {
			return nil, err
		}
		*entry.target = strings.TrimSpace(text)
	}
	if count, present := fields["count"]; present {
		out.Count, err = nodeValueBool(count, "query.count")
		if err != nil {
			return nil, err
		}
	}
	if selectFields, present := fields["select"]; present {
		out.Select, err = nodeValueStringSequence(selectFields, "query.select")
		if err != nil {
			return nil, err
		}
	}
	out.hydratePaths()
	return &out, nil
}
