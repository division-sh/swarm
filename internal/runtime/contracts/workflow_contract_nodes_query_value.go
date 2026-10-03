package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

var nodeQueryFields = map[string]struct{}{
	"source": {}, "entities": {}, "filter": {}, "group_by": {},
	"count": {}, "select": {}, "store_as": {},
}

func projectNodeQueryValue(value yamlsource.Value) (*QuerySpec, error) {
	fields, err := nodeValueFields(value, "query", nodeQueryFields)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("query at %s must not be empty", value.Location())
	}
	var out QuerySpec
	if err := nodeValueTexts(fields, map[string]*string{
		"source": &out.Source, "entities": &out.Entities,
		"filter": &out.Filter, "group_by": &out.GroupBy, "store_as": &out.StoreAs,
	}, true); err != nil {
		return nil, err
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
