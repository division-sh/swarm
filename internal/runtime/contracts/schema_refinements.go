package contracts

func (r SchemaRefinements) Empty() bool {
	return r.Pattern == "" && r.EqualTo == "" && r.Length.Empty() && r.Range.Empty()
}

func (r SchemaRefinements) Clone() SchemaRefinements {
	out := SchemaRefinements{
		Pattern: r.Pattern,
		EqualTo: r.EqualTo,
	}
	if r.Length.Min != nil {
		value := *r.Length.Min
		out.Length.Min = &value
	}
	if r.Length.Max != nil {
		value := *r.Length.Max
		out.Length.Max = &value
	}
	if r.Range.Min != nil {
		value := *r.Range.Min
		out.Range.Min = &value
	}
	if r.Range.Max != nil {
		value := *r.Range.Max
		out.Range.Max = &value
	}
	return out
}

func (r SchemaLengthRefinement) Empty() bool {
	return r.Min == nil && r.Max == nil
}

func (r SchemaRangeRefinement) Empty() bool {
	return r.Min == nil && r.Max == nil
}
