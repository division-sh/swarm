package canonicalrouting

// ArrivalJoinRetirementGuardCases supplies syntax-census specimens, not
// executable bundles or authoring/admission APIs.
func ArrivalJoinRetirementGuardCases() []ConnectionAdmissionCase {
	cases := []ConnectionAdmissionCase{
		{Name: "homonyms", Source: `pins:
  inputs:
    events:
      - {event: requested, resolution: {mode: fan-out}}
      - {event: replied, resolution: {mode: reply, replies_to: requested, correlation_key: task_id}}
worker:
  event_handlers:
    arrived:
      accumulate: {into: reports, from: payload, key: payload.id}
      data_accumulation:
        writes: [{op: set, target: entity.window, value: payload.aggregation}]
      emit: {event: forwarded, fields: {window: payload.window, dedup_by: payload.dedup_by, aggregation: payload.aggregation}}
    batch:
      join: {id: children, members: {from_fan_out: true}, on_complete: {emit: done}}
business_event:
  window: text
  aggregation: text
  dedup_by: text
`},
		{Name: "aliases", Source: `policy: &policy {mode: reply, aggregation: null}
arrival: &arrival {accumulate: {window: null}}
entries: &entries [{event: arrived, resolution: {<<: *policy}}]
pins:
  inputs:
    events: *entries
worker:
  event_handlers:
    arrived: *arrival
`},
	}
	for _, retired := range []string{"aggregation", "window", "dedup_by", "singleton"} {
		cases = append(cases, ConnectionAdmissionCase{
			Name: "pin/" + retired, WantError: retired,
			Source: "pins:\n  inputs:\n    events:\n      - event: arrived\n        resolution: {mode: reply, " + retired + ": null}\n",
		})
	}
	return cases
}
