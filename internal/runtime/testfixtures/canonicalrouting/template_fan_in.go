package canonicalrouting

import "testing"

type TemplateFanInOptions struct {
	MissingDedup             bool
	DedupTuple               bool
	MissingWindow            bool
	BarrierAggregation       bool
	MissingSingleton         bool
	WrongSingleton           bool
	AccumulateDedupMismatch  bool
	AccumulateWindowMismatch bool
	DeliveryMany             bool
	LegacyConnectMap         bool
	EventIDDedup             bool
	NonSingletonReceiver     bool
	MissingReceiverHandler   bool
	MissingAccumulate        bool
	AmbiguousReceiverInput   bool
}

func CopyTemplateFanIn(t testing.TB, opts TemplateFanInOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	connectExtras := ""
	if opts.DeliveryMany {
		connectExtras += "    delivery: many\n"
	}
	if opts.LegacyConnectMap {
		connectExtras += "    map:\n      report_id:\n        source: payload.report_id\n        target: entity.report_id\n"
	}
	receiverMode := "singleton"
	if opts.NonSingletonReceiver {
		receiverMode = "static"
	}
	writeClosedVariantFile(t, root, "package.yaml", `name: template-fan-in
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: operating, flow: operating, mode: template}
  - {id: portfolio, flow: portfolio/default, mode: `+receiverMode+`}
connect:
  - from: operating.operating_reported
    to: portfolio.operating_reported
`+connectExtras)
	for _, name := range []string{"schema.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		body := "{}\n"
		if name == "schema.yaml" {
			body = "name: template-fan-in\n"
		}
		writeClosedVariantFile(t, root, name, body)
	}
	writeTemplateFanInProducer(t, root)
	writeTemplateFanInReceiver(t, root, receiverMode, opts)
	return root
}

func writeTemplateFanInProducer(t testing.TB, root string) {
	writeClosedVariantFile(t, root, "flows/operating/schema.yaml", `name: operating
mode: template
instance:
  by: operating_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs: {events: []}
  outputs:
    events:
      - name: operating_reported
        event: operating.reported
        key: report_id
        carries: [portfolio_id, report_id, period_id, operating_id, revenue]
`)
	writeClosedVariantFile(t, root, "flows/operating/entities.yaml", "operating_state:\n  operating_id:\n    type: text\n    _unused_reason: template fan-in source identity fixture field\n")
	writeClosedVariantFile(t, root, "flows/operating/events.yaml", fanInEventSchema())
	for _, name := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, "flows/operating/"+name, "{}\n")
	}
}

func writeTemplateFanInReceiver(t testing.TB, root, mode string, opts TemplateFanInOptions) {
	aggregation := "stream"
	if opts.BarrierAggregation {
		aggregation = "barrier"
	}
	directives := ""
	if !opts.MissingWindow {
		directives += "          window: payload.period_id\n"
	}
	if !opts.MissingDedup {
		switch {
		case opts.DedupTuple:
			directives += "          dedup_by: [payload.report_id, payload.operating_id]\n"
		case opts.EventIDDedup:
			directives += "          dedup_by: event.id\n"
		default:
			directives += "          dedup_by: payload.report_id\n"
		}
	}
	if !opts.MissingSingleton {
		singleton := "portfolio/default"
		if opts.WrongSingleton {
			singleton = "treasury/default"
		}
		directives += "          singleton: " + singleton + "\n"
	}
	extraInput := ""
	if opts.AmbiguousReceiverInput {
		extraInput = `      - name: operating_reported_duplicate
        event: operating.reported
        resolution:
          mode: fan-in
          aggregation: stream
          window: payload.period_id
          dedup_by: payload.report_id
          singleton: portfolio/default
`
	}
	writeClosedVariantFile(t, root, "flows/portfolio/default/schema.yaml", `name: portfolio
mode: `+mode+`
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: operating_reported
        event: operating.reported
        resolution:
          mode: fan-in
          aggregation: `+aggregation+`
`+directives+`        carries:
          report_id: {from: payload.report_id, type: text}
          period_id: {from: payload.period_id, type: text}
`+extraInput+`  outputs: {events: []}
`)
	writeClosedVariantFile(t, root, "flows/portfolio/default/entities.yaml", "portfolio_state:\n  portfolio_id:\n    type: text\n    _unused_reason: singleton fan-in fixture identity field\n  reports:\n    type: map[text]text\n    _unused_reason: singleton fan-in fixture state carrier\n")
	writeClosedVariantFile(t, root, "flows/portfolio/default/events.yaml", fanInEventSchema())
	for _, name := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, "flows/portfolio/default/"+name, "{}\n")
	}
	handler := "  event_handlers: {}\n"
	if !opts.MissingReceiverHandler {
		if opts.MissingAccumulate {
			handler = "  event_handlers:\n    operating.reported:\n      advances_to: active\n"
		} else {
			handler = "  event_handlers:\n    operating.reported:\n      accumulate:\n        into: operating_reports\n        from: payload\n"
			if opts.AccumulateWindowMismatch {
				handler += "        window: payload.operating_id\n"
			}
			if opts.AccumulateDedupMismatch {
				handler += "        dedup_by: payload.operating_id\n"
			}
		}
	}
	writeClosedVariantFile(t, root, "flows/portfolio/default/nodes.yaml", "portfolio-collector:\n  id: portfolio-collector\n  execution_type: system_node\n  subscribes_to: [operating.reported]\n"+handler)
}

func fanInEventSchema() string {
	return "operating.reported:\n  portfolio_id: text\n  report_id: text\n  period_id: text\n  operating_id: text\n  revenue: integer\n  required: [portfolio_id, report_id, period_id, operating_id, revenue]\n"
}
