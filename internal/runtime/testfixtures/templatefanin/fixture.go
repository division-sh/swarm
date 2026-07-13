package templatefanin

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	ProducerFlowID       = "operating"
	ProducerOutputPin    = "operating_reported"
	ProducerEvent        = "operating.reported"
	ReceiverFlowID       = "portfolio"
	ReceiverInputPin     = "operating_reported"
	ReceiverEvent        = "operating.reported"
	ReceiverNodeID       = "portfolio-collector"
	ReceiverFlowInstance = "portfolio/default"
)

type Options struct {
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

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadBundleResult(t, opts)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadBundleResult(t testing.TB, opts Options) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	root := Write(t, opts)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := t.TempDir()
	writeRoot(t, root, opts)
	writeOperating(t, root)
	writePortfolio(t, root, opts)
	return root
}

func writeRoot(t testing.TB, root string, opts Options) {
	// routing-example-census: different-concept issue=2023 owner=resolution.fan_in proof=TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup
	t.Helper()
	writeFile(t, filepath.Join(root, "package.yaml"), `
name: template-fan-in
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: operating
    flow: operating
    mode: template
  - id: portfolio
    flow: portfolio/default
    mode: `+receiverPackageModeYAML(opts)+`
connect:
  - from: operating.operating_reported
    to: portfolio.operating_reported
`+deliveryYAML(opts)+legacyMapYAML(opts))
	writeFile(t, filepath.Join(root, "schema.yaml"), "name: template-fan-in\n")
	writeFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
}

func deliveryYAML(opts Options) string {
	// routing-example-census: different-concept issue=2023 owner=resolution.fan_in proof=TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup
	if opts.DeliveryMany {
		return "    delivery: many\n"
	}
	return ""
}

func legacyMapYAML(opts Options) string {
	if !opts.LegacyConnectMap {
		return ""
	}
	return `    map:
      report_id:
        source: payload.report_id
        target: entity.report_id
`
}

func writeOperating(t testing.TB, root string) {
	// routing-example-census: different-concept issue=2023 owner=resolution.fan_in proof=TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "operating", "schema.yaml"), `
name: operating
mode: template
instance:
  by: operating_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events: []
  outputs:
    events:
      - name: operating_reported
        event: operating.reported
        key: report_id
        carries: [portfolio_id, report_id, period_id, operating_id, revenue]
`)
	writeFile(t, filepath.Join(root, "flows", "operating", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "operating", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "operating", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "operating", "nodes.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "operating", "entities.yaml"), `
operating_state:
  operating_id:
    type: text
    _unused_reason: template fan-in source identity fixture field
`)
	writeFile(t, filepath.Join(root, "flows", "operating", "events.yaml"), `
operating.reported:
  portfolio_id: text
  report_id: text
  period_id: text
  operating_id: text
  revenue: integer
  required: [portfolio_id, report_id, period_id, operating_id, revenue]
`)
}

func writePortfolio(t testing.TB, root string, opts Options) {
	// routing-example-census: different-concept issue=2023 owner=resolution.fan_in proof=TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup
	t.Helper()
	writeFile(t, filepath.Join(root, "flows", "portfolio", "default", "schema.yaml"), `
name: portfolio
mode: `+receiverSchemaModeYAML(opts)+`
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: operating_reported
        event: operating.reported
        resolution:
          mode: fan-in
          aggregation: `+aggregationYAML(opts)+`
`+windowYAML(opts)+dedupYAML(opts)+singletonYAML(opts)+`        carries:
          report_id:
            from: payload.report_id
            type: text
          period_id:
            from: payload.period_id
            type: text
`+ambiguousReceiverInputYAML(opts)+`
  outputs:
    events: []
`)
	writeFile(t, filepath.Join(root, "flows", "portfolio", "default", "policy.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "portfolio", "default", "tools.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "portfolio", "default", "agents.yaml"), "{}\n")
	writeFile(t, filepath.Join(root, "flows", "portfolio", "default", "entities.yaml"), `
portfolio_state:
  portfolio_id:
    type: text
    _unused_reason: singleton fan-in fixture identity field
  reports:
    type: map[text]text
    _unused_reason: singleton fan-in fixture state carrier
`)
	writeFile(t, filepath.Join(root, "flows", "portfolio", "default", "events.yaml"), `
operating.reported:
  portfolio_id: text
  report_id: text
  period_id: text
  operating_id: text
  revenue: integer
  required: [portfolio_id, report_id, period_id, operating_id, revenue]
`)
	writeFile(t, filepath.Join(root, "flows", "portfolio", "default", "nodes.yaml"), `
portfolio-collector:
  id: portfolio-collector
  execution_type: system_node
  subscribes_to: [operating.reported]
`+receiverHandlersYAML(opts))
}

func ambiguousReceiverInputYAML(opts Options) string {
	// routing-example-census: different-concept issue=2023 owner=resolution.fan_in proof=TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup
	if !opts.AmbiguousReceiverInput {
		return ""
	}
	return `      - name: operating_reported_duplicate
        event: operating.reported
        resolution:
          mode: fan-in
          aggregation: stream
          window: payload.period_id
          dedup_by: payload.report_id
          singleton: portfolio/default
`
}

func receiverHandlersYAML(opts Options) string {
	if opts.MissingReceiverHandler {
		return "  event_handlers: {}\n"
	}
	if opts.MissingAccumulate {
		return `  event_handlers:
    operating.reported:
      advances_to: active
`
	}
	return `  event_handlers:
    operating.reported:
      accumulate:
        into: operating_reports
        from: payload
` + accumulateWindowYAML(opts) + accumulateDedupYAML(opts) + `
`
}

func aggregationYAML(opts Options) string {
	if opts.BarrierAggregation {
		return "barrier"
	}
	return "stream"
}

func windowYAML(opts Options) string {
	if opts.MissingWindow {
		return ""
	}
	return "          window: payload.period_id\n"
}

func dedupYAML(opts Options) string {
	if opts.MissingDedup {
		return ""
	}
	if opts.DedupTuple {
		return "          dedup_by: [payload.report_id, payload.operating_id]\n"
	}
	if opts.EventIDDedup {
		return "          dedup_by: event.id\n"
	}
	return "          dedup_by: payload.report_id\n"
}

func receiverPackageModeYAML(opts Options) string {
	if opts.NonSingletonReceiver {
		return "static"
	}
	return "singleton"
}

func receiverSchemaModeYAML(opts Options) string {
	if opts.NonSingletonReceiver {
		return "static"
	}
	return "singleton"
}

func singletonYAML(opts Options) string {
	if opts.MissingSingleton {
		return ""
	}
	if opts.WrongSingleton {
		return "          singleton: treasury/default\n"
	}
	return "          singleton: portfolio/default\n"
}

func accumulateWindowYAML(opts Options) string {
	if opts.AccumulateWindowMismatch {
		return "        window: payload.operating_id\n"
	}
	return ""
}

func accumulateDedupYAML(opts Options) string {
	if opts.AccumulateDedupMismatch {
		return "        dedup_by: payload.operating_id\n"
	}
	return ""
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func writeFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
