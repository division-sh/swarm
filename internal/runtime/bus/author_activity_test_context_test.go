package bus

import (
	"context"
	"sort"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
}

var authorActivityTestDifferentEventTypes = strings.Fields(`
account.ready child/child.start child/grandchild/micro.done child/grandchild/micro.started child/inst-1/micro.started
child/output.done custom.bad custom.claimed custom.completion_failure custom.direct custom.direct.empty custom.emitted
custom.followup custom.good custom.in_flight custom.internal custom.leaf custom.markerless custom.middle custom.mixed
custom.mixed_node_agent custom.no_subscribers custom.node_only custom.node_only_outbox custom.node_only_sweep
custom.node_only_tx custom.non_transactional custom.paused custom.pool_saturation custom.publish_mutation_post_commit
custom.receipt_failure custom.replay.checked custom.replay_pool_saturation custom.root custom.routed custom.run_control
custom.run_control.acked custom.run_control.deferred custom.run_control.intercepted custom.run_control.postcommit
custom.run_control.postcommit.deferred custom.shared_claim custom.snapshot custom.trigger deploy.done human_task.approved
inbound.proof inbound.proof.normalized item.received legacy.event mailbox.card_decided opco.spinup_requested
operating/11111111-1111-4111-8111-111111111111/opco.product_initialization_requested
operating/inst-1/opco.product_initialization_requested operating/opco.product_initialization_requested pipeline.start
platform.agent_failed platform.boot platform.budget_threshold_crossed platform.paused platform.recovery_failed
platform.run_stalled platform.runtime_log producer/account.ready producer/audit.seen producer/deploy.done
producer/scan.requested producer/ticket.ready producer/validation.requested producer/work.ready review/inst-1/task.started
review/task.started root.ready scan.requested task.completed task.failed task.requested task.started test.duplicate_route
test.identity_route test.new test.old test.retained test.route_generation test.route_generation_ack
test.route_generation_mutation test.tokenless thing.created validate.requested validation.requested
validation/thing.reviewed worker/work.assign
`)

type authorActivityTestCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash,
	))
}

func newScopedTestEventBus(store EventStore, options ...EventBusOptions) (*EventBus, error) {
	opts := EventBusOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if registrar, ok := store.(authorActivityTestCatalogRegistrar); ok {
		descriptors := authorActivityTestEventDescriptors(opts.ContractBundle)
		lease, err := registrar.RegisterAuthorActivityEventCatalog(
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash), descriptors,
		)
		if err != nil {
			return nil, err
		}
		_ = lease // The store and its catalog are scoped to the test that owns them.
	}
	return NewEventBusWithOptions(store, opts)
}

func authorActivityTestEventDescriptors(source semanticview.Source) []runtimeauthoractivity.EventDescriptor {
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(authorActivityTestDifferentEventTypes))
	for _, name := range authorActivityTestDifferentEventTypes {
		byName[name] = runtimeauthoractivity.EventDescriptor{EventType: name, Disposition: runtimeauthoractivity.StoryDifferent}
	}
	if source != nil {
		resolved := source.ResolvedEventCatalog()
		authored := source.AuthoredResolvedEventCatalog()
		add := func(name string, summaryField string, disposition runtimeauthoractivity.StoryDisposition) {
			name = strings.TrimSpace(name)
			if name == "" {
				return
			}
			byName[name] = runtimeauthoractivity.EventDescriptor{
				EventType: name, Disposition: disposition, AuthorSummaryField: strings.TrimSpace(summaryField),
			}
		}
		for name, entry := range resolved {
			disposition := runtimeauthoractivity.StoryDifferent
			if _, ok := authored[name]; ok {
				disposition = runtimeauthoractivity.StoryAuthored
			}
			add(name, entry.AuthorSummaryField, disposition)
		}
		census := semanticview.BuildAuthoredEventEndpointCensus(source)
		endpoints := append(census.Producers(), census.Consumers()...)
		endpoints = append(endpoints, census.InputPins()...)
		endpoints = append(endpoints, census.OutputPins()...)
		for _, endpoint := range endpoints {
			if endpoint.Event.HasSchema {
				disposition := runtimeauthoractivity.StoryDifferent
				if _, ok := authored[strings.TrimSpace(endpoint.Event.CatalogKey)]; ok {
					disposition = runtimeauthoractivity.StoryAuthored
				}
				add(endpoint.Event.EventKey(), endpoint.Event.Entry.AuthorSummaryField, disposition)
			}
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(names))
	for _, name := range names {
		descriptors = append(descriptors, byName[name])
	}
	return descriptors
}
