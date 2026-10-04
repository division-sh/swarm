package canonicalrouting

import "testing"

// PinRewriteSyntaxSource supplies finite syntax specimens to the one-shot rewrite.
// They are not executable legacy bundles or runtime compatibility readers.
func PinRewriteSyntaxSource(t testing.TB, name string) string {
	t.Helper()
	switch name {
	case "RewriteGeneratedPositivePins-1":
		return "name: generated\nstages:\n  active: {initial: true}\npins:\n  inputs:\n    events:\n      - {event: work.requested, source: harness}\n  outputs:\n    events:\n      - {event: work.completed, sink: harness}\nconnect:\n  - {event: work.completed, from: ., to: .}\n"
	case "RewriteGeneratedPinsRejectsUnratifiedOptions-2":
		return "resolution: {mode: select}"
	case "RewriteNamesOnlyPreservesOtherSourceAndIsIdempotent-1":
		return "# original heading\nname: 'unchanged'\npins:\n  inputs:\n    events: [work.started]\n  outputs:\n    events:\n      - event: work.done\n        sink: harness\n\nstages: {idle: {initial: true}}\nconnect:\n  - {event: work.started, from: ., to: child, resolution: create}\n"
	case "RewriteNamesOnlyPreservesOtherSourceAndIsIdempotent-2":
		return "\nstages: {idle: {initial: true}}\nconnect:\n  - {event: work.started, from: ., to: child, resolution: create}\n"
	case "replySources-3":
		return "name: requester\npins:\n  inputs:\n    events:\n      - event: provider.replied\n        resolution:\n          mode: reply\n          replies_to: provider.requested\n"
	case "replySources-4":
		return "name: parent\npins:\n  inputs:\n    events: [request.start]\nconnect:\n  - {event: provider.requested, from: requester, to: provider}\n  - {event: provider.replied, from: provider, to: requester}\n  - {event: other.requested, from: requester, to: other, resolution: select, key_from: payload.other_id}\n"
	case "RewriteRejectsUnratifiedOrAmbiguousForms-5":
		return "pins: {inputs: {events: [{event: work.start, resolution: {mode: fan-out}}]}}\n"
	case "RewriteRejectsUnratifiedOrAmbiguousForms-6":
		return RetiredFanInCoordinatorSchema()
	default:
		t.Fatalf("unknown pins rewrite specimen %q", name)
		return ""
	}
}
