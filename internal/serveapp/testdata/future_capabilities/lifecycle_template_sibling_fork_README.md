# T17 Dynamic Sibling Fork: Future Success Oracle

Binding proof correction: [#2415 lead ruling](https://github.com/division-sh/swarm/issues/2415#issuecomment-5578253156).
Future capability owner: #642. This is not an implemented capability or a passing,
skipped, or expected-failure default-suite test.

`lifecycle_template_sibling_fork_test.go.txt` retains the original
`TestServedCompiledTransitionTemplateSiblingForkOnBothStores` function verbatim
from checkpoint `f602090e7`. A minimal package/import header makes it executable
as an additional Go test file. All original dynamic source, duplicate fork,
reminted entity/instance/card/activation, frozen source, child final-consumer,
sibling distinction and parent isolation assertions are preserved.

Artifact SHA256:
`0dfbf9e98c51a630e0694901b28841b0bfdb43a87570f57fc4b7ac94c3f557c8`.

To execute it when #642 is enabled, use a Go overlay mapping the new virtual file
`internal/serveapp/lifecycle_template_sibling_future_test.go` to the absolute
artifact path. Do not replace or blank any existing test file. Run the original
test name explicitly; large repeated/both-store matrices use `swarm-test`.
The current expected result is RED, not proof credit.

The default-suite counterpart is
`TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores`.
It runs the same source loops and gates, pauses the actual run, publishes the
same frontier, and requires exact typed dynamic-capability refusal on duplicate
attempts. It compares every application table and both source gates before/after.
Its success proves refusal and isolation, not child execution. Both-store
execution remains required after the #2423 publication prerequisite is integrated.

Supported static fork success is covered separately by
`TestServedCompiledTransitionStaticForkEvidenceOnBothStores`; it is not a
substitute for this future dynamic oracle.
