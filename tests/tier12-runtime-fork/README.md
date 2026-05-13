# Tier 12 Runtime Fork Fixtures

Tier 12 owns deterministic runtime-fork acceptance fixtures. These fixtures are
product-independent: they construct source runs through the supported runtime
harness, use scripted or fake agent behavior, and prove fork invariants without
Empire contracts or live LLM credentials.

`test-selected-contract-fork-execution` is the canonical generic proof fixture
for selected-contract timestamp forks. It proves selected execution under the
fork run, typed fork-local runtime lineage, source/fork isolation,
and non-corrupting cleanup.

`test-non-agent-replay-fail-closed` is the paired negative fixture. It proves
unsupported node/system historical replay remains fail-closed from a supported
runtime-persisted source event with pending non-agent work.

Empire cassette fixtures may be added later as product compatibility evidence
after Empire #64/#59 expose supported post-review/post-approval source runs.
Those cassettes are intentionally not the generic fork acceptance proof.
