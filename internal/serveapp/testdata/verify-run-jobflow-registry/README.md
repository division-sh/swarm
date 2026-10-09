# Verify Run Registry Integration V1

This is the pinned replacement authorized by #2591 acceptance amendment
6079750506, not the historical S03 workload or a performance benchmark.

The three input rows project slug, name and eng_roles from the first three
documents of internal/durabledata/testdata/jobflow-gems.jsonl.gz. The complete
decompressed source is 666737 bytes, SHA256
7f91b3f892fd32e8605c9155bd4f7b4d90f4ff8dda1b556b1a68f0bb2d865a67.
This projection and its smaller denominator are deliberate replacement inputs.

The registry keeps jobflow's keyed company intake/evaluation/settlement shape,
using current contract syntax. A real system-node evaluation classifies seven
or more engineering roles as qualified; fewer roles are watchlist. No LLM or
external service is invoked or certified. Publication, routing, construction,
handler execution, lifecycle, mutation/history commits and settlement are real.

Expected results: Aptos and Hyperbolic are watchlist; Ellipsis Labs is qualified.
Each keyed company traverses registered, investigating, assessed and done;
one company.status event per company is delivered and settled. The root is a
fieldless completed container. The clean run has four entities.

Runner: TestVerifyRunJobflowRegistryIntegrationBothStores in internal/serveapp.
It starts the original public run command with --data
company.registered=<this fixture's data/companies.jsonl>, verifies text and JSON,
then changes exactly one verdict through a private test-only fixture transaction
without modifying mutation history. All eight backend/clean-or-drift/format
leaves are mandatory. Fixture hashes and the admitted bundle identity are logged.
