Source: ffa979bbaa7ca5cf8e3120e16f6249670ba136de (unchanged approved source).
Trusted origin/master: 1d3cb55f05f738dc40fb112bac78bd975490e778.
Publication preflight: 20 phases passed, store phase failed, 4 phases unstarted.
This is a real branch-caused stale registry failure, not the earlier trust-ref drift.

Failing root: internal/store.TestPersistenceAuthorityFindingRegistry.
Unknown findings: none. Invalid raw-authority findings: none.
Exactly two stale rows, both typed-process-local/callback-type/typed:
- internal/runtime/contracts/pack_platform_provenance.go
  populatePlatformCatalogProvenance param:add
- internal/runtime/contracts/pack_platform_provenance.go
  populatePlatformLawProvenance param:add
Both signatures: func(string, github.com/division-sh/swarm/internal/yamlsource.Value, string) error.

The approved deletion removes that file and both private ledger-builder callbacks.
Registry lines 268-269 still describe them. No source, registry or policy edits
were made after identifying this failure. No retry, PR push or hosted run.
Proposal for reviewer-d: confirm the live closed-registry invariant/reach, then
retire precisely those two stale rows through the existing registry owner,
retaining all assertions, negative cases, classifications and no-raise policy.
