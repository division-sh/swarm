# #2729 Gate D RED-to-GREEN Evidence

Signed source: 5919825fcf057442f82b6556d231f2a513f8fe83.
Source tree: e37fe94e564750eee5f166b4a77d656bff42db53.
Base: 909222b50cf88232d81af10b6fcc46ab78916fbe.
Pre-rebase local source: b3f9194464 (backed up separately).
The one-patch clean rebase is range-diff equal. No code changed during rebase.

Retained original counterexamples:
- Owner RED: credential observation lacks its winning source reference.
- Presenter RED: serve presenter omits required credential source facts.
- Provider atomicity RED: provider value and source bypass the same atomic
  observation, on all four active provider profiles.
- Predicate guard RED: original successful-credential projection changed the
  pinned checkContractSecrets body. Refreshed through its canonical owner with
  exact owner/role/proof accounting in the tracked #2729 audit.

All fixture/compiler failures are also retained, not erased: the omitted
compileChannelBindings caller, unsupported channel credentials config field,
retired explicit empty-object schema, and incomplete typed schema constructor
in the CLI proof. Each was corrected as test setup, without dropping a source,
refusal, redaction, authority or output assertion. The initial grouped race log
retains its CLI fixture failure; its other package passes carry unchanged and
the corrected CLI packet separately passes race x3.

New-head lean-rebase packet: 73 roots / 133 distinct cases, zero fail/skip.
The public boot proof passes all four SQLite/Postgres concise/verbose leaves
in 2.87s total. Earlier exact implementation race x3 covers owner/currentness,
provider one-observation, channel mapping, CLI redaction, presenter and both
stores. The clean rebase has the same patch; no concurrency changes were made.

The canonical lifecycle plan was emitted using the planned PR body (no PR has
been opened), real policy, weights and discovered inventory. Its nine key
memberships are in 2729-unit-memberships.json, including the new public root
exactly once in serveapp-journeys-first. Selection infrastructure is unchanged.

Exact-head canonical preflight: PASS, 715.148968727s end to end, all 25 phases
complete, no unstarted phase. Receipt hash is
faf06c8a708aa31af801c617a3a09863bdcc2779a0999d43acca84de43c07ae8.
OpenRPC owner check also passes without regeneration.

One new root, no mirrored roots, no new guard/registry framework, no authority,
credential default/scope/precedence, severity, timeout, budget or CI-selection
change. No C2 certification, PR push or hosted dispatch is claimed.
