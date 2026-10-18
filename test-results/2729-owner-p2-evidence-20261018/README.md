# #2729 owner SourceFacts P2 repair

Signed repair source: ab2d9305757b2c5ace6a1e933cefe1ced5ddcae9.
Parent accepted implementation checkpoint: 5919825fcf057442f82b6556d231f2a513f8fe83.
Base master: 909222b50cf88232d81af10b6fcc46ab78916fbe.

Reviewer-d ruling and exact assertion:
https://github.com/division-sh/swarm/issues/2729#issuecomment-6253270672
Gate D:
https://github.com/division-sh/swarm/issues/2729#issuecomment-6252188543
Original implementation RED-to-GREEN packet:
https://github.com/division-sh/swarm/issues/2729#issuecomment-6253228047
Original signed proof evidence:
https://github.com/division-sh/swarm/tree/3ec5a79ed920bd5c6187531da02732fa70ed08e1/test-results/2729-evidence-20261018

Test-first RED: add the reviewer's SourceFacts assertion to the existing
source_locator_is_not_authority loop, then run that reach once through
swarm-test admission. It fails with retained env:provenance.key-name when
env:provenance_key_name is required. No retry of the unchanged failing source.

Repair: the existing SnapshotOwner mutex now protects refreshing the retained
snapshot on every successful observation. Token rotation still occurs only
when sameAtomicSnapshot differs. No second credential lookup, no new lock,
and no changes to authority equality, metadata JSON, receipts or seals.
The existing root also requires frozen-projection provenance to remain at
its original spelling. Original metadata/token stability, value/tier
invalidation and refusal/absence controls remain intact.

Credentials package race x3 passed on the repair tree before the unchanged
source was signed; no concurrency source changes followed that run. At the
signed head, the 73-root affected focused/guard selection passes with 133 cases,
zero failures and zero skips. It includes the P2 reach, both-store concise and
verbose real boot (all four leaves pass), source-free/redaction, provider
precedence, currentness, mapped channels and existing accounting guards.

The canonical planner emits lifecycle with the same nine recorded exact-once
source-proof memberships. No CI-Units, new selection infrastructure, new root,
accounting-policy or budget/timeout change. This P2 adds assertion reaches only.
Exact-head preflight evidence and machine footer are retained here separately.

The full PR has one new lifecycle root for the distinct both-store real boot
credential-source presentation reach, as authorized in the original Gate D.
No duplicate local tier, hosted diagnostic or server2 invocation was performed.
