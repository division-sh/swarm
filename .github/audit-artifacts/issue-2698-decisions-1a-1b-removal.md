# #2698 delegated proof-scope reduction

Track: FULL. This is the finite removal authorized by USER6217048948 and
reviewer-b6217090854/6217310536 (1A) and USER6217112145 plus
reviewer-b6217219917 (1B). It is not authority-migration credit, a passing
historical scan, or disposition of an original runtime RED.

## Frozen source and retained evidence

- Base: 56c8969ea64d2bc0b90f80eff47bce676bf0a26c.
- Pre-removal head: 2db25d4a39a029fec8ef913e6ee022791e1a296e.
- Signed remote backup: agent-b/backup-2698-runtime-ledger-20261016.
- Original RED, timeout, phase and completed-root receipts remain under
  /home/youmew/.cache/swarm-2542-family145/. They are not rewritten by this
  removal, and no removed test is claimed covered by an identical retained one.

## 1A: two historical typed reconciliation roots

Remove the complete Freshness and NativeEntry
TestPersistenceAuthorityDebt*CatchUpReconcilesActualOrigins declarations only.
Keep both files' other controls, source packets, constants, provenance rules,
shared archive/cache machinery and necessary trusted-base checking in the
UNCHANGED TestPersistenceAuthorityDebtRatchet. Keep the separate actual-current
TestPersistenceAuthorityDebtRepair2698ReconcilesLiveCensus. No replacement root
or indirect wrapper reintroduces the removed historical loads.

Both control files are outside the collector/analyzer fingerprint inputs.
There is no new permission, fingerprint successor, retained-key change or debt
baseline increase. #2714 records the ROI question for BOTH roots; it is not a
promised reimplementation, automatic nightly move or landing dependency.

## 1B: exact package/root manifest

issue-2698-decision1b-root-manifest.tsv enumerates every frozen root by package
and test name, with its original source file for review. The source file is not
its identity. The finite set contains:

- 360 KEEP_PREEXISTING top-level roots, including 355 in codemod-owner-guards
  and five assigned to other units.
- 45 KEEP_OWNER and 10 KEEP_TOOL added top-level roots, exactly the two lists
  in reviewer-b6217219917. Their complete declarations and assertions stay.
- 72 KEEP_SUBPACKAGE roots in their existing units.
- 254 DEFER added top-level declarations, removed rather than skipped,
  renamed, selected out, or called indirectly.

Expected current top-level discovery is 415 roots, with 410 in the unchanged
codemod-owner-guards selection. Retained added subtree roots total 127.
The manifest is finite review evidence, not an executable historical-proof
framework. Reintroduction of a DEFER root requires an uncovered invariant and
maintenance/cost case under #2535 R1-R7; no follow-up implementation is promised.

Remove imports and helpers only where dependency/unused checking proves they
are exclusive to the removed roots. Shared source/inverse/currentness
validators, immutable Before/After/Successors and packets used by retained
tests, the real ratchet or codemod commands stay. The existing planner,
profiles, package/job ceilings and runtime assertions are unchanged. A finite
reference search found no removed 1B root named by the current planner, catalog,
timing inventory or platform spec.

## Verification Status

Pending actual Go discovery, dependency/unused checks, the complete retained
selection and store unit measured sequentially at their original settings,
and the live ratchet. Counts above are expected cross-checks, not receipts.
The frozen-head runtime diagnostics are independent and close no hold unless
their cause/disposition is explicitly reviewed. Final canonical preflight and
HumanTask both-store receipts must bind the final integrated head, including
the separately reserved observation-only H2 capture.
