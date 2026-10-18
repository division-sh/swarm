# #2741 active-provider observation preservation

Final signed head: 50e3a8f275f9c323d0940e58b81b5bc3bf99726d.
Tree: 7d0170af95ffe704399437686c92db6294f0cb65.
Rebased master: 8d58e2074af652f01f9d1f28816b10f4993f39d1.
Pre-rebase repair: 9c39a2ad63c01fdb5845c397b416150bb2a47755.
Previous published head: ab2d9305757b2c5ace6a1e933cefe1ced5ddcae9.

Scope/proof ruling:
https://github.com/division-sh/swarm/issues/2729#issuecomment-6253978913
Original repair authorization:
https://github.com/division-sh/swarm/pull/2741#issuecomment-6253809347

Boundary RED: the extended existing private manager-only fixture uses a real
file store. A signature-only adapter leaves the old successful fact-discard in
place; it returns an empty SourceFact with nil error, where the known file
source is required. The retained patch identifies that counterexample source.
No unchanged-source retry was made.

Repair: the helper returns its existing provider validation result. Start keeps
the fact only after successful validation and before readiness. The existing
credential-readback mutex protects the new append and the readback copy; there
is no second resolution, fallback, new production helper or recovery change.
Injected LLM, mock/no-agent behavior, refusal and startup error order remain.

Evidence layers are intentionally separate:
- Boundary: private-helper manager-only fixture, actual file credential owner,
  missing file provider despite an environment value, exact file source,
  identical declared/active facts, value/receipt redaction, live/mock no-agent
  controls and existing mock/live-descriptor refusal.
- Native reconciliation/hydration/order: unchanged
  TestRuntimeStartHydratesPersistedAgentsBeforeRecoveringNodeDeliveriesParity,
  PostgreSQL and SQLite, race x3 and final rebased focused run. Its injected LLM
  bypasses credential admission, so this is NOT a repaired-observation proof.
- Runtime/readback/presenter: unchanged supported both-store concise/verbose
  tool/channel boot; presenter deduplication/redaction and file-only provider
  precedence/currentness controls. NOT restored-provider admission evidence.
- Actual Start caller inspected: append follows its original error return,
  within the non-injected-runtime branch and before readiness can consume it.

No lawful no-declarations manager-only public recovered boot has been shown;
source reconciliation retires unmatched static actors before hydration. No
source plan was mutated, retirement bypassed or registration called hydration.

Proof cost: 0 new roots. Existing boundary/mock roots gain assertion reaches.
Runtime focused/guards race x3: 16 roots, 48 root passes / 72 case passes,
zero failures/skips. Rebase leaves all five changed runtime source/test files
byte-identical; that race receipt carries by exact content. Final rebased
focused/guards: 52 roots / 79 case passes, zero failures/skips, five packages.
All 52 have exactly one primary unit in the final-head emitted lifecycle plan.

Rebase: three commits. The initial implementation differs only in its canonical
serve-composition census body hash; the owner-P2 and active-fact repair commits
are '=' in range-diff. Sole conflict was the generated predicate census, rebuilt
through SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1 and its canonical test. No
owner classifications, proof-family assignments, guards, budgets or policies
were widened. The initial census RED and all diagnostic results are retained.

Before running final preflight, its upload check initially refused the unpushed
head, before any proof phase. The signed head was uploaded to the non-PR backup
ref agent-d/backup-2741-active-source-rebased-20261018 and preflight then ran on
that exact clean head. No PR push or hosted dispatch was used to satisfy upload.

Qualification remains CI-Tier lifecycle / Local-Tier focused; no CI-Units,
duplicate local tier, server2 run or new hosted diagnostic.

Preflight: pass (500.675s), all 25 phases, zero unstarted, exact signed clean
50e3a8f275; preflight/local published. Its soft target notice does not change
any test deadline or selection. Receipt SHA256:
d9afabbe75b1b04d8ecf9a7299c15dea069f4b687f840f09a07fd1ce10b9b720.
Affected guards include both runtime owner/ledger controls, update-OFF admission
census and its hostile refusal controls, credential currentness, preflight's
cross-package guards/codemod/ratchet and the explicit lifecycle plan memberships.
