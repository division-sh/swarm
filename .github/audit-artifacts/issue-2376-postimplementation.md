# Post-Implementation Proof Audit: #2376 / #2322

Current qualification: hosted head `301539c51c6482ad1ccefe07ddf27d2414f78114`
FAILED 14 substantive CI units in run 37122328426 after the approved queue rebase
onto master `e63f4bdb197e473fe35e94f65ff3c77655957123`. Earlier R13 passes and
cycle-2 approval are historical evidence, not final-head CI qualification.
CI-quiet has ended. The bounded qualification repair updates incoming journeys
to selected-server/permanent replay identity, regenerates the migrated static-data
golden through admitted source/static-data owners, supplies fake run-header identity,
and reconciles seven exact private-backend signature rows plus the spec ratchet.
Only production delta since that rebase is the word "runtime" in the existing
multi-context Claude refusal; the guard and execution semantics are unchanged.
Local qualification of this repair is in progress. Merge remains gated on the
new candidate's supported proofs and exact-head required CI.

Historical R13 status: supported proofs for the approved matrix passed. The user's
2026-10-03 ruling resolves the two additional label-safety manifestations; permanent
red/green proofs now cover them at the existing owner and supported consumers.
Final local default qualification passed all 14 planned units on committed head
1ce88a3500d5cf1e69ae276091e4d5bb773e9292 (R13). Hosted qualification and independent
merge approval remain outstanding. Changes are batched locally under the user's CI-quiet rule;
the GitHub PR head does not yet contain this label-safety repair.
The earlier default profile and native unused check remain FAILED due to shared
disk exhaustion (R12). After the user reclaimed shared cache space, a fresh default
profile and native analysis passed (R13); no product or timing waiver was used.
This is one class-closure PR, not publication/archive work or a first slice.
The local candidate is rebased on `0fa24140aaa05e397509f451ba780fef3a260867`,
including merged #2537. No source or docs push is permitted until #2544 merges.
Receipts R1-R7 below retain their original timing/log attribution. Receipt R9
records actual post-rebase repetitions; an old pass is not relabeled as a new run.

## Governing Context And Changed Concepts

- Pre-Implementation Coverage Audit: https://github.com/division-sh/swarm/issues/2376#issuecomment-5958910917
- Original Gate F: https://github.com/division-sh/swarm/issues/2376#issuecomment-5959596045
- Approved bounded fork amendment: https://github.com/division-sh/swarm/issues/2376#issuecomment-5961544626
- User/lead label-safety ruling: https://github.com/division-sh/swarm/issues/2376#issuecomment-5965029428
- Authoritative spec: `filesystem_source_model` (admission, manifest_metadata, human_source_presentation, bundle_v2, durable_lifecycle, retired_surface, projection_cache_lifetime); `versioning.author_facing` and `versioning.compatibility`; `multi_bundle_persistence.bundle_identity`; selected-contract fork execution/readiness and `store.run_fork.selected_child_decision_execution_identity`; decision-card `replay_restart_fork` / `fork_generation_correspondence`.

Category: semantic drift, authored grammar, supported-surface/lifecycle parity.
Exact concepts: immutable admitted source bytes and finite flow metadata; tree-first
public identity selection versus exact internal execution authority; source-run fork
pin defaults; display-only source labels; parent historical evidence versus child
gate/card/proposed-effect hash AND workflow-version execution pins.

Chosen working failure class: author-facing source admission/presentation and its
execution consumers disagree with exact-source authority. This includes all twelve
obsolete selectors and the amended fork-local decision family, not just verify or
the first failing activity insert. This PR aims to eliminate that whole chosen class.
The former framing was symptom-shaped; the amended framing is broad enough.

Immediate parent: identity/admission consistency across public selection, immutable
bytes, compilation, persistence, lifecycle and diagnostics. The relevant siblings
are absorbed, not deferred. Broader authored/effective-model and presence-preserving
decode programs remain OPEN under #2300/#2287/#2293. Closed #2486/#2487 are substrate,
not outstanding repair trackers; this PR does not change their decoder work.

## Consolidated Design

The author selects a directory, never a source hash. Captured invocation and the
existing root resolver choose the tree; one admitted artifact owns exact members,
dispositions, raw bytes, tagged framing, metadata and labels. An optional flow
manifest is strict when present: exactly name/version/platform_version. Version is
strict SemVer; the existing platform owner checks the selected root range before
execution or explicit local identity selection. Nested metadata cannot change scope.
Each decoded field is limited to 256 UTF-8 bytes, including whitespace, at the typed
owner before SemVer/range parsing, with exact-path construct/decode refusal.
Hashing never canonicalizes authored bytes or includes platform authority.

Internal APIs, runs, workspace/effect/channel coordinates, recovery and historical
evidence remain full exact identities. Fork's optional --source computes only an
already-stored target identity, with no upload or implicit serve. Default fork pins
use the source run, even when serving another artifact. All four channel --source
selectors preserve their exact target/interface/security and ambiguity contracts.

Human output uses root name@version, genuinely matching directory@hash7, or stored
hash7 alone. SourceLabel is never an equality, sorting, deduplication, selection,
idempotency or fence input. Stored decode never invents a basename from dot/cwd.
HumanLabel replaces Unicode Cf/control characters and line/paragraph separators
with spaces for display only. Authored metadata and source bytes are not rewritten.
Existing machine JSON/API/explicit YAML remains exact, with additive display data.

For forks, admitted compiled target C owns child executable pins; parent A history,
frozen snapshots/routes, provenance and terminal recorded results remain A evidence.
The selected readiness owner seals compiled target identity. Generic materialization
derives the existing machine source tuple. Gate activations, cards and proposed
continuations receive that explicit target before construction/hash computation.
Strict activity scope, hash/version checks, frozen-route validation and selected
control refusal remain. No fresh child approval or external effect is authorized
by copying a card. Terminal evidence stays no-call reuse; ambiguous evidence refuses.

Tar/register/publication/catalog promises are retired or moot, not implemented.
There is no compiled cache surviving a binary: parse caches and pack/projection
generations are process-owned. Persisted exact source, causal evidence and projection
cleanup intents are not reusable compiled caches. No persistent stamp/key was added.

## Canonical Owners And Exhaustive Consumption

Every named owner is semantic authority, not the first file encountered. Statuses
below refer to actual production paths. Supporting tests execute the named consumer;
shared ownership alone is not closure evidence.

| Owner | Consumer family / disposition | Execution evidence |
| --- | --- | --- |
| Captured InvocationRoot / ResolveSourceRoot | Local verify/describe/test/run/serve/packs/import already consume it; fork and four channel identity selectors now consume it. Remote operation-ID resume remains source-free, a different authority concept. | Test2376SourceInvocationParity; Test2376LocalIdentitySelectorsUseAdmittedSource; whole CLI controls |
| sourceartifact.AdmittedSourceArtifact / newArtifact / DecodeLogical | Local admitted construction AND persisted decode now validate every flow manifest through one typed projection. Sorted entries, classification and hash remain the existing authority. | Test2376Manifest* construct/decode root/nested table; Test2376IndependentHashRecomputation; Test2376MemberDispositionMutationParity |
| sourceartifact.projectManifest / yamlsource.Value | Metadata, exact missing/unknown/duplicate/kind/256-byte/version/range admission and alias/merge provenance now consume admitted presence/provenance. The byte limit precedes semantic parsing during construction and stored decode. No raw YAML decoder or flatten/reparse path added. | Test2376ManifestOptionalStrictPresence, UnknownKeysAndDuplicates, MalformedDocument, StrictVersion, RangeGrammar, AliasMergeProvenance; Test2376ManifestUTF8ByteBoundaries; Test2376ManifestOversizeEvidencePrecedesSemanticParsing; Test2376ManifestReaderLedgerIsAdditive |
| Existing platform range owner | Executable contracts artifact compile and local identity-only selector both consume ValidatePlatformVersion. Nested grammar is checked but only selected-root compatibility gates. | Test2376RootManifestRejectsBeforePublicationBothStores; Test2376NestedManifestPreservesSemanticScope; Test2376LocalIdentitySelectorsUseAdmittedSource |
| Artifact source/platform separation | External platform spec/base-pack input is distinct from exact source identity. Project packs are admitted source resource bytes, not external platform cache authority. | Test2376SourceHashIndependentOfPlatformAdmission; Test2376MemberDispositionMutationParity; existing contracts/base-pack controls |
| Selected SourceArtifact persistence / exact stored loader | Serve ingests before publication; internal standing recovery, budget recovery, fork, retained reset and missing-original-root reconstruction retain exact decode/compile. Public hash-only boot is deleted, not aliased. A failed COMMIT acknowledgment remains an error; only a separate exact acknowledged attempt can grant source publication. | Test2376VerifyServeArtifactParityBothStores; Test2376SourceArtifactCommitUncertaintyBothStores; Test2376ServeSourcePublicationRequiresAcknowledgedIngest; TestSourceArtifactSelectedStoreParity; TestRunServeSourceArtifactIntegrityRejectsBeforeReadinessBothStores; reset/standing/release journeys |
| Existing run/request authority | New event/data/run work consumes exact served source; existing-run event execution remains run-bound. run.get exposes the durable hash for default fork pins. No health substitution for existing authority. | Test2376EventPublishDerivesExactRuntimeSource; Test2376ForkSourceAndPinsFollowSourceRunBothStores; TestPayloadlessEventPublicPersistenceJourneyBothStores; TestDurableDataOperationAggregatePublicRestartBothStores |
| Existing fork availability/materialization | Explicit local C selects stored exact C, missing D refuses before freeze/mutation, default selects source A rather than serving B. Existing fixed-revision feed and conflicting-pin guards remain. | Test2376ForkSourceAndPinsFollowSourceRunBothStores default/pin/explicit/missing/contradictory/repeat cases |
| runforkreadiness.Admission.ExecutionIdentity | Seals compiled hash/version, validates selected source fact, returns a value copy. Selected materializer consumes it; zero admission fails. | Test2376AdmissionSealsCompiledExecutionIdentity; fork public and both-store owner controls |
| contracts.SourceExecutionIdentity | One existing machine tuple rule consumed by compilation and generic fork materialization. Display manifest version is not an executable version pin. | TestBootBundleIdentity*; Test2376AdmissionSealsCompiledExecutionIdentity; generic fork controls |
| Shared fork gate/card producer | Both PostgreSQL/SQLite materializers pass explicit target to gate activation and stage-card construction/change story. Open/committed/routed/superseded dispositions and source history remain distinct. | Test2376ForkDecisionExecutionSourceBothStores; TestMaterializeRunForkRootAuthoritiesExecuteWithForkIdentitySelectedStoreParity |
| Shared fork proposed-effect producer | Both stores remint IDs/generation and target hash/version before effect hashing, card creation and continuation validation. Source reply authority is detached; input/frozen evidence retained; no inherited approval. | Test2376ForkDecisionExecutionSourceBothStores; TestForkPendingGenerationCorrespondenceBothStores; approved/rejected terminal-evidence matrix |
| Existing final card/continuation/gate/activity admission | BundleScopeForSource stays strict; continuation.Validate, frozen gate transition, activity hash AND version pin guards and selected control refusal remain authoritative. | Public explicit-C story; committed root coordinator route/repeat; Test2376ActivityContractPinsConsumeAdmittedSource; TestServedCompiledFrozenGateForkControlRefusalOnBothStores; TestSelectedForkMailboxControlRefusalsBothStores |
| Existing recorded-result / transactional fork owners | Approved terminal effects remain no-call evidence; uncertain/missing/nonterminal refusal, human-task exclusion, atomic rollback and exact/conflicting-repeat semantics retained. | TestPrepareRunForkApprovedProposedEffectRequiresUnambiguousTerminalEvidence; Test2376ForkDecisionExecutionSourceBothStores; existing activity no-call controls |
| channelonboarding.CandidateCatalog / exact activation readback | Four CLI directory selectors send full hash into existing exact selection. Catalog cardinality and authority order use exact coordinates, never labels. Security/currentness/revision fences unchanged. | TestOperatorChannelCLIUsesAuthenticatedAPIAndExactSelectors exact-selectors connect/reconnect/rebind/status; Test2376ChannelExactSelectionAndStoredPresentationBothStores; Test2376ChannelPrefixAndNameCollisionsRemainExact |
| sourceartifact.HumanLabel / LoadHumanLabel | Verify, describe, serve, health, version server/quiet, fork, effective frame, channel list/status/candidates and test diagnostics now consume artifact label or explicit abbreviated fallback. The single sanitizer removes Cf/control/line separators only from that projection; stored label consumers decode exact artifact, no saved display store. | Test2376HumanLabelsRemoveEveryFormatControl; Test2376FormatControlPresentationPreservesExactArtifact; Test2376HumanSourceIdentityConsumerTable including hostile verify human/JSON; real verify/serve proof; effective_frame_exact_source mock restart proof; stored channel proof; Test2376TestDiagnosticsKeepExactLookupButHumanTeaching |
| Full machine identity owners | JSON/API/explicit YAML, persisted run/card/effect/workspace/channel keys and historical coordinate strings already consume full exact identities and remain authoritative. Labels are additive only. | Test2376MachineIdentityRemainsExact; exact collision/readback and fork child/activity SQL assertions; both-store persistence/restart controls |
| Process cache/projection lifecycle owners | Parse caches, pack generations and retained projection handles are process-local. Durable cleanup/evidence is a different concept; no reusable compiled projection key exists. | TestRuntimeProjectionOwnsExactGenerationAndLifetime; synchronization/release controls; actual process restart and exact stored reconstruction |
| Canonical fixture / reader ledger owners | All tracked finite flow manifests and fixture generators are migrated without exemptions. Global source reader ledger is additive: historical 45 rows / 11 decoders / 67 sites / 24 roots retained; new document family, no custom decoder/raw-node root. | TestTrackedManifestRootsUseFiniteSourceGrammar; Test2376ManifestReaderLedgerIsAdditive; canonical decoder inventory and surface/spec census |

Twelve invalid public readers removed: serve, event publish, data import, data show,
data prune, run start, run fork, agent frame --bundle-hash; channel connect/reconnect/
rebind/status --bundle. No hidden/no-op alias, public hash-only boot, static frame,
ambient source re-read, persisted display basename, or dual metadata reader survives.
Internal exact hash-based loading is still authoritative and deliberately retained.
Logs --source and secrets list --source are producer/credential filters: different
concepts, proved by the complete real Cobra-tree source-selector census.

## Supported Proof Boundaries

Receipt R1: whole CLI plus Test2376 source/selector/presentation tables. Includes all
twelve retired selectors before I/O, five local source selectors, command/API flag
placement consistency and the unchanged 45-row compiled describe characterization.
Describe fixtures were regenerated only after independent base/head binaries
reproduced ALL 45 old rows: 15 route outputs unchanged; text changes only first
source line; JSON changes only source_label and platform-spec source-file/line
provenance. No expectation normalization or characterization assertion was weakened.
Whole CLI PASS, 207.899s (`/tmp/swarm-2376-cli-final-v2.log`).

Receipt R2: sourceartifact/contracts/readiness/channel owner tables, including
independent literal hash recomputation, strict manifest construction AND decode,
scope, process projections and corpus; PASS on final development tree (sourceartifact
0.733s, contracts 8.056s, readiness 0.447s, channelonboarding 0.028s;
`/tmp/swarm-2376-source-owner-final.log`).

Receipt R3: coordinated runtimepersistence existing gate, generation, selected
principal, approved terminal-evidence, human-task and persistence controls. PASS, 8.860s
(`/tmp/swarm-2376-fork-final-controls.log`); PostgreSQL cells ran, not skipped.
The new Test2376 fork disposition/rollback/repeat, channel collision/readback and
source commit tests separately passed the explicit final-tree matrix. The 8.860s
run's exact regex selected existing controls, not these new test roots. The
explicit `go run ./cmd/swarm-test -- ./internal/store/internal/runtimepersistence
./internal/serveapp -run '^Test2376' -count=1 -v` receipt includes every new root
and all SQLite/PostgreSQL cells: runtimepersistence 5.616s, serveapp 16.430s
(`/tmp/swarm-2376-final-new-surface-matrix.log`).
The additional source-only AND source-with-data physical-COMMIT injection matrix
passed, 4.759s: actual commit then lost acknowledgment, and actual rollback then
lost acknowledgment, on both stores, no automatic retry and exact explicit retry
without duplicate rows. This is a driver-boundary fault, not a TCP/process-death
claim. The paired serve caller contract passed, 0.267s: error plus candidate or no
candidate grants no source fact and makes no implicit retry
(`/tmp/swarm-2376-source-ack-final.log`).

Receipt R4: coordinated real serve/API both-store reset/integrity/mock-frame/
selected-control journeys. The new Test2376 verify/durable-source/public fork
A/B/C/pins/root-refusal cases separately passed the explicit final-tree matrix,
serveapp 16.430s (`/tmp/swarm-2376-final-new-surface-matrix.log`), no backend skips.
These are real in-process served entrypoints, not a claim of public binary boot.
The mock retained lifecycle explicitly uses the existing internal MockOnly host;
it is the real API/CLI frame proof, not another public live serve posture. PASS,
31.071s (`/tmp/swarm-2376-supported-final-v2.log`).

Receipt R5: compiled CLI/process release journeys, real public payloadless event
persistence/restart and durable data-operation/run-creation receipt restart on both
stores. PASS, 30.598s (`/tmp/swarm-2376-binary-journeys.log`). Their existing internal mock-only lifecycle executable stays accurately
identified; the compiled CLI and authenticated API are real public surfaces.
`Test2376SourceVerificationBinaryExactMemberTable` separately builds the real
release CLI, verifies bare/dot and changed exact trees without credentials or
runtime harness flags, and independently recomputes its full hash/member table.
PASS, 5.461s (`/tmp/swarm-2376-public-binary-verify.log`).

Receipt R6: changed-source standing cold-start matrix on both stores, internal
stored fork after original source removal, activity hash/version refusals, gate
recorded pin wait, proposed route replay, and no-call recorded result controls.
PASS, serveapp 19.406s and pipeline 3.682s
(`/tmp/swarm-2376-lifecycle-pin-controls.log`), both-store standing cells ran.

Receipt R7: OpenRPC generation check, additive canonical reader census, backend
matrix validation and diff checks. Required CI must qualify the pushed exact head.
The closed fork schema permits only the additive optional display `source_label`;
all execution fields remain required and additional properties remain forbidden.
All new store Test2376 roots are owned by the existing bounded partition. The
initial default run found obsolete schema/partition assertions; those were repaired,
not skipped or weakened (targeted API/planning/timing controls PASS, 0.125s / 1.544s /
13.996s; `/tmp/swarm-2376-ci-census-final.log`).
Full API spec/planning/timing packages subsequently passed, 2.560s / 19.983s /
3.758s / 15.286s (`/tmp/swarm-2376-spec-planning-final.log`). The explicit additive
conformance root is checked by name with every pre-existing root preserved.
Both-store backend matrix controls PASS, 14.695s. Source integrity/readback/channel
collision controls PASS, 2.786s; source projection/exclusion controls 0.006s and
serve writer control 0.137s (`/tmp/swarm-2376-integrity-controls-final.log`).

Receipt R8: default coordinated `go run ./cmd/swarm-test` profile. No --full or
direct go test ./... is used; this does not relabel default coverage as --full.
The pre-rebase rerun passed broad-01 (179 packages, pipeline 310.659s) but was
canceled by F while queued after master advanced. It is NOT a whole-profile pass.
The first post-rebase default profile passed broad-01 (179 packages) and was
canceled by F while queued after the bounded static-gate repairs. It is NOT a
whole-profile or final-head pass (`/tmp/swarm-2376-default-rebased.log`).
The label-amended committed candidate was attempted; R12 records its environmental
failure, not a completed default-profile pass. After coordinated disk recovery,
R13 records the completed fresh default profile on committed head 1ce88a350.
The redundant queued multi-package repetition was canceled; finite focused
post-rebase proofs below actually ran using ordinary go test, as directed.

Receipt R9: post-rebase proof on master 276d7723f, no PostgreSQL skips:
- All Test2376 fork/channel/source-COMMIT owner roots: PASS, 7.217s.
- All Test2376 CLI source/selector/presentation and agent-frame CLI controls:
  PASS, 0.758s; strict artifact/corpus/projection table PASS, 0.124s.
- Compiled identity/nested scope/readiness controls: PASS, 3.518s / 0.210s.
- All Test2376 real served admission/fork A/B/C/root-refusal roots: PASS, 24.162s.
- Public release-binary verify/member/hash test: PASS, 6.097s. All 45 incoming
  compiled describe baseline rows were independently reproduced; 15 route rows
  remain identical, and the other 30 change only labels/spec provenance. Embedded
  spec filename changes match the actual base/head SHA-256 digests, not a wildcard
  normalization. Capture: `/tmp/swarm-2376-describe-rebased-capture.log`.
- Compiled public event restart: PASS, 15.021s; durable data-operation restart:
  PASS, 27.364s, both stores.
- Effective frame after retained mock restart: PASS, 17.448s; changed-source
  standing restart: PASS, 29.063s; retained/clear reset: PASS, 10.688s, both stores.
- Legal inherited committed gate routing, approved terminal evidence and pending
  generation controls: PASS, 26.498s. Rejected-effect before/after-point proof
  separately passed, 3.665s, and also ran in the 7.217s owner matrix. The initial
  PostgreSQL fixture used one bind parameter for UUID and text; that fixture was
  corrected without changing production or weakening its guard.
- API/spec controls PASS, 0.233s; named partition root controls PASS, 0.626s;
  additive reader ledger PASS, 0.047s. OpenRPC generation and diff checks pass.
Logs: `/tmp/swarm-2376-rebased-{owner-matrix,cli-source,artifact,identity,
public-fork,binary-verify,event-restart,data-restart,effective-frame,standing,
reset,legal-fork,spec-plan,reader-ledger}.log`.

Receipt R10: bounded static-gate closeout. Formatting, all-package build and vet
passed. Channel source-label projection and fork-card repeat/disposition settlement
were separated into owner-local helpers without changing any guard. The exact
same two-store fork matrix and legal inherited-routing control passed (35.454s);
channel source/readback/catalog controls passed (0.689s).
Independent complexity comparison against master: cyclo >=30 remains 265;
cognit >=30 decreases 578 -> 577 and >=50 decreases 193 -> 191. The baseline was
regenerated from committed source, never used to waive growth or change policy.
The real public fork suite also passed after the factoring (16.646s). Native
analysis identified the now-unused public hash-boot projection wrapper; it was
deleted, while all exact stored-source loaders remain. A second queued default
profile was canceled before executing any unit for this final static cleanup.
Native default/race/issue2413 unused analysis passed (Linux only, not the required
Linux/Darwin union); formatting/build/vet/OpenRPC also pass after cleanup.
Exact-head hosted qualification remains pending.

## Label-Safety Amendment And Local Proof

The user accepted 256 UTF-8 bytes per metadata field and display-only format-control
replacement, recorded at issuecomment-5965029428. This resolves the bounded policy
question raised at issuecomment-5963222256; it does not grant merge approval.
M36a/M36b below are same-owner siblings, absorbed here rather than deferred.

Receipt R11, local candidate on master 0fa24140a:
- Permanent failing tests first reproduced all three oversized fields at root and
  nested construct/decode, alias/merge evidence, every Unicode Cf character,
  renderer labels and persisted channel readback on both stores. The real served
  owner also reproduced oversized-name acceptance on both stores. Red logs:
  `/tmp/swarm-2376-label-red-{owner,consumers,stores,admission}.log`.
- Complete sourceartifact package PASS, 0.184s: 255/256/257-byte ASCII and multibyte
  boundaries, byte refusal before semantic parsing, exact introduction/resolution
  evidence, no oversized payload echo, and unchanged metadata/bytes/framing/hash.
- All Test2376 CLI roots PASS, 3.650s: hostile labels across the existing renderers,
  real verify human/JSON original metadata/full hash, exact machine and retired
  selector controls.
- All Test2376 runtimepersistence roots PASS, 8.270s: SQLite/PostgreSQL exact channel
  selection and prefix/name collisions, hostile stored labels after source deletion,
  unchanged raw metadata/hash, plus all fork disposition/rollback/repeat and physical
  source-COMMIT controls.
- All Test2376 served roots PASS, 26.587s: verify/serve member parity, every oversized
  field refused before listener publication/source/domain mutation on both stores,
  and existing real A/B/C fork execution/pin controls.
- Compiled identity/nested scope/readiness controls PASS, 0.309s / 0.333s.
- Real release-binary verify/member/hash proof PASS, 5.182s. All 45 incoming describe
  rows reproduced with independent current-base/head binaries; 15 routes unchanged,
  30 changed only first-line/source_label and exact embedded-spec provenance.
  The digest filenames match actual spec SHA-256, not wildcard normalization.
  The compiled characterization test passes, 98.763s. Logs:
  `/tmp/swarm-2376-label-{binary-verify,describe-capture,describe-validation,describe-test}.log`.
Green logs: `/tmp/swarm-2376-label-green-{owner,consumers,stores,admission,identity}.log`.
Spec records the same finite limit and display-only policy. These are local receipts;
the repair is intentionally unpublished while CI-quiet is active.

Receipt R12, qualification attempt on committed local head f31c665da:
- Formatting, all-package build/vet, OpenRPC and four watchlist YAML/82 unique IDs
  passed. Complete API-spec and channel packages passed, 11.885s / 7.503s.
- Complexity baseline was regenerated from committed source and checked again
  against the exact amended head. Policy is unchanged: cyclo >=30 remains265;
  cognit >=30 decreases578 ->577 and >=50 decreases193 ->191. Evidence:
  `/tmp/swarm-2376-label-complexity-check/{head,delta}.json`.
- Default `go run ./cmd/swarm-test` planned14 units; broad-01 acquired its slot and
  reached140 passing packages before qualification failed. SQLite reports
  `database or disk is full (13)`; PostgreSQL reports `No space left on device
  (53100)`, then recovery/checkpoint errors; the linker also failed for disk space.
  This is an incomplete FAILED profile, not a pass or a timing waiver. Log:
  `/tmp/swarm-2376-label-default.log`.
- Native Linux default/race/issue2413 unused analysis failed writing the shared Go
  cache with `no space left on device`, NOT unused-symbol closure. Log:
  `/tmp/swarm-2376-label-unused.log`. Earlier successful R10 analysis stays attributed
  to its earlier snapshot, not this final candidate.
- The460-GB shared volume was100% used; Go build cache measured198GB. F removed
  only four inactive F verification executables (about472MB), preserving all
  logs/proof artifacts, worktrees, shared cache and other workers' processes.
  About1GB remained, insufficient for a responsible large rerun. Coordinated
  shared-space reclamation was requested from the user; no broad cache deletion,
  test cancellation belonging to another worker, source push or docs push occurred.

Receipt R13, fresh qualification after user-coordinated disk recovery:
- Exact tested head: `1ce88a3500d5cf1e69ae276091e4d5bb773e9292`, based on merged
  master `0fa24140aaa05e397509f451ba780fef3a260867` including #2537. Source remained
  clean and unchanged throughout the run. Any subsequent receipt-only amendment
  does not relabel this test run as execution on a different commit.
- Default `go run ./cmd/swarm-test` exited 0: all 14 planned units passed required
  execution, with zero failure records. No --full or direct go test ./... was used.
  Execution ran 2026-10-03 04:01-04:14 UTC after a 5m39s coordinated slot wait.
  Both SQLite and PostgreSQL cases executed. Log:
  `/tmp/swarm-2376-disk-recovery-default.log`.
- The following are reported test-package timings, not wrapper/queue budgets:

  | Required unit | Result / package execution |
  | --- | --- |
  | broad-01 | PASS, 179 tested packages; sourceartifact 0.302s, runforkexecution 194.510s, pipeline 302.808s |
  | catalog-required-inventory | PASS, 1.356s |
  | catalog-required-verify | PASS, complete CLI package 202.863s |
  | local-catalog-smoke | PASS, 2.455s |
  | local-generated-fanout-fixture | PASS, 1.530s |
  | local-fanout-handoff-ack-loss | PASS, 5.082s |
  | local-api-matrix-registry | PASS, 2.291s |
  | local-api-routing-canaries | PASS, 2.404s |
  | local-routing-reporter | PASS, 20.278s |
  | local-delivery-continuation | PASS, 0.159s |
  | local-release-golden-restart | PASS, 35.538s; actual restart/forced-kill cases on both stores |
  | local-release-lifecycle-smoke | PASS, 22.617s |
  | local-serveapp-canaries | PASS, 57.346s |
  | local-runtime-bus-full | PASS, complete package 42.166s |

- Fresh `go run ./cmd/swarm-unused` exited 0: native Linux default/race/issue2413
  analysis matrix, not hosted Linux/Darwin union and not a race execution claim.
  Log: `/tmp/swarm-2376-disk-recovery-unused.log`.
- Exact-head complexity validation exited 0: cyclo >=30 remains 265; cognit >=30
  decreases 578 -> 577 and >=50 decreases 193 -> 191. Existing policy/baseline
  unchanged. Evidence: `/tmp/swarm-2376-disk-recovery-complexity/{head,delta}.json`.
- OpenRPC check passed (71 methods, 244 schemas, 68 errors, 30 mutating methods,
  five subscriptions); diff check and four watchlist YAML documents/82 named IDs
  passed. Log: `/tmp/swarm-2376-disk-recovery-openrpc.log`.
- Disk headroom remained about 70 GB after qualification. No code, assertion,
  timeout, skip, shared-cache cleanup or other worker cancellation was needed.
  R12 remains failed historical evidence. No source/docs push occurred; CI-quiet
  and final hosted/independent qualification still gate merge readiness.

## Manifestation Coverage

Each final row has exactly one closure disposition. A named consumer/control proof
is required even when production previously used the correct owner.

| Row / known manifestation | Disposition | Exact execution proof |
| --- | --- | --- |
| M01 source invocation spellings/aliases | execution-proven through the same corrected path | Test2376SourceInvocationParity: bare/dot/relative/absolute/root_alias/symlink_invocation (R1) |
| M02 framed hash independent recomputation | reproduced and fixed | Test2376IndependentHashRecomputation; Test2376VerifyJSONExactMemberTable literal BE framing/vector; Test2376SourceVerificationBinaryExactMemberTable real release CLI/independent encoder; Test2376PublicSurfaceSpecCensus (R1/R2/R5) |
| M03 mutation after admission | execution-proven through the same corrected path | Test2376VerifyEvidenceUsesAdmittedGeneration; TestDirectoryMetadataRemainsBoundToOpenHandleAfterPathReplacement (R1/R2) |
| M04 included/excluded dispositions | execution-proven through the same corrected path | Test2376MemberDispositionMutationParity; TestExcludedMutationCannotChangeArtifact (R2) |
| M05 verify/serve classification parity | execution-proven through the same corrected path | Test2376VerifyServeArtifactParityBothStores pre-publication member/blob equality; Test2376RootManifestRejectsBeforePublicationBothStores/unknown_path (R4) |
| M06 absent manifest | reproduced and fixed | Test2376ManifestOptionalStrictPresence/absent; Test2376VerifyJSONExactMemberTable/absent (R1/R2) |
| M07 required field presence | reproduced and fixed | Test2376ManifestOptionalStrictPresence all eight presence bitsets, root/nested construct/decode (R2) |
| M08 null/empty/wrong kinds | reproduced and fixed | Test2376ManifestOptionalStrictPresence document/each-field kind table (R2) |
| M09 unknown/retired metadata keys | reproduced and fixed | Test2376ManifestUnknownKeysAndDuplicates/description/requires/publisher/unknown (R2) |
| M10 duplicates/alias/merge/expansion | reproduced and fixed | Test2376ManifestUnknownKeysAndDuplicates; Test2376ManifestAliasMergeProvenance and expansion refusal (R2) |
| M11 malformed/multidocument manifest | reproduced and fixed | Test2376ManifestMalformedDocument root/nested construct/decode (R2) |
| M12 free-text name/directory mismatch | reproduced and fixed | Test2376ManifestHumanNamePreservesMetadataButNotTerminalControls; Test2376NestedManifestPreservesSemanticScope; Test2376StoredSourcePresentationHasNoInventedRoot rename (R2) |
| M13 strict SemVer | reproduced and fixed | Test2376ManifestStrictVersion positive/prerelease/build/loose/malformed table (R2) |
| M14 root platform compatibility | reproduced and fixed | Test2376ManifestRangeGrammar; Test2376LocalIdentitySelectorsUseAdmittedSource; Test2376RootManifestRejectsBeforePublicationBothStores/incompatible/malformed_range, no publication/mutation (R1/R2/R4) |
| M15 nested manifests/scopes | reproduced and fixed | Test2376ManifestDispositionAndSelectedRootCompatibility; Test2376NestedManifestPreservesSemanticScope topology/pins and nested-as-selected-root refusal (R2) |
| M16 manifest lookalikes | execution-proven through the same corrected path | Test2376ManifestDispositionAndSelectedRootCompatibility packs/docs resource/document cases (R2) |
| M17 platform contamination | execution-proven through the same corrected path | Test2376SourceHashIndependentOfPlatformAdmission; TestBootBundleIdentityStableAcrossRootsAndFileOrder; TestBootBundleIdentityChangesWithLoadedContent (R2) |
| M18 corpus/generated fixtures | reproduced and fixed | TestTrackedManifestRootsUseFiniteSourceGrammar and actual generated serve/CLI/release fixtures (R1/R2/R4/R5) |
| M19 public serve hash boot | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/serve; Test2376DirectoryBootUsesExactSelectedStoreSource; directory restart (R1/R4/R5) |
| M20 event hash guard | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/event_publish; Test2376EventPublishDerivesExactRuntimeSource; TestPayloadlessEventPublicPersistenceJourneyBothStores new/existing run (R1/R5) |
| M21 data import hash guard | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/data_import; Test2376ForkSourceAndPinsFollowSourceRunBothStores public import exact source; durable receipt restart (R1/R4/R5) |
| M22 data show hash guard | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/data_show; Test2376ForkSourceAndPinsFollowSourceRunBothStores public exact version show; TestDurableDataOperationAggregatePublicRestartBothStores durable receipt controls (R1/R4/R5) |
| M23 data prune hash guard | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/data_prune; Test2376ForkSourceAndPinsFollowSourceRunBothStores public current-head refusal; TestDurableDataOperationAggregatePublicRestartBothStores prune idempotency/restart (R1/R4/R5) |
| M24 run start hash guard | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/run_start; Test2376ForkSourceAndPinsFollowSourceRunBothStores public pinned start; TestDurableDataOperationAggregatePublicRestartBothStores compiled accepted/rejected/replayed creation (R1/R4/R5) |
| M25 static/hash agent frame | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/agent_frame; TestAgentFrameCLIRejectsSelectorConflictsBeforeAPIRequest static/flow negatives; TestMockAgentSupportedSurfaceSQLitePostgres/effective_frame_exact_source after restart, real --api-server human+JSON; TestCLIAPIConnectionFlagsSurfaceAndIsolation (R1/R4) |
| M26 explicit already-stored C | reproduced and fixed | Test2376ForkSourceAndPinsFollowSourceRunBothStores/explicit, real CLI/API target and child card/activity pins (R4) |
| M27 missing D target | reproduced and fixed | Test2376ForkSourceAndPinsFollowSourceRunBothStores/missing: unchanged run/event/source/domain counts and serve-first teaching (R4) |
| M28 default and pin source A versus health B | reproduced and fixed | Test2376ForkSourceAndPinsFollowSourceRunBothStores default/pin/contradictory/explicit/repeat with real fixed-revision feed (R4) |
| M28a pending gate source C | reproduced and fixed | public explicit C child activation/card/activity hash+workflow version; Test2376ForkDecisionExecutionSourceBothStores/open (R3/R4) |
| M28b restored gate dispositions | reproduced and fixed | Test2376ForkDecisionExecutionSourceBothStores/open/decision_committed/routed/superseded on both stores; TestMaterializeRunForkRootAuthoritiesExecuteWithForkIdentitySelectedStoreParity committed inherited route/repeat (R3) |
| M28c pending proposed pair/generation | reproduced and fixed | Test2376ForkDecisionExecutionSourceBothStores pair Validate/target hash+version/fresh IDs/input/reply-detached; TestForkPendingGenerationCorrespondenceBothStores; Test2376ActivityContractPinsConsumeAdmittedSource; selected control refusals (R3/R4/R6) |
| M28d historical effects/human exclusion | execution-proven through the same corrected path | TestPrepareRunForkApprovedProposedEffectRequiresUnambiguousTerminalEvidence compatible stored C succeeded/failed/uncertain/started/missing; Test2376ForkRejectedEffectHistoricalBoundaryBothStores rejection before/after point, exact parent readback, fresh child pending and repeat; Test2376ForkDecisionExecutionSourceBothStores human exclusion+parent A exact readback; activity no-call reuse (R3/R6) |
| M28e strict final guards/rollback/repeats | reproduced and fixed | Test2376ForkDecisionExecutionSourceBothStores wrong hash/version/late failure rollback/exact and conflicting repeats; committed root coordinator repeat one output; Test2376ActivityContractPinsConsumeAdmittedSource six hash/version cases; public selected control refusal (R3/R4/R6) |
| M28f invalid feed fixture | reproduced and fixed | Test2376ForkSourceAndPinsFollowSourceRunBothStores public import->pinned run start->chosen revision->fork; TestSelectedDeploymentFeedAgreementUsesFixedSourceDeclarations missing/extra/contradictory; TestSelectedDeploymentRevisionFrontierRequiresExactFeedWork (R4/R7) |
| M29 fork hash selector | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/run_fork; Test2376NoAuthoredHashSelectorInCommandTree; Test2376PublicSurfaceSpecCensus (R1) |
| M30 channel connect alias | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/channel_connect; TestOperatorChannelCLIUsesAuthenticatedAPIAndExactSelectors/exact_selectors/connect; exact both-store catalog/readback (R1/R3) |
| M31 reconnect alias | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/channel_reconnect; TestOperatorChannelCLIUsesAuthenticatedAPIAndExactSelectors/exact_selectors/reconnect; original revision/security controls retained (R1/R3) |
| M32 rebind alias | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/channel_rebind; TestOperatorChannelCLIUsesAuthenticatedAPIAndExactSelectors/exact_selectors/rebind; original principal/confirmation controls retained (R1/R3) |
| M33 status alias/pairing | reproduced and fixed | Test2376RetiredHashSelectorsFailBeforeIO/channel_status; Test2376ChannelStatusRequiresPairedSourceAndTargetBeforeIO; TestOperatorChannelCLIUsesAuthenticatedAPIAndExactSelectors exact activation status selector/machine modes (R1/R3) |
| M34 changed source/ambiguity/collisions | reproduced and fixed | Test2376ChannelExactSelectionAndStoredPresentationBothStores zero/one/many/both orders; actual fa84932 collision and equal-name artifacts; Test2376ChannelPrefixAndNameCollisionsRemainExact (R2/R3) |
| M35 verify exact JSON table | reproduced and fixed | Test2376VerifyJSONExactMemberTable present/absent body/disposition/label/hash and outsider recomputation; Test2376SourceVerificationBinaryExactMemberTable bare/dot/mutation; serve durable comparison (R1/R4/R5) |
| M36 human hash/placeholder leakage | reproduced and fixed | Test2376HumanSourceIdentityConsumerTable; Test2376TestDiagnosticsKeepExactLookupButHumanTeaching; actual verify/serve/frame; collision candidate diagnostics (R1/R2/R4) |
| M36a oversized metadata at construct/decode and pre-publication admission | reproduced and fixed | Test2376ManifestUTF8ByteBoundaries all three fields and multibyte name, 255/256/257 bytes, root/nested construct/decode; Test2376ManifestOversizeEvidencePrecedesSemanticParsing exact field/alias/merge evidence; Test2376RootManifestRejectsBeforePublicationBothStores all three oversized fields with source/domain counts unchanged (R11) |
| M36b Unicode format controls in human labels | reproduced and fixed | Test2376HumanLabelsRemoveEveryFormatControl exhaustive Cf census; Test2376FormatControlPresentationPreservesExactArtifact raw metadata/bytes/hash; Test2376HumanSourceIdentityConsumerTable every renderer and real verify human/JSON; Test2376ChannelExactSelectionAndStoredPresentationBothStores hostile stored readback after source deletion with exact authority unchanged (R11) |
| M37 machine identities/equality/order | execution-proven through the same corrected path | Test2376MachineIdentityRemainsExact JSON/explicit YAML; both-store prefix/name selection and stored historical coordinate equality; public fork child SQL identity (R1/R3/R4) |
| M38 stored source missing basename | reproduced and fixed | Test2376StoredSourcePresentationHasNoInventedRoot; Test2376ChannelExactSelectionAndStoredPresentationBothStores removed-origin readback, no invented store-/cwd basename (R2/R3) |
| M39 same-tree public process restart | execution-proven through the same corrected path | TestPayloadlessEventPublicPersistenceJourneyBothStores; TestDurableDataOperationAggregatePublicRestartBothStores exact retained source/receipts after directory boot (R5) |
| M40 changed-tree retained standing N | execution-proven through the same corrected path | TestStandingIngressSupportedSurfaceSQLiteRestartPreservesAuthorityAndReplies and TestStandingIngressSupportedSurfacePostgresRestartPreservesAuthorityAndReplies invoke requireChangedStandingColdStartMatrix; TestStandingRestartMixedHealthyAndTerminalProcessParity (R4/R6) |
| M41 corrupt/missing stored fail-closed | execution-proven through the same corrected path | TestRunServeSourceArtifactIntegrityRejectsBeforeReadinessBothStores; TestSourceArtifactStartupIntegrityParityPreservesRunHistory (R3/R4) |
| M42 internal recovery/fork/reset no files | execution-proven through the same corrected path | TestServedResetRetainClearAndHistoricalReplayBothStores removes original root before retained successor; TestRunForkRuntimeOwnerHarness_PersistedBundleDoesNotRequireAmbientSource; TestStaticDataArtifactReconstructionIgnoresOriginalRoot (R2/R4/R6) |
| M43 uncertain persistence acknowledgment | execution-proven through the same corrected path | Test2376SourceArtifactCommitUncertaintyBothStores eight physical commit/rollback-ack cells, real selected owner/readback/exact explicit retry; Test2376ServeSourcePublicationRequiresAcknowledgedIngest caller result+error refuses source publication; TestPrepareServeSourceArtifactRequiresSelectedIngestWriter; verify durable-before-publication (R3/R4) |
| M44 obsolete publication/spec promises | reproduced and fixed | Test2376PublicSurfaceSpecCensus actual Cobra/spec/known vector/retired names; OpenRPC/backend matrix/reader ledger (R1/R7) |
| M45 durable compiled-cache suspicion | execution-proven through the same corrected path | TestRuntimeProjectionOwnsExactGenerationAndLifetime; retained handle/release synchronization; real process restart reconstructs stored source. Lifetime census/spec explicitly distinguishes cleanup/evidence from compiled cache (R2/R5/R6/R7) |
| M46 tar/register/catalog/eligibility/receipts/upgrades | split / escalated as separate class | Gate F explicitly retires/moots publication obligations; Test2376PublicSurfaceSpecCensus absent bundle commands and retired archive/publication spec. Source vendoring/upgrades retain only their own open #2311/#2377 scope; no tar/registry claim (R1/R7) |

## Closure, Tracking And Architecture

Intended level: failure class eliminated for the approved author-source class,
including M28 and the newly reported label-safety siblings. Actual current level:
approved paths canonicalized with passing supported proofs, including the two
same-class label-safety repairs and their permanent red/green tests. No known
same-concept interpreter remains in the enumerated class; the final local default
profile passed (R13), while exact-head hosted CI and independent acceptance remain
outstanding. No new
owner, deferred follow-up or framework is needed.
User-coordinated disk recovery enabled fresh local qualification (R13); the earlier
failed attempt (R12) is not reclassified as a pass. No production repair, relaxed
assertion or timing waiver was required for the successful rerun.
Broader presence/effective-model program remains open; the pre-audit estimate is
unchanged: four to six groups, medium confidence (public scenario/fixture grammar
#2532; provider-trigger body carriers #2533; composition/activation #2438; remaining
census/derivation #2300/#2287/#2293; source-vendoring #2311 and upgrade #2377).
These are independently tracked concepts, not a residual tail of this chosen class.
The implementation discovery added M28 now,
not a child-slice tail. Investigation #2425 remains evidence/investigation-only.

Parent/sibling probes: every public selector and presentation family, resource and
document lookalikes, nested roots, remote resume, stored recovery/reset, channel
exact selection/order/security, generic and selected fork materializers, frozen gate
routing, proposed pair/activity pins, terminal recorded evidence and human exclusion.
No previously unknown same-concept interpreter remains in this enumerated boundary.
Different semantic concepts retain their named owners/controls; unsupported selected
control approval is not quietly expanded to obtain a positive effect proof.

Generic failing proof: constructed Cobra retirement census and strict manifest
construct/decode table, plus real public A/B/C explicit fork reproduction. The latter
was independently reproduced on both stores before amendment; corrected child story
and downstream pins are now proved, with rollback and repeat controls.

Watchlist: existing canonical_bundle_input_filesystem_admission and
canonical_bundle_identity_and_source_fact nodes only. Record implementation/proof
status, keep review/CI/merge closure pending until independently accepted. No new
issue/node/POTENTIAL_ISSUES entry; #2376/#2322 remain open until merge. Source reader
ledger remains additive; no closure claim for #2300/#2287/#2293 or D/C decoder work.

Architecture smell addressed now: metadata, display and machine selection had mixed
ownership; copying records conflated parent provenance with child executable pins.
Long-run direction is the existing typed immutable artifact and explicit admitted
target, not another registry/source container/fork/cache framework. Tracking is this
PR and the two existing watchlist nodes, with mixed-pin evidence also in #2425.
Estimated further effort for this chosen-class smell: one bounded repair/proof
cycle in this PR (now implemented locally), zero planned follow-up child slices. Parent
program effort is not estimated from this PR. ROI is high: twelve unsafe/manual
public hash surfaces deleted, strict metadata admitted once, coupled fork pins fixed,
while machine authority/fences and historical evidence remain exact. No migration,
fallback, compatibility, archive, display database, third-party vendoring, or new
global semantic hash was introduced.

Production accounting against merged base 0fa24140a: 48 Go production files,
701 additions and 424 removals (net +277), below #2407's 3,000-addition ceiling.
The original net-negative estimate did not materialize: strict metadata admission
and the approved coupled fork repair outweigh the removed public surface. Public
selectors/static-client/hash-boot paths are actually deleted, but this audit does
not mislabel the total diff as net-negative. Tests, fixtures and spec account for
most of the overall change; the 199 manifest files are a mechanical grammar migration.
The issue body's original net-negative expectation needs explicit closeout
disposition against this disclosed variance; this audit does not self-authorize
a waiver or unrelated deletions to force a negative count.
