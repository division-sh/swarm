# #1745 Connected Implementation Audit

Track: FULL. One connected PR; not separate parser/state/filename landings.
Binding contract: user6207714184; reviewer-c gate6207972014.
Implementation base: master9beb615299e14741752ea6c92128b8e6036e9ee6.
Status: implementation checkpoint, not landing approval. Hosted qualification and
the final A/F/B integration remain outstanding.

## Owners And Boundaries

- `contracts/catalog_embedding.go` is the single deferred declaration-to-flat-field
  expansion owner. It runs after declaring catalogs load, before effective views
  are published. It calls `CatalogTypeReference.Resolve`, preserves admitted
  field facts, and never opens ambient source files.
- Event metadata is the closed `_embed`/`_key`/`_description` namespace. Ordinary
  business names remain data. Malformed wrappers still refuse by field grammar;
  they are not decoded by a retired-syntax compatibility reader.
- Entity declarations consume the same expansion. Role attributes stay on own
  fields. Existing constructors, normalized operations and selected-store commit
  owners are unchanged; no SQL, migration, scheduling or transaction redesign.
- `sourceartifact/classify.go`, contract discovery/admission, current authored
  corpus and fixture sources switch atomically to `state.yaml`. Old-only and
  old+new layouts refuse finite source admission. Internal entity/SQL identities
  are not renamed. Historical source cannot be migrated through this change.
- Derived field evidence names the defining type-field coordinate and the actual
  authored `_embed[i]`. Event/state clones detach catalogs, refinements and
  provenance. Public describe consumes this evidence, not working files.

Watchlist: manifestation of existing
`loader_tree_identity_loss_before_boot_validation`, not a new census. The risk is
producer fields being reinterpreted through a receiver's catalog or their
defining coordinates being lost during expansion. E03/E07/E08/E10 are the
connected controls for that manifestation.

## E01-E10 Mapping

| Cell | Active proof owners and observation |
| --- | --- |
| E01 | `TestFlatEmbeddingEventAndStateEquivalence`, `TestFlatEmbeddingTwentyTwoFieldsAndNestedDomains`: inline, embedded-only and embedded+own acceptance/state shape equivalence. The 22-field case starts with the checked jobflow company's four fields and adds explicit nested/list/map/enum/optional/scalar domains. It is not substitute registry-v5 acceptance. |
| E02 | `TestFlatEmbeddingPermutationProvenanceAndDescription`: disjoint-record permutations retain acceptance bytes and actual authored indexes; repeated type refuses in E04. Source bytes need not be permutation invariant. |
| E03 | `TestFlatEmbeddingRefusesInvalidReferencesAndCollisions` plus alias/merge cells in `TestFlatEmbeddingCapturedCatalogAndRefinements`: identical embed/embed and embed/own collisions refuse with both defining locations; no per-use role annotation. |
| E04 | Same refusal root and existing catalog namespace/body controls: non-record kinds, optional root, missing/empty/repeated records, null/scalar/map/empty sequence, unused invalid catalogs and recursion remain refused. |
| E05 | Business-name and expanded-key cells in the equivalence root, current event/metadata/payloadless/platform-catalog controls. `_key` checks the expanded required scalar field; `_description` survives and does not change acceptance. Old test source cases remain, but no longer assert name-only bans contradicted by6207714184. |
| E06 | Named-field descriptions/optionality/refinements survive. Own initial/indexed/immutable declarations retain their role. The native journey constructs own initial state; the existing normalized field-operation owner checks immutability. Existing citation publication controls remain active. |
| E07 | Scope/detachment cells plus existing `TestCatalogTypeDetachment*`/`TestCatalogTypeNamespace*`: root, child and sibling catalogs, consumer mutations, copied views and refinements. A captured producer is not rebound by a receiver's same-named type. |
| E08 | `TestFlatEmbeddingVerifyAndDescribe`, `TestEffectiveEventResolutionPrefersConnectedProducerOverUnrelatedSameNameRoot` (inline and embedded), `TestFlatEmbeddingPublicationTool`: real verify/describe, connected pins, source typing/CEL/lowering, generated tool schema and publication refusals. |
| E09 | `TestFlatEmbeddingExecutionBothStores` and `TestFlatEmbeddingPublicEventPublicationBothStores`: actual canonical construction/write/emit/cold readback plus API publication on SQLite/PostgreSQL. Invalid type/refinement/missing/extra payloads and invalid state candidates leave application storage unchanged. No new raw SQL or production test hooks. |
| E10 | `TestFlatEmbeddingStateFilenameAndRetainedSource` plus existing retained-artifact both-store controls: newly admitted state.yaml source encodes/decodes and reconstructs after the host directory is removed. No legacy-artifact migration claim. |

## Receipts And Honest Boundaries

Receipts are retained under
`~/.local/state/agent-a-1745/proofs/`; original RED receipts are not overwritten.

- `flat-equivalence-initial-red`: state.yaml refused by the baseline finite source
  classifier. `flat-equivalence-candidate1`: GREEN after the single filename
  cutover and expansion boundary.
- `flat-admission-and-consumers-candidate3`: focused admission, shape, name and
  consumer controls GREEN (7.371s).
- `flat-connected-consumers-candidate5`: contracts GREEN0.294s and public CLI
  verify/describe GREEN0.277s. Native construction initially RED because the new
  fixture supplied arguments to the existing no-argument constructor. The
  fixture was corrected; no constructor behavior changed.
- `flat-runtime-and-history-candidate6`: native both-store write/emit/readback
  GREEN1.115s, publication tool GREEN0.136s, exact historical/mechanical and new
  spelling-successor controls GREEN1.219s.
- `flat-owner-guards-candidate7`: finite-source controls GREEN0.093s and selected
  codemod historical/fixture guards GREEN12.209s. Its one contracts RED was a
  newly authored, unquoted optional type inside a YAML flow mapping; corrected
  to the existing quoted scalar grammar.
- `flat-binding-candidate8`: namespace/detachment, connected producer catalog,
  event/platform metadata and embedding controls GREEN4.157s.
- `flat-publication-candidate9`: public event.publish both-store positive and
  unchanged-storage negative cells GREEN1.415s; checked-company 22-field
  equivalence GREEN0.131s.
- `describe-corpus-regeneration`: existing compiled CLI generator and all45
  fixture/surface cells pass (package56.019s). Only current filename-bearing JSON
  goldens change; source identities legitimately change with filenames.
- `contracts-first-complete`: RED at the unchanged180s package limit during
  repeated existing static Go-package guards. It also exposed old metadata-name
  oracles. This is not package-wide green credit; no broad retry or budget raise.
- `flat-connected-owner-matrix-candidate10`: canceled before test execution when
  its shared regex would have selected unrelated native pipeline roots. No
  proof credit. The corrected bounded selection uses `candidate10b`.
- `candidate10b`: preserves additional old name-ban REDs and a new test's incorrect
  assumption that the complete-snapshot sink enforces field-operation
  immutability. That sink takes an already-admitted candidate. The corrected
  fixture uses the existing normalized operation owner; it does not change
  production semantics or relax the unchanged-storage check.
- `flat-current-oracles-candidate11`: corrected contract/source cases
  GREEN0.608s and both-store construction/write/emit/refusal proof GREEN2.235s.
- Exact68d45d9207 preflight RED109.865s: the closed required-field projection
  census omitted `contracts/catalog_embedding.go`. Add that one declaration
  projection owner with no behavioral consumer permission. Format, OpenRPC,
  early structural owners and complexity passed before this stop.
- The extra Native codemod guard selection exhausted its180s outer package
  budget after earlier controls. Its last nested execution control had only13s
  before the outer timeout, versus its unchanged4m local context. This is
  incomplete evidence, not regression/pass credit; no broad retry or budget
  change. The named finite spelling/history controls remain separately GREEN.
- The old-head unused invocation was interrupted when the census correction
  required a new head. It supplies no unused qualification; rerun on the final
  candidate. Canonical hosted selection validation at68d45d9207 succeeded,
  including all ten new feature roots, without duplicate ownership.
- Exactd6494ff189 preflight RED100.676s: the route-authority inventory needs
  the newly appended spelling packet classified with its existing retained
  fixture-migration evidence. The packet contains frozen source strings, not
  executable delivery authority. Required-projection and preceding structural
  owners pass. Add the one exact evidence path; no classifier/budget waiver.
- `remaining-structural-guards-candidate12`: all remaining selected packages
  except the serveapp documentation-reference check passed. That check
  matched the ordinary word "builders" in this new audit, not retired product
  code; describe those fixture inputs as sources instead, without changing
  the guard or its exact reference counts.
- `spelling-inventory-repairs-candidate13`: the route/evidence inventory and
  documentation check pass, as do the original finite-stage fixture check and
  a new exact spelling-successor control. The original finite-stage ledger
  stays byte-identical (SHA256
  `3da9e2f24fb5126eede32087a4fbb2131152bcf4110901fd029c3626bf173b01`);
  six current endpoints are separately pinned, and exact inversion must
  recover each historical whole-file output. This is archival corpus-check
  evidence, never a state.yaml/entities.yaml runtime compatibility reader.

Historical codemod `recipes.json` stays byte-identical (SHA256
`4fd3948cf98e828528dd5681e1f93cf79102ac5a0887573a65893e27dd531c70`).
The new `state-spelling-successors-1745.json` appends26 exact spelling endpoints;
its generator refuses changes beyond the ruled filenames/event metadata.
Independent tests pin the packet, every predecessor, actual current source,
idempotence and refusal of changed/unlisted pairs. Prior Before/After/Mechanical
and #2617 packets remain unchanged. Recompose against B's actual merged ledger
before landing; never update a historical receipt to erase a mismatch.

## Integration And Qualification

Overlap pin: #1745/6208289264 and #2667/6208002311.
F3adf66ff103a shares classifier/spec; F owns provider membership/namespace.
B#2698 e857a1e482 shares current fixtures and codemod owners. Rebase and regenerate
against the actual landing order before the PR; no provider or B owner repair is
implicitly absorbed.

Gate selection, to validate with the canonical planner at the final head:

```
CI-Tier: lifecycle
CI-Units: catalog-replay-2-3, codemod-candidate-overlay, hitl-releasee2e-invocation-1, hitl-releasee2e-invocation-2, hitl-releasee2e-invocation-3, hitl-releasee2e-invocation-4, hitl-releasee2e-invocation-5, hitl-releasee2e-invocation-6
Local-Tier: focused
```

Lifecycle owns contracts/type/source, compiled runtime/store/API/served and
catalog-runtime/replay-1 paths. The filename/corpus cutover reaches replay2-3
and static-data invocation's root/registry declarations; appended codemod
endpoints reach candidate-overlay. No local lifecycle/core/full duplicate or
server2 tier. No concurrent execution owner changed, so no new race-x3 demand.

Preflight: PENDING exact committed-head execution. Exact receipt and later
focused/guard results will be posted on #1745 without changing the qualified tree.
Unused, final source/codemod guard refresh and hosted qualification remain open.
