# In-Process Head Census Sharing

Standalone preparation on master 395fd6ade8acbab4d07f338d0dda3d09caf58609.
Cross-run cache commits remain separate on
`agent-d/2353-ratchet-analysis-cache-review`; this branch does not depend on them.
No PR, CI configuration, shared-cache artifact or timeout change is included.

## Owner And Consumers

`debtLoadPersistenceAuthorityFindings` remains the complete typed census owner.
Native fixture-family checks and the debt ratchet consume its same complete head
result through a process-local memo when all live inputs are unchanged. The base
census is still independently computed from `materializeDebtBase`; it never
consumes the head memo. Every existing family assertion, site identity,
multiplicity, bootstrap check, baseline comparison and output format is retained.

The existing active-package selection is mechanically extracted into
`debtAuthorityPackagePatterns`, used by both collection and input enumeration.
There is no second source-selection interpreter or filtered permission inventory.

## Freshness And Isolation

The key covers the actual root, all owned Go source (including dirty, untracked,
Git-ignored and inactive files), complete dependency source/build/embedded files,
module files, process environment, Go runtime and effective build settings.
Package metadata enumeration requests no syntax/types/type-info and does not
perform a second type-check. The fingerprint is rechecked after collection;
changed or incompletely enumerated inputs cannot publish a memo entry. Every
consumer receives an independent findings slice. A mutex prevents simultaneous
consumers from performing duplicate collection or mutating shared state.

Full `go env -json` proved unsuitable because GOGCCFLAGS embeds a randomized
temporary compiler path. The final key binds stable effective settings instead;
the failed early sharing controls are retained as diagnostic evidence, not
reported as passes. The complete final source/context and result-isolation
controls pass at race/count-three, including local dependency and embedded-data
changes and a source mutation during collection.

The permission fingerprint includes the sharing implementation. The exact,
metadata-only transition is b42ea974646e7b666459091174645baa501d91da16871813568db23858def7b7
to 3a3f7f6eba988ba7815cb5069dc03e315c6495b344e64707e9289eacd50f89ef.
Debt rows are byte-identical; foreign/reverse/future transitions and row changes
remain rejected. The existing three ratchet comparisons are unchanged.

## Qualification And Measurement

Final focused race/count-three controls: PASS (86.462s package).
Vet, touched-package guard/census/inventory/registry sweep and the complete core
structural-owner guard selection: PASS. No full/lifecycle qualification claimed.

Measure the complete existing `persistence-authority-debt-census` unit on server2
only after explicit handoff, using the managed runner and original two roots and
seven required native-family children. Pair unchanged master with this standalone
head. Preserve shared Go compilation/module caches, original timeout and all
assertions. Use the existing GOPACKAGESDEBUG facility equally in both runs to
count actual typed (-export=true) loads, distinguishing them from untyped input
metadata enumeration. A proposed benefit is three typed loads becoming two;
measurements and hosted estimates are not invented in this pre-measurement note.
