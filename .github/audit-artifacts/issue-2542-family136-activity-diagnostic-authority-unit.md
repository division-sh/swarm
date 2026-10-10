# Cohort 136: meaningful activity diagnostic authority control

The old DoesNotUseAmbientPostCommitAuthority unit injected test-private context
keys for SQL transaction, post-commit and rollback arrays. Those keys exist only
in the fake transaction fixture; production WriteActivityIntents cannot observe
them. An untouched empty callback slice was not meaningful publication-authority
evidence. It also kept a bogus raw sql.Tx inside an otherwise pure unit.

The unit now retains the same preview coordinator, source, intent, context scope,
successful call, exact immediate diagnostic count and intent_persisted action.
It asserts zero outbox plans and zero actual publication using the existing
recordingPipelineBus counters. No DB is created, no fixture transaction is
simulated, and this is not a native persistence/commit/rollback witness.

One bounded overlay modifies the REAL production activity writer to publish the
computed request before logging. The old fake-context root still PASSES; the new
root FAILS specifically at published events=1, want no dispatch. Production in the
integration branch remains unchanged. This is a reproduced narrow test-contract
repair within the authorized fake-protocol retirement, not a runtime defect or
permission to weaken a real temporal cut.

Four focused roots pass under race: corrected immediate diagnostic, existing
validation-without-persistence, mock request/result bytes and causal mode conflict.
The finite whole-function recipe preserves all other statements and rejects lost
log, outbox, dispatch and intent assertions. The renamed root no longer claims
coverage of obsolete ambient transaction authority. Test-private protocols and
other consumers in transaction_fixture_test.go remain explicitly parent debt.

No new framework, compatibility key, delegated grant, retry, skip, timing or
production-spec change. Existing #2151/#2542 approval and tracking suffice.
