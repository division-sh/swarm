# #2700 complexity extraction admission accounting

Scope: the exact transaction extraction at `9cd800efa5b0136d09c8d069bd63bfdd828fccb0`,
reviewed by both the native owner and assignment reviewer:
https://github.com/division-sh/swarm/issues/2700#issuecomment-6249418081
https://github.com/division-sh/swarm/issues/2700#issuecomment-6249448559

The original A19/A28 owner `Backend.runTransactionOutcome` retains its public
RunTransaction/RunTransactionOutcome/RunTransactionWithOptions/RunReadTransaction
callers, callback context, ObserveWriteStartContext/write.End lifetime, mutation
admission, commit acknowledgment and exact serialization-conflict disposition.
Its deferred cleanup calls the private rollbackAndCloseTransactionConnection
helper, which joins rollback and releases ordering before the private
closeTransactionConnection helper performs Raw/discard/Close. These are the same
contiguous statements and same error joins, not additional admission owners.
The discard copy has no later outer observer. No cancellation-only repair is
included; that behavior remains C's separate ownership.

The generated whole-file census retains A19,A28 and P18,P24 for the original
owner and both helpers. It does not drop the owner or a refusal. The accepted
inlining witness reconstructs the exact parent file byte-for-byte, SHA256
604eab38ee4d7c4ad899a4b82662320ea3a7070be41a92da0277309de5d02197.
Native execution witnesses remain TestPostgresTransactionSoleErrorIdentity,
TestPostgresTransactionExpiredBeforeAdmissionSkipsCallback,
TestPostgresTransactionCancellationRollbackOrders,
TestPostgresTransactionPanicAndFailureExits,
TestPostgresSerializationConflictRequiresCleanNativeSettlement,
TestRetainedPostgresTransactionExitMatrix and TestPostgresTransactionAdmissionExits.
The donor's reviewed race receipt covers 54 roots with zero failures/skips;
recipient source/accounting/preflight remains independently required.

This accounting neither changes the check registry nor authenticates the
separate R04 custody/collector relocation. That remains B's exact-source
continuation, without a baseline increase or blanket hash acceptance.
