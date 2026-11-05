# rc.1 Proof Artifact

`rc1-credential-proof-v2.json` is the unmodified synthetic artifact written by the
published Linux/amd64 `v0.7.0-rc.1` binary through public `serve` and
`channel connect telegram --yes`, using the existing release-channel provider
double and a fresh isolated HOME. It contains no live account or credential.

- Source commit: `335839f7f1db9422e18e0acb36ae4e63b9ffe445`.
- Release archive SHA256: `f9d9b60358491cfe6e52c4a31edf27130221ebc61e7c1ad3775bc972988a3bbf`.
- Artifact SHA256: `c48eec9b6c441ea55608bde1f4d109bfe81b7ce4cb8c0fecd56e8d67054a5747`.
- Public reproduction: https://github.com/division-sh/swarm/issues/2942#issuecomment-6651843368.

The active credential proof exercises the historical external-account member.
The zero session was omitted by rc.1's `session_account,omitzero` codec. Separate
unit cases cover the source-supported session and session-plus-credential codec
shapes; they do not claim an executed rc.1 phone/session ceremony.
