# Building the fork for desktop clients

The desktop client can send structured feature configuration when creating a thread.
An old binary can initialize and list threads successfully while still rejecting
`features.guardianv2` at `thread/start`. Checking only `--version` or `config/read`
does not establish compatibility.

The **Fork desktop compatibility build** workflow builds the checked-out source for
Intel macOS and uploads an artifact containing the CLI, runtime helpers, source
revision, compiler version, and SHA-256 of every binary. It uses the `dev-small`
profile for a practical native compatibility build; it is not an optimized release.

The workflow runs the existing Guardian parser/validation tests and a real App
Server smoke test. The smoke creates one thread with structured Guardian settings
in a new temporary workspace and private Codex home. It starts no model turns and
does not access personal login state. Unknown or invalid policy settings continue
to fail validation.

After downloading an artifact, verify its manifest and test it in an isolated
workspace before replacing a running daemon. Keep the original package for rollback.
When using a desktop WebSocket override, keep the GUI's bundled CLI for its own
version-specific startup and organization-policy checks.
