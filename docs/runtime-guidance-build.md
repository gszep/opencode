# Runtime-guidance candidate

This fork candidate uses runtime version **`2.0.15-chi.2`**, not the previously
installed `2.0.15-chi.1`. Source review, artifact creation and installation are
separate gates. No installation or service restart is authorized by this recipe.

The generic policy is owned by `packages/core/src/session/runtime-guidance.ts`.
Paseo's copy must cite an immutable reviewed source commit and match its text;
OpenCode supplies the segment itself, including to native children. Update
OpenCode before deploying a Paseo build that excludes its own OpenCode copy.

## Build

In an isolated checkout at the reviewed immutable commit, use Node 24 and the
repository's Bun 1.4.2. Record `git rev-parse HEAD` with artifact hashes. Use an
absolute private artifact directory outside the repository for `ARTIFACT_DIR`.

```sh
bun install --frozen-lockfile --os='*' --cpu='*'
export OPENCODE_VERSION=2.0.15-chi.2
export OPENCODE_CHANNEL=latest
bun run packages/cli/script/build.ts --target=opencode-darwin-x64 --skip-install --outdir="$ARTIFACT_DIR/darwin"
bun run packages/cli/script/build.ts --target=opencode-linux-x64 --skip-install --outdir="$ARTIFACT_DIR/linux"
```

Run targets sequentially because the existing builder regenerates the shared
embedded web assets. Do not use `--skip-web-ui`. The existing builder checks each
artifact. Probe `--version` on each target platform and record SHA-256 hashes;
cross-compilation alone does not prove the binary runs on its target.

## Gates

Run `bun run check` at the root and the affected core tests through its existing
test script: session runner, model request/hooks, generate, local/native compaction
and plugin optimize. The pre-push hook runs the root check; it is local evidence,
not a substitute for required CI or protected review.

Before installation, the owning integration must explicitly validate and admit
the new runtime version wherever it enforces an exact native-version contract.
Coordinate client/server and both-host updates, verify runtime-loaded guidance and
roles, and retain existing local instructions until their tracked replacements
are verified. Provider-native children outside OpenCode and ACP transport remain
separate coverage questions; this candidate does not certify them.
