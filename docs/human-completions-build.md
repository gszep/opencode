# Staged annotated-completion build

September 28, 2026. Candidate only; no installed executable or user configuration
is changed by producing these artifacts.

## Source and validation

- Build source: `719b8470bb95310358b096be3ad40b1de7be62fd`, branch
  `gszep/opencode:human-completions`, based on the preserved linkfix branch.
- Includes hyperlink fix `e0690b7b80f846c98a8ec45bc641a2de250209fa`.
- Generic-only upstream candidate: `49d1473c6debefaf96957c774ec1828e5208cc9c`,
  branch `prompt-completions`, based on V2 `0caae608a28819981510989d28768cd9d4e4a663`.
  No feature PR is open; upstream requires prior feature design approval.
- Carry: 146 prompt/plugin/link tests and root `bun run check` passed.
  Upstream candidate: 148 prompt/plugin tests and root `bun run check` passed.
- Both CLI targets passed the build script's artifact checks; their native
  `--version` probes report `opencode v2.0.15-mentions.1`.

## Reproducing the staged artifacts

Built on Linux x64 with user-local Bun 1.4.2 and Node 24.18.0:

```sh
git checkout 719b8470bb95310358b096be3ad40b1de7be62fd
bun install --frozen-lockfile --os='*' --cpu='*'
export OPENCODE_VERSION=2.0.15-mentions.1
export OPENCODE_CHANNEL=latest
bun run packages/cli/script/build.ts --target=opencode-linux-x64 --skip-install
bun run packages/cli/script/build.ts --target=opencode-darwin-x64 --skip-install --outdir=dist-calcifer
```

The web assets are built and embedded. There is no `--skip-web-ui` shortcut.

| Target | Output | SHA-256 |
| --- | --- | --- |
| Linux x64 | `packages/cli/dist/cli-linux-x64/bin/opencode` | `23bfbc6858b50fc0fa8ad062b00c7c67138e562233cfc49fe98ff6b200f79ce1` |
| Intel macOS | `packages/cli/dist-calcifer/cli-darwin-x64/bin/opencode` | `e39945f80bd02bf9b2367860f8b136ed875d31de9a0b3e96db624ee7ee76ca40` |

Hashes identify these artifacts, not a claim that independent rebuilds are
bit-for-bit reproducible. Private copies and logs are held by the operator.

## Rollout prerequisites and steps

The existing Chi-native exact version predicate rejects `2.0.15-mentions.1`.
After checking this build's installed schema and exact import/fork payloads,
the operator's separate change must extend `backend/scripts/native-operations.ts`
line 194 from:

```ts
server.version === '2.0.10' || server.version === '2.0.15-linkfix.1'
```

to:

```ts
server.version === '2.0.10' || server.version === '2.0.15-linkfix.1' || server.version === '2.0.15-mentions.1'
```

Update its contract comment and regression coverage after validation; the build
work does not modify that backend gate. An old running server does not gain the
hook merely because the client executable was replaced.

For each host, preserve its current binary, verify the matching artifact hash,
copy to a sibling temporary file of `$HOME/.opencode/bin/opencode`, set mode 755,
and atomically rename it into place. The operator must coordinate the background
service rollout and restart the TUI after review. Do not mix client/server
versions inadvertently through automatic service discovery.

Install the private `@henkaku-center/opencode-chi-mentions` tarball and merge its
documented server and full-TUI plugin entries, using the existing deployment's
explicit endpoint and `gh` identity. Its server RPC acquires participants; it
does not publish handoffs from bare TUI sessions. Mini is outside this contract.
