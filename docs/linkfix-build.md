# Reproducing the carried TUI hyperlink fix

Verified September 28, 2026. Runtime version stays `2.0.15-linkfix.1`.

## Source and installed artifacts

- Upstream build base: `8ce629be225b551416d3009e87f02031824c5747`.
- Preserved patch and original regression test: `e0690b7b80f846c98a8ec45bc641a2de250209fa`
  on `gszep/opencode`, branch `fix/tui-modified-link-click`.
- Artemis source: `$HOME/.local/share/opencode/repos/opencode-tui-fix`.
- Both hosts install the compiled executable at `$HOME/.opencode/bin/opencode`.
- Artemis's installed executable exactly matches
  `packages/cli/dist/cli-linux-x64/bin/opencode`, SHA-256
  `827f6014dc5fb0eea5cfaa1a71be9bfbbb95d89b88fb14be1e79836977df7715`.
- Calcifer's installed executable exactly matches Artemis's cross-build artifact
  `packages/cli/dist-calcifer/cli-darwin-x64/bin/opencode`, SHA-256
  `3c9fb4beed713517efb882d9c5e0ffe785f93b5e7e2141123eb45c72cefcc4a5`.
- Both binaries embed the `opencode/latest/2.0.15-linkfix.1/cli` user agent and
  the built web application's assets. Do not use `--skip-web-ui` to reproduce them.

The exact original shell commands were not retained in the inspected history.
The recipe below is reconstructed from the pinned build scripts, package manager
requirement, artifact targets and binary contents. It has **not** been rerun or
verified for bit-for-bit reproducibility. No binary was replaced or service
restarted during this investigation.

## Build recipe (Artemis, Linux x64)

Use a user-local Bun 1.4.2 installation (`packageManager` pins `bun@1.4.2`) and
Node 24.18.0. The September 28 checks used the user-local npm package binary at
`$HOME/.local/share/opencode/build-tools/bun-1.4.2/node_modules/@oven/bun-linux-x64/bin/bun`.

In a clean build checkout:

```sh
git clone https://github.com/gszep/opencode.git opencode-linkfix-build
cd opencode-linkfix-build
git checkout e0690b7b80f846c98a8ec45bc641a2de250209fa
export PATH="$HOME/.local/share/opencode/build-tools/bun-1.4.2/node_modules/@oven/bun-linux-x64/bin:$HOME/.nvm/versions/node/v24.18.0/bin:$PATH"
bun install --frozen-lockfile --os='*' --cpu='*'
export OPENCODE_VERSION=2.0.15-linkfix.1
export OPENCODE_CHANNEL=latest
bun run packages/cli/script/build.ts --target=opencode-linux-x64 --skip-install
bun run packages/cli/script/build.ts --target=opencode-darwin-x64 --skip-install --outdir=dist-calcifer
```

The script builds and embeds the web application, compiles the CLI/TUI with Bun,
embeds platform-specific native dependencies, writes target metadata and runs
artifact verification. The second build is the Intel Mac target, not ARM64.
Each invocation clears its selected output directory. Keep the separate
`dist-calcifer` directory when building both targets.

For a later authorized installation, transfer the corresponding target executable
to a sibling temporary file of `$HOME/.opencode/bin/opencode`, compare its SHA-256
with the build artifact, set mode 755, and atomically rename it into place.
The current installed files already match the preserved artifacts; installation
is unnecessary for this source-preservation change. Runtime version changes need
the separate Chi-native compatibility check.

## Verification and upstream status

```sh
# From packages/tui:
bun test test/ui/link.test.tsx
bun typecheck
# From the repository root:
bun run check
```

All passed on the preserved Artemis source. The regression exercises plain,
Ctrl-, Shift-, Alt- and secondary-button clicks against the rendered production
Link component with only the browser opener replaced.

The separate upstream-ready `modified-link-click` branch targets V2 commit
`96f23508bed1f36e758ad6eedd01024e1782d860`; its patch commit is
`60b7eefc39468b2e4670783fe4eae9f6c0a2c9ac`. Only the opener import/mock changes
to current upstream's `openUrl` API. Its regression, TUI typecheck and root
`bun run check` passed on Calcifer.

After operator approval, issue anomalyco/opencode#51756 and focused PR #51757
were opened against `v2`. The PR links a rendered before/after regression
recording with injected mouse events and a counted browser opener; it does not
claim native terminal/browser reproduction. Related PRs #40905/#40912 added
authorization-link handling; #46261 concerns Markdown links, not this fix.
