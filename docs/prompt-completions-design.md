# Plugin-provided human @ completions

Design for review, September 28, 2026. No implementation or deployment is claimed.
Source baseline: upstream V2 `96f23508bed1f36e758ad6eedd01024e1782d860`; also
checked the installed build's base `8ce629be225b551416d3009e87f02031824c5747`.

## Existing extension boundary

`packages/tui/src/component/prompt/autocomplete.tsx` owns the full TUI's `@`
popup, trigger/range detection, source acquisition, filtering and acceptance.
Files come from `client.api.file.find`; agents, skills and reference aliases
come from location-scoped cached data. `options()` combines them. Non-files are
fuzzy-sorted; files retain their search service's order. Tab calls
`prompt.autocomplete.complete`, which either expands a directory or calls
`select()`. Enter and mouse acceptance also call `select()`. The keymap layer is
enabled only while the popup is open.

`packages/plugin/src/tui/context.ts` exposes commands/keymaps, UI slots, routes,
dialogs and cached data, but no @ item provider. Slash commands are extensible.
Server plugins can transform real skills/references and intercept submitted
prompts, but cannot contribute arbitrary editor completion candidates. Representing
humans as agents, files, skills or repository references would give them incorrect
attachment/execution semantics. This agrees with the V2 guides at
`https://opencode.ai/v2/docs/build/plugins` and
`https://opencode.ai/v2/docs/build/plugins/cli`.

`packages/tui/src/mini/footer.prompt.tsx` is a separate composer. A full-TUI
extension alone does not establish Mini support.

## Proposed generic hook

Add a TUI-only provider registry through `packages/tui/src/plugin/api.tsx` and
`plugin/context.tsx`, using the existing plugin-owned registration/disposal
lifecycle. Expose it as `context.prompt.completions.register(provider)`.
Illustrative public shape (proposed, not available):

```ts
interface CompletionItem {
  id: string                 // stable within this provider; never a row index
  value: string              // token text without @
  description?: string
  data?: JsonValue           // opaque plugin-owned selection metadata
}
interface CompletionProvider {
  id: string
  complete(input: {
    query: string
    location: LocationRef
    sessionID?: string
    signal: AbortSignal
  }): readonly CompletionItem[] | Promise<readonly CompletionItem[]>
}
// register returns an idempotent disposer; unloading also disposes it.
```

The host namespaces IDs by plugin/provider, supplies the current editor context,
and merges contributed items into the existing candidate pipeline. The provider
controls its source filtering; Chi uses case-insensitive handle-prefix matching.
Do not independently fuzzy-filter those results and lose the provider's semantics.
Keep pending provider results out of the previous query/context, cancel them on
change/close/unload, and ignore late responses even when cancellation is ignored.
Failures must not block other sources. Never require a provider round trip before
showing already-available built-in results.

## One list, one selection, one acceptance

Mirror the Paseo contract across files, agents, skills, references and people:

1. Normalize candidates with stable IDs, explicit source and matching/ranking
   information before applying grouping/display order. Preserve file-search rank
   within that source; choose the cross-source priority centrally, not per plugin.
2. Track automatic versus user-selected state. Automatic selection follows the
   best-ranked candidate when asynchronous sources arrive. Arrow navigation pins
   the selected candidate ID; a re-sort must not silently select another item.
   Reset that explicit selection on a new token/query; reconcile missing IDs.
3. Retain one active-popup keymap dispatcher and acceptance operation for Tab,
   Enter and click. Directory expansion remains the directory-specific acceptance
   action. With no active popup, Tab falls through to normal editor/keymap behavior.
4. Acceptance receives the exact candidate, including opaque metadata, before
   replacing text. Chi carries `{ ownerId: "github:sava-the-owl", handle:
   "sava-the-owl" }`; display text is not the recipient identity. A callback that
   only inserts `@sava-the-owl` is insufficient for the agreed contract.

## Metadata is the non-small part

`prompt/history.tsx` currently stores only text, files, agents, skills and pasted
text. `PromptPartRef` has no plugin annotation type. The submit path in
`component/prompt/index.tsx` explicitly assembles those fields. A generic callback
with an out-of-band recipient map would become stale after editing, history recall,
draft switching or cancellation.

Before implementing, agree a namespaced completion-annotation contract owned by
the composer: selected item identity, original display token/range and plugin
data, with extmark-backed edit reconciliation. Specify history/draft restoration,
removal/replacement and undo behavior, disposal semantics, and how accepted
annotations enter the existing prompt admission metadata. Reuse the current
metadata channel if its validation permits this shape; regenerate the public
client if the Protocol/HttpApi must change. Do not smuggle humans through agent
mentions or rediscover identity from mutable display text after selection.

This also requires replacing index-only selection in today's autocomplete logic.
Together these are a public prompt-state contract change, not a small extra
source callback. A plain-text provider spike confirmed registry integration and
async cancellation are modest, but it was withdrawn rather than presenting it
as fulfillment of the exact-recipient contract.

## Chi plugin and upstream split

**Upstreamable:** generic provider/lifecycle API, stable candidate identity,
centralized ranking/selection/acceptance, namespaced annotation lifecycle,
documentation and deterministic TUI tests. No Chi/GitHub/Paseo names, endpoint
assumptions or credentials in core.

**Chi-owned:** participant retrieval and validation, prefix matching, owner-ID
metadata, authenticated actor/repository/deployment scoping, and mention delivery
through the existing Chi/Paseo path. Paseo's `packages/server/src/server/chi/mentions.ts`
already requests `/participants` with the repository and authenticated actor,
validates the response's `self`, and preserves `{ ownerId, handle }`. Reuse that
authority instead of creating a second credential store. The OpenCode plugin
still needs a deliberate bridge to it: the TUI's OpenCode client is not a Paseo
client, and a standalone TUI session must not guess a Paseo agent association.
Prefer a companion server plugin method returning bounded participants for the
current authorized context; use the existing plugin RPC transport. Validate the
actual deployed bridge before implementation, including remote OpenCode servers.
Clear participant data on authorization/context loss; no guessed default backend.

Completion acceptance selects a recipient; it does not send a message. Delivery
still belongs to Chi's durable admission/capture path and must revalidate access.

## Recommendation and acceptance gates

Recommend design review of the metadata/selection contract first. No Task 2
implementation PR, runtime patch, plugin configuration or deployment is authorized
by this note. Upstream CONTRIBUTING also requires a feature request and design
approval before an implementation PR. Once agreed, carry the implementation in
our fork and prepare the separate upstream contribution; do not wait on upstream
to run our validated build.

Required checks: mixed-source ranking with delayed arrivals; automatic versus
explicit-ID selection; case-insensitive `@sa`; Tab/Enter/click equivalence with
exact owner metadata; no-popup Tab fallback; directory and built-in attachment
behavior; token replacement in Unicode/multiline input; annotation edit/undo,
history and draft lifecycle; plugin unload and late/cross-context responses;
participant authorization loss; and rendered full-TUI acceptance with a synthetic
Chi participant source. Decide Mini scope explicitly. Keep the runtime version
gate unchanged until a new build's Chi-native compatibility has been checked.
