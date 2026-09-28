import { expect, test } from "bun:test"
import type {
  PromptCompletionInput,
  PromptCompletionItem,
  PromptCompletionProvider,
} from "@opencode/plugin/tui/context"
import { TextareaRenderable } from "@opentui/core"
import { createTestRenderer } from "@opentui/core/testing"
import { createRoot, createSignal } from "solid-js"
import { createPluginContext, type Registry, type usePluginHost } from "../../src/plugin/api"
import { createPromptCompletions, insertPromptCompletion } from "../../src/prompt/completions"

test("providers unregister explicitly and on plugin cleanup", async () => {
  const providers = new Map<string, PromptCompletionProvider>()
  const registry: Registry = {
    has: (kind, name) => kind === "completions" && providers.has(name),
    set(kind, name, value) {
      if (kind === "completions") providers.set(name, value as PromptCompletionProvider)
    },
    remove: (_, name) => void providers.delete(name),
    active: () => true,
  }
  const host = {
    app: {},
    client: {},
    keymap: {},
    shortcuts: {},
    keymapState: {},
    sessionTabs: {},
    local: {},
  } as unknown as ReturnType<typeof usePluginHost>
  const owned: (() => Promise<void>)[] = []
  const context = createPluginContext({ host, id: "test", options: undefined, owned, registry })
  const provider: PromptCompletionProvider = { id: "people", complete: () => [] }
  const remove = context.prompt.completions.register(provider)
  expect(providers.get("people")?.id).toBe(JSON.stringify(["test", "people"]))
  expect(() => context.prompt.completions.register(provider)).toThrow("already registered")
  remove()
  remove()
  expect(providers.size).toBe(0)
  context.prompt.completions.register(provider)
  for (const cleanup of owned.reverse()) await cleanup()
  expect(providers.size).toBe(0)
})

test("query, location, session, close and provider changes discard pending results", async () => {
  const calls: { input: PromptCompletionInput; resolve: (items: readonly PromptCompletionItem[]) => void }[] = []
  const provider: PromptCompletionProvider = {
    id: "people",
    complete(input) {
      const pending = Promise.withResolvers<readonly PromptCompletionItem[]>()
      calls.push({ input, resolve: pending.resolve })
      return pending.promise
    },
  }
  const setup = createRoot((dispose) => {
    const [sources, setSources] = createSignal<readonly PromptCompletionProvider[]>([provider])
    const [input, setInput] = createSignal<Omit<PromptCompletionInput, "signal"> | undefined>({
      query: "s",
      location: { directory: "/first" },
      sessionID: "one",
    })
    return { dispose, setInput, setSources, items: createPromptCompletions(sources, input) }
  })
  try {
    await Bun.sleep(0)
    expect(calls[0].input.query).toBe("s")
    setup.setInput({ query: "sa", location: { directory: "/first" }, sessionID: "one" })
    await Bun.sleep(0)
    expect(calls[0].input.signal.aborted).toBe(true)
    calls[0].resolve([{ id: "stale", value: "stale" }])
    calls[1].resolve([{ id: "sava", value: "sava-the-owl" }])
    await Bun.sleep(0)
    expect(setup.items()).toEqual([{ provider: "people", id: "sava", value: "sava-the-owl" }])

    setup.setInput({ query: "sa", location: { directory: "/second" }, sessionID: "two" })
    await Bun.sleep(0)
    expect(setup.items()).toEqual([])
    expect(calls[2].input.location.directory).toBe("/second")
    expect(calls[2].input.sessionID).toBe("two")
    setup.setSources([])
    await Bun.sleep(0)
    expect(calls[2].input.signal.aborted).toBe(true)
    calls[2].resolve([{ id: "wrong", value: "wrong-repository" }])
    await Bun.sleep(0)
    expect(setup.items()).toEqual([])

    setup.setSources([provider])
    await Bun.sleep(0)
    setup.setInput(undefined)
    await Bun.sleep(0)
    expect(calls[3].input.signal.aborted).toBe(true)
    calls[3].resolve([{ id: "closed", value: "closed" }])
    await Bun.sleep(0)
    expect(setup.items()).toEqual([])
    setup.setInput({ query: "sa", location: { directory: "/second" } })
    await Bun.sleep(0)
    setup.dispose()
    expect(calls[4].input.signal.aborted).toBe(true)
  } finally {
    setup.dispose()
  }
})

test("failed and slow providers do not block independent results", async () => {
  const slow = Promise.withResolvers<readonly PromptCompletionItem[]>()
  const setup = createRoot((dispose) => ({
    dispose,
    items: createPromptCompletions(
      () => [
        { id: "slow", complete: () => slow.promise },
        {
          id: "failure",
          complete: () => {
            throw new Error("unavailable")
          },
        },
        { id: "rejection", complete: () => Promise.reject(new Error("denied")) },
        { id: "people", complete: () => [{ id: "sava", value: "sava-the-owl", description: "Person" }] },
      ],
      () => ({ query: "sa", location: { directory: "/project" } }),
    ),
  }))
  try {
    await Bun.sleep(0)
    expect(setup.items()).toEqual([{ provider: "people", id: "sava", value: "sava-the-owl", description: "Person" }])
    slow.resolve([{ id: "sam", value: "sam" }])
    await Bun.sleep(0)
    expect(setup.items().map((item) => item.value)).toEqual(["sam", "sava-the-owl"])
  } finally {
    setup.dispose()
  }
})

test("plain-text completion preserves surrounding wide and multiline text", async () => {
  const app = await createTestRenderer({ width: 80, height: 10, useThread: false })
  const input = new TextareaRenderable(app.renderer, { id: "prompt" })
  app.renderer.root.add(input)
  try {
    input.setText("前の行\n界 @sa suffix")
    input.cursorOffset = 13
    insertPromptCompletion(input, 10, "@sava-the-owl")
    expect(input.plainText).toBe("前の行\n界 @sava-the-owl suffix")
    expect(input.extmarks.getAll()).toEqual([])
  } finally {
    app.renderer.destroy()
  }
})
