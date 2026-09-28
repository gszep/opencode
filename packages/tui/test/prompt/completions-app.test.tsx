import { expect, test } from "bun:test"
import { createAppFixture } from "../fixture/app"
import { tmpdir } from "../fixture/fixture"
import { directory, json } from "../fixture/tui-client"

test.each(["tab", "enter", "click"])("%s accepts the same plugin item and submits its exact metadata", async (key) => {
  await using state = await tmpdir()
  const plugin = state.path + "/people"
  await Bun.write(plugin + "/package.json", JSON.stringify({ type: "module", exports: { "./tui": "./tui.ts" } }))
  await Bun.write(
    plugin + "/tui.ts",
    `import { Plugin } from "@opencode/plugin/tui"
export default Plugin.define({ id: "fixture.people", setup(ctx) {
  return ctx.prompt.completions.register({ id: "people", async complete({ query }) {
    return "sava-the-owl".startsWith(query.toLowerCase()) ? [{ id: "github:sava-the-owl", value: "sava-the-owl", description: "Fixture person", data: { ownerId: "github:sava-the-owl" } }] : []
  } })
} })`,
  )
  const session = {
    id: "ses_completion",
    projectID: "project",
    title: "Completions",
    agent: "build",
    model: { providerID: "demo", id: "first" },
    location: { directory },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0 },
  }
  const location = { directory, project: { id: "project", directory, canonical: directory } }
  const sent: Record<string, unknown>[] = []
  await using app = await createAppFixture({
    state: state.path,
    config: { animations: false, plugins: [plugin] },
    args: { sessionID: session.id },
    fetch: async (url, request) => {
      if (url.pathname === "/api/fs/find") return json({ location, data: [] })
      if (url.pathname === "/api/agent")
        return json({ location, data: [{ id: "build", mode: "primary", hidden: false, permissions: [] }] })
      if (url.pathname === "/api/provider") return json({ location, data: [{ id: "demo", name: "Demo" }] })
      if (url.pathname === "/api/model")
        return json({
          location,
          data: [{ id: "first", providerID: "demo", name: "first", variants: [], cost: [], time: { released: 0 } }],
        })
      if (url.pathname === `/api/session/${session.id}`) return json({ data: session })
      if (url.pathname === `/api/session/${session.id}/prompt`) {
        sent.push(await request.json())
        return json({ id: "msg_test" })
      }
      if (url.pathname === `/api/session/${session.id}/model`) return new Response(null, { status: 204 })
      if (/^\/api\/session\/[^/]+\/(message|inbox|permission)$/.test(url.pathname))
        return json({ data: [], cursor: {} })
    },
  })
  await app.ready
  await app.waitForFrame((frame) => frame.includes("commands"))
  app.mockInput.pressKey("u", { ctrl: true })
  await app.mockInput.typeText("@SA")
  await app.waitForFrame((frame) => frame.includes("Fixture person"))
  if (key === "tab") app.mockInput.pressTab()
  if (key === "enter") app.mockInput.pressEnter()
  if (key === "click") {
    const lines = app.captureCharFrame().split("\n")
    const row = lines.findIndex((line) => line.includes("Fixture person"))
    await app.mockMouse.click(lines[row].indexOf("@sava"), row)
  }
  await app.waitFor(() => app.renderer.currentFocusedEditor?.plainText === "@sava-the-owl ")
  app.mockInput.pressEnter()
  await app.waitFor(() => sent.length === 1)
  expect(sent[0].text).toBe("@sava-the-owl ")
  expect(sent[0].metadata).toEqual({
    "opencode.prompt.completions": [
      {
        provider: '["fixture.people","people"]',
        id: "github:sava-the-owl",
        data: { ownerId: "github:sava-the-owl" },
        mention: { start: 0, end: 13, text: "@sava-the-owl" },
      },
    ],
  })
})

test("closed popup leaves Tab to the ordinary key dispatcher", async () => {
  await using state = await tmpdir()
  const plugin = state.path + "/people"
  await Bun.write(plugin + "/package.json", JSON.stringify({ type: "module", exports: { "./tui": "./tui.ts" } }))
  await Bun.write(
    plugin + "/tui.ts",
    `import { Plugin } from "@opencode/plugin/tui"
export default Plugin.define({ id: "fixture.people", setup(ctx) {
  ctx.prompt.completions.register({ id: "people", complete() { throw new Error("closed popup requested a provider") } })
} })`,
  )
  await using app = await createAppFixture({
    state: state.path,
    config: { animations: false, plugins: [plugin], keybinds: { "command.palette.show": "tab" } },
  })
  await app.ready
  await app.waitForFrame((frame) => frame.includes("commands"))
  app.mockInput.pressKey("u", { ctrl: true })
  app.mockInput.pressTab()
  await app.waitForFrame((frame) => frame.includes("Switch model"))
  expect(app.captureCharFrame()).toContain("Switch model")
})

test.each([false, true])("late provider arrival respects explicit navigation: %s", async (explicit) => {
  await using state = await tmpdir()
  const plugin = state.path + "/people"
  await Bun.write(plugin + "/package.json", JSON.stringify({ type: "module", exports: { "./tui": "./tui.ts" } }))
  await Bun.write(
    plugin + "/tui.ts",
    `import { Plugin } from "@opencode/plugin/tui"
export default Plugin.define({ id: "fixture.people", setup(ctx) {
  return ctx.prompt.completions.register({ id: "people", async complete({ query, signal }) {
    if (query !== "sa") return []
    await ctx.client.file.find({ query: "__people_gate__", limit: 1 }, { signal })
    return [{ id: "person", value: "sa", description: "Exact person" }]
  } })
} })`,
  )
  const gate = Promise.withResolvers<void>()
  const location = { directory, project: { id: "project", directory, canonical: directory } }
  await using app = await createAppFixture({
    state: state.path,
    config: { animations: false, plugins: [plugin] },
    fetch: (url) => {
      if (url.pathname === "/api/fs/find")
        return url.searchParams.get("query") === "__people_gate__"
          ? gate.promise.then(() => json({ location, data: [] }))
          : json({
              location,
              data: [
                { path: "sally.txt", type: "file" },
                { path: "sam.txt", type: "file" },
              ],
            })
    },
  })
  try {
    await app.ready
    await app.waitForFrame((frame) => frame.includes("commands"))
    app.mockInput.pressKey("u", { ctrl: true })
    await app.mockInput.typeText("@sa")
    await app.waitForFrame((frame) => frame.includes("sam.txt"))
    if (explicit) app.mockInput.pressArrow("down")
    gate.resolve()
    await app.waitForFrame((frame) => frame.includes("Exact person"))
    app.mockInput.pressTab()
    await app.renderOnce()
    expect(app.renderer.currentFocusedEditor?.plainText).toBe(explicit ? "@sam.txt " : "@sa ")
  } finally {
    gate.resolve()
  }
})
