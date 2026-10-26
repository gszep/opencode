import { afterEach, expect, test } from "bun:test"
import { CodeRenderable, MarkdownRenderable, SyntaxStyle } from "@opentui/core"
import { createTestRenderer, MockTreeSitterClient } from "@opentui/core/testing"
import { testRender } from "@opentui/solid"
import { batch, createSignal } from "solid-js"
import { waitForHighlightedFrame } from "./fixture/highlighted-frame"

const renderers: Awaited<ReturnType<typeof testRender>>["renderer"][] = []
const syntaxStyle = SyntaxStyle.fromStyles({ default: { fg: "#ffffff" } })

test.each([true, false])("joins pending parser work before capturing Markdown (streaming=%s)", async (streaming) => {
  const client = new MockTreeSitterClient()
  const output = await createTestRenderer({ width: 80, height: 12, useThread: false })
  renderers.push(output.renderer)
  const markdown = new MarkdownRenderable(output.renderer, {
    content: streaming ? "```text\ninitial" : "```text\ninitial final\n```\n\nMARKDOWN_END",
    streaming,
    syntaxStyle,
    treeSitterClient: client,
    internalBlockMode: "top-level",
  })
  output.renderer.root.add(markdown)
  try {
    await output.renderOnce()
    const marker = streaming ? "initial" : "MARKDOWN_END"
    expect(client.isHighlighting()).toBe(true)
    expect(output.captureCharFrame()).not.toContain(marker)
    // More frames alone cannot finish externally scheduled parser work.
    await expect(output.waitForFrame((frame) => frame.includes(marker), { maxPasses: 2 })).rejects.toThrow(
      "Timed out waiting for frame predicate",
    )
    const pending = markdown.getChildren().find((node) => node instanceof CodeRenderable && node.isHighlighting)
    if (!(pending instanceof CodeRenderable)) throw new Error("Expected pending parser work")
    const done = pending.highlightingDone
    const joined = Promise.withResolvers<void>()
    // Observe this fixture-owned renderable, not a global parser or prototype.
    // Releasing only after the helper reads its real completion promise proves
    // the positive path joins work rather than winning an immediate-work race.
    Object.defineProperty(pending, "highlightingDone", {
      configurable: true,
      get() {
        joined.resolve()
        return done
      },
    })
    const frame = waitForHighlightedFrame(output, (frame) => frame.includes(marker))
    await joined.promise
    expect(client.isHighlighting()).toBe(true)
    client.resolveAllHighlightOnce()
    expect(await frame).toContain(marker)
    Reflect.deleteProperty(pending, "highlightingDone")
    if (!streaming) expect(output.captureCharFrame()).toContain("initial final")
  } finally {
    await client.destroy()
  }
})

afterEach(() => {
  renderers.splice(0).forEach((renderer) => renderer.destroy())
})

test.each(["completion-first", "content-first"])("applies final fence text in a Solid batch: %s", async (order) => {
  const [content, setContent] = createSignal("```text\ninitial")
  const [streaming, setStreaming] = createSignal(true)
  const output = await testRender(
    () => (
      <markdown syntaxStyle={syntaxStyle} content={content()} streaming={streaming()} internalBlockMode="top-level" />
    ),
    { width: 80, height: 12, remote: true, useThread: false },
  )
  renderers.push(output.renderer)
  await output.renderOnce()

  batch(() => {
    if (order === "completion-first") setStreaming(false)
    setContent("```text\ninitial final\n```")
    if (order === "content-first") setStreaming(false)
  })
  await output.renderOnce()

  const markdown = output.renderer.root.getChildren()[0]
  expect(markdown).toBeInstanceOf(MarkdownRenderable)
  const block = markdown?.getChildren()[0]
  expect(block).toBeInstanceOf(CodeRenderable)
  if (!(block instanceof CodeRenderable)) throw new Error("Expected a code fence")
  expect(block.content).toBe("initial final")
  expect(markdown?.getChildren()).toHaveLength(1)
})
