import { expect, test } from "bun:test"
import { TextareaRenderable } from "@opentui/core"
import { createTestRenderer } from "@opentui/core/testing"
import {
  completionMetadata,
  editCompletionAnnotations,
  readCompletionAnnotations,
  restoreCompletionAnnotations,
} from "../../src/prompt/annotations"
import { parsePromptHistory } from "../../src/prompt/history"
import { saveDraft, takeDraft } from "../../src/component/prompt/draft-stash"

test("annotation identity follows edits, drops on token editing, and returns with undo", async () => {
  const app = await createTestRenderer({ width: 80, height: 10, useThread: false })
  const input = new TextareaRenderable(app.renderer, { id: "prompt" })
  app.renderer.root.add(input)
  try {
    input.setText("@sava-the-owl ")
    restoreCompletionAnnotations(input, [
      {
        provider: "people",
        id: "github:sava-the-owl",
        data: { ownerId: "github:sava-the-owl" },
        mention: { start: 0, end: 13, text: "@sava-the-owl" },
      },
    ])
    expect(readCompletionAnnotations(input)).toHaveLength(1)
    input.cursorOffset = 0
    input.insertText("界 ")
    expect(readCompletionAnnotations(input)[0]?.mention.start).toBe(3)
    input.cursorOffset = 6
    input.insertText("x")
    expect(readCompletionAnnotations(input)).toEqual([])
    input.undo()
    expect(readCompletionAnnotations(input)[0]?.data).toEqual({ ownerId: "github:sava-the-owl" })
    input.redo()
    expect(readCompletionAnnotations(input)).toEqual([])
  } finally {
    app.renderer.destroy()
  }
})

test("external edits retain exact untouched ranges and expanded paste shifts submission coordinates", () => {
  const annotation = {
    provider: "people",
    id: "exact",
    data: { ownerId: "github:sava" },
    mention: { start: 4, end: 9, text: "@sava" },
  }
  expect(editCompletionAnnotations("hey @sava", "hello @sava", [annotation])[0]?.mention).toEqual({
    start: 6,
    end: 11,
    text: "@sava",
  })
  expect(editCompletionAnnotations("hey @sava", "hey @sava!", [annotation])[0]?.id).toBe("exact")
  expect(editCompletionAnnotations("hey @sava", "hey @someone", [annotation])).toEqual([])
  expect(
    completionMetadata(
      "界\n@sa",
      [{ ...annotation, mention: { start: 4, end: 7, text: "@sa" } }],
      [{ source: { start: 0, end: 3 }, text: "界" }],
    )?.["opencode.prompt.completions"][0]?.mention,
  ).toEqual({ start: 3, end: 6, text: "@sa" })
})

test("draft/history round trips preserve exact data and reject edited-away tokens", async () => {
  const app = await createTestRenderer({ width: 80, height: 10, useThread: false })
  const input = new TextareaRenderable(app.renderer, { id: "prompt" })
  app.renderer.root.add(input)
  try {
    const prompt = {
      text: "@sava-the-owl",
      pasted: [],
      annotations: [
        {
          provider: "people",
          id: "github:sava",
          data: { ownerId: "github:sava" },
          mention: { start: 0, end: 13, text: "@sava-the-owl" },
        },
      ],
    }
    saveDraft("annotation-test", { prompt, cursor: 13 })
    const restored = parsePromptHistory(JSON.stringify(takeDraft("annotation-test")?.prompt))[0]
    input.setText(restored.text)
    restoreCompletionAnnotations(input, restored.annotations ?? [])
    const annotations = readCompletionAnnotations(input)
    expect(completionMetadata(input.plainText, annotations)?.["opencode.prompt.completions"][0]?.data).toEqual({
      ownerId: "github:sava",
    })
    input.setText("@someone-else")
    restoreCompletionAnnotations(input, annotations)
    expect(readCompletionAnnotations(input)).toEqual([])
    expect(completionMetadata(input.plainText, annotations)).toBeUndefined()
  } finally {
    app.renderer.destroy()
  }
})
