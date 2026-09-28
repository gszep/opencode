import type {
  PromptCompletionInput,
  PromptCompletionItem,
  PromptCompletionProvider,
} from "@opencode/plugin/tui/context"
import type { TextareaRenderable } from "@opentui/core"
import { createEffect, createSignal, onCleanup } from "solid-js"

export type CompletionSelection = PromptCompletionItem & { provider: string }

export function createPromptCompletions(
  providers: () => readonly PromptCompletionProvider[],
  input: () => Omit<PromptCompletionInput, "signal"> | undefined,
) {
  const [items, setItems] = createSignal<readonly CompletionSelection[]>([])
  createEffect(() => {
    const request = input()
    const sources = providers()
    setItems([])
    if (!request || !sources.length) return
    const controller = new AbortController()
    onCleanup(() => controller.abort())
    // A slow or failed provider must not delay built-in results or other plugins.
    const results = sources.map(() => [] as readonly CompletionSelection[])
    sources.forEach((provider, index) => {
      Promise.resolve()
        .then(() => {
          if (controller.signal.aborted) return []
          return provider.complete({ ...request, signal: controller.signal })
        })
        .catch(() => [])
        .then((value) => {
          if (controller.signal.aborted) return
          const seen = new Set<string>()
          results[index] = value.slice(0, 100).flatMap((item) => {
            if (!item.id || !item.value || /[\s\x00-\x1f\x7f]/.test(item.value) || seen.has(item.id)) return []
            seen.add(item.id)
            return [{ ...structuredClone(item), provider: provider.id }]
          })
          setItems(results.flat())
        })
        .catch(() => {})
    })
  })
  return items
}

/** Replace the active completion token using the textarea's display-coordinate API. */
export function insertPromptCompletion(input: TextareaRenderable, index: number, text: string) {
  const cursorOffset = input.cursorOffset
  input.cursorOffset = index
  const start = input.logicalCursor
  input.cursorOffset = cursorOffset
  const end = input.logicalCursor
  input.deleteRange(start.row, start.col, end.row, end.col)
  input.insertText(text)
}
