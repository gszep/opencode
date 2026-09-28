import type { PromptCompletionAnnotation } from "@opencode/plugin/tui/context"
import type { TextareaRenderable } from "@opentui/core"
import { unwrap } from "solid-js/store"
import { displaySlice, promptOffsetWidth } from "./display"

export const completionMetadataKey = "opencode.prompt.completions"

/** External editors supply a replacement buffer: only unchanged prefix/suffix ranges retain identity. */
export function editCompletionAnnotations(
  before: string,
  after: string,
  annotations: readonly PromptCompletionAnnotation[],
) {
  if (before === after) return annotations.map((item) => ({ ...item }))
  const old = [...before]
  const next = [...after]
  let prefix = 0
  while (prefix < old.length && prefix < next.length && old[prefix] === next[prefix]) prefix++
  let suffix = 0
  while (
    suffix < old.length - prefix &&
    suffix < next.length - prefix &&
    old[old.length - suffix - 1] === next[next.length - suffix - 1]
  )
    suffix++
  const start = promptOffsetWidth(old.slice(0, prefix).join(""))
  const end = promptOffsetWidth(old.slice(0, old.length - suffix).join(""))
  const delta = promptOffsetWidth(after) - promptOffsetWidth(before)
  return annotations.flatMap((item) => {
    const range = item.mention
    const mention =
      range.end <= start
        ? range
        : range.start >= end
          ? { ...range, start: range.start + delta, end: range.end + delta }
          : undefined
    return mention && displaySlice(after, mention.start, mention.end) === mention.text ? [{ ...item, mention }] : []
  })
}

export function restoreCompletionAnnotations(
  input: TextareaRenderable,
  annotations: readonly PromptCompletionAnnotation[],
) {
  const typeId = input.extmarks.registerType(completionMetadataKey)
  for (const annotation of annotations) {
    const range = annotation.mention
    if (displaySlice(input.plainText, range.start, range.end) !== range.text) continue
    input.extmarks.create({ ...range, typeId, virtual: false, data: structuredClone(unwrap(annotation)) })
  }
}

export function readCompletionAnnotations(input: TextareaRenderable): PromptCompletionAnnotation[] {
  const typeId = input.extmarks.getTypeId(completionMetadataKey)
  // Read the restored snapshot directly: the controller's type index is not restored by undo.
  return input.extmarks.getAll().flatMap((mark) => {
    if (mark.typeId !== typeId) return []
    const annotation = mark.data as PromptCompletionAnnotation
    if (displaySlice(input.plainText, mark.start, mark.end) !== annotation.mention.text) {
      input.extmarks.delete(mark.id)
      return []
    }
    return [{ ...annotation, mention: { ...annotation.mention, start: mark.start, end: mark.end } }]
  })
}

export function completionMetadata(
  text: string,
  annotations: readonly PromptCompletionAnnotation[] = [],
  pasted: readonly { text: string; source: { start: number; end: number } }[] = [],
) {
  const values = annotations.flatMap((item) => {
    const delta = pasted.reduce(
      (total, part) =>
        part.source.end <= item.mention.start
          ? total + promptOffsetWidth(part.text) - (part.source.end - part.source.start)
          : total,
      0,
    )
    const mention = { ...item.mention, start: item.mention.start + delta, end: item.mention.end + delta }
    return displaySlice(text, mention.start, mention.end) === mention.text ? [{ ...item, mention }] : []
  })
  return values.length ? { [completionMetadataKey]: structuredClone(unwrap(values)) } : undefined
}
