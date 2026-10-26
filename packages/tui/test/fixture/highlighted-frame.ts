import { CodeRenderable, type Renderable } from "@opentui/core"
import type { TestRendererSetup } from "@opentui/core/testing"

// A render frame does not join the parser worker. Streaming fences and newly
// completed paragraphs can have content but no visible text until highlighting
// finishes. Await that dependency without spending extra render passes.
export function waitForHighlightedFrame(setup: TestRendererSetup, predicate: (frame: string) => boolean) {
  return setup.waitForFrame(async (frame) => {
    const pending = highlights(setup.renderer.root)
    if (!pending.length) return predicate(frame)
    await Promise.all(pending)
    // Highlight completion requests a new frame; the captured one is stale.
    return false
  })
}

function highlights(node: Renderable): Promise<void>[] {
  return [
    ...(node instanceof CodeRenderable && node.isHighlighting ? [node.highlightingDone] : []),
    ...node.getChildren().flatMap(highlights),
  ]
}
