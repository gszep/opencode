# Settled session exports

`SessionTransfer.export` returns the complete projected message list only when
every record is settled. An assistant's completed-step timestamp is not enough:
all of its tool parts must also be terminal. Streaming/running tools, unfinished
assistant steps, and running shell or compaction records block export.

Core reports `SessionTransfer.UnsettledError`. The HTTP export endpoint returns
the existing `409 ConflictError` shape, with the session ID as `resource` and no
transcript `data`. This applies before sanitization as well. Consumers should
wait for settlement or terminal cancellation and then retry; they must not
upload a partial transcript or treat the error as an empty session.

This is deliberately whole-export rejection, not individual filtering. A hosted
tool result can arrive after its original assistant step completes. Filtering
that assistant out while exporting a later user message would create a hole;
inserting the settled assistant later would violate an append-only consumer's
confirmed prefix. Rejecting the request preserves the earlier confirmed export
until the complete settled snapshot is available.

No `SessionTransfer.Data`, persisted message, event, or database-schema fields
change. The export endpoint's declared error statuses now include 409, and its
generated client is regenerated accordingly. Import retains its existing
filter-based behavior, with the same stronger tool-settlement predicate; it is
not a substitute for obtaining a complete settled export.
