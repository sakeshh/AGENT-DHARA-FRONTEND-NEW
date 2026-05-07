# Fixes — `master`

This file documents backend/UX fixes that were applied directly to `master`.

## 1. `Agent Dhara Backend/agent/session_store.py`

- Deep-merges `session["context"]` in `save_session` instead of overwriting the
  entire context dict. This prevents keys like `selected_source`,
  `selected_blob_files`, etc. from being lost when multiple nodes write
  partial context.
- Ensures that new or recovered sessions always have
  `context.last_step = "awaiting_source_selection"`, avoiding the
  `last_step: unknown` display in the UI.

## 2. `Agent Dhara Backend/agent/routing_guards.py` (new)

- Provides `normalize_source_message(msg, ctx)` to map bare keywords like
  `"blob"`, `"sql"`, `"local"` to deterministic commands such as
  `"select source blob"` **before** the LLM router sees the message.
- Provides `guard_needs_assessment(action, ctx)` to block report-only
  actions when no assessment result is present yet, returning helpful
  source/assessment guidance instead.
- Provides `guard_needs_source(action, ctx)` to block non-navigation
  actions when no source has been selected.
- Exposes `RESET_CONTEXT_KEYS` so `reset_flow` / new-chat logic can clear
  all chat-selection state consistently from one place.

### How to wire into `chat_graph.py`

Add at the top of the file:

```python
from agent.routing_guards import (
    guard_needs_assessment,
    guard_needs_source,
    normalize_source_message,
    RESET_CONTEXT_KEYS,
)
```

Before the LLM router call, normalise the message:

```python
ctx = state["session"].setdefault("context", {})
state["message"] = normalize_source_message(state["message"], ctx)
```

After the router chooses `action`, guard before dispatch:

```python
blocked = guard_needs_assessment(action, ctx)
if blocked:
    return blocked

blocked = guard_needs_source(action, ctx)
if blocked:
    return blocked
```

In your `reset_flow` node, reset keys using `RESET_CONTEXT_KEYS`:

```python
for key in RESET_CONTEXT_KEYS:
    ctx.pop(key, None)
ctx["last_step"] = "awaiting_source_selection"
```

These small edits ensure that:

- Typing `blob` at the start of a fresh session is treated as
  "select Blob source" instead of jumping directly into report
  clarification.
- Report navigation actions are only available **after** an
  assessment has actually been run.
