# Fixes — `fix/routing-session-guards`

This PR applies **5 targeted fixes** to resolve the broken source-selection flow,
`last_step: unknown` display, and report-UI appearing before any assessment exists.

---

## Files Changed

### 1. `Agent Dhara Backend/agent/session_store.py`
**Fix: Deep-merge context on `save_session`**

Previously, `save_session` would overwrite the entire session object including the
`context` dict. This caused keys set by one node (e.g. `selected_source`) to be
lost when a subsequent node saved back a partial context.

**Change:** Before writing, load the existing session from SQLite and deep-merge the
`context` dict so new keys win on conflict but existing keys are preserved.

Also: `load_session` now always seeds `context.last_step = "awaiting_source_selection"`
on a fresh session so `last_step` is never `"unknown"`.

---

### 2. `Agent Dhara Backend/agent/routing_guards.py` *(new file)*
**Fix: Centralised pre-dispatch guards + source keyword normalisation**

Extracts reusable logic that was previously missing or scattered:

- `REPORT_ACTIONS` — set of actions that require a completed assessment.
- `guard_needs_assessment(action, ctx)` — returns a ready-made blocked reply
  (with source-selection or "run assessment" buttons) when a report action is
  requested before any assessment has been run. Returns `None` to allow.
- `guard_needs_source(action, ctx)` — similar guard for actions that need a source.
- `normalize_source_message(msg, ctx)` — maps bare user input like `"blob"` or
  `"sql"` to deterministic commands like `"select source blob"` before the LLM
  router sees the message. Only active when no source is selected yet.
- `RESET_CONTEXT_KEYS` — canonical list of keys to wipe on `reset_flow` / new chat.

**How to wire into `chat_graph.py`:**

```python
from agent.routing_guards import (
    REPORT_ACTIONS,
    guard_needs_assessment,
    guard_needs_source,
    normalize_source_message,
    RESET_CONTEXT_KEYS,
)

# 1. Normalise message BEFORE calling the LLM router:
state["message"] = normalize_source_message(state["message"], ctx)

# 2. After the LLM returns `action`, guard before dispatch:
blocked = guard_needs_assessment(action, ctx)
if blocked:
    return blocked

# 3. In reset_flow node — use RESET_CONTEXT_KEYS:
for key in RESET_CONTEXT_KEYS:
    ctx.pop(key, None)
ctx["last_step"] = "awaiting_source_selection"
```

---

### 3. `Agent Dhara Backend/agent/chat_graph.py` — inline changes needed

The guards module is a **drop-in helper**. The three wiring points above need to be
added manually to `chat_graph.py` at the relevant locations:

| Location in `chat_graph.py` | Change |
|---|---|
| Top of the master router node, before LLM call | Add `normalize_source_message` |
| After LLM resolves `action`, before dispatch switch | Add `guard_needs_assessment` + `guard_needs_source` |
| Inside `_node_reset_flow` | Replace ad-hoc key deletions with `RESET_CONTEXT_KEYS` loop + set `last_step` |
| Every `_node_*` function that sets `selected_*` keys | Add `ctx["last_step"] = "<step_name>"` |

---

### 4. `components/ChatWindow.tsx` — inline changes needed

Add these two blocks to `ChatWindow.tsx`:

#### a) Step indicator (above chat messages)

```tsx
const FLOW_STEPS = ["Select Source", "Select Files/Tables", "Run Assessment", "View Report"];
const currentFlowStep =
  !sessionContext?.selected_source ? 0
  : (!sessionContext?.selected_blob_files?.length &&
     !sessionContext?.selected_tables?.length &&
     !sessionContext?.selected_local_files?.length) ? 1
  : !sessionContext?.last_assessment_result ? 2
  : 3;

// Render:
<div className="flex gap-2 px-4 py-2 border-b border-gray-700 text-xs">
  {FLOW_STEPS.map((step, i) => (
    <span key={step} className={`px-2 py-1 rounded-full ${
      i === currentFlowStep
        ? "bg-blue-600 text-white"
        : i < currentFlowStep
        ? "bg-green-800 text-green-200"
        : "bg-gray-800 text-gray-400"
    }`}>
      {i < currentFlowStep ? "✓ " : ""}{step}
    </span>
  ))}
</div>
```

#### b) Source selection buttons (shown when no source selected)

```tsx
{!sessionContext?.selected_source && (
  <div className="flex gap-3 flex-wrap p-3">
    {[
      { label: "☁️ Azure Blob",  send: "select source blob" },
      { label: "🗄️ Database",    send: "select source database" },
      { label: "📁 Local Files", send: "select source local" },
    ].map(opt => (
      <button
        key={opt.send}
        onClick={() => sendMessage(opt.send)}
        className="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium transition-colors"
      >
        {opt.label}
      </button>
    ))}
  </div>
)}
```

---

## Testing Checklist

- [ ] Fresh session → only source buttons shown, free text still works
- [ ] Type `blob` → normalised to `select source blob`, lists blob files
- [ ] Ask `summarize report` before assessment → blocked with source buttons
- [ ] Run assessment → `last_step` shows `assessment_complete` not `unknown`
- [ ] Restart → all context keys cleared, source buttons shown again
- [ ] Reload page → `last_step` correctly restored from SQLite, not `unknown`
