'use client';

import { useState, useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  FaDatabase,
  FaCloud,
  FaStream,
  FaPlug,
  FaFolder,
  FaChevronDown,
  FaPaperPlane,
  FaArrowLeft,
  FaCopy,
  FaShareAlt,
  FaEdit,
  FaStop,
  FaPlus,
  FaFileUpload,
} from 'react-icons/fa';
import { Message } from '@/types';
import Image from 'next/image';

interface ChatOption {
  id: string;
  text: string;
  category: string;
  action?: string;
  icon?: React.ReactNode;
}

const DATA_SOURCE_OPTIONS: ChatOption[] = [
  { id: 'local', text: 'Local data', category: 'Data Source', action: 'local-data', icon: <FaFolder className="text-[#0070AD]/80" /> },
  { id: 'sql', text: 'SQL data', category: 'Data Source', action: 'sql-data', icon: <FaDatabase className="text-[#0070AD]/80" /> },
  { id: 'blob', text: 'Blob data', category: 'Data Source', action: 'blob-data', icon: <FaCloud className="text-[#12ABDB]/80" /> },
  { id: 'streams', text: 'Real-time streams', category: 'Data Source', action: 'realtime-streams', icon: <FaStream className="text-[#0070AD]/80" /> },
  { id: 'apis', text: 'APIs', category: 'Data Source', action: 'apis', icon: <FaPlug className="text-[#12ABDB]/80" /> },
];

/* Options shown after selecting a data source — vary by source type */
const CHAT_OPTIONS_BY_SOURCE: Record<string, ChatOption[]> = {
  sql: [
    { id: 'sql-view', text: '👁️ View data in files/tables', category: 'Explore', action: 'guided-view' },
    { id: 'sql-report', text: '📄 Extract report from files/tables', category: 'Reports', action: 'guided-report' },
  ],
  local: [
    { id: 'local-view', text: '👁️ View data in files', category: 'Explore', action: 'guided-view' },
    { id: 'local-report', text: '📄 Extract report from files', category: 'Reports', action: 'guided-report' },
  ],
  blob: [
    { id: 'blob-view', text: '👁️ View data in files', category: 'Explore', action: 'guided-view' },
    { id: 'blob-report', text: '📄 Extract report from files', category: 'Reports', action: 'guided-report' },
  ],
  streams: [
    { id: 'streams-view', text: '👁️ View data in stream files', category: 'Explore', action: 'guided-view' },
    { id: 'streams-report', text: '📄 Extract report from stream files', category: 'Reports', action: 'guided-report' },
  ],
  apis: [
    { id: 'apis-1', text: '📊 Start Data Pipeline Workflow', category: 'Pipeline', action: '/data-pipeline' },
    { id: 'apis-2', text: '🔗 Connect to REST / GraphQL API', category: 'API', action: 'connect' },
    { id: 'apis-2b', text: '📄 View Report', category: 'Reports', action: 'view-report' },
    { id: 'apis-3', text: '📥 Fetch API Data', category: 'Import', action: 'fetch' },
    { id: 'apis-4', text: '🔄 Transform Data', category: 'Transform', action: 'transform' },
    { id: 'apis-5', text: '📤 Export Data', category: 'Export', action: 'export' },
    { id: 'apis-6', text: '🔍 Analyze API Response', category: 'Analysis', action: 'analyze' },
  ],
};

function getChatOptionsForSource(sourceId: string | null): ChatOption[] {
  if (!sourceId) return [];
  return CHAT_OPTIONS_BY_SOURCE[sourceId] ?? CHAT_OPTIONS_BY_SOURCE.local;
}

/** Format time the same on server and client to avoid hydration mismatch (e.g. "pm" vs "PM"). */
function formatTime(d: Date): string {
  const h = d.getHours();
  const m = d.getMinutes();
  const ampm = h >= 12 ? 'PM' : 'AM';
  const h12 = h % 12 || 12;
  const min = m < 10 ? `0${m}` : String(m);
  return `${h12}:${min} ${ampm}`;
}

export default function ChatWindow() {
  // Important: do not inject canned "bot" content. Bot messages should come only from the agent/backend.
  const [messages, setMessages] = useState<Message[]>([]);
  const [hasSelectedDataSource, setHasSelectedDataSource] = useState(false);
  const [selectedDataSource, setSelectedDataSource] = useState<string | null>(null);
  const [agentError, setAgentError] = useState<string | null>(null);
  const [showOptions, setShowOptions] = useState(false);
  const [chatInput, setChatInput] = useState('');
  const [isLoadingAgent, setIsLoadingAgent] = useState(false);
  const [agentThreadId, setAgentThreadId] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string>('default');
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editingText, setEditingText] = useState<string>('');
  const [localFolderFiles, setLocalFolderFiles] = useState<File[]>([]);
  const [guidedMode, setGuidedMode] = useState<'none' | 'view' | 'report'>('none');
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesScrollRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const localFolderInputRef = useRef<HTMLInputElement | null>(null);

  const getEffectiveSessionId = (): string => {
    if (typeof window === 'undefined') return 'default';
    return window.localStorage.getItem('dharaSessionId') || 'default';
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const saved = window.localStorage.getItem('agentThreadId');
    if (saved && !agentThreadId) setAgentThreadId(saved);
    setSessionId(getEffectiveSessionId());
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (agentThreadId) window.localStorage.setItem('agentThreadId', agentThreadId);
  }, [agentThreadId]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const handler = async () => {
      const sid = getEffectiveSessionId();
      setSessionId(sid);
      try {
        const res = await fetch(`/api/sessions/${encodeURIComponent(sid)}`);
        const data = await res.json();
        const session = data?.session;
        const msgs = Array.isArray(session?.messages) ? session.messages : [];
        if (!msgs.length) return;
        setMessages(
          msgs
            .filter((m: any) => m?.content && (m?.role === 'user' || m?.role === 'assistant'))
            .map((m: any, idx: number) => ({
              id: String(m?.id ?? `${sid}-${idx}-${Date.now()}`),
              text: String(m.content),
              sender: m.role === 'user' ? ('user' as const) : ('bot' as const),
              timestamp: m.ts ? new Date(Number(m.ts) * 1000) : new Date(),
            }))
        );
      } catch {
        // ignore
      }
    };
    window.addEventListener('dhara-session-change', handler as EventListener);
    return () => window.removeEventListener('dhara-session-change', handler as EventListener);
  }, []);

  /** Build request payload for Azure agent from current messages */
  const getMessagesForApi = (msgs: Message[]) =>
    msgs.map((m) => ({
      role: m.sender === 'user' ? ('user' as const) : ('assistant' as const),
      content: m.text,
    }));

  /** Call our API route which invokes your Azure AI Foundry agent (threads + runs). */
  const fetchAgentReply = async (
    msgs: Message[]
  ): Promise<{ content: string; threadId: string | null; payload: any }> => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: controller.signal,
      body: JSON.stringify({
        messages: getMessagesForApi(msgs),
        threadId: agentThreadId,
        sessionId,
      }),
    });
    const data = await res.json();
    const content = typeof data?.content === 'string' ? data.content : '';
    const threadId = data?.threadId ?? null;
    const payload = data?.payload ?? null;
    if (!res.ok || !content.trim()) {
      const errText =
        typeof data?.error === 'string'
          ? data.error
          : !res.ok
            ? `HTTP_${res.status}`
            : 'EMPTY_REPLY';
      throw new Error(errText);
    }
    return { content, threadId, payload };
  };

  const stopGeneration = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setIsLoadingAgent(false);
  };

  const uploadReport = async (file: File) => {
    // Upload via Next.js API route (proxies to backend /upload?format=md)
    const form = new FormData();
    form.append('file', file, file.name);
    const res = await fetch('/api/upload', { method: 'POST', body: form });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.detail || data?.error || `Upload failed (${res.status})`);
    const report = String(data?.report ?? '');
    if (!report) throw new Error('No report returned.');

    // Persist in backend session context for later Q&A/compare.
    await fetch('/api/session-context', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId,
        context: {
          uploaded_report_name: file.name,
          uploaded_report_markdown: report,
          uploaded_report_uploaded_at: Date.now(),
        },
      }),
    }).catch(() => null);
    // No canned bot response here—user can ask the agent about the uploaded report next.
  };

  const copyText = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // ignore
    }
  };

  const shareText = async (text: string) => {
    try {
      // @ts-ignore - web share optional
      if (navigator.share) {
        // @ts-ignore
        await navigator.share({ text });
        return;
      }
    } catch {
      // ignore
    }
    await copyText(text);
  };

  const beginEdit = (m: Message) => {
    setEditingId(m.id);
    setEditingText(m.text);
  };

  const cancelEdit = () => {
    setEditingId(null);
    setEditingText('');
  };

  const saveEditAndRegenerate = async () => {
    if (!editingId) return;
    const newText = editingText.trim();
    if (!newText) return;

    // Build a truncated conversation up to the edited message, replace that message, and regenerate.
    const idx = messages.findIndex((m) => m.id === editingId);
    if (idx < 0) return;
    const editedMsg = { ...messages[idx], text: newText };
    const base = [...messages.slice(0, idx), editedMsg];
    setMessages(base);
    cancelEdit();

    setIsLoadingAgent(true);
    setAgentError(null);
    try {
      const { content, threadId } = await fetchAgentReply(base);
      if (threadId) setAgentThreadId(threadId);
      setMessages((prev) => [
        ...prev,
        { id: (Date.now() + 1).toString(), text: content, sender: 'bot', timestamp: new Date() },
      ]);
    } catch (err: any) {
      const isAbort = err?.name === 'AbortError';
      if (!isAbort) {
        setAgentError("Couldn't reach the agent to regenerate. Check backend/agent configuration and try again.");
      }
    } finally {
      setIsLoadingAgent(false);
    }
  };

  /** Scroll only the messages panel — avoid scrollIntoView (it scrolls ancestor windows and can hide the chat header). */
  const scrollMessagesToBottom = () => {
    const el = messagesScrollRef.current;
    if (!el) return;
    requestAnimationFrame(() => {
      el.scrollTop = el.scrollHeight;
    });
  };

  useEffect(() => {
    scrollMessagesToBottom();
  }, [messages, isLoadingAgent]);

  const handleBack = () => {
    if (hasSelectedDataSource) {
      setHasSelectedDataSource(false);
      setSelectedDataSource(null);
      setGuidedMode('none');
      const userMessage: Message = {
        id: Date.now().toString(),
        text: '← Back',
        sender: 'user',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, userMessage]);
    }
    setShowOptions(false);
  };

  const sendUserText = async (text: string) => {
    const userMessage: Message = {
      id: Date.now().toString(),
      text,
      sender: 'user',
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMessage]);
    setIsLoadingAgent(true);
    setAgentError(null);
    try {
      const messagesWithUser = [...messages, userMessage];
      const { content, threadId, payload } = await fetchAgentReply(messagesWithUser);
      if (threadId) setAgentThreadId(threadId);

      const interactiveOptions: Message['options'] = Array.isArray(payload?.options)
        ? payload.options.map((o: any, i: number) => ({
            id: String(o?.id ?? `opt-${i}`),
            text: String(o?.text ?? ''),
            send: String(o?.send ?? ''),
          })).filter((o: any) => o.text && o.send)
        : Array.isArray(payload?.tables)
        ? payload.tables.slice(0, 12).map((t: any, i: number) => ({
            id: `table-${i}`,
            text: String(t?.name ?? t ?? `Table ${i + 1}`),
            send: `select table ${Number(t?.index ?? i) + 1}`,
          }))
        : Array.isArray(payload?.files)
          ? [
              ...payload.files.slice(0, 12).map((f: any, i: number) => ({
                id: `file-${i}`,
                text: String(f),
                send:
                  selectedDataSource === 'blob'
                    ? `select files ${i + 1}`
                    : selectedDataSource === 'streams'
                      ? `select local files ${i + 1}`
                      : `select local files ${i + 1}`,
              })),
              ...(payload.files.length > 0
                ? [
                    {
                      id: 'select-all',
                      text: 'Select all',
                      send: selectedDataSource === 'blob' ? 'select files all' : 'select local files all',
                    },
                  ]
                : []),
            ]
          : undefined;

      const botResponse: Message = {
        id: (Date.now() + 1).toString(),
        text: content,
        sender: 'bot',
        timestamp: new Date(),
        options: interactiveOptions,
      };
      setMessages((prev) => [...prev, botResponse]);
    } catch (err) {
      setAgentError("Couldn't reach the agent. Check your backend/agent configuration and try again.");
    } finally {
      setIsLoadingAgent(false);
    }
  };

  const handleOptionSelect = async (option: ChatOption) => {
    const userMessage: Message = {
      id: Date.now().toString(),
      text: option.text,
      sender: 'user',
      timestamp: new Date(),
    };

    const isDataSourceSelection = DATA_SOURCE_OPTIONS.some(ds => ds.id === option.id);
    if (isDataSourceSelection) {
      setHasSelectedDataSource(true);
      setSelectedDataSource(option.id);
      setGuidedMode('none');
    }

    setMessages((prev) => [...prev, userMessage]);

    // Local data: open folder picker (dynamic path selection)
    if (option.id === 'local') {
      // Ensure user sees the local panel options, but open explorer immediately.
      setHasSelectedDataSource(true);
      setSelectedDataSource('local');
      setShowOptions(false);
      setTimeout(() => localFolderInputRef.current?.click(), 0);
      return;
    }

    // Redirect to data pipeline without calling agent
    if (option.action === '/data-pipeline') {
      setTimeout(() => {
        window.location.href = '/data-pipeline';
      }, 1500);
      return;
    }

    // Guided UX: for SQL/Blob/Streams/Local, turn "View" and "Report" into deterministic steps.
    if (option.action === 'guided-view') {
      setGuidedMode('view');
      // Ask backend to list selectable entities
      const cmd =
        option.id.startsWith('sql') ? 'list tables' : option.id.startsWith('blob') ? 'list files' : 'list local files';
      await sendUserText(cmd);
      return;
    }
    if (option.action === 'guided-report') {
      setGuidedMode('report');
      const cmd =
        option.id.startsWith('sql') ? 'list tables' : option.id.startsWith('blob') ? 'list files' : 'list local files';
      await sendUserText(cmd);
      return;
    }

    await sendUserText(option.text);
  };

  return (
    <div className="flex h-full min-h-0 min-w-0 w-full flex-1 flex-col bg-transparent">
      {/* Top chrome only (borders / bar) — no title text; keeps alignment with Sign out on large screens */}
      <div
        className="shrink-0 w-full border-y border-[#0070AD]/20 bg-gradient-to-r from-[#0070AD]/10 via-white/60 to-[#12ABDB]/10 pt-4 pb-6 backdrop-blur-xl lg:pr-24"
        aria-hidden
      />

      {/* Chat Messages Area — basis-0 + flex-1 fills space above composer */}
      <div
        ref={messagesScrollRef}
        className="min-h-0 flex-1 basis-0 space-y-4 overflow-y-auto overflow-x-hidden p-6 bg-gradient-to-b from-white/40 via-white/10 to-[#0070AD]/10"
      >
        <AnimatePresence initial={false}>
          {messages.map((message, idx) => {
            const isUser = message.sender === 'user';
            const staggerDelay = Math.min(idx * 0.04, 0.2);
            return (
            <motion.div
              key={message.id}
              initial={{ opacity: 0, x: isUser ? 40 : -40, scale: 0.95 }}
              animate={{ opacity: 1, x: 0, scale: 1 }}
              exit={{ opacity: 0, x: isUser ? 20 : -20, scale: 0.98 }}
              transition={{ duration: 0.4, ease: [0.25, 0.46, 0.45, 0.94], delay: staggerDelay }}
              className={`group flex ${isUser ? 'justify-end' : 'justify-start'}`}
            >
              <div className="max-w-[78%]">
                <motion.div
                  initial={{ scale: 0.92, opacity: 0 }}
                  animate={{ scale: 1, opacity: 1 }}
                  transition={{ duration: 0.35, delay: 0.05 + staggerDelay, ease: 'easeOut' }}
                  className={`rounded-lg border px-4 py-3 ${
                    message.sender === 'user'
                      ? 'border-[#0070AD]/40 bg-gradient-to-r from-[#0070AD] to-[#12ABDB] text-white shadow-[0_10px_30px_rgba(0,112,173,0.18)]'
                      : 'border-[#0070AD]/25 bg-gradient-to-r from-[#0070AD]/10 to-[#12ABDB]/10 text-zinc-900'
                  }`}
                >
                  {editingId === message.id ? (
                    <div className="space-y-2">
                      <textarea
                        value={editingText}
                        onChange={(e) => setEditingText(e.target.value)}
                        className="w-full resize-none rounded-lg border border-black/10 bg-white/85 px-3 py-2 text-sm text-zinc-900 outline-none focus:border-[#0070AD]/40 focus:ring-2 focus:ring-[#0070AD]/20"
                        rows={3}
                      />
                      <div className="flex justify-end gap-2">
                        <button
                          type="button"
                          onClick={cancelEdit}
                          className="rounded-lg border border-black/10 bg-white/85 px-3 py-1.5 text-xs font-semibold text-black/70 hover:bg-white"
                        >
                          Cancel
                        </button>
                        <button
                          type="button"
                          onClick={saveEditAndRegenerate}
                          className="rounded-lg border border-[#0070AD]/40 bg-[#0070AD] px-3 py-1.5 text-xs font-semibold text-white hover:bg-[#12ABDB]"
                        >
                          Save & regenerate
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      <p className="text-sm leading-relaxed whitespace-pre-wrap">{message.text}</p>
                      {message.sender === 'bot' && Array.isArray(message.options) && message.options.length > 0 && (
                        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                          {message.options.map((opt) => (
                            <button
                              key={opt.id}
                              type="button"
                              onClick={() => sendUserText(opt.send)}
                              className="rounded-lg border border-black/10 bg-white/90 px-3 py-2 text-left text-xs font-semibold text-zinc-900 hover:bg-white hover:border-[#0070AD]/30"
                              title={opt.send}
                            >
                              {opt.text}
                            </button>
                          ))}
                          <button
                            type="button"
                            onClick={handleBack}
                            className="rounded-lg border border-black/10 bg-white/90 px-3 py-2 text-left text-xs font-semibold text-zinc-900 hover:bg-white hover:border-[#0070AD]/30"
                          >
                            ← Back
                          </button>
                        </div>
                      )}
                    </div>
                  )}
                  <span className={`mt-2 block text-xs ${message.sender === 'user' ? 'text-white/80' : 'text-black/45'}`}>
                    {formatTime(message.timestamp)}
                  </span>
                </motion.div>

                {/* Message actions */}
                {editingId !== message.id && (
                  <div className={`mt-1 flex items-center gap-2 text-[11px] opacity-0 transition-opacity group-hover:opacity-100 ${isUser ? 'justify-end' : 'justify-start'}`}>
                    {message.sender === 'bot' ? (
                      <>
                        <button
                          type="button"
                          onClick={() => copyText(message.text)}
                          className="inline-flex items-center gap-1 rounded-md border border-black/10 bg-white/85 px-2 py-1 text-black/70 hover:bg-white"
                          title="Copy"
                        >
                          <FaCopy />
                          Copy
                        </button>
                        <button
                          type="button"
                          onClick={() => shareText(message.text)}
                          className="inline-flex items-center gap-1 rounded-md border border-black/10 bg-white/85 px-2 py-1 text-black/70 hover:bg-white"
                          title="Share"
                        >
                          <FaShareAlt />
                          Share
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        onClick={() => beginEdit(message)}
                        className="inline-flex items-center gap-1 rounded-md border border-black/10 bg-white/85 px-2 py-1 text-black/70 hover:bg-white"
                        title="Edit"
                      >
                        <FaEdit />
                        Edit
                      </button>
                    )}
                  </div>
                )}
              </div>
            </motion.div>
            );
          })}
        </AnimatePresence>
        {isLoadingAgent && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="flex justify-start"
          >
            <div className="flex max-w-[70%] items-center rounded-lg border border-black/10 bg-white/85 px-4 py-3">
              <span className="inline-flex gap-1.5">
                {[0, 1, 2].map((i) => (
                  <motion.span
                    key={i}
                          className="h-2 w-2 rounded-full bg-[#0070AD]/80"
                    animate={{ y: [0, -6, 0] }}
                    transition={{ duration: 0.5, repeat: Infinity, delay: i * 0.12, ease: 'easeInOut' }}
                  />
                ))}
              </span>
            </div>
          </motion.div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Chat Box at Bottom */}
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3, delay: 0.2 }}
        className="shrink-0 border-t border-[#0070AD]/20 bg-gradient-to-r from-[#0070AD]/10 via-white/60 to-[#12ABDB]/10 px-6 py-4 backdrop-blur-xl"
      >
        {agentError && (
          <div className="mb-3 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
            {agentError}
          </div>
        )}
        {isLoadingAgent && (
          <div className="mb-3 flex justify-end">
            <button
              type="button"
              onClick={stopGeneration}
              className="inline-flex items-center gap-2 rounded-xl border border-black/10 bg-white/85 px-3 py-2 text-xs font-semibold text-black/70 hover:bg-white"
              title="Stop generating"
              aria-label="Stop generating"
            >
              <FaStop />
              Stop
            </button>
          </div>
        )}

        {/* Hidden file input for report upload */}
        <input
          ref={fileInputRef}
          type="file"
          accept=".md,.txt,.csv,.json,.jsonl,.xlsx,.xls,.parquet"
          className="hidden"
          onChange={async (e) => {
            const f = e.target.files?.[0];
            e.target.value = '';
            if (!f) return;
            try {
              await uploadReport(f);
            } catch (err: any) {
              setMessages((prev) => [
                ...prev,
                { id: String(Date.now()), text: `Upload failed: ${err?.message ?? String(err)}`, sender: 'bot', timestamp: new Date() },
              ]);
            }
          }}
        />

        {/* Hidden folder input for Local data (directory picker) */}
        <input
          ref={localFolderInputRef}
          type="file"
          multiple
          className="hidden"
          // @ts-ignore - webkitdirectory is supported by Chromium-based browsers
          webkitdirectory="true"
          // @ts-ignore - directory is non-standard but commonly supported
          directory="true"
          onChange={async (e) => {
            const files = Array.from(e.target.files || []);
            e.target.value = '';
            if (!files.length) return;

            setLocalFolderFiles(files);

            const rel = (files[0] as any)?.webkitRelativePath as string | undefined;
            const folder = rel ? rel.split('/')[0] : 'Selected folder';
            const names = files
              .slice(0, 12)
              .map((f) => (f as any)?.webkitRelativePath || f.name)
              .join('\n');
            const more = files.length > 12 ? `\n…(+${files.length - 12} more)` : '';

            // Persist file list metadata into session context (names only).
            await fetch('/api/session-context', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                session_id: sessionId,
                context: {
                  local_folder_name: folder,
                  local_folder_file_count: files.length,
                  local_folder_files: files.map((f) => (f as any)?.webkitRelativePath || f.name),
                  local_folder_selected_at: Date.now(),
                },
              }),
            }).catch(() => null);

            setMessages((prev) => [
              ...prev,
              {
                id: String(Date.now()),
                text:
                  `Local folder selected: ${folder}\n` +
                  `Files found: ${files.length}\n\n` +
                  `Top files:\n${names}${more}\n\n` +
                  `Next: you can ask me to assess a specific file, or use “+ Upload” to upload one report/file for analysis.`,
                sender: 'bot',
                timestamp: new Date(),
              },
            ]);
          }}
        />
        {/* Options dropdown - shown when "Choose option" is clicked */}
        <AnimatePresence>
          {showOptions && (
            <>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                onClick={() => setShowOptions(false)}
                className="fixed inset-0 z-[5] bg-black/50"
                style={{ margin: 0, top: 0, left: 0, right: 0, bottom: 0 }}
              />
              <motion.div
                initial={{ opacity: 0, height: 0, scale: 0.96, y: -8 }}
                animate={{ opacity: 1, height: 'auto', scale: 1, y: 0 }}
                exit={{ opacity: 0, height: 0, scale: 0.96, y: -8 }}
                transition={{ duration: 0.28, ease: [0.25, 0.46, 0.45, 0.94] }}
                style={{ transformOrigin: 'top left' }}
                className="mb-4 relative z-[6] rounded-xl border border-black/10 bg-white/85 backdrop-blur-xl"
              >
                {!hasSelectedDataSource ? (
                  <>
                    <div className="p-3 flex flex-wrap items-center gap-3 border-b border-black/10">
                      <motion.button
                        whileHover={{ scale: 1.05, x: -2 }}
                        whileTap={{ scale: 0.95 }}
                        onClick={() => setShowOptions(false)}
                        className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium text-black/55 transition-colors hover:text-black"
                      >
                        <FaArrowLeft className="w-3 h-3" />
                        Back
                      </motion.button>
                      <p className="text-sm font-medium text-zinc-900">Choose one:</p>
                    </div>
                    <div className="options-scroll p-3 max-h-48 scroll-smooth">
                      <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
                        {DATA_SOURCE_OPTIONS.map((option, idx) => (
                          <motion.button
                            key={option.id}
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: idx * 0.03 }}
                            whileHover={{ scale: 1.02 }}
                            whileTap={{ scale: 0.98 }}
                            onClick={() => {
                              handleOptionSelect(option);
                              setShowOptions(false);
                            }}
                            className="flex items-center gap-2 rounded-lg border border-black/10 bg-white/90 p-3 text-left text-sm font-medium text-zinc-900 transition-all hover:border-[#0070AD]/30 hover:bg-white"
                          >
                            {option.icon && <span className="flex-shrink-0">{option.icon}</span>}
                            <span>{option.text}</span>
                          </motion.button>
                        ))}
                      </div>
                    </div>
                  </>
                ) : (
                  <>
                    <div className="p-3 flex flex-wrap items-center gap-3 border-b border-black/10">
                      <motion.button
                        whileHover={{ scale: 1.05, x: -2 }}
                        whileTap={{ scale: 0.95 }}
                        onClick={handleBack}
                        className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium text-black/55 transition-colors hover:text-black"
                      >
                        <FaArrowLeft className="w-3 h-3" />
                        Back
                      </motion.button>
                      <p className="text-sm font-medium text-zinc-900">What would you like to do next?</p>
                    </div>
                    <div className="options-scroll p-3 max-h-52 scroll-smooth">
                      <div className="space-y-2">
                        {getChatOptionsForSource(selectedDataSource).map((option, idx) => (
                          <motion.button
                            key={option.id}
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: Math.min(idx * 0.02, 0.2) }}
                            whileHover={{ scale: 1.01, x: 4 }}
                            whileTap={{ scale: 0.99 }}
                            onClick={() => {
                              handleOptionSelect(option);
                              setShowOptions(false);
                            }}
                            className="w-full flex items-center gap-3 rounded-lg border border-black/10 bg-white/90 px-4 py-3 text-left text-sm font-medium text-zinc-900 transition-all hover:border-[#0070AD]/30 hover:bg-white"
                          >
                            {option.icon && <span className="flex-shrink-0">{option.icon}</span>}
                            <span>{option.text}</span>
                          </motion.button>
                        ))}
                      </div>
                    </div>
                  </>
                )}
              </motion.div>
            </>
          )}
        </AnimatePresence>

        {/* Chat input with options trigger */}
        <div className="flex gap-2 items-center">
          {/* Upload only */}
          <motion.button
            whileHover={{ scale: 1.02, y: -1 }}
            whileTap={{ scale: 0.98 }}
            onClick={() => fileInputRef.current?.click()}
            className="flex shrink-0 items-center justify-center gap-2 rounded-lg border border-black/10 bg-white/90 px-3 py-2.5 text-sm font-medium text-zinc-900 transition-all hover:border-[#0070AD]/30 hover:bg-white"
            title="Upload report"
            aria-label="Upload report"
          >
            <FaPlus className="w-4 h-4" />
            <span className="hidden sm:inline">Upload</span>
          </motion.button>

          <motion.button
            whileHover={{ scale: 1.02, y: -1 }}
            whileTap={{ scale: 0.98 }}
            onClick={() => setShowOptions(!showOptions)}
            className="flex shrink-0 items-center gap-2 rounded-lg border border-black/10 bg-white/90 px-4 py-2.5 text-sm font-medium text-zinc-900 transition-all hover:border-[#0070AD]/30 hover:bg-white"
          >
            <motion.div
              animate={{ rotate: showOptions ? 180 : 0 }}
              transition={{ duration: 0.25 }}
            >
              <FaChevronDown className="w-4 h-4" />
            </motion.div>
            Choose option
          </motion.button>
          <div className="flex-1 flex gap-2">
            <input
              type="text"
              value={chatInput}
              onChange={(e) => setChatInput(e.target.value)}
              onKeyDown={async (e) => {
                if (e.key === 'Enter' && chatInput.trim()) {
                  const text = chatInput.trim();
                  setChatInput('');
                  await sendUserText(text);
                }
              }}
              placeholder="Type a message or choose an option..."
              className="flex-1 rounded-lg border border-black/10 bg-white/90 px-4 py-2.5 text-sm text-zinc-900 outline-none placeholder-black/40 transition-all focus:border-[#0070AD]/40 focus:ring-2 focus:ring-[#0070AD]/20"
            />
            <motion.button
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: !chatInput.trim() || isLoadingAgent ? 1 : 0.88 }}
              transition={{ type: 'spring', stiffness: 400, damping: 17 }}
              disabled={!chatInput.trim() || isLoadingAgent}
              onClick={async () => {
                if (!chatInput.trim() || isLoadingAgent) return;
                const text = chatInput.trim();
                setChatInput('');
                await sendUserText(text);
              }}
              className="flex shrink-0 items-center justify-center rounded-lg border border-[#0070AD]/50 bg-[#0070AD] px-4 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-[#12ABDB] disabled:cursor-not-allowed disabled:opacity-40"
            >
              <motion.span
                className="relative z-10"
                animate={{ x: 0, y: 0 }}
                whileHover={{ x: 2, y: -2 }}
                transition={{ type: 'spring', stiffness: 500 }}
              >
                <FaPaperPlane />
              </motion.span>
            </motion.button>
          </div>
        </div>
      </motion.div>
    </div>
  );
}
