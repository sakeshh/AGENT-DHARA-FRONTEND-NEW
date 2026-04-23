'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { FaFileAlt, FaCheckCircle, FaSearch, FaTable, FaDatabase } from 'react-icons/fa';

interface FileSelectorProps {
  database: string;
  onSelect: (files: string[], availableFiles: string[]) => void;
  onNext: () => void;
  selectedFiles: string[];
}

type Item = { name: string; size: string; rows: number; type: string };

function getSessionId(): string {
  if (typeof window === 'undefined') return 'default';
  return window.localStorage.getItem('dharaSessionId') || 'default';
}

function parseSourceToken(token: string): { kind: 'sql' | 'blob' | 'streams' | 'unknown'; index: number } {
  // token: "src:<kind>:<index>"
  const parts = String(token || '').split(':');
  const kind = (parts[1] as any) || 'unknown';
  const index = Number(parts[2] || 0);
  return { kind, index: Number.isFinite(index) ? index : 0 };
}

export default function FileSelector({ database, onSelect, onNext, selectedFiles }: FileSelectorProps) {
  const [files, setFiles] = useState<Item[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<string[]>(selectedFiles);

  useEffect(() => {
    let alive = true;
    (async () => {
      setLoading(true);
      try {
        const src = parseSourceToken(database);
        if (src.kind === 'sql') {
          const res = await fetch('/api/list-tables', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: '',
          });
          const data = await res.json().catch(() => null);
          const tables = Array.isArray(data?.tables) ? data.tables : [];
          if (!alive) return;
          setFiles(
            tables.map((t: string) => ({
              name: String(t),
              size: 'SQL table',
              rows: 0,
              type: 'table',
            }))
          );
        } else if (src.kind === 'blob') {
          const sid = getSessionId();
          const res = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId: sid, messages: [{ role: 'user', content: 'list files' }] }),
          });
          const data = await res.json().catch(() => null);
          const names = Array.isArray(data?.payload?.files) ? data.payload.files : [];
          if (!alive) return;
          setFiles(
            names.map((n: string) => ({
              name: String(n),
              size: 'Blob object',
              rows: 0,
              type: 'blob',
            }))
          );
        } else if (src.kind === 'streams') {
          const sid = getSessionId();
          const res = await fetch('/api/chat', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId: sid, messages: [{ role: 'user', content: 'list local files' }] }),
          });
          const data = await res.json().catch(() => null);
          const names = Array.isArray(data?.payload?.files) ? data.payload.files : [];
          if (!alive) return;
          setFiles(
            names.map((n: string) => ({
              name: String(n),
              size: 'File',
              rows: 0,
              type: 'file',
            }))
          );
        } else {
          if (!alive) return;
          setFiles([]);
        }
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, [database]);

  const filteredFiles = files.filter(file =>
    file.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const toggleFile = (fileName: string) => {
    if (selected.includes(fileName)) {
      setSelected(selected.filter(f => f !== fileName));
    } else {
      setSelected([...selected, fileName]);
    }
  };

  const handleNext = () => {
    onSelect(selected, files.map((f) => f.name));
    onNext();
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-zinc-900 mb-2">Select Files</h2>
          <p className="text-black/60">Choose one or more files to analyze</p>
        </div>
        <div className="text-right">
          <div className="text-sm text-black/60">Selected</div>
          <div className="text-2xl font-bold text-[#0070AD]">{selected.length}</div>
        </div>
      </div>

      {/* Search Bar */}
      <div className="relative">
        <FaSearch className="absolute left-4 top-1/2 -translate-y-1/2 text-black/45" />
        <input
          type="text"
          placeholder="Search files..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="w-full pl-12 pr-4 py-3 border border-black/10 rounded-xl focus:ring-2 focus:ring-[#0070AD]/25 focus:border-[#0070AD]/40 outline-none bg-white/90 text-zinc-900 placeholder-black/40"
        />
      </div>

      {/* Files List */}
      {loading ? (
        <div className="space-y-3">
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="h-20 bg-white/10 animate-pulse rounded-xl" />
          ))}
        </div>
      ) : (
        <div className="space-y-3 max-h-96 overflow-y-auto">
          {filteredFiles.map((file, index) => {
            const isSelected = selected.includes(file.name);
            return (
              <motion.button
                key={file.name}
                onClick={() => toggleFile(file.name)}
                className={`w-full p-4 border rounded-xl transition-all text-left ${
                  isSelected
                    ? 'border-[#0070AD]/50 bg-[#0070AD]/10'
                    : 'border-black/10 hover:border-[#0070AD]/30 bg-white/85'
                }`}
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: index * 0.05 }}
              >
                <div className="flex items-center gap-4">
                  <div className={`w-12 h-12 rounded-lg flex items-center justify-center ${
                    isSelected ? 'bg-[#0070AD]' : 'bg-black/5'
                  }`}>
                    {isSelected ? (
                      <FaCheckCircle className="text-2xl text-white" />
                    ) : (
                      <FaTable className="text-2xl text-black/45" />
                    )}
                  </div>
                  <div className="flex-1">
                    <h3 className="font-semibold text-zinc-900">{file.name}</h3>
                    <div className="flex gap-4 text-sm text-black/60 mt-1">
                      <span>{file.size}</span>
                      <span>•</span>
                      <span>{file.rows.toLocaleString()} rows</span>
                      <span>•</span>
                      <span className="capitalize">{file.type}</span>
                    </div>
                  </div>
                </div>
              </motion.button>
            );
          })}
        </div>
      )}

      {filteredFiles.length === 0 && !loading && (
        <div className="text-center py-12">
          <FaFileAlt className="text-6xl text-black/30 mx-auto mb-4" />
          <p className="text-black/60">No files found matching your search</p>
        </div>
      )}

      {/* Next Button */}
      {selected.length > 0 && (
        <motion.button
          onClick={handleNext}
          className="w-full py-4 rounded-xl border border-[#0070AD]/40 bg-[#0070AD]/10 text-[#0070AD] font-semibold hover:bg-[#0070AD]/15 hover:border-[#0070AD]/60 transition-all"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          Continue with {selected.length} file{selected.length !== 1 ? 's' : ''}
        </motion.button>
      )}
    </div>
  );
}
