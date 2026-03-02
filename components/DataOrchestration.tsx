'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import { FaPlay, FaStop, FaFileAlt } from 'react-icons/fa';

export default function DataOrchestration() {
  const [schedule, setSchedule] = useState('batch');
  const [isRunning, setIsRunning] = useState(false);

  return (
    <motion.div
      className="space-y-4"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, delay: 0.3 }}
    >
      <h3 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">
        Adaptive Data Orchestration
      </h3>

      {/* Schedule Selector */}
      <div>
        <label className="block text-xs font-medium text-slate-600 mb-2">
          Schedule Selector
        </label>
        <div className="relative">
          <select
            value={schedule}
            onChange={(e) => setSchedule(e.target.value)}
            className="w-full px-3 py-2 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none bg-white appearance-none"
          >
            <option value="batch">Batch</option>
            <option value="near-realtime">Near Real-Time</option>
            <option value="streaming">Streaming</option>
          </select>
          <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-400">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        </div>
      </div>

      {/* Pipeline Controls */}
      <div className="space-y-2">
        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          onClick={() => setIsRunning(true)}
          disabled={isRunning}
          className={`w-full flex items-center justify-center gap-2 px-3 py-2.5 text-sm font-medium rounded-lg transition-all ${
            isRunning
              ? 'bg-slate-300 text-slate-500 cursor-not-allowed'
              : 'text-white bg-gradient-to-r from-green-600 to-teal-600 hover:shadow-md'
          }`}
        >
          <FaPlay />
          Start Pipeline
        </motion.button>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          onClick={() => setIsRunning(false)}
          disabled={!isRunning}
          className={`w-full flex items-center justify-center gap-2 px-3 py-2.5 text-sm font-medium rounded-lg transition-all ${
            !isRunning
              ? 'bg-slate-300 text-slate-500 cursor-not-allowed'
              : 'text-white bg-gradient-to-r from-red-600 to-orange-600 hover:shadow-md'
          }`}
        >
          <FaStop />
          Stop Pipeline
        </motion.button>
      </div>

      {/* Logs Section */}
      <div className="mt-4">
        <label className="block text-xs font-medium text-slate-600 mb-2">
          Pipeline Logs
        </label>
        <div className="p-3 bg-slate-900 rounded-lg border border-slate-700 text-xs font-mono text-green-400 h-32 overflow-y-auto">
          {isRunning ? (
            <div className="space-y-1">
              <div>[{new Date().toLocaleTimeString()}] Pipeline started...</div>
              <div>[{new Date().toLocaleTimeString()}] Loading data source...</div>
              <div>[{new Date().toLocaleTimeString()}] Applying transformations...</div>
              <div className="animate-pulse">[{new Date().toLocaleTimeString()}] Processing...</div>
            </div>
          ) : (
            <div className="text-slate-500">
              No active pipeline. Click Start Pipeline to begin.
            </div>
          )}
        </div>
      </div>
    </motion.div>
  );
}
