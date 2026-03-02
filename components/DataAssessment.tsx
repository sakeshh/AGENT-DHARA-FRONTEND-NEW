'use client';

import { motion } from 'framer-motion';
import {
  FaDatabase,
  FaFileAlt,
  FaCloud,
  FaPlug,
  FaEye,
  FaDownload,
  FaCheckCircle,
} from 'react-icons/fa';

export default function DataAssessment() {
  const dataSourceOptions = ['File', 'Database', 'Azure Blob', 'API'];

  return (
    <motion.div
      className="space-y-4"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <h3 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">
        Intelligent Data Assessment
      </h3>

      {/* Data Source Selector */}
      <div>
        <label className="block text-xs font-medium text-slate-600 mb-2">
          Select Data Source
        </label>
        <div className="relative">
          <select className="w-full px-3 py-2 text-sm border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none bg-white appearance-none">
            {dataSourceOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
          <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none text-slate-400">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="space-y-2">
        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          className="w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 hover:border-indigo-300 transition-all"
        >
          <FaEye className="text-indigo-600" />
          View Schema
        </motion.button>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          className="w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 hover:border-indigo-300 transition-all"
        >
          <FaFileAlt className="text-indigo-600" />
          View Metadata
        </motion.button>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          className="w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-indigo-600 to-purple-600 rounded-lg hover:shadow-md transition-all"
        >
          <FaDownload />
          Download Assessment Report
        </motion.button>
      </div>
    </motion.div>
  );
}
