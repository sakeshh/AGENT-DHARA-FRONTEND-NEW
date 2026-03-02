'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import { FaExclamationTriangle, FaCheckCircle, FaTimesCircle, FaPlus } from 'react-icons/fa';

export default function DataQuality() {
  const [humanInLoop, setHumanInLoop] = useState<'approve' | 'reject' | null>(null);

  return (
    <motion.div
      className="space-y-4"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, delay: 0.1 }}
    >
      <h3 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">
        Data Quality & Validation
      </h3>

      {/* Action Buttons */}
      <div className="space-y-2">
        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          className="w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 hover:border-orange-300 transition-all"
        >
          <FaExclamationTriangle className="text-orange-500" />
          View Anomalies
        </motion.button>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          className="w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 hover:border-green-300 transition-all"
        >
          <FaCheckCircle className="text-green-600" />
          Apply Validation Rules
        </motion.button>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          className="w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-green-600 to-teal-600 rounded-lg hover:shadow-md transition-all"
        >
          <FaPlus />
          Add Custom Rule
        </motion.button>
      </div>

      {/* Human-in-the-Loop Toggle */}
      <div className="mt-4 p-3 bg-slate-50 rounded-lg border border-slate-200">
        <label className="block text-xs font-medium text-slate-600 mb-3">
          Human-in-the-Loop
        </label>
        <div className="flex gap-2">
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={() => setHumanInLoop(humanInLoop === 'approve' ? null : 'approve')}
            className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium rounded-lg transition-all ${
              humanInLoop === 'approve'
                ? 'bg-green-600 text-white shadow-md'
                : 'bg-white text-slate-700 border border-slate-300 hover:border-green-500'
            }`}
          >
            <FaCheckCircle />
            Approve
          </motion.button>

          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={() => setHumanInLoop(humanInLoop === 'reject' ? null : 'reject')}
            className={`flex-1 flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium rounded-lg transition-all ${
              humanInLoop === 'reject'
                ? 'bg-red-600 text-white shadow-md'
                : 'bg-white text-slate-700 border border-slate-300 hover:border-red-500'
            }`}
          >
            <FaTimesCircle />
            Reject
          </motion.button>
        </div>
      </div>
    </motion.div>
  );
}
