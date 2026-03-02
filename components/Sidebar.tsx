'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import { FaBars, FaTimes } from 'react-icons/fa';
import DataAssessment from './DataAssessment';
import DataQuality from './DataQuality';
import DataTransformation from './DataTransformation';
import DataOrchestration from './DataOrchestration';
import Monitoring from './Monitoring';

export default function Sidebar() {
  const [isCollapsed, setIsCollapsed] = useState(false);

  return (
    <>
      {/* Mobile Toggle Button */}
      <motion.button
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
        onClick={() => setIsCollapsed(!isCollapsed)}
        className="fixed top-4 left-4 z-50 lg:hidden p-3 bg-white rounded-lg shadow-lg text-slate-700"
      >
        {isCollapsed ? <FaBars /> : <FaTimes />}
      </motion.button>

      {/* Sidebar */}
      <motion.aside
        initial={{ x: 0 }}
        animate={{ x: isCollapsed ? -320 : 0 }}
        transition={{ duration: 0.3 }}
        className="fixed lg:static inset-y-0 left-0 z-40 w-80 bg-white border-r border-slate-200 overflow-y-auto"
      >
        <div className="p-6 space-y-8">
          {/* Header */}
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-xl font-bold bg-gradient-to-r from-indigo-600 to-purple-600 bg-clip-text text-transparent">
                Fluxo AI
              </h2>
              <p className="text-xs text-slate-500 mt-1">Data Transformation Suite</p>
            </div>
          </div>

          {/* Divider */}
          <div className="border-t border-slate-200" />

          {/* Section A: Data Assessment */}
          <DataAssessment />

          {/* Divider */}
          <div className="border-t border-slate-200" />

          {/* Section B: Data Quality */}
          <DataQuality />

          {/* Divider */}
          <div className="border-t border-slate-200" />

          {/* Section C: Data Transformation */}
          <DataTransformation />

          {/* Divider */}
          <div className="border-t border-slate-200" />

          {/* Section D: Data Orchestration */}
          <DataOrchestration />

          {/* Divider */}
          <div className="border-t border-slate-200" />

          {/* Section E: Monitoring */}
          <Monitoring />
        </div>
      </motion.aside>

      {/* Overlay for mobile */}
      {!isCollapsed && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={() => setIsCollapsed(true)}
          className="fixed inset-0 bg-black/50 z-30 lg:hidden"
        />
      )}
    </>
  );
}
