'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { FaChartBar, FaExclamationTriangle, FaCheckCircle, FaThumbsUp, FaThumbsDown } from 'react-icons/fa';

interface DataAssessmentReportProps {
  files: string[];
  database: string;
  onComplete: (data: any) => void;
  onFeedback: (liked: boolean, comment?: string) => void;
}

interface AssessmentResult {
  fileName: string;
  totalRows: number;
  totalColumns: number;
  missingValues: number;
  duplicates: number;
  dataQualityScore: number;
  issues: Array<{ type: string; severity: string; description: string }>;
  columnStats: Array<{ name: string; type: string; nullCount: number; uniqueCount: number }>;
}

export default function DataAssessmentReport({ files, database, onComplete, onFeedback }: DataAssessmentReportProps) {
  const [assessing, setAssessing] = useState(true);
  const [progress, setProgress] = useState(0);
  const [results, setResults] = useState<AssessmentResult[]>([]);
  const [showFeedback, setShowFeedback] = useState(false);

  useEffect(() => {
    simulateAssessment();
  }, []);

  const simulateAssessment = async () => {
    for (let i = 0; i <= 100; i += 5) {
      await new Promise(resolve => setTimeout(resolve, 100));
      setProgress(i);
    }

    const mockResults: AssessmentResult[] = files.map((file, idx) => ({
      fileName: file,
      totalRows: Math.floor(Math.random() * 100000) + 10000,
      totalColumns: Math.floor(Math.random() * 30) + 5,
      missingValues: Math.floor(Math.random() * 1000),
      duplicates: Math.floor(Math.random() * 500),
      dataQualityScore: Math.floor(Math.random() * 30) + 70,
      issues: [
        { type: 'Missing Values', severity: 'medium', description: `${Math.floor(Math.random() * 10) + 5}% of values are missing` },
        { type: 'Duplicates', severity: 'low', description: `Found ${Math.floor(Math.random() * 500)} duplicate records` },
        { type: 'Data Type Mismatch', severity: 'high', description: 'Some columns have inconsistent data types' },
      ],
      columnStats: Array.from({ length: 8 }, (_, i) => ({
        name: `column_${i + 1}`,
        type: ['string', 'integer', 'float', 'date'][Math.floor(Math.random() * 4)],
        nullCount: Math.floor(Math.random() * 100),
        uniqueCount: Math.floor(Math.random() * 1000),
      })),
    }));

    setResults(mockResults);
    setAssessing(false);
    onComplete(mockResults);
  };

  const handleLike = () => {
    onFeedback(true);
    setShowFeedback(false);
  };

  const handleDislike = () => {
    const comment = prompt('What would you like us to improve?');
    onFeedback(false, comment || undefined);
    setShowFeedback(false);
    
    setTimeout(() => {
      alert('Thank you for your feedback! Re-assessing data with improvements...');
      setAssessing(true);
      setProgress(0);
      simulateAssessment();
    }, 500);
  };

  if (assessing) {
    return (
      <div className="space-y-6">
        <div className="text-center">
          <h2 className="text-3xl font-bold text-zinc-900 mb-2">Analyzing Data</h2>
          <p className="text-black/60">Please wait while we assess your data quality</p>
        </div>

        <div className="space-y-4">
          <div className="relative h-4 bg-black/10 rounded-full overflow-hidden">
            <motion.div
              className="h-full bg-gradient-to-r from-[#0070AD] to-[#12ABDB]"
              initial={{ width: 0 }}
              animate={{ width: `${progress}%` }}
              transition={{ duration: 0.3 }}
            />
          </div>
          <div className="text-center text-2xl font-bold text-[#0070AD]">{progress}%</div>
        </div>

        <div className="grid grid-cols-3 gap-4">
          {['Scanning Data', 'Detecting Duplicates', 'Analyzing Quality'].map((task, idx) => (
            <motion.div
              key={task}
              className="p-4 bg-white/90 border border-black/10 rounded-lg text-center"
              initial={{ opacity: 0.3 }}
              animate={{ opacity: progress > idx * 30 ? 1 : 0.3 }}
            >
              <div className="text-sm font-medium text-black/80">{task}</div>
            </motion.div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-zinc-900 mb-2">Assessment Complete</h2>
          <p className="text-black/60">Review the data quality analysis below</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleLike}
            className="p-3 bg-[#0070AD]/10 text-[#0070AD] rounded-lg hover:bg-[#0070AD]/15 transition-colors"
            title="Like this assessment"
          >
            <FaThumbsUp className="text-xl" />
          </button>
          <button
            onClick={handleDislike}
            className="p-3 bg-red-500/20 text-red-400 rounded-lg hover:bg-red-500/30 transition-colors"
            title="Dislike this assessment"
          >
            <FaThumbsDown className="text-xl" />
          </button>
        </div>
      </div>

      {/* Results */}
      <div className="space-y-4">
        {results.map((result, idx) => (
          <motion.div
            key={result.fileName}
            className="border border-black/10 rounded-lg p-6 bg-white/90"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: idx * 0.1 }}
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-xl font-bold text-zinc-900">{result.fileName}</h3>
              <div className="flex items-center gap-2">
                <div className={`text-2xl font-bold ${
                  result.dataQualityScore >= 80 ? 'text-[#0070AD]' :
                  result.dataQualityScore >= 60 ? 'text-amber-400' : 'text-red-400'
                }`}>
                  {result.dataQualityScore}%
                </div>
                <div className="text-sm text-black/60">Quality Score</div>
              </div>
            </div>

            <div className="grid grid-cols-4 gap-4 mb-4">
              <div className="bg-white rounded-lg border border-black/10 p-4">
                <div className="text-sm text-black/60">Total Rows</div>
                <div className="text-2xl font-bold text-zinc-900">{result.totalRows.toLocaleString()}</div>
              </div>
              <div className="bg-white rounded-lg border border-black/10 p-4">
                <div className="text-sm text-black/60">Columns</div>
                <div className="text-2xl font-bold text-zinc-900">{result.totalColumns}</div>
              </div>
              <div className="bg-amber-500/20 p-4 rounded-lg">
                <div className="text-sm text-amber-400">Missing Values</div>
                <div className="text-2xl font-bold text-amber-400">{result.missingValues.toLocaleString()}</div>
              </div>
              <div className="bg-red-500/20 p-4 rounded-lg">
                <div className="text-sm text-red-400">Duplicates</div>
                <div className="text-2xl font-bold text-red-400">{result.duplicates.toLocaleString()}</div>
              </div>
            </div>

            <div>
              <h4 className="font-semibold text-zinc-900 mb-2">Issues Found</h4>
              <div className="space-y-2">
                {result.issues.map((issue, issueIdx) => (
                  <div key={issueIdx} className="flex items-start gap-3 p-3 bg-white rounded-lg border border-black/10">
                    <FaExclamationTriangle className={`text-lg mt-1 ${
                      issue.severity === 'high' ? 'text-red-500' :
                      issue.severity === 'medium' ? 'text-yellow-500' : 'text-blue-500'
                    }`} />
                    <div className="flex-1">
                      <div className="font-medium text-zinc-900">{issue.type}</div>
                      <div className="text-sm text-black/60">{issue.description}</div>
                    </div>
                    <span className={`text-xs px-2 py-1 rounded ${
                      issue.severity === 'high' ? 'bg-red-500/30 text-red-400' :
                      issue.severity === 'medium' ? 'bg-amber-500/30 text-amber-400' :
                      'bg-black/5 text-black/70'
                    }`}>
                      {issue.severity}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </motion.div>
        ))}
      </div>
    </div>
  );
}
