'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { FaFileAlt, FaCheckCircle, FaSearch, FaTable, FaDatabase } from 'react-icons/fa';

interface FileSelectorProps {
  database: string;
  onSelect: (files: string[]) => void;
  onNext: () => void;
  selectedFiles: string[];
}

const mockFiles: Record<string, Array<{ name: string; size: string; rows: number; type: string }>> = {
  /* SQL databases */
  'postgres-prod': [
    { name: 'customers', size: '2.4 MB', rows: 15420, type: 'table' },
    { name: 'orders', size: '8.7 MB', rows: 45230, type: 'table' },
    { name: 'products', size: '1.2 MB', rows: 8930, type: 'table' },
    { name: 'transactions', size: '15.3 MB', rows: 102340, type: 'table' },
    { name: 'user_activity', size: '5.6 MB', rows: 32100, type: 'table' },
    { name: 'inventory', size: '3.1 MB', rows: 12450, type: 'table' },
  ],
  'mysql-analytics': [
    { name: 'sales_data', size: '12.4 MB', rows: 67890, type: 'table' },
    { name: 'customer_demographics', size: '4.2 MB', rows: 23450, type: 'table' },
    { name: 'web_analytics', size: '9.8 MB', rows: 54320, type: 'table' },
  ],
  'mongodb-logs': [
    { name: 'application_logs', size: '25.6 MB', rows: 125430, type: 'collection' },
    { name: 'error_logs', size: '8.3 MB', rows: 34210, type: 'collection' },
    { name: 'access_logs', size: '18.9 MB', rows: 89340, type: 'collection' },
  ],
  'mssql-sales': [
    { name: 'sales_transactions', size: '14.7 MB', rows: 72340, type: 'table' },
    { name: 'revenue_reports', size: '6.2 MB', rows: 28910, type: 'table' },
    { name: 'customer_orders', size: '10.4 MB', rows: 56780, type: 'table' },
  ],
  'oracle-erp': [
    { name: 'employee_records', size: '7.8 MB', rows: 34560, type: 'table' },
    { name: 'payroll_data', size: '5.4 MB', rows: 23450, type: 'table' },
    { name: 'department_budget', size: '3.2 MB', rows: 12340, type: 'table' },
  ],
  'postgres-dev': [
    { name: 'test_data', size: '1.8 MB', rows: 8920, type: 'table' },
    { name: 'dev_users', size: '0.9 MB', rows: 4560, type: 'table' },
  ],
  'azure-sql-db': [
    { name: 'sales_db', size: '12.4 MB', rows: 85000, type: 'table' },
    { name: 'customer_data', size: '5.2 MB', rows: 32000, type: 'table' },
    { name: 'inventory', size: '2.1 MB', rows: 15000, type: 'table' },
  ],
  /* Local data */
  'local-csv': [
    { name: 'sales_2024.csv', size: '4.2 MB', rows: 52000, type: 'file' },
    { name: 'customers.csv', size: '1.1 MB', rows: 15000, type: 'file' },
    { name: 'products.csv', size: '0.3 MB', rows: 2500, type: 'file' },
  ],
  'local-excel': [
    { name: 'reports.xlsx', size: '8.5 MB', rows: 45000, type: 'file' },
    { name: 'quarterly_data.xlsx', size: '2.1 MB', rows: 12000, type: 'file' },
  ],
  'local-json': [
    { name: 'config.json', size: '0.5 MB', rows: 1, type: 'file' },
    { name: 'events.json', size: '15.2 MB', rows: 89000, type: 'file' },
  ],
  'local-parquet': [
    { name: 'analytics.parquet', size: '12.4 MB', rows: 250000, type: 'file' },
    { name: 'logs.parquet', size: '45.1 MB', rows: 500000, type: 'file' },
  ],
  'local-other': [
    { name: 'custom_export.dat', size: '3.2 MB', rows: 0, type: 'file' },
    { name: 'legacy_data.txt', size: '1.5 MB', rows: 0, type: 'file' },
  ],
  /* Blob storage */
  'blob-azure': [
    { name: 'data/raw/source1', size: '32.1 MB', rows: 125000, type: 'blob' },
    { name: 'data/raw/source2', size: '18.5 MB', rows: 78000, type: 'blob' },
  ],
  'blob-s3': [
    { name: 'bucket/input/data.csv', size: '22.4 MB', rows: 95000, type: 'object' },
    { name: 'bucket/archive/logs.parquet', size: '56.2 MB', rows: 320000, type: 'object' },
  ],
  'blob-gcs': [
    { name: 'gs://bucket/data/export.csv', size: '14.8 MB', rows: 67000, type: 'object' },
  ],
  /* Real-time streams */
  'streams-eventhubs': [
    { name: 'telemetry-hub', size: 'streaming', rows: 0, type: 'stream' },
    { name: 'events-hub', size: 'streaming', rows: 0, type: 'stream' },
  ],
  'streams-kafka': [
    { name: 'orders-topic', size: 'streaming', rows: 0, type: 'topic' },
    { name: 'analytics-topic', size: 'streaming', rows: 0, type: 'topic' },
  ],
  'streams-kinesis': [
    { name: 'click-stream', size: 'streaming', rows: 0, type: 'stream' },
  ],
  /* APIs */
  'api-rest': [
    { name: 'users endpoint', size: 'API', rows: 0, type: 'endpoint' },
    { name: 'orders endpoint', size: 'API', rows: 0, type: 'endpoint' },
  ],
  'api-graphql': [
    { name: 'graphql schema', size: 'API', rows: 0, type: 'endpoint' },
  ],
  'api-soap': [
    { name: 'soap service', size: 'API', rows: 0, type: 'endpoint' },
  ],
};

export default function FileSelector({ database, onSelect, onNext, selectedFiles }: FileSelectorProps) {
  const [files, setFiles] = useState<typeof mockFiles[string]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<string[]>(selectedFiles);

  useEffect(() => {
    setTimeout(() => {
      setFiles(mockFiles[database] || []);
      setLoading(false);
    }, 600);
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
    onSelect(selected);
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
