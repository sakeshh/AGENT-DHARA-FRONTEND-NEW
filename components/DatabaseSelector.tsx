'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { FaDatabase, FaSearch, FaFolder, FaCloud, FaStream, FaPlug, FaArrowLeft } from 'react-icons/fa';

interface DatabaseSelectorProps {
  onSelect: (database: string) => void;
  onBack?: () => void;
}

/* Data source options matching the chat "Choose one" box */
const DATA_SOURCE_OPTIONS = [
  { id: 'local', text: 'Local data', icon: FaFolder },
  { id: 'sql', text: 'SQL data', icon: FaDatabase },
  { id: 'blob', text: 'Blob data', icon: FaCloud },
  { id: 'streams', text: 'Real-time streams', icon: FaStream },
  { id: 'apis', text: 'APIs', icon: FaPlug },
];

const SQL_DATABASES = [
  { id: 'postgres-prod', name: 'PostgreSQL Production', type: 'postgresql' },
  { id: 'mysql-analytics', name: 'MySQL Analytics', type: 'mysql' },
  { id: 'azure-sql-db', name: 'Azure SQL Database', type: 'azure-sql' },
  { id: 'mongodb-logs', name: 'MongoDB Logs', type: 'mongodb' },
  { id: 'mssql-sales', name: 'SQL Server Sales', type: 'mssql' },
  { id: 'oracle-erp', name: 'Oracle ERP', type: 'oracle' },
  { id: 'postgres-dev', name: 'PostgreSQL Development', type: 'postgresql' },
];

const LOCAL_OPTIONS = [
  { id: 'local-csv', name: 'CSV Files', type: 'csv' },
  { id: 'local-excel', name: 'Excel Files', type: 'xlsx' },
  { id: 'local-json', name: 'JSON Files', type: 'json' },
  { id: 'local-parquet', name: 'Parquet Files', type: 'parquet' },
  { id: 'local-other', name: 'Other', type: 'other' },
];

const BLOB_OPTIONS = [
  { id: 'blob-azure', name: 'Azure Blob Storage', type: 'azure' },
  { id: 'blob-s3', name: 'AWS S3', type: 's3' },
  { id: 'blob-gcs', name: 'Google Cloud Storage', type: 'gcs' },
];

const STREAMS_OPTIONS = [
  { id: 'streams-eventhubs', name: 'Azure Event Hubs', type: 'eventhubs' },
  { id: 'streams-kafka', name: 'Apache Kafka', type: 'kafka' },
  { id: 'streams-kinesis', name: 'AWS Kinesis', type: 'kinesis' },
];

const API_OPTIONS = [
  { id: 'api-rest', name: 'REST API', type: 'rest' },
  { id: 'api-graphql', name: 'GraphQL API', type: 'graphql' },
  { id: 'api-soap', name: 'SOAP API', type: 'soap' },
];

const getOptionsForDataSource = (sourceId: string | null) => {
  switch (sourceId) {
    case 'sql': return SQL_DATABASES;
    case 'local': return LOCAL_OPTIONS;
    case 'blob': return BLOB_OPTIONS;
    case 'streams': return STREAMS_OPTIONS;
    case 'apis': return API_OPTIONS;
    default: return [];
  }
};

export default function DatabaseSelector({ onSelect, onBack }: DatabaseSelectorProps) {
  const [selectedDataSource, setSelectedDataSource] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [loading, setLoading] = useState(true);

  const options = getOptionsForDataSource(selectedDataSource);
  const filteredOptions = options.filter(item =>
    item.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    item.type.toLowerCase().includes(searchTerm.toLowerCase())
  );

  useEffect(() => {
    setTimeout(() => setLoading(false), 600);
  }, []);

  useEffect(() => {
    if (selectedDataSource) {
      setSearchTerm('');
    }
  }, [selectedDataSource]);

  return (
    <div className="space-y-6">
      <div className="flex items-start gap-4">
        {onBack && (
          <motion.button
            onClick={onBack}
            className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-black/60 hover:text-black transition-colors shrink-0 order-first"
            whileHover={{ x: -2 }}
            whileTap={{ scale: 0.98 }}
          >
            <FaArrowLeft className="w-4 h-4" />
            Back
          </motion.button>
        )}
        <div className="flex-1">
          <h2 className="text-3xl font-bold text-zinc-900 mb-2">Select Data Source</h2>
          <p className="text-black/60">Choose where your data comes from, then select the specific source</p>
        </div>
      </div>

      {/* Choose one - Data source type options (matching chat choose box) */}
      <div>
        <p className="text-sm font-medium text-black/70 mb-3">Choose one:</p>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          {DATA_SOURCE_OPTIONS.map((option, idx) => {
            const Icon = option.icon;
            const isSelected = selectedDataSource === option.id;
            return (
              <motion.button
                key={option.id}
                onClick={() => setSelectedDataSource(option.id)}
                className={`flex items-center gap-3 p-4 text-sm font-medium text-left rounded-xl border transition-all ${
                  isSelected
                    ? 'bg-[#0070AD]/15 border-[#0070AD]/50 text-zinc-900'
                    : 'bg-white/85 border-black/10 text-black/70 hover:border-[#0070AD]/30 hover:bg-white'
                }`}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: idx * 0.05 }}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                <Icon className="text-xl text-[#0070AD]/80 flex-shrink-0" />
                <span>{option.text}</span>
              </motion.button>
            );
          })}
        </div>
      </div>

      {/* Back button when a data source is selected (step 2) */}
      {selectedDataSource && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="flex items-center gap-4"
        >
          <motion.button
            onClick={() => setSelectedDataSource(null)}
            className="flex items-center gap-2 text-sm font-medium text-black/60 hover:text-black transition-colors"
            whileHover={{ x: -2 }}
            whileTap={{ scale: 0.98 }}
          >
            <FaArrowLeft className="w-4 h-4" />
            Back to data source type
          </motion.button>
          {onBack && (
            <motion.button
              onClick={onBack}
              className="flex items-center gap-2 text-sm text-black/60 hover:text-black transition-colors"
            >
              ← Back
            </motion.button>
          )}
        </motion.div>
      )}

      {/* Options grid based on selected data source */}
      <AnimatePresence mode="wait">
        {selectedDataSource ? (
          <motion.div
            key={selectedDataSource}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="space-y-4"
          >
            <p className="text-sm font-medium text-black/70">
              Select {selectedDataSource === 'sql' ? 'database' : 'connection'}:
            </p>
            <div className="relative">
              <FaSearch className="absolute left-4 top-1/2 -translate-y-1/2 text-black/45" />
              <input
                type="text"
                placeholder={`Search ${selectedDataSource === 'sql' ? 'databases' : 'options'}...`}
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-12 pr-4 py-3 border border-black/10 rounded-xl focus:ring-2 focus:ring-[#0070AD]/25 focus:border-[#0070AD]/40 outline-none bg-white/90 text-zinc-900 placeholder-black/40"
              />
            </div>
            {loading ? (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {[1, 2, 3, 4, 5, 6].map((i) => (
                  <div key={i} className="h-32 bg-white/10 animate-pulse rounded-xl" />
                ))}
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredOptions.map((item, index) => (
                  <motion.button
                    key={item.id}
                    onClick={() => onSelect(item.id)}
                    className="p-6 bg-white/85 border border-black/10 rounded-xl hover:border-[#0070AD]/30 hover:bg-white transition-all text-left group"
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.05 }}
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                  >
                    <div className="flex items-start gap-4">
                      <div className="p-3 bg-black/5 rounded-lg group-hover:bg-[#0070AD]/10 transition-colors">
                        <FaDatabase className="text-2xl text-[#0070AD]/80" />
                      </div>
                      <div className="flex-1">
                        <h3 className="font-semibold text-zinc-900 mb-1">{item.name}</h3>
                        <p className="text-sm text-black/60 capitalize">{item.type}</p>
                      </div>
                    </div>
                  </motion.button>
                ))}
              </div>
            )}
            {filteredOptions.length === 0 && !loading && (
              <div className="text-center py-12">
                <FaDatabase className="text-6xl text-black/30 mx-auto mb-4" />
                <p className="text-black/60">No options found matching your search</p>
              </div>
            )}
          </motion.div>
        ) : (
          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="text-sm text-black/60"
          >
            Select a data source type above to see available options.
          </motion.p>
        )}
      </AnimatePresence>
    </div>
  );
}
