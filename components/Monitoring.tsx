'use client';

import { motion } from 'framer-motion';
import { FaCheckCircle, FaExclamationTriangle, FaTachometerAlt, FaClock } from 'react-icons/fa';

export default function Monitoring() {
  const performanceMetrics = [
    { label: 'Throughput', value: '1.2K/s', icon: FaTachometerAlt, color: 'text-blue-600' },
    { label: 'Latency', value: '45ms', icon: FaClock, color: 'text-purple-600' },
    { label: 'Success Rate', value: '99.8%', icon: FaCheckCircle, color: 'text-green-600' },
  ];

  const alerts = [
    { id: 1, type: 'warning', message: 'High memory usage detected', time: '2m ago' },
    { id: 2, type: 'info', message: 'Pipeline completed successfully', time: '15m ago' },
  ];

  const logs = [
    { id: 1, timestamp: '14:23:45', status: 'success', message: 'Data validation passed' },
    { id: 2, timestamp: '14:22:30', status: 'success', message: 'Transformation applied' },
    { id: 3, timestamp: '14:21:15', status: 'info', message: 'Pipeline started' },
  ];

  return (
    <motion.div
      className="space-y-4"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, delay: 0.4 }}
    >
      <h3 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">
        Monitoring & Optimization
      </h3>

      {/* Pipeline Health */}
      <div className="p-3 bg-gradient-to-r from-green-50 to-teal-50 rounded-lg border border-green-200">
        <div className="flex items-center gap-2 mb-1">
          <FaCheckCircle className="text-green-600" />
          <span className="text-xs font-semibold text-green-700">Pipeline Health</span>
        </div>
        <p className="text-lg font-bold text-green-800">Healthy</p>
      </div>

      {/* Performance Metrics */}
      <div>
        <label className="block text-xs font-medium text-slate-600 mb-2">
          Performance Metrics
        </label>
        <div className="space-y-2">
          {performanceMetrics.map((metric) => {
            const Icon = metric.icon;
            return (
              <div
                key={metric.label}
                className="flex items-center justify-between p-2 bg-white rounded-lg border border-slate-200"
              >
                <div className="flex items-center gap-2">
                  <Icon className={`${metric.color} text-sm`} />
                  <span className="text-xs font-medium text-slate-700">{metric.label}</span>
                </div>
                <span className="text-xs font-bold text-slate-900">{metric.value}</span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Alerts Section */}
      <div>
        <label className="block text-xs font-medium text-slate-600 mb-2">
          Recent Alerts
        </label>
        <div className="space-y-2 max-h-32 overflow-y-auto">
          {alerts.map((alert) => (
            <div
              key={alert.id}
              className={`p-2 rounded-lg border text-xs ${
                alert.type === 'warning'
                  ? 'bg-orange-50 border-orange-200'
                  : 'bg-blue-50 border-blue-200'
              }`}
            >
              <div className="flex items-start gap-2">
                {alert.type === 'warning' ? (
                  <FaExclamationTriangle className="text-orange-600 mt-0.5 flex-shrink-0" />
                ) : (
                  <FaCheckCircle className="text-blue-600 mt-0.5 flex-shrink-0" />
                )}
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-slate-700 truncate">{alert.message}</p>
                  <p className="text-slate-500 text-[10px]">{alert.time}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Historical Logs */}
      <div>
        <label className="block text-xs font-medium text-slate-600 mb-2">
          Historical Logs
        </label>
        <div className="bg-white rounded-lg border border-slate-200 overflow-hidden">
          <div className="max-h-40 overflow-y-auto">
            <table className="w-full text-xs">
              <thead className="bg-slate-50 sticky top-0">
                <tr>
                  <th className="px-2 py-1.5 text-left font-semibold text-slate-600">Time</th>
                  <th className="px-2 py-1.5 text-left font-semibold text-slate-600">Status</th>
                  <th className="px-2 py-1.5 text-left font-semibold text-slate-600">Message</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((log) => (
                  <tr key={log.id} className="border-t border-slate-100 hover:bg-slate-50">
                    <td className="px-2 py-1.5 text-slate-600 whitespace-nowrap">
                      {log.timestamp}
                    </td>
                    <td className="px-2 py-1.5">
                      <span
                        className={`inline-block px-2 py-0.5 rounded-full text-[10px] font-medium ${
                          log.status === 'success'
                            ? 'bg-green-100 text-green-700'
                            : 'bg-blue-100 text-blue-700'
                        }`}
                      >
                        {log.status}
                      </span>
                    </td>
                    <td className="px-2 py-1.5 text-slate-700">{log.message}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </motion.div>
  );
}
