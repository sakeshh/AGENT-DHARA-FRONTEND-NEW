export interface Message {
  id: string;
  text: string;
  /**
   * Optional hidden payload sent to the API instead of `text`.
   * Used for deterministic UI button actions (no hardcoded replies).
   */
  apiContent?: string;
  sender: 'user' | 'bot';
  timestamp: Date;
}

export interface TransformationStep {
  id: string;
  type: string;
  parameters: Record<string, any>;
  order: number;
}

export interface DataSource {
  type: 'File' | 'Database' | 'Azure Blob' | 'API';
}

export interface TransformationType {
  value: string;
  label: string;
}

export const TRANSFORMATION_TYPES: TransformationType[] = [
  { value: 'rename', label: 'Rename Columns' },
  { value: 'datatype', label: 'Change Data Types' },
  { value: 'filter', label: 'Filter Rows' },
  { value: 'drop', label: 'Drop Columns' },
  { value: 'aggregate', label: 'Aggregate' },
  { value: 'join', label: 'Join/Merge' },
  { value: 'normalize', label: 'Normalize' },
  { value: 'custom', label: 'Custom Script' },
];

export const QUICK_PROMPTS = [
  'Clean null values',
  'Standardize date formats',
  'Join sales and customer tables',
  'Detect anomalies',
  'Aggregate monthly revenue',
];
