export type JsonSchema = Record<string, unknown>;

export interface Operation {
  /** send = producer, receive = consumer */
  action: 'send' | 'receive';
  /** Env var holding the topic name, e.g. TOPIC_AWARD_INITIATE */
  envVar: string;
  /** Literal suffix appended at runtime, e.g. ".dlq" */
  suffix?: string;
  /** Resolved topic name when an env file was given */
  address?: string;
  className: string;
  method: string;
  file: string;
  line: number;
  /** How the binding was found */
  via: 'KafkaTopic' | 'KafkaCron' | 'registerHandler' | 'emit' | 'emitToKafka';
  /** Message key expression, producers only */
  key?: string;
  /** Name of the payload schema in components */
  schemaName?: string;
}

export interface ServiceDoc {
  service: string;
  version?: string;
  operations: Operation[];
  schemas: Record<string, JsonSchema>;
  /** env var -> topic, from --env-file */
  env: Record<string, string>;
  generatedAt: string;
}

export interface ScanOptions {
  project: string;
  tsconfig?: string;
  service: string;
  version?: string;
  /** Path to a .env / helm values / JSON file; undefined = auto-detect .env.sample in the project; null = none */
  envFile?: string | null;
  /** Regex for files to skip (default: spec/test/migrations) */
  exclude?: RegExp;
  log?: (msg: string) => void;
}
