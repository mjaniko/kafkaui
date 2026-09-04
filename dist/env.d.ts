/** Reads KEY=value (.env), flat YAML (`  KEY: value`, helm values) or JSON into a map. */
export declare function loadEnvFile(path: string): Record<string, string>;
