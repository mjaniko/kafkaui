import { ScanOptions, ServiceDoc } from './types';
/** Scans one NestJS project with the TypeScript checker and returns its Kafka contract. */
export declare function scanProject(opts: ScanOptions): ServiceDoc;
