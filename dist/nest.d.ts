import { AsyncApiDoc } from './asyncapi';
/** Structural subset of INestApplication so @nestjs/* stay optional peers. */
interface HttpAdapterLike {
    get(path: string, handler: (req: unknown, res: unknown) => void): unknown;
    setHeader(res: unknown, name: string, value: string): unknown;
    reply(res: unknown, body: unknown, statusCode?: number): unknown;
}
interface NestAppLike {
    getHttpAdapter(): HttpAdapterLike;
    get<T = unknown>(token: unknown, options?: {
        strict?: boolean;
    }): T;
}
export interface KafkaDocsOptions {
    /** Generated document to serve. Default: `documentPath` read from disk if it exists. */
    document?: AsyncApiDoc;
    /** Where `kafkaui generate` wrote the document. Default: <cwd>/asyncapi.json */
    documentPath?: string;
    /** Metadata key the service's KafkaTopic decorator stores the env var under. */
    topicMetadataKey?: string;
    /** Resolve an env var to a topic name. Default: ConfigService if present, else process.env. */
    resolve?: (envVar: string) => string | undefined;
    /** Service name for operations discovered at runtime. Default: document title or 'app'. */
    service?: string;
}
/**
 * Swagger-style docs for the Kafka contract of a running NestJS app.
 *
 *   KafkaDocsModule.setup('/kafka-docs', app);
 *
 *   GET <path>       → HTML viewer
 *   GET <path>-json  → AsyncAPI JSON
 *
 * Loads `asyncapi.json` from the working directory (the file `kafkaui generate`
 * writes) when present, then adds the consumers discovered from the decorators
 * on the live controllers and resolves every topic name from the live config.
 * Works with both the Express and Fastify adapters.
 */
export declare class KafkaDocsModule {
    static setup(path: string, app: NestAppLike, documentOrOptions?: AsyncApiDoc | KafkaDocsOptions, options?: KafkaDocsOptions): AsyncApiDoc;
    /** Consumers bound in this process, from decorator metadata. */
    static discover(app: NestAppLike, options?: KafkaDocsOptions): DiscoveredConsumer[];
}
export interface DiscoveredConsumer {
    envVar?: string;
    topics: string[];
    className: string;
    method: string;
}
export {};
