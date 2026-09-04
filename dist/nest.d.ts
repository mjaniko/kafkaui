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
 *   GET <path>       → HTML viewer
 *   GET <path>-json  → AsyncAPI JSON
 *
 * Consumers are discovered from the decorators on the live controllers
 * (`@KafkaTopic`, `@KafkaCron`, or the MessagePattern they were rewritten
 * into), so the served document always lists the topics this pod is bound to,
 * with names resolved from the live config. The compile-time document from
 * `kafka-docs generate` adds producers and payload schemas; every channel in
 * it is re-resolved against the live config too.
 */
export declare class KafkaDocsModule {
    static setup(path: string, app: NestAppLike, document?: AsyncApiDoc, options?: KafkaDocsOptions): AsyncApiDoc;
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
