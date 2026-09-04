import { JsonSchema, Operation, ServiceDoc } from './types';
export interface AsyncApiDoc {
    asyncapi: '3.0.0';
    info: {
        title: string;
        version: string;
        description?: string;
    };
    channels: Record<string, AsyncApiChannel>;
    operations: Record<string, AsyncApiOperation>;
    components: {
        schemas: Record<string, JsonSchema>;
        messages: Record<string, {
            name: string;
            payload: JsonSchema;
        }>;
    };
    'x-generated-at': string;
}
export interface AsyncApiChannel {
    address: string | null;
    messages: Record<string, {
        $ref: string;
    }>;
    'x-env-var': string;
    'x-services'?: string[];
}
export interface AsyncApiOperation {
    action: 'send' | 'receive';
    channel: {
        $ref: string;
    };
    messages?: {
        $ref: string;
    }[];
    'x-service': string;
    'x-class': string;
    'x-method': string;
    'x-source': string;
    'x-via': Operation['via'];
    'x-key'?: string;
}
/** One service's contract as an AsyncAPI 3 document. */
export declare function toAsyncApi(doc: ServiceDoc): AsyncApiDoc;
/**
 * Merges per-service documents into one platform document.
 * Channels are joined by resolved address when known, else by env var, so
 * producer and consumer meet even when they name the variable differently.
 */
export declare function mergeAsyncApi(docs: AsyncApiDoc[], title?: string): AsyncApiDoc;
