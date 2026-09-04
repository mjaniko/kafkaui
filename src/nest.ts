import { existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { AsyncApiDoc, AsyncApiOperation } from './asyncapi';
import { renderHtml } from './html';

/** Structural subset of INestApplication so @nestjs/* stay optional peers. */
interface HttpAdapterLike {
  get(path: string, handler: (req: unknown, res: unknown) => void): unknown;
  setHeader(res: unknown, name: string, value: string): unknown;
  reply(res: unknown, body: unknown, statusCode?: number): unknown;
}
interface NestAppLike {
  getHttpAdapter(): HttpAdapterLike;
  get<T = unknown>(token: unknown, options?: { strict?: boolean }): T;
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

const DEFAULT_KEY = '__kafka-topic-candidate';
// reflect-metadata is an optional peer; every NestJS app loads it, but the package must not require it.
const metadata = (key: string, target: object): unknown => {
  const R = Reflect as unknown as { getMetadata?: (k: string, t: object) => unknown };
  return R.getMetadata ? R.getMetadata(key, target) : undefined;
};
const NEST_PATTERN_KEY = 'microservices:pattern';

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
export class KafkaDocsModule {
  static setup(path: string, app: NestAppLike, documentOrOptions?: AsyncApiDoc | KafkaDocsOptions, options: KafkaDocsOptions = {}): AsyncApiDoc {
    const isDoc = (x: unknown): x is AsyncApiDoc => !!x && typeof x === 'object' && 'asyncapi' in (x as object);
    const opts: KafkaDocsOptions = isDoc(documentOrOptions) ? { ...options, document: documentOrOptions } : { ...(documentOrOptions ?? {}), ...options };
    const document = opts.document ?? loadDocument(opts.documentPath ?? join(process.cwd(), 'asyncapi.json'));
    const doc = enrich(app, document, opts);
    const adapter = app.getHttpAdapter();
    const html = renderHtml(doc);
    const json = JSON.stringify(doc);
    adapter.get(path, (_req, res) => {
      adapter.setHeader(res, 'Content-Type', 'text/html; charset=utf-8');
      adapter.reply(res, html, 200);
    });
    adapter.get(`${path}-json`, (_req, res) => {
      adapter.setHeader(res, 'Content-Type', 'application/json; charset=utf-8');
      adapter.reply(res, json, 200);
    });
    return doc;
  }

  /** Consumers bound in this process, from decorator metadata. */
  static discover(app: NestAppLike, options: KafkaDocsOptions = {}): DiscoveredConsumer[] {
    return discoverConsumers(app, options);
  }
}

function loadDocument(file: string): AsyncApiDoc | undefined {
  if (!existsSync(file)) return undefined;
  try {
    return JSON.parse(readFileSync(file, 'utf8')) as AsyncApiDoc;
  } catch {
    return undefined;
  }
}

export interface DiscoveredConsumer {
  envVar?: string;
  topics: string[];
  className: string;
  method: string;
}

function makeResolver(app: NestAppLike, options: KafkaDocsOptions): (v: string) => string | undefined {
  if (options.resolve) return options.resolve;
  try {
    const { ConfigService } = require('@nestjs/config');
    const cfg = app.get<{ get(k: string): unknown }>(ConfigService, { strict: false });
    return (v) => {
      const x = cfg.get(v);
      return typeof x === 'string' ? x : process.env[v];
    };
  } catch {
    return (v) => process.env[v];
  }
}

function discoverConsumers(app: NestAppLike, options: KafkaDocsOptions): DiscoveredConsumer[] {
  const key = options.topicMetadataKey ?? DEFAULT_KEY;
  const resolve = makeResolver(app, options);
  let container: Map<string, { controllers: Map<unknown, { instance?: object }>; providers: Map<unknown, { instance?: object }> }>;
  try {
    const { ModulesContainer } = require('@nestjs/core');
    container = app.get(ModulesContainer, { strict: false });
  } catch {
    return [];
  }
  const out: DiscoveredConsumer[] = [];
  const seen = new Set<string>();
  for (const mod of container.values()) {
    for (const wrappers of [mod.controllers, mod.providers]) {
      for (const w of wrappers.values()) {
        const inst = w.instance;
        if (!inst || typeof inst !== 'object') continue;
        const proto = Object.getPrototypeOf(inst);
        if (!proto || proto === Object.prototype) continue;
        for (const name of Object.getOwnPropertyNames(proto)) {
          if (name === 'constructor') continue;
          const fn = proto[name];
          if (typeof fn !== 'function') continue;
          const envVar = metadata(key, fn) as string | undefined;
          const pattern = metadata(NEST_PATTERN_KEY, fn);
          if (!envVar && !pattern) continue;
          const id = `${proto.constructor.name}.${name}`;
          if (seen.has(id)) continue;
          seen.add(id);
          const topics = new Set<string>();
          if (envVar) for (const t of (resolve(envVar) ?? '').split(',')) if (t.trim()) topics.add(t.trim());
          for (const p of Array.isArray(pattern) ? pattern : [pattern]) if (typeof p === 'string') topics.add(p);
          out.push({ envVar, topics: [...topics], className: proto.constructor.name, method: name });
        }
      }
    }
  }
  return out;
}

/** Re-resolve channel addresses from live config and add consumers found at runtime. */
function enrich(app: NestAppLike, document: AsyncApiDoc | undefined, options: KafkaDocsOptions): AsyncApiDoc {
  const resolve = makeResolver(app, options);
  const doc: AsyncApiDoc = document ? JSON.parse(JSON.stringify(document)) : {
    asyncapi: '3.0.0', info: { title: options.service ?? 'app', version: '0.0.0', description: 'Kafka contract discovered at runtime.' },
    channels: {}, operations: {}, components: { schemas: {}, messages: {} },
  };
  const service = options.service ?? doc.info.title;
  for (const ch of Object.values(doc.channels)) {
    const v = ch['x-env-var'];
    const m = v.match(/^([A-Z][A-Z0-9_]*)(.*)$/);
    if (!m) continue;
    const live = resolve(m[1]);
    if (live) ch.address = live + m[2];
  }
  const byVar = new Map<string, string>();
  for (const [cid, ch] of Object.entries(doc.channels)) byVar.set(ch['x-env-var'], cid);
  for (const c of discoverConsumers(app, options)) {
    const cid = (c.envVar && byVar.get(c.envVar)) ?? `runtime:${c.envVar ?? c.topics.join(',')}`;
    doc.channels[cid] ??= { address: c.topics.join(',') || null, messages: {}, 'x-env-var': c.envVar ?? '' };
    if (!doc.channels[cid].address && c.topics.length) doc.channels[cid].address = c.topics.join(',');
    const already = Object.values(doc.operations).some((o) => o.action === 'receive' && o['x-class'] === c.className && o['x-method'] === c.method);
    if (already) continue;
    const op: AsyncApiOperation = {
      action: 'receive', channel: { $ref: `#/channels/${cid}` }, 'x-service': service,
      'x-class': c.className, 'x-method': c.method, 'x-source': 'runtime', 'x-via': 'KafkaTopic',
    };
    doc.operations[`runtime.${c.className}.${c.method}`] = op;
  }
  return doc;
}
