import { JsonSchema, Operation, ServiceDoc } from './types';

export interface AsyncApiDoc {
  asyncapi: '3.0.0';
  info: { title: string; version: string; description?: string };
  channels: Record<string, AsyncApiChannel>;
  operations: Record<string, AsyncApiOperation>;
  components: { schemas: Record<string, JsonSchema>; messages: Record<string, { name: string; payload: JsonSchema }> };
  'x-generated-at'?: string;
}

export interface AsyncApiChannel {
  address: string | null;
  messages: Record<string, { $ref: string }>;
  'x-env-var': string;
  'x-services'?: string[];
}

export interface AsyncApiOperation {
  action: 'send' | 'receive';
  channel: { $ref: string };
  messages?: { $ref: string }[];
  'x-service': string;
  'x-class': string;
  'x-method': string;
  'x-source': string;
  'x-via': Operation['via'];
  'x-key'?: string;
}

const channelId = (o: Operation) => (o.envVar + (o.suffix ?? '')).replace(/[^A-Za-z0-9_.:-]/g, '_');

/** One service's contract as an AsyncAPI 3 document. */
export function toAsyncApi(doc: ServiceDoc): AsyncApiDoc {
  const out: AsyncApiDoc = {
    asyncapi: '3.0.0',
    info: { title: doc.service, version: doc.version ?? '0.0.0', description: `Kafka contract of ${doc.service}, generated from source.` },
    channels: {},
    operations: {},
    components: { schemas: { ...doc.schemas }, messages: {} },
  };
  for (const op of doc.operations) {
    const cid = channelId(op);
    const ch = (out.channels[cid] ??= { address: op.address ?? null, messages: {}, 'x-env-var': op.envVar + (op.suffix ?? '') });
    let msgRef: { $ref: string } | undefined;
    if (op.schemaName) {
      const mid = op.schemaName.replace(/[^A-Za-z0-9_.]/g, '_');
      out.components.messages[mid] ??= { name: op.schemaName, payload: { $ref: `#/components/schemas/${op.schemaName}` } };
      ch.messages[mid] = { $ref: `#/components/messages/${mid}` };
      msgRef = { $ref: `#/channels/${cid}/messages/${mid}` };
    }
    const oid = `${op.action}.${op.className}.${op.method}.${op.line}`;
    out.operations[oid] = {
      action: op.action, channel: { $ref: `#/channels/${cid}` }, messages: msgRef ? [msgRef] : undefined,
      'x-service': doc.service, 'x-class': op.className, 'x-method': op.method, 'x-source': `${op.file}:${op.line}`, 'x-via': op.via, 'x-key': op.key,
    };
  }
  return out;
}

/**
 * Merges per-service documents into one platform document.
 * Channels are joined by resolved address when known, else by env var, so
 * producer and consumer meet even when they name the variable differently.
 */
export function mergeAsyncApi(docs: AsyncApiDoc[], title = 'Promofy event bus'): AsyncApiDoc {
  const out: AsyncApiDoc = {
    asyncapi: '3.0.0', info: { title, version: docs.map((d) => `${d.info.title}@${d.info.version}`).join(', ') },
    channels: {}, operations: {}, components: { schemas: {}, messages: {} },
  };
  // A service that never declares a name for a var still meets the one that does:
  // first pass collects env var → address across all docs.
  const known: Record<string, string> = {};
  for (const d of docs) for (const ch of Object.values(d.channels)) if (ch.address && !known[ch['x-env-var']]) known[ch['x-env-var']] = ch.address;
  for (const d of docs) {
    const svc = d.info.title;
    const prefix = (n: string) => `${svc}.${n}`;
    for (const [n, s] of Object.entries(d.components.schemas)) out.components.schemas[prefix(n)] = rewriteRefs(s, svc) as JsonSchema;
    for (const [n, m] of Object.entries(d.components.messages)) out.components.messages[prefix(n)] = { name: m.name, payload: rewriteRefs(m.payload, svc) as JsonSchema };
    const chanMap: Record<string, string> = {};
    for (const [cid, ch] of Object.entries(d.channels)) {
      const address = ch.address ?? known[ch['x-env-var']] ?? null;
      const key = address ? `topic:${address}` : `env:${ch['x-env-var']}`;
      chanMap[cid] = key;
      const target = (out.channels[key] ??= { address, messages: {}, 'x-env-var': ch['x-env-var'], 'x-services': [] });
      if (!target['x-env-var'].split(',').includes(ch['x-env-var'])) target['x-env-var'] += ',' + ch['x-env-var'];
      if (!target['x-services']!.includes(svc)) target['x-services']!.push(svc);
      for (const mid of Object.keys(ch.messages)) target.messages[prefix(mid)] = { $ref: `#/components/messages/${prefix(mid)}` };
    }
    for (const [oid, op] of Object.entries(d.operations)) {
      const cid = op.channel.$ref.split('/').pop()!;
      const key = chanMap[cid];
      out.operations[prefix(oid)] = {
        ...op, channel: { $ref: `#/channels/${key}` },
        messages: op.messages?.map((m) => ({ $ref: `#/channels/${key}/messages/${prefix(m.$ref.split('/').pop()!)}` })),
      };
    }
  }
  return out;
}

function rewriteRefs(s: unknown, svc: string): unknown {
  if (Array.isArray(s)) return s.map((x) => rewriteRefs(x, svc));
  if (s && typeof s === 'object') {
    const o: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(s as Record<string, unknown>)) {
      o[k] = k === '$ref' && typeof v === 'string' && v.startsWith('#/components/schemas/') ? `#/components/schemas/${svc}.${v.slice('#/components/schemas/'.length)}` : rewriteRefs(v, svc);
    }
    return o;
  }
  return s;
}
