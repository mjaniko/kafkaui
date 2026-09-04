"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaDocsModule = void 0;
const html_1 = require("./html");
const DEFAULT_KEY = '__kafka-topic-candidate';
// reflect-metadata is an optional peer; every NestJS app loads it, but the package must not require it.
const metadata = (key, target) => {
    const R = Reflect;
    return R.getMetadata ? R.getMetadata(key, target) : undefined;
};
const NEST_PATTERN_KEY = 'microservices:pattern';
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
class KafkaDocsModule {
    static setup(path, app, document, options = {}) {
        const doc = enrich(app, document, options);
        const adapter = app.getHttpAdapter();
        const html = (0, html_1.renderHtml)(doc);
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
    static discover(app, options = {}) {
        return discoverConsumers(app, options);
    }
}
exports.KafkaDocsModule = KafkaDocsModule;
function makeResolver(app, options) {
    if (options.resolve)
        return options.resolve;
    try {
        const { ConfigService } = require('@nestjs/config');
        const cfg = app.get(ConfigService, { strict: false });
        return (v) => {
            const x = cfg.get(v);
            return typeof x === 'string' ? x : process.env[v];
        };
    }
    catch {
        return (v) => process.env[v];
    }
}
function discoverConsumers(app, options) {
    const key = options.topicMetadataKey ?? DEFAULT_KEY;
    const resolve = makeResolver(app, options);
    let container;
    try {
        const { ModulesContainer } = require('@nestjs/core');
        container = app.get(ModulesContainer, { strict: false });
    }
    catch {
        return [];
    }
    const out = [];
    const seen = new Set();
    for (const mod of container.values()) {
        for (const wrappers of [mod.controllers, mod.providers]) {
            for (const w of wrappers.values()) {
                const inst = w.instance;
                if (!inst || typeof inst !== 'object')
                    continue;
                const proto = Object.getPrototypeOf(inst);
                if (!proto || proto === Object.prototype)
                    continue;
                for (const name of Object.getOwnPropertyNames(proto)) {
                    if (name === 'constructor')
                        continue;
                    const fn = proto[name];
                    if (typeof fn !== 'function')
                        continue;
                    const envVar = metadata(key, fn);
                    const pattern = metadata(NEST_PATTERN_KEY, fn);
                    if (!envVar && !pattern)
                        continue;
                    const id = `${proto.constructor.name}.${name}`;
                    if (seen.has(id))
                        continue;
                    seen.add(id);
                    const topics = new Set();
                    if (envVar)
                        for (const t of (resolve(envVar) ?? '').split(','))
                            if (t.trim())
                                topics.add(t.trim());
                    for (const p of Array.isArray(pattern) ? pattern : [pattern])
                        if (typeof p === 'string')
                            topics.add(p);
                    out.push({ envVar, topics: [...topics], className: proto.constructor.name, method: name });
                }
            }
        }
    }
    return out;
}
/** Re-resolve channel addresses from live config and add consumers found at runtime. */
function enrich(app, document, options) {
    var _a;
    const resolve = makeResolver(app, options);
    const doc = document ? JSON.parse(JSON.stringify(document)) : {
        asyncapi: '3.0.0', info: { title: options.service ?? 'app', version: '0.0.0', description: 'Kafka contract discovered at runtime.' },
        channels: {}, operations: {}, components: { schemas: {}, messages: {} }, 'x-generated-at': new Date().toISOString(),
    };
    const service = options.service ?? doc.info.title;
    for (const ch of Object.values(doc.channels)) {
        const v = ch['x-env-var'];
        const m = v.match(/^([A-Z][A-Z0-9_]*)(.*)$/);
        if (!m)
            continue;
        const live = resolve(m[1]);
        if (live)
            ch.address = live + m[2];
    }
    const byVar = new Map();
    for (const [cid, ch] of Object.entries(doc.channels))
        byVar.set(ch['x-env-var'], cid);
    for (const c of discoverConsumers(app, options)) {
        const cid = (c.envVar && byVar.get(c.envVar)) ?? `runtime:${c.envVar ?? c.topics.join(',')}`;
        (_a = doc.channels)[cid] ?? (_a[cid] = { address: c.topics.join(',') || null, messages: {}, 'x-env-var': c.envVar ?? '' });
        if (!doc.channels[cid].address && c.topics.length)
            doc.channels[cid].address = c.topics.join(',');
        const already = Object.values(doc.operations).some((o) => o.action === 'receive' && o['x-class'] === c.className && o['x-method'] === c.method);
        if (already)
            continue;
        const op = {
            action: 'receive', channel: { $ref: `#/channels/${cid}` }, 'x-service': service,
            'x-class': c.className, 'x-method': c.method, 'x-source': 'runtime', 'x-via': 'KafkaTopic',
        };
        doc.operations[`runtime.${c.className}.${c.method}`] = op;
    }
    doc['x-generated-at'] = new Date().toISOString();
    return doc;
}
