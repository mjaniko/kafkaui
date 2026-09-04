#!/usr/bin/env node
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const asyncapi_1 = require("./asyncapi");
const html_1 = require("./html");
const scan_1 = require("./scan");
const USAGE = `kafkaui — AsyncAPI for the Kafka topics a NestJS service consumes and produces

  kafkaui generate --project <dir> --service <name> [--env-file <file> | --no-env] [--tsconfig tsconfig.json]
                      [--version <v>] [--out asyncapi.json] [--html kafka-docs.html]
  kafkaui merge <a.json> <b.json> ... [--title "Promofy event bus"] [--out platform.json] [--html platform.html]
  kafkaui html <asyncapi.json> [--out doc.html]

Topics are identified by the env var named in the decorator / emit call. Their names are taken from the
project's own .env.sample (or .env) by default; --env-file overrides with a helm values file for one environment.
At runtime KafkaDocsModule resolves them from the live ConfigService instead.`;
function arg(args, name) {
    const i = args.indexOf(name);
    return i >= 0 ? args[i + 1] : undefined;
}
function positional(args) {
    const out = [];
    for (let i = 0; i < args.length; i++) {
        if (args[i].startsWith('--')) {
            i++;
            continue;
        }
        out.push(args[i]);
    }
    return out;
}
function main() {
    const [cmd, ...rest] = process.argv.slice(2);
    const log = (m) => process.stderr.write(m + '\n');
    if (cmd === 'generate') {
        const project = arg(rest, '--project') ?? '.';
        const service = arg(rest, '--service') ?? path.basename(path.resolve(project));
        let version = arg(rest, '--version');
        if (!version) {
            try {
                version = JSON.parse(fs.readFileSync(path.join(project, 'package.json'), 'utf8')).version;
            }
            catch { /* optional */ }
        }
        const envFile = rest.includes('--no-env') ? null : arg(rest, '--env-file');
        const doc = (0, asyncapi_1.toAsyncApi)((0, scan_1.scanProject)({ project, service, version, envFile, tsconfig: arg(rest, '--tsconfig'), log }));
        const out = arg(rest, '--out') ?? 'asyncapi.json';
        fs.writeFileSync(out, JSON.stringify(doc, null, 1));
        log(`wrote ${out}`);
        const html = arg(rest, '--html');
        if (html) {
            fs.writeFileSync(html, (0, html_1.renderHtml)(doc));
            log(`wrote ${html}`);
        }
        return;
    }
    if (cmd === 'merge') {
        const files = positional(rest);
        const docs = files.map((f) => JSON.parse(fs.readFileSync(f, 'utf8')));
        const merged = (0, asyncapi_1.mergeAsyncApi)(docs, arg(rest, '--title'));
        const out = arg(rest, '--out') ?? 'platform.asyncapi.json';
        fs.writeFileSync(out, JSON.stringify(merged, null, 1));
        log(`merged ${files.length} docs → ${out} (${Object.keys(merged.channels).length} channels)`);
        const html = arg(rest, '--html');
        if (html) {
            fs.writeFileSync(html, (0, html_1.renderHtml)(merged));
            log(`wrote ${html}`);
        }
        return;
    }
    if (cmd === 'html') {
        const [file] = positional(rest);
        const doc = JSON.parse(fs.readFileSync(file, 'utf8'));
        const out = arg(rest, '--out') ?? file.replace(/\.json$/, '') + '.html';
        fs.writeFileSync(out, (0, html_1.renderHtml)(doc));
        log(`wrote ${out}`);
        return;
    }
    log(USAGE);
    process.exit(cmd ? 1 : 0);
}
main();
