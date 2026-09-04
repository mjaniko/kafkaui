#!/usr/bin/env node
import * as fs from 'fs';
import * as path from 'path';
import { AsyncApiDoc, mergeAsyncApi, toAsyncApi } from './asyncapi';
import { renderHtml } from './html';
import { scanProject } from './scan';

const USAGE = `kafkaui — AsyncAPI for the Kafka topics a NestJS service consumes and produces

  kafkaui generate --project <dir> --service <name> [--env-file <file> | --no-env] [--tsconfig tsconfig.json]
                      [--version <v>] [--out asyncapi.json] [--html kafka-docs.html]
  kafkaui merge <a.json> <b.json> ... [--title "Promofy event bus"] [--out platform.json] [--html platform.html]
  kafkaui html <asyncapi.json> [--out doc.html]

Topics are identified by the env var named in the decorator / emit call. Their names are taken from the
project's own .env.sample (or .env) by default; --env-file overrides with a helm values file for one environment.
At runtime KafkaDocsModule resolves them from the live ConfigService instead.`;

function arg(args: string[], name: string): string | undefined {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
}

function positional(args: string[]): string[] {
  const out: string[] = [];
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) { i++; continue; }
    out.push(args[i]);
  }
  return out;
}

function main(): void {
  const [cmd, ...rest] = process.argv.slice(2);
  const log = (m: string) => process.stderr.write(m + '\n');
  if (cmd === 'generate') {
    const project = arg(rest, '--project') ?? '.';
    const service = arg(rest, '--service') ?? path.basename(path.resolve(project));
    let version = arg(rest, '--version');
    if (!version) {
      try { version = JSON.parse(fs.readFileSync(path.join(project, 'package.json'), 'utf8')).version; } catch { /* optional */ }
    }
    const envFile = rest.includes('--no-env') ? null : arg(rest, '--env-file');
    const doc = toAsyncApi(scanProject({ project, service, version, envFile, tsconfig: arg(rest, '--tsconfig'), log }));
    const out = arg(rest, '--out') ?? 'asyncapi.json';
    fs.writeFileSync(out, JSON.stringify(doc, null, 1));
    log(`wrote ${out}`);
    const html = arg(rest, '--html');
    if (html) { fs.writeFileSync(html, renderHtml(doc)); log(`wrote ${html}`); }
    return;
  }
  if (cmd === 'merge') {
    const files = positional(rest);
    const docs = files.map((f) => JSON.parse(fs.readFileSync(f, 'utf8')) as AsyncApiDoc);
    const merged = mergeAsyncApi(docs, arg(rest, '--title'));
    const out = arg(rest, '--out') ?? 'platform.asyncapi.json';
    fs.writeFileSync(out, JSON.stringify(merged, null, 1));
    log(`merged ${files.length} docs → ${out} (${Object.keys(merged.channels).length} channels)`);
    const html = arg(rest, '--html');
    if (html) { fs.writeFileSync(html, renderHtml(merged)); log(`wrote ${html}`); }
    return;
  }
  if (cmd === 'html') {
    const [file] = positional(rest);
    const doc = JSON.parse(fs.readFileSync(file, 'utf8')) as AsyncApiDoc;
    const out = arg(rest, '--out') ?? file.replace(/\.json$/, '') + '.html';
    fs.writeFileSync(out, renderHtml(doc));
    log(`wrote ${out}`);
    return;
  }
  log(USAGE);
  process.exit(cmd ? 1 : 0);
}

main();
