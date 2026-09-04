import * as path from 'path';
import ts from 'typescript';
import { loadEnvFile } from './env';
import { SchemaBuilder } from './schema';
import { Operation, ScanOptions, ServiceDoc } from './types';

const ENV_VAR = /^[A-Z][A-Z0-9_]*$/;
const CONSUMER_DECORATORS = new Set(['KafkaTopic', 'KafkaCron']);
const DEFAULT_EXCLUDE = /(\.spec\.ts|\.test\.ts|\.d\.ts|[\\/]migrations[\\/]|[\\/]node_modules[\\/]|[\\/]test[\\/])/;

/** Scans one NestJS project with the TypeScript checker and returns its Kafka contract. */
export function scanProject(opts: ScanOptions): ServiceDoc {
  const log = opts.log ?? (() => undefined);
  const project = path.resolve(opts.project);
  const tsconfig = path.resolve(project, opts.tsconfig ?? 'tsconfig.json');
  const parsed = ts.getParsedCommandLineOfConfigFile(tsconfig, {}, {
    ...ts.sys,
    onUnRecoverableConfigFileDiagnostic: (d) => {
      throw new Error(ts.flattenDiagnosticMessageText(d.messageText, '\n'));
    },
  });
  if (!parsed) throw new Error(`cannot parse ${tsconfig}`);
  const exclude = opts.exclude ?? DEFAULT_EXCLUDE;
  const files = parsed.fileNames.filter((f) => !exclude.test(f));
  const program = ts.createProgram(files, { ...parsed.options, noEmit: true });
  const checker = program.getTypeChecker();
  const schemas: Record<string, Record<string, unknown>> = {};
  const builder = new SchemaBuilder(checker, schemas);
  // Topic identity in code is the env var; the name is whatever the repo declares for it
  // (.env.sample first, then a local .env), unless the caller points at another file.
  const envSource = opts.envFile ? path.resolve(opts.envFile)
    : opts.envFile === null ? undefined
    : ['.env.sample', '.env.example', '.env'].map((f) => path.join(project, f)).find((f) => ts.sys.fileExists(f));
  const env = envSource ? loadEnvFile(envSource) : {};
  if (envSource) log(`topic names from ${path.relative(process.cwd(), envSource)} (${Object.keys(env).filter((k) => /TOPIC/.test(k)).length} topic vars)`);
  const operations: Operation[] = [];
  // Producer sites whose topic is a runtime value (a method parameter, data-driven routing).
  // Not part of the static contract; reported on stderr so they are not forgotten.
  const skipped: string[] = [];

  for (const sf of program.getSourceFiles()) {
    if (sf.isDeclarationFile || !sf.fileName.startsWith(project) || exclude.test(sf.fileName)) continue;
    const rel = path.relative(project, sf.fileName);
    const lineOf = (n: ts.Node) => sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1;

    const visit = (node: ts.Node) => {
      if (ts.isClassDeclaration(node) && node.name) {
        const className = node.name.text;
        for (const member of node.members) {
          if (ts.isMethodDeclaration(member) && member.name) {
            const method = member.name.getText(sf);
            // ---- consumers via decorators
            for (const dec of ts.getDecorators(member) ?? []) {
              const call = dec.expression;
              if (!ts.isCallExpression(call) || !ts.isIdentifier(call.expression)) continue;
              const name = call.expression.text;
              if (!CONSUMER_DECORATORS.has(name)) continue;
              const arg = call.arguments[0];
              if (!arg || !ts.isStringLiteralLike(arg)) continue;
              const isVar = ENV_VAR.test(arg.text);
              operations.push({
                action: 'receive', envVar: isVar ? arg.text : `literal:${arg.text}`, className, method, file: rel, line: lineOf(dec),
                via: name as 'KafkaTopic' | 'KafkaCron', address: isVar ? env[arg.text] : arg.text,
                schemaName: paramSchema(member),
              });
            }
            // ---- consumers via registerHandler({ [config.get('X')]: this.fn.bind(this) })
            ts.forEachChild(member, function walk(n) {
              if (ts.isCallExpression(n) && ts.isPropertyAccessExpression(n.expression) && n.expression.name.text === 'registerHandler') {
                const obj = n.arguments[0];
                if (obj && ts.isObjectLiteralExpression(obj)) {
                  for (const p of obj.properties) {
                    if (!ts.isPropertyAssignment(p) || !ts.isComputedPropertyName(p.name)) continue;
                    const envVar = findEnvVar(p.name.expression);
                    const target = handlerName(p.initializer);
                    if (!envVar || !target) continue;
                    const m = node.members.find((x) => ts.isMethodDeclaration(x) && x.name?.getText(sf) === target) as ts.MethodDeclaration | undefined;
                    operations.push({
                      action: 'receive', envVar: envVar.envVar, className, method: target, file: rel, line: lineOf(p),
                      via: 'registerHandler', address: resolve(envVar.envVar), schemaName: m ? paramSchema(m) : undefined,
                    });
                  }
                }
              }
              ts.forEachChild(n, walk);
            });
            // ---- producers
            ts.forEachChild(member, function walk(n) {
              if (ts.isCallExpression(n) && ts.isPropertyAccessExpression(n.expression)) {
                const callee = n.expression.name.text;
                if (callee === 'emit' && isKafkaClient(n.expression.expression)) {
                  const topic = findEnvVar(n.arguments[0], node, sf);
                  if (!topic) { skipped.push(`${rel}:${lineOf(n)} ${className}.${method}`); ts.forEachChild(n, walk); return; }
                  const payload = payloadOf(n.arguments[1]);
                  operations.push({
                    action: 'send', envVar: topic.envVar, suffix: topic.suffix,
                    className, method, file: rel, line: lineOf(n), via: 'emit',
                    address: resolve(topic.envVar, topic.suffix),
                    key: payload.key, schemaName: payload.expr ? exprSchema(payload.expr) : undefined,
                  });
                } else if (callee === 'emitToKafka' && n.arguments.length >= 3) {
                  const topic = findEnvVar(n.arguments[0], node, sf);
                  if (!topic) { skipped.push(`${rel}:${lineOf(n)} ${className}.${method}`); ts.forEachChild(n, walk); return; }
                  operations.push({
                    action: 'send', envVar: topic.envVar, suffix: topic.suffix,
                    className, method, file: rel, line: lineOf(n), via: 'emitToKafka',
                    address: resolve(topic.envVar, topic.suffix),
                    key: n.arguments[1].getText(sf), schemaName: exprSchema(n.arguments[2]),
                  });
                }
              }
              ts.forEachChild(n, walk);
            });
          }
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(sf);

    // ---------------------------------------------------------------- helpers bound to this file
    function paramSchema(m: ts.MethodDeclaration): string | undefined {
      const p = m.parameters[0];
      if (!p) return undefined;
      const t = checker.getTypeAtLocation(p);
      return register(t, `${m.name.getText(sf)}Payload`);
    }

    function exprSchema(e: ts.Expression): string | undefined {
      const t = checker.getTypeAtLocation(e);
      return register(t, `${path.basename(rel, '.ts')}.${lineOf(e)}`);
    }

    /** Stores the schema and returns its name; anonymous types get a synthetic name. */
    function register(t: ts.Type, fallback: string): string | undefined {
      if (t.flags & (ts.TypeFlags.Any | ts.TypeFlags.Unknown)) return undefined;
      const s = builder.build(t);
      const ref = s.$ref as string | undefined;
      if (ref) return ref.split('/').pop();
      const name = fallback.replace(/[^A-Za-z0-9_.]/g, '_');
      schemas[name] = s;
      return name;
    }

    function isKafkaClient(recv: ts.Expression): boolean {
      const t = checker.getTypeAtLocation(recv);
      const n = t.getSymbol()?.getName();
      if (n === 'ClientKafka' || n === 'ClientProxy') return true;
      return /kafka/i.test(recv.getText(sf));
    }

    function payloadOf(e: ts.Expression | undefined): { key?: string; expr?: ts.Expression } {
      if (!e) return {};
      if (ts.isObjectLiteralExpression(e)) {
        let key: string | undefined;
        let value: ts.Expression | undefined;
        for (const p of e.properties) {
          if (ts.isPropertyAssignment(p) && ts.isIdentifier(p.name)) {
            if (p.name.text === 'key') key = p.initializer.getText(sf);
            if (p.name.text === 'value') value = p.initializer;
          } else if (ts.isShorthandPropertyAssignment(p)) {
            if (p.name.text === 'key') key = 'key';
            if (p.name.text === 'value') value = p.name;
          }
        }
        if (value) return { key, expr: unwrapStringify(value) };
        return { key, expr: e };
      }
      return { expr: unwrapStringify(e) };
    }

    function unwrapStringify(e: ts.Expression): ts.Expression {
      if (ts.isCallExpression(e) && e.expression.getText(sf) === 'JSON.stringify' && e.arguments[0]) return e.arguments[0];
      return e;
    }

    function resolve(envVar: string, suffix?: string): string | undefined {
      const v = envVar.startsWith('literal:') ? envVar.slice(8) : env[envVar];
      return v ? v + (suffix ?? '') : undefined;
    }

    /** Finds the env var an expression reads: config.get('X'), process.env.X, `${process.env.X}.dlq`, this.field, local const. */
    function findEnvVar(e: ts.Expression | undefined, cls?: ts.ClassDeclaration, file?: ts.SourceFile, depth = 0): { envVar: string; suffix?: string; literal?: string } | undefined {
      if (!e || depth > 4) return undefined;
      if (ts.isStringLiteralLike(e)) return ENV_VAR.test(e.text) ? { envVar: e.text } : { envVar: `literal:${e.text}`, literal: e.text };
      if (ts.isCallExpression(e)) {
        for (const a of e.arguments) {
          const r = findEnvVar(a, cls, file, depth + 1);
          if (r) return r;
        }
        return undefined;
      }
      if (ts.isPropertyAccessExpression(e)) {
        if (e.expression.getText(sf) === 'process.env' && ENV_VAR.test(e.name.text)) return { envVar: e.name.text };
        if (e.expression.kind === ts.SyntaxKind.ThisKeyword && cls) {
          const field = e.name.text;
          // this.field = this.config.getOrThrow('X') somewhere in the class, or an initializer
          let found: { envVar: string; suffix?: string; literal?: string } | undefined;
          ts.forEachChild(cls, function walk(n) {
            if (found) return;
            if (ts.isBinaryExpression(n) && n.operatorToken.kind === ts.SyntaxKind.EqualsToken && ts.isPropertyAccessExpression(n.left) && n.left.name.text === field && n.left.expression.kind === ts.SyntaxKind.ThisKeyword) {
              found = findEnvVar(n.right, cls, file, depth + 1);
            }
            if (ts.isPropertyDeclaration(n) && n.name.getText(sf) === field && n.initializer) found = findEnvVar(n.initializer, cls, file, depth + 1);
            ts.forEachChild(n, walk);
          });
          return found;
        }
      }
      if (ts.isTemplateExpression(e)) {
        const head = findEnvVar(e.templateSpans[0]?.expression, cls, file, depth + 1);
        if (head) return { envVar: head.envVar, suffix: e.templateSpans.map((s) => s.literal.text).join('') };
      }
      if (ts.isIdentifier(e)) {
        const sym = checker.getSymbolAtLocation(e);
        const decl = sym?.valueDeclaration;
        if (decl && ts.isVariableDeclaration(decl) && decl.initializer) return findEnvVar(decl.initializer, cls, file, depth + 1);
      }
      if (ts.isParenthesizedExpression(e) || ts.isAsExpression(e) || ts.isNonNullExpression(e)) return findEnvVar(e.expression, cls, file, depth + 1);
      return undefined;
    }

    function handlerName(e: ts.Expression): string | undefined {
      // this.fn.bind(this) | this.fn
      if (ts.isCallExpression(e) && ts.isPropertyAccessExpression(e.expression) && e.expression.name.text === 'bind') return handlerName(e.expression.expression);
      if (ts.isPropertyAccessExpression(e) && e.expression.kind === ts.SyntaxKind.ThisKeyword) return e.name.text;
      return undefined;
    }
  }

  log(`${opts.service}: ${operations.filter((o) => o.action === 'receive').length} receive, ${operations.filter((o) => o.action === 'send').length} send, ${Object.keys(schemas).length} schemas`);
  if (skipped.length) log(`${opts.service}: skipped ${skipped.length} producer site(s) with a runtime topic: ${skipped.join('; ')}`);
  return { service: opts.service, version: opts.version, operations, schemas, env, generatedAt: new Date().toISOString() };
}
