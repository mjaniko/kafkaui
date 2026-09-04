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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.scanProject = scanProject;
const path = __importStar(require("path"));
const typescript_1 = __importDefault(require("typescript"));
const env_1 = require("./env");
const schema_1 = require("./schema");
const ENV_VAR = /^[A-Z][A-Z0-9_]*$/;
const CONSUMER_DECORATORS = new Set(['KafkaTopic', 'KafkaCron']);
const DEFAULT_EXCLUDE = /(\.spec\.ts|\.test\.ts|\.d\.ts|[\\/]migrations[\\/]|[\\/]node_modules[\\/]|[\\/]test[\\/])/;
/** Scans one NestJS project with the TypeScript checker and returns its Kafka contract. */
function scanProject(opts) {
    const log = opts.log ?? (() => undefined);
    const project = path.resolve(opts.project);
    const tsconfig = path.resolve(project, opts.tsconfig ?? 'tsconfig.json');
    const parsed = typescript_1.default.getParsedCommandLineOfConfigFile(tsconfig, {}, {
        ...typescript_1.default.sys,
        onUnRecoverableConfigFileDiagnostic: (d) => {
            throw new Error(typescript_1.default.flattenDiagnosticMessageText(d.messageText, '\n'));
        },
    });
    if (!parsed)
        throw new Error(`cannot parse ${tsconfig}`);
    const exclude = opts.exclude ?? DEFAULT_EXCLUDE;
    const files = parsed.fileNames.filter((f) => !exclude.test(f));
    const program = typescript_1.default.createProgram(files, { ...parsed.options, noEmit: true });
    const checker = program.getTypeChecker();
    const schemas = {};
    const builder = new schema_1.SchemaBuilder(checker, schemas);
    // Topic identity in code is the env var; the name is whatever the repo declares for it
    // (.env.sample first, then a local .env), unless the caller points at another file.
    const envSource = opts.envFile ? path.resolve(opts.envFile)
        : opts.envFile === null ? undefined
            : ['.env.sample', '.env.example', '.env'].map((f) => path.join(project, f)).find((f) => typescript_1.default.sys.fileExists(f));
    const env = envSource ? (0, env_1.loadEnvFile)(envSource) : {};
    if (envSource)
        log(`topic names from ${path.relative(process.cwd(), envSource)} (${Object.keys(env).filter((k) => /TOPIC/.test(k)).length} topic vars)`);
    const operations = [];
    // Producer sites whose topic is a runtime value (a method parameter, data-driven routing).
    // Not part of the static contract; reported on stderr so they are not forgotten.
    const skipped = [];
    for (const sf of program.getSourceFiles()) {
        if (sf.isDeclarationFile || !sf.fileName.startsWith(project) || exclude.test(sf.fileName))
            continue;
        const rel = path.relative(project, sf.fileName);
        const lineOf = (n) => sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1;
        const visit = (node) => {
            if (typescript_1.default.isClassDeclaration(node) && node.name) {
                const className = node.name.text;
                for (const member of node.members) {
                    if (typescript_1.default.isMethodDeclaration(member) && member.name) {
                        const method = member.name.getText(sf);
                        // ---- consumers via decorators
                        for (const dec of typescript_1.default.getDecorators(member) ?? []) {
                            const call = dec.expression;
                            if (!typescript_1.default.isCallExpression(call) || !typescript_1.default.isIdentifier(call.expression))
                                continue;
                            const name = call.expression.text;
                            if (!CONSUMER_DECORATORS.has(name))
                                continue;
                            const arg = call.arguments[0];
                            if (!arg || !typescript_1.default.isStringLiteralLike(arg))
                                continue;
                            const isVar = ENV_VAR.test(arg.text);
                            operations.push({
                                action: 'receive', envVar: isVar ? arg.text : `literal:${arg.text}`, className, method, file: rel, line: lineOf(dec),
                                via: name, address: isVar ? env[arg.text] : arg.text,
                                schemaName: paramSchema(member),
                            });
                        }
                        // ---- consumers via registerHandler({ [config.get('X')]: this.fn.bind(this) })
                        typescript_1.default.forEachChild(member, function walk(n) {
                            if (typescript_1.default.isCallExpression(n) && typescript_1.default.isPropertyAccessExpression(n.expression) && n.expression.name.text === 'registerHandler') {
                                const obj = n.arguments[0];
                                if (obj && typescript_1.default.isObjectLiteralExpression(obj)) {
                                    for (const p of obj.properties) {
                                        if (!typescript_1.default.isPropertyAssignment(p) || !typescript_1.default.isComputedPropertyName(p.name))
                                            continue;
                                        const envVar = findEnvVar(p.name.expression);
                                        const target = handlerName(p.initializer);
                                        if (!envVar || !target)
                                            continue;
                                        const m = node.members.find((x) => typescript_1.default.isMethodDeclaration(x) && x.name?.getText(sf) === target);
                                        operations.push({
                                            action: 'receive', envVar: envVar.envVar, className, method: target, file: rel, line: lineOf(p),
                                            via: 'registerHandler', address: resolve(envVar.envVar), schemaName: m ? paramSchema(m) : undefined,
                                        });
                                    }
                                }
                            }
                            typescript_1.default.forEachChild(n, walk);
                        });
                        // ---- producers
                        typescript_1.default.forEachChild(member, function walk(n) {
                            if (typescript_1.default.isCallExpression(n) && typescript_1.default.isPropertyAccessExpression(n.expression)) {
                                const callee = n.expression.name.text;
                                if (callee === 'emit' && isKafkaClient(n.expression.expression)) {
                                    const topic = findEnvVar(n.arguments[0], node, sf);
                                    if (!topic) {
                                        skipped.push(`${rel}:${lineOf(n)} ${className}.${method}`);
                                        typescript_1.default.forEachChild(n, walk);
                                        return;
                                    }
                                    const payload = payloadOf(n.arguments[1]);
                                    operations.push({
                                        action: 'send', envVar: topic.envVar, suffix: topic.suffix,
                                        className, method, file: rel, line: lineOf(n), via: 'emit',
                                        address: resolve(topic.envVar, topic.suffix),
                                        key: payload.key, schemaName: payload.expr ? exprSchema(payload.expr) : undefined,
                                    });
                                }
                                else if (callee === 'emitToKafka' && n.arguments.length >= 3) {
                                    const topic = findEnvVar(n.arguments[0], node, sf);
                                    if (!topic) {
                                        skipped.push(`${rel}:${lineOf(n)} ${className}.${method}`);
                                        typescript_1.default.forEachChild(n, walk);
                                        return;
                                    }
                                    operations.push({
                                        action: 'send', envVar: topic.envVar, suffix: topic.suffix,
                                        className, method, file: rel, line: lineOf(n), via: 'emitToKafka',
                                        address: resolve(topic.envVar, topic.suffix),
                                        key: n.arguments[1].getText(sf), schemaName: exprSchema(n.arguments[2]),
                                    });
                                }
                            }
                            typescript_1.default.forEachChild(n, walk);
                        });
                    }
                }
            }
            typescript_1.default.forEachChild(node, visit);
        };
        visit(sf);
        // ---------------------------------------------------------------- helpers bound to this file
        function paramSchema(m) {
            const p = m.parameters[0];
            if (!p)
                return undefined;
            const t = checker.getTypeAtLocation(p);
            return register(t, `${m.name.getText(sf)}Payload`);
        }
        function exprSchema(e) {
            const t = checker.getTypeAtLocation(e);
            return register(t, `${path.basename(rel, '.ts')}.${lineOf(e)}`);
        }
        /** Stores the schema and returns its name; anonymous types get a synthetic name. */
        function register(t, fallback) {
            if (t.flags & (typescript_1.default.TypeFlags.Any | typescript_1.default.TypeFlags.Unknown))
                return undefined;
            const s = builder.build(t);
            const ref = s.$ref;
            if (ref)
                return ref.split('/').pop();
            const name = fallback.replace(/[^A-Za-z0-9_.]/g, '_');
            schemas[name] = s;
            return name;
        }
        function isKafkaClient(recv) {
            const t = checker.getTypeAtLocation(recv);
            const n = t.getSymbol()?.getName();
            if (n === 'ClientKafka' || n === 'ClientProxy')
                return true;
            return /kafka/i.test(recv.getText(sf));
        }
        function payloadOf(e) {
            if (!e)
                return {};
            if (typescript_1.default.isObjectLiteralExpression(e)) {
                let key;
                let value;
                for (const p of e.properties) {
                    if (typescript_1.default.isPropertyAssignment(p) && typescript_1.default.isIdentifier(p.name)) {
                        if (p.name.text === 'key')
                            key = p.initializer.getText(sf);
                        if (p.name.text === 'value')
                            value = p.initializer;
                    }
                    else if (typescript_1.default.isShorthandPropertyAssignment(p)) {
                        if (p.name.text === 'key')
                            key = 'key';
                        if (p.name.text === 'value')
                            value = p.name;
                    }
                }
                if (value)
                    return { key, expr: unwrapStringify(value) };
                return { key, expr: e };
            }
            return { expr: unwrapStringify(e) };
        }
        function unwrapStringify(e) {
            if (typescript_1.default.isCallExpression(e) && e.expression.getText(sf) === 'JSON.stringify' && e.arguments[0])
                return e.arguments[0];
            return e;
        }
        function resolve(envVar, suffix) {
            const v = envVar.startsWith('literal:') ? envVar.slice(8) : env[envVar];
            return v ? v + (suffix ?? '') : undefined;
        }
        /** Finds the env var an expression reads: config.get('X'), process.env.X, `${process.env.X}.dlq`, this.field, local const. */
        function findEnvVar(e, cls, file, depth = 0) {
            if (!e || depth > 4)
                return undefined;
            if (typescript_1.default.isStringLiteralLike(e))
                return ENV_VAR.test(e.text) ? { envVar: e.text } : { envVar: `literal:${e.text}`, literal: e.text };
            if (typescript_1.default.isCallExpression(e)) {
                for (const a of e.arguments) {
                    const r = findEnvVar(a, cls, file, depth + 1);
                    if (r)
                        return r;
                }
                return undefined;
            }
            if (typescript_1.default.isPropertyAccessExpression(e)) {
                if (e.expression.getText(sf) === 'process.env' && ENV_VAR.test(e.name.text))
                    return { envVar: e.name.text };
                if (e.expression.kind === typescript_1.default.SyntaxKind.ThisKeyword && cls) {
                    const field = e.name.text;
                    // this.field = this.config.getOrThrow('X') somewhere in the class, or an initializer
                    let found;
                    typescript_1.default.forEachChild(cls, function walk(n) {
                        if (found)
                            return;
                        if (typescript_1.default.isBinaryExpression(n) && n.operatorToken.kind === typescript_1.default.SyntaxKind.EqualsToken && typescript_1.default.isPropertyAccessExpression(n.left) && n.left.name.text === field && n.left.expression.kind === typescript_1.default.SyntaxKind.ThisKeyword) {
                            found = findEnvVar(n.right, cls, file, depth + 1);
                        }
                        if (typescript_1.default.isPropertyDeclaration(n) && n.name.getText(sf) === field && n.initializer)
                            found = findEnvVar(n.initializer, cls, file, depth + 1);
                        typescript_1.default.forEachChild(n, walk);
                    });
                    return found;
                }
            }
            if (typescript_1.default.isTemplateExpression(e)) {
                const head = findEnvVar(e.templateSpans[0]?.expression, cls, file, depth + 1);
                if (head)
                    return { envVar: head.envVar, suffix: e.templateSpans.map((s) => s.literal.text).join('') };
            }
            if (typescript_1.default.isIdentifier(e)) {
                const sym = checker.getSymbolAtLocation(e);
                const decl = sym?.valueDeclaration;
                if (decl && typescript_1.default.isVariableDeclaration(decl) && decl.initializer)
                    return findEnvVar(decl.initializer, cls, file, depth + 1);
            }
            if (typescript_1.default.isParenthesizedExpression(e) || typescript_1.default.isAsExpression(e) || typescript_1.default.isNonNullExpression(e))
                return findEnvVar(e.expression, cls, file, depth + 1);
            return undefined;
        }
        function handlerName(e) {
            // this.fn.bind(this) | this.fn
            if (typescript_1.default.isCallExpression(e) && typescript_1.default.isPropertyAccessExpression(e.expression) && e.expression.name.text === 'bind')
                return handlerName(e.expression.expression);
            if (typescript_1.default.isPropertyAccessExpression(e) && e.expression.kind === typescript_1.default.SyntaxKind.ThisKeyword)
                return e.name.text;
            return undefined;
        }
    }
    log(`${opts.service}: ${operations.filter((o) => o.action === 'receive').length} receive, ${operations.filter((o) => o.action === 'send').length} send, ${Object.keys(schemas).length} schemas`);
    if (skipped.length)
        log(`${opts.service}: skipped ${skipped.length} producer site(s) with a runtime topic: ${skipped.join('; ')}`);
    return { service: opts.service, version: opts.version, operations, schemas, env, generatedAt: new Date().toISOString() };
}
