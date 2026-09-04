"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SchemaBuilder = void 0;
const typescript_1 = __importDefault(require("typescript"));
const MAX_DEPTH = 6;
/**
 * Converts a checker type into JSON Schema, registering named object types
 * in `schemas` and returning `$ref`s to them so the document stays small.
 */
class SchemaBuilder {
    constructor(checker, schemas) {
        this.checker = checker;
        this.schemas = schemas;
        this.inProgress = new Set();
    }
    /** Returns a schema for `type`; named types are stored and referenced. */
    build(type, depth = 0) {
        const name = this.nameOf(type);
        if (name && this.isObjectLike(type)) {
            if (!this.schemas[name] && !this.inProgress.has(name)) {
                this.inProgress.add(name);
                this.schemas[name] = { title: name }; // placeholder for recursion
                this.schemas[name] = this.objectSchema(type, depth, name);
                this.inProgress.delete(name);
            }
            return { $ref: `#/components/schemas/${name}` };
        }
        return this.inline(type, depth);
    }
    /** Name used for the schema entry. Returns undefined for anonymous types. */
    nameOf(type) {
        const sym = type.aliasSymbol ?? type.getSymbol();
        if (!sym)
            return undefined;
        const n = sym.getName();
        if (!n || n === '__type' || n === '__object' || n === 'Array' || n === 'Promise')
            return undefined;
        if (['Record', 'Partial', 'Required', 'Pick', 'Omit', 'Readonly', 'Date', 'Map', 'Set', 'Buffer'].includes(n))
            return undefined;
        // alias with type args (Partial<X>) → readable name
        if (type.aliasSymbol && type.aliasTypeArguments?.length) {
            const args = type.aliasTypeArguments.map((t) => this.nameOf(t) ?? this.checker.typeToString(t)).join(',');
            return `${n}<${args}>`.replace(/[^A-Za-z0-9_<>,.]/g, '');
        }
        return n;
    }
    isObjectLike(type) {
        if (type.isUnion() || type.isIntersection())
            return false;
        if (type.flags & typescript_1.default.TypeFlags.EnumLike)
            return false;
        if (this.checker.isArrayType(type))
            return false;
        return !!(type.flags & typescript_1.default.TypeFlags.Object) && !this.isDate(type) && !this.isFunction(type);
    }
    isDate(type) {
        return type.getSymbol()?.getName() === 'Date';
    }
    isFunction(type) {
        return type.getCallSignatures().length > 0 && type.getProperties().length === 0;
    }
    inline(type, depth) {
        const f = type.flags;
        if (depth > MAX_DEPTH)
            return { description: this.checker.typeToString(type) };
        if (f & typescript_1.default.TypeFlags.StringLiteral)
            return { type: 'string', const: type.value };
        if (f & typescript_1.default.TypeFlags.NumberLiteral)
            return { type: 'number', const: type.value };
        if (f & typescript_1.default.TypeFlags.BooleanLiteral)
            return { type: 'boolean', const: this.checker.typeToString(type) === 'true' };
        if (f & typescript_1.default.TypeFlags.EnumLike)
            return this.enumSchema(type);
        if (f & typescript_1.default.TypeFlags.String)
            return { type: 'string' };
        if (f & typescript_1.default.TypeFlags.Number)
            return { type: 'number' };
        if (f & typescript_1.default.TypeFlags.Boolean)
            return { type: 'boolean' };
        if (f & typescript_1.default.TypeFlags.BigInt)
            return { type: 'integer' };
        if (f & typescript_1.default.TypeFlags.Null)
            return { type: 'null' };
        if (f & typescript_1.default.TypeFlags.Undefined || f & typescript_1.default.TypeFlags.Void)
            return { type: 'null', description: 'undefined' };
        if (f & typescript_1.default.TypeFlags.Any || f & typescript_1.default.TypeFlags.Unknown)
            return {};
        if (f & typescript_1.default.TypeFlags.Never)
            return { not: {} };
        if (type.isUnion()) {
            const members = type.types.filter((t) => !(t.flags & typescript_1.default.TypeFlags.Undefined));
            // boolean shows up as true|false
            if (members.length === 2 && members.every((t) => t.flags & typescript_1.default.TypeFlags.BooleanLiteral))
                return { type: 'boolean' };
            const literals = members.filter((t) => t.flags & (typescript_1.default.TypeFlags.StringLiteral | typescript_1.default.TypeFlags.NumberLiteral));
            if (literals.length === members.length && literals.length > 0) {
                return { enum: literals.map((t) => t.value) };
            }
            const parts = members.map((t) => this.build(t, depth + 1));
            return parts.length === 1 ? parts[0] : { anyOf: parts };
        }
        if (type.isIntersection()) {
            return { allOf: type.types.map((t) => this.build(t, depth + 1)) };
        }
        if (this.isDate(type))
            return { type: 'string', format: 'date-time' };
        if (this.checker.isArrayType(type) || this.checker.isTupleType(type)) {
            const el = this.checker.getTypeArguments(type)[0];
            return { type: 'array', items: el ? this.build(el, depth + 1) : {} };
        }
        if (this.isFunction(type))
            return { description: 'function' };
        if (f & typescript_1.default.TypeFlags.Object)
            return this.objectSchema(type, depth);
        return { description: this.checker.typeToString(type) };
    }
    enumSchema(type) {
        const name = this.nameOf(type);
        const values = [];
        const collect = (t) => {
            if (t.isUnion())
                t.types.forEach(collect);
            else if (t.flags & (typescript_1.default.TypeFlags.StringLiteral | typescript_1.default.TypeFlags.NumberLiteral))
                values.push(t.value);
        };
        collect(type);
        const schema = values.length ? { enum: values } : { type: 'string' };
        if (name) {
            this.schemas[name] = { title: name, ...schema };
            return { $ref: `#/components/schemas/${name}` };
        }
        return schema;
    }
    objectSchema(type, depth, title) {
        const props = {};
        const required = [];
        for (const p of this.checker.getPropertiesOfType(type)) {
            const decl = p.valueDeclaration ?? p.declarations?.[0];
            if (!decl)
                continue;
            if (typescript_1.default.isMethodDeclaration(decl) || typescript_1.default.isMethodSignature(decl) || typescript_1.default.isGetAccessor(decl))
                continue;
            // skip TypeORM relations and private-ish members on entities
            if (typescript_1.default.canHaveModifiers(decl) && typescript_1.default.getModifiers(decl)?.some((m) => m.kind === typescript_1.default.SyntaxKind.PrivateKeyword || m.kind === typescript_1.default.SyntaxKind.StaticKeyword))
                continue;
            const pt = this.checker.getTypeOfSymbolAtLocation(p, decl);
            if (this.isFunction(pt))
                continue;
            props[p.getName()] = this.build(pt, depth + 1);
            const optional = !!(p.flags & typescript_1.default.SymbolFlags.Optional) || (pt.isUnion() && pt.types.some((t) => t.flags & typescript_1.default.TypeFlags.Undefined));
            if (!optional)
                required.push(p.getName());
        }
        const schema = { type: 'object', properties: props };
        if (title)
            schema.title = title;
        if (required.length)
            schema.required = required;
        const idx = this.checker.getIndexInfoOfType(type, typescript_1.default.IndexKind.String);
        if (idx)
            schema.additionalProperties = this.build(idx.type, depth + 1);
        return schema;
    }
}
exports.SchemaBuilder = SchemaBuilder;
