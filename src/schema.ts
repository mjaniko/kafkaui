import ts from 'typescript';
import { JsonSchema } from './types';

const MAX_DEPTH = 6;

/**
 * Converts a checker type into JSON Schema, registering named object types
 * in `schemas` and returning `$ref`s to them so the document stays small.
 */
export class SchemaBuilder {
  private inProgress = new Set<string>();

  constructor(private readonly checker: ts.TypeChecker, readonly schemas: Record<string, JsonSchema>) {}

  /** Returns a schema for `type`; named types are stored and referenced. */
  build(type: ts.Type, depth = 0): JsonSchema {
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
  nameOf(type: ts.Type): string | undefined {
    const sym = type.aliasSymbol ?? type.getSymbol();
    if (!sym) return undefined;
    const n = sym.getName();
    if (!n || n === '__type' || n === '__object' || n === 'Array' || n === 'Promise') return undefined;
    if (['Record', 'Partial', 'Required', 'Pick', 'Omit', 'Readonly', 'Date', 'Map', 'Set', 'Buffer'].includes(n)) return undefined;
    // alias with type args (Partial<X>) → readable name
    if (type.aliasSymbol && type.aliasTypeArguments?.length) {
      const args = type.aliasTypeArguments.map((t) => this.nameOf(t) ?? this.checker.typeToString(t)).join(',');
      return `${n}<${args}>`.replace(/[^A-Za-z0-9_<>,.]/g, '');
    }
    return n;
  }

  private isObjectLike(type: ts.Type): boolean {
    if (type.isUnion() || type.isIntersection()) return false;
    if (type.flags & ts.TypeFlags.EnumLike) return false;
    if (this.checker.isArrayType(type)) return false;
    return !!(type.flags & ts.TypeFlags.Object) && !this.isDate(type) && !this.isFunction(type);
  }

  private isDate(type: ts.Type): boolean {
    return type.getSymbol()?.getName() === 'Date';
  }

  private isFunction(type: ts.Type): boolean {
    return type.getCallSignatures().length > 0 && type.getProperties().length === 0;
  }

  private inline(type: ts.Type, depth: number): JsonSchema {
    const f = type.flags;
    if (depth > MAX_DEPTH) return { description: this.checker.typeToString(type) };
    if (f & ts.TypeFlags.StringLiteral) return { type: 'string', const: (type as ts.StringLiteralType).value };
    if (f & ts.TypeFlags.NumberLiteral) return { type: 'number', const: (type as ts.NumberLiteralType).value };
    if (f & ts.TypeFlags.BooleanLiteral) return { type: 'boolean', const: this.checker.typeToString(type) === 'true' };
    if (f & ts.TypeFlags.EnumLike) return this.enumSchema(type);
    if (f & ts.TypeFlags.String) return { type: 'string' };
    if (f & ts.TypeFlags.Number) return { type: 'number' };
    if (f & ts.TypeFlags.Boolean) return { type: 'boolean' };
    if (f & ts.TypeFlags.BigInt) return { type: 'integer' };
    if (f & ts.TypeFlags.Null) return { type: 'null' };
    if (f & ts.TypeFlags.Undefined || f & ts.TypeFlags.Void) return { type: 'null', description: 'undefined' };
    if (f & ts.TypeFlags.Any || f & ts.TypeFlags.Unknown) return {};
    if (f & ts.TypeFlags.Never) return { not: {} };
    if (type.isUnion()) {
      const members = type.types.filter((t) => !(t.flags & ts.TypeFlags.Undefined));
      // boolean shows up as true|false
      if (members.length === 2 && members.every((t) => t.flags & ts.TypeFlags.BooleanLiteral)) return { type: 'boolean' };
      const literals = members.filter((t) => t.flags & (ts.TypeFlags.StringLiteral | ts.TypeFlags.NumberLiteral));
      if (literals.length === members.length && literals.length > 0) {
        return { enum: literals.map((t) => (t as ts.LiteralType).value) };
      }
      const parts = members.map((t) => this.build(t, depth + 1));
      return parts.length === 1 ? parts[0] : { anyOf: parts };
    }
    if (type.isIntersection()) {
      return { allOf: type.types.map((t) => this.build(t, depth + 1)) };
    }
    if (this.isDate(type)) return { type: 'string', format: 'date-time' };
    if (this.checker.isArrayType(type) || this.checker.isTupleType(type)) {
      const el = this.checker.getTypeArguments(type as ts.TypeReference)[0];
      return { type: 'array', items: el ? this.build(el, depth + 1) : {} };
    }
    if (this.isFunction(type)) return { description: 'function' };
    if (f & ts.TypeFlags.Object) return this.objectSchema(type, depth);
    return { description: this.checker.typeToString(type) };
  }

  private enumSchema(type: ts.Type): JsonSchema {
    const name = this.nameOf(type);
    const values: (string | number)[] = [];
    const collect = (t: ts.Type) => {
      if (t.isUnion()) t.types.forEach(collect);
      else if (t.flags & (ts.TypeFlags.StringLiteral | ts.TypeFlags.NumberLiteral)) values.push((t as ts.LiteralType).value as string | number);
    };
    collect(type);
    const schema: JsonSchema = values.length ? { enum: values } : { type: 'string' };
    if (name) {
      this.schemas[name] = { title: name, ...schema };
      return { $ref: `#/components/schemas/${name}` };
    }
    return schema;
  }

  private objectSchema(type: ts.Type, depth: number, title?: string): JsonSchema {
    const props: Record<string, JsonSchema> = {};
    const required: string[] = [];
    for (const p of this.checker.getPropertiesOfType(type)) {
      const decl = p.valueDeclaration ?? p.declarations?.[0];
      if (!decl) continue;
      if (ts.isMethodDeclaration(decl) || ts.isMethodSignature(decl) || ts.isGetAccessor(decl)) continue;
      // skip TypeORM relations and private-ish members on entities
      if (ts.canHaveModifiers(decl) && ts.getModifiers(decl)?.some((m) => m.kind === ts.SyntaxKind.PrivateKeyword || m.kind === ts.SyntaxKind.StaticKeyword)) continue;
      const pt = this.checker.getTypeOfSymbolAtLocation(p, decl);
      if (this.isFunction(pt)) continue;
      props[p.getName()] = this.build(pt, depth + 1);
      const optional = !!(p.flags & ts.SymbolFlags.Optional) || (pt.isUnion() && pt.types.some((t) => t.flags & ts.TypeFlags.Undefined));
      if (!optional) required.push(p.getName());
    }
    const schema: JsonSchema = { type: 'object', properties: props };
    if (title) schema.title = title;
    if (required.length) schema.required = required;
    const idx = this.checker.getIndexInfoOfType(type, ts.IndexKind.String);
    if (idx) schema.additionalProperties = this.build(idx.type, depth + 1);
    return schema;
  }
}
