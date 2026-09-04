import ts from 'typescript';
import { JsonSchema } from './types';
/**
 * Converts a checker type into JSON Schema, registering named object types
 * in `schemas` and returning `$ref`s to them so the document stays small.
 */
export declare class SchemaBuilder {
    private readonly checker;
    readonly schemas: Record<string, JsonSchema>;
    private inProgress;
    constructor(checker: ts.TypeChecker, schemas: Record<string, JsonSchema>);
    /** Returns a schema for `type`; named types are stored and referenced. */
    build(type: ts.Type, depth?: number): JsonSchema;
    /** Name used for the schema entry. Returns undefined for anonymous types. */
    nameOf(type: ts.Type): string | undefined;
    private isObjectLike;
    private isDate;
    private isFunction;
    private inline;
    private enumSchema;
    private objectSchema;
}
