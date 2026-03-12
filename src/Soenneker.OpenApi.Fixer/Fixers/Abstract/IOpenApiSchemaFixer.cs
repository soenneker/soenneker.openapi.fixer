using Microsoft.OpenApi;
using System.Collections.Generic;

namespace Soenneker.OpenApi.Fixer.Fixers.Abstract;

/// <summary>
/// Provides functionality to clean, transform, and fix OpenAPI schemas, including removing empty schemas,
/// fixing defaults, cleaning for serialization, and injecting types for nullable schemas.
/// </summary>
public interface IOpenApiSchemaFixer
{
    /// <summary>
    /// Removes empty inline schemas from the document's components section.
    /// </summary>
    /// <param name="document">The OpenAPI document to clean.</param>
    void RemoveEmptyInlineSchemas(OpenApiDocument document);

    /// <summary>
    /// Cleans a schema by removing empty composition objects (allOf, oneOf, anyOf) and their empty children.
    /// </summary>
    /// <param name="schema">The schema to clean.</param>
    /// <param name="visited">Set of already visited schemas to prevent infinite recursion.</param>
    void Clean(OpenApiSchema schema, HashSet<OpenApiSchema> visited);

    /// <summary>
    /// Determines if a schema is empty (has no meaningful content).
    /// </summary>
    /// <param name="schema">The schema to check.</param>
    /// <returns>True if the schema is empty, false otherwise.</returns>
    bool IsSchemaEmpty(IOpenApiSchema schema);

    /// <summary>
    /// Performs a deep clean of a schema, removing invalid examples, empty defaults, and cleaning enum values.
    /// </summary>
    /// <param name="schema">The schema to clean.</param>
    /// <param name="visited">Set of already visited schemas to prevent infinite recursion.</param>
    void DeepCleanSchema(OpenApiSchema? schema, HashSet<OpenApiSchema> visited);

    /// <summary>
    /// Cleans the entire document's schemas for serialization by removing control characters and invalid values.
    /// </summary>
    /// <param name="document">The OpenAPI document to clean.</param>
    void CleanDocumentForSerialization(OpenApiDocument document);

    /// <summary>
    /// Cleans a schema for serialization by removing control characters from strings and validating enum/default/example values.
    /// </summary>
    /// <param name="schema">The schema to clean.</param>
    /// <param name="visited">Set of already visited schemas to prevent infinite recursion.</param>
    void CleanSchemaForSerialization(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited);

    /// <summary>
    /// Fixes invalid default values in all schemas throughout the document.
    /// </summary>
    /// <param name="document">The OpenAPI document to fix.</param>
    void FixInvalidDefaults(OpenApiDocument document);

    /// <summary>
    /// Fixes schema default values to ensure they match the schema's type and enum constraints.
    /// </summary>
    /// <param name="schema">The schema to fix.</param>
    /// <param name="visited">Set of already visited schemas to prevent infinite recursion.</param>
    void FixSchemaDefaults(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited);

    /// <summary>
    /// Removes invalid default values from schemas (e.g., non-object defaults on object schemas).
    /// </summary>
    /// <param name="document">The OpenAPI document to clean.</param>
    void RemoveInvalidDefaults(OpenApiDocument document);

    /// <summary>
    /// Removes empty composition objects (allOf, oneOf, anyOf) from a schema.
    /// </summary>
    /// <param name="schema">The schema to clean.</param>
    /// <param name="visited">Set of already visited schemas to prevent infinite recursion.</param>
    void RemoveEmptyCompositionObjects(OpenApiSchema schema, HashSet<OpenApiSchema> visited);

    /// <summary>
    /// Injects type information for nullable schemas that are missing explicit type declarations.
    /// </summary>
    /// <param name="schema">The schema to fix.</param>
    /// <param name="visited">Set of already visited schemas to prevent infinite recursion.</param>
    void InjectTypeForNullable(OpenApiSchema schema, HashSet<OpenApiSchema> visited);

    /// <summary>
    /// Removes duplicate composition branches (e.g. duplicate <c>$ref</c> entries) from <c>anyOf</c>, <c>oneOf</c>, and <c>allOf</c> across the document.
    /// This is primarily to satisfy generators (e.g. Kiota) that fail on duplicate branches.
    /// </summary>
    /// <param name="document">The OpenAPI document to fix.</param>
    void DeduplicateCompositionBranches(OpenApiDocument document);
}

