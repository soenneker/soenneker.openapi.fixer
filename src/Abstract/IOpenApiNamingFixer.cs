using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer.Abstract;

/// <summary>
/// Provides functionality to validate, sanitize, and normalize names and identifiers in OpenAPI documents,
/// including schema names, operation IDs, and path names.
/// </summary>
public interface IOpenApiNamingFixer
{
    /// <summary>
    /// Renames invalid component schema names to valid identifiers suitable for code generation.
    /// </summary>
    /// <param name="document">The OpenAPI document to fix.</param>
    void RenameInvalidComponentSchemas(OpenApiDocument document);

    /// <summary>
    /// Validates and fixes schema names throughout the document to ensure they are valid identifiers.
    /// </summary>
    /// <param name="doc">The OpenAPI document to fix.</param>
    void ValidateAndFixSchemaNames(OpenApiDocument doc);

    /// <summary>
    /// Ensures all operation IDs in the document are unique by appending numeric suffixes if necessary.
    /// </summary>
    /// <param name="doc">The OpenAPI document to fix.</param>
    void EnsureUniqueOperationIds(OpenApiDocument doc);

    /// <summary>
    /// Normalizes operation IDs to be valid identifiers (removes invalid characters, ensures proper casing).
    /// </summary>
    /// <param name="doc">The OpenAPI document to fix.</param>
    void NormalizeOperationIds(OpenApiDocument doc);

    /// <summary>
    /// Resolves naming collisions between schema names and operation IDs by renaming conflicting schemas.
    /// </summary>
    /// <param name="doc">The OpenAPI document to fix.</param>
    void ResolveSchemaOperationNameCollisions(OpenApiDocument doc);

    /// <summary>
    /// Renames conflicting path names to ensure uniqueness.
    /// </summary>
    /// <param name="doc">The OpenAPI document to fix.</param>
    void RenameConflictingPaths(OpenApiDocument doc);

    /// <summary>
    /// Sanitizes a name by removing invalid characters and ensuring it starts with a letter.
    /// </summary>
    /// <param name="input">The input string to sanitize.</param>
    /// <returns>A sanitized name suitable for use as an identifier.</returns>
    string SanitizeName(string input);

    /// <summary>
    /// Normalizes an operation ID to be a valid identifier.
    /// </summary>
    /// <param name="input">The operation ID to normalize.</param>
    /// <returns>A normalized operation ID.</returns>
    string NormalizeOperationId(string input);

    /// <summary>
    /// Checks if a string is a valid identifier (starts with letter, contains only alphanumeric and underscore).
    /// </summary>
    /// <param name="id">The identifier to validate.</param>
    /// <returns>True if the identifier is valid, false otherwise.</returns>
    bool IsValidIdentifier(string id);

    /// <summary>
    /// Generates a safe part of a name from input, falling back to a default if the input is invalid.
    /// </summary>
    /// <param name="input">The input string to process.</param>
    /// <param name="fallback">The fallback value to use if input is invalid.</param>
    /// <returns>A safe name part.</returns>
    string GenerateSafePart(string? input, string fallback = "unnamed");

    /// <summary>
    /// Validates and normalizes a component name to ensure it's a valid identifier.
    /// </summary>
    /// <param name="name">The component name to validate.</param>
    /// <returns>A validated component name.</returns>
    string ValidateComponentName(string name);
}

