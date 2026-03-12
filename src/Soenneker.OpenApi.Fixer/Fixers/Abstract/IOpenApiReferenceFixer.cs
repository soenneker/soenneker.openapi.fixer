using Microsoft.OpenApi;
using System.Collections.Generic;
using System.Threading;

namespace Soenneker.OpenApi.Fixer.Fixers.Abstract;

/// <summary>
/// Provides functionality to manage and fix OpenAPI schema references, including replacing, validating, and scrubbing broken references.
/// </summary>
public interface IOpenApiReferenceFixer
{
    /// <summary>
    /// Replaces all references to a schema component with a new key throughout the document.
    /// </summary>
    /// <param name="document">The OpenAPI document to update.</param>
    /// <param name="oldKey">The old schema component key to replace.</param>
    /// <param name="newKey">The new schema component key to use.</param>
    void ReplaceAllRefs(OpenApiDocument document, string oldKey, string newKey);

    /// <summary>
    /// Updates all references in the document according to the provided mapping of old keys to new keys.
    /// </summary>
    /// <param name="doc">The OpenAPI document to update.</param>
    /// <param name="mapping">Dictionary mapping old schema keys to new schema keys.</param>
    void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping);

    /// <summary>
    /// Validates whether a schema reference is valid and points to an existing schema in the document.
    /// </summary>
    /// <param name="reference">The schema reference to validate.</param>
    /// <param name="doc">The OpenAPI document containing the schemas.</param>
    /// <returns>True if the reference is valid, false otherwise.</returns>
    bool IsValidSchemaReference(OpenApiSchemaReference? reference, OpenApiDocument doc);

    /// <summary>
    /// Removes broken references from a content dictionary (e.g., request body or response content).
    /// </summary>
    /// <param name="contentDict">The content dictionary to clean.</param>
    /// <param name="doc">The OpenAPI document containing the schemas.</param>
    void ScrubBrokenRefs(IDictionary<string, IOpenApiMediaType>? contentDict, OpenApiDocument doc);

    /// <summary>
    /// Fixes references that incorrectly point into path examples.
    /// </summary>
    /// <param name="doc">The OpenAPI document to fix.</param>
    void FixRefsPointingIntoPathsExamples(OpenApiDocument doc);

    /// <summary>
    /// Recursively scrubs all broken references from a schema and its nested schemas.
    /// </summary>
    /// <param name="schema">The schema to clean.</param>
    /// <param name="doc">The OpenAPI document containing the schemas.</param>
    /// <param name="visited">Set of already visited schemas to prevent infinite recursion.</param>
    void ScrubAllRefs(IOpenApiSchema? schema, OpenApiDocument doc, HashSet<IOpenApiSchema> visited);

    /// <summary>
    /// Scrubs all broken component references from the document's components section.
    /// </summary>
    /// <param name="doc">The OpenAPI document to clean.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    void ScrubComponentRefs(OpenApiDocument doc, CancellationToken cancellationToken);
}

