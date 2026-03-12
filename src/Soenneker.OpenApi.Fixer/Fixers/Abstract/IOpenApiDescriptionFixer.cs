using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer.Fixers.Abstract;

/// <summary>
/// Provides functionality to fix and sanitize descriptions in OpenAPI documents, particularly handling YAML-unsafe strings.
/// </summary>
public interface IOpenApiDescriptionFixer
{
    /// <summary>
    /// Fixes YAML-unsafe descriptions throughout the OpenAPI document by quoting strings that contain patterns like "key: value".
    /// This prevents YAML parsing errors when the document is serialized.
    /// </summary>
    /// <param name="document">The OpenAPI document to fix.</param>
    void FixYamlUnsafeDescriptions(OpenApiDocument document);
}

