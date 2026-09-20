using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer;

/// <summary>
/// Optional behaviors for <see cref="OpenApiFixer"/>.
/// </summary>
public sealed class OpenApiFixerOptions
{
    /// <summary>
    /// Infers missing JSON media schemas from inline or locally referenced examples and widens integer
    /// formats when payload examples demonstrate values beyond Int32. Existing schema constraints are retained.
    /// Enabled by default. Inferred media schemas are marked with <c>x-schema-inferred-from-examples</c>.
    /// </summary>
    public bool InferSchemasFromExamples { get; set; } = true;

    /// <summary>
    /// Enables detailed progress, repair summaries, and individual repair details at Information severity.
    /// Disabled by default. File paths, major stages, completion timing, warnings, and errors remain logged, subject to the host's logging filters.
    /// </summary>
    public bool VerboseLogging { get; set; }

    /// <summary>
    /// Overrides the OpenAPI version used for the fixed document. When unset, the source document version is preserved.
    /// </summary>
    public OpenApiSpecVersion? OutputSpecVersion { get; set; }

    /// <summary>
    /// Converts integer properties and parameters whose names end with <c>Id</c> from <c>int32</c> to <c>int64</c>.
    /// </summary>
    public bool Int32IdTransform { get; set; }

    /// <summary>
    /// Removes trailing date version tokens from generated path prefixes, operation IDs, and schema names.
    /// For example, <c>/assistant_control_2026-04</c> becomes <c>/assistant_control</c> and
    /// <c>AssistantControl202604ErrorResponse</c> becomes <c>AssistantControlErrorResponse</c>.
    /// </summary>
    public bool StripDateSuffixesFromGeneratedNames { get; set; }

    /// <summary>
    /// Redacts credential-like values from examples and descriptions during raw JSON preprocessing.
    /// Disabled by default because enabling it intentionally mutates documentation content.
    /// </summary>
    public bool RedactCredentialLikeValues { get; set; }
}
