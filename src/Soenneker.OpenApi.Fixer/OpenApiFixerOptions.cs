using Microsoft.OpenApi;
using System.Collections.Generic;

namespace Soenneker.OpenApi.Fixer;

/// <summary>
/// Optional behaviors for <see cref="OpenApiFixer"/>.
/// </summary>
public sealed class OpenApiFixerOptions
{
    /// <summary>
    /// Gives divergent successful JSON responses a shared union return type so Kiota does not discard
    /// alternate statuses or representations. Mixed JSON and binary/text representations return a stream.
    /// Enabled by default. This widens per-status validation; original media schemas are retained in
    /// x-client-original-success-schema. Shared component responses remain unchanged.
    /// </summary>
    public bool ProjectDivergentSuccessResponses { get; set; } = true;

    /// <summary>
    /// Projects compatible object unions used by error responses into concrete error models so Kiota can
    /// generate error mappings. Enabled by default. Shared request and success schemas remain unchanged.
    /// This widens validation: required fields become the intersection of branch requirements and oneOf
    /// exclusivity is lost. The original schema is retained in x-client-original-error-schema.
    /// Conflicting property schemas, closed objects, and unsupported constraints are not projected.
    /// </summary>
    public bool ProjectCompatibleErrorResponses { get; set; } = true;

    /// <summary>
    /// Moves known string formats unsupported by Kiota (such as email and uri) to x-client-original-format.
    /// Disabled by default because validators no longer enforce the moved format. Supported formats such
    /// as uuid and date-time, unknown formats, and example payloads are preserved.
    /// </summary>
    public bool NormalizeUnsupportedStringFormats { get; set; }

    /// <summary>
    /// Projects open object anyOf branches with identical required fields and identical overlapping property schemas
    /// into a single client model. Enabled by default. This is a client-generation projection, not a validation-equivalent
    /// rewrite: optional fields become constrained across all alternatives. The original anyOf is retained in
    /// x-client-original-anyOf. Disable to retain strict composition semantics and generated wrappers.
    /// </summary>
    public bool FlattenCompatibleObjectAlternatives { get; set; } = true;

    /// <summary>
    /// Overrides schema types before parsing and normalization, keyed by an exact JSON Pointer into the
    /// preprocessed source document (for example, <c>/components/schemas/Library/properties/trustScore</c>).
    /// Escape <c>~</c> as <c>~0</c> and <c>/</c> as <c>~1</c> in pointer segments.
    /// Targets must be existing schema objects, not references or example payloads; invalid targets throw.
    /// Other schema constraints and metadata are retained and must be compatible with the replacement type.
    /// Empty by default.
    /// </summary>
    public Dictionary<string, OpenApiSchemaTypeOverride> SchemaTypeOverrides { get; set; } = new();

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
