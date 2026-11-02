using Microsoft.OpenApi;
using System.Collections.Generic;

namespace Soenneker.OpenApi.Fixer;

/// <summary>
/// Optional behaviors for <see cref="OpenApiFixer"/>.
/// </summary>
public sealed class OpenApiFixerOptions
{
    /// <summary>
    /// Projects unions of objects with distinct required string tags into a discriminated class hierarchy.
    /// Uses dedicated derived schemas so models shared with other unions retain their original shape.
    /// This widens base-schema validation; the original union is retained in x-client-original-tagged-union.
    /// Unknown tags remain available through the base model's additional data. Enabled by default.
    /// </summary>
    public bool ProjectTaggedObjectUnions { get; set; } = true;

    /// <summary>
    /// Repairs object examples encoded as strings, JSON Pointer examples, and incompatible inherited
    /// examples when compatible defaults or property examples exist. Original examples are retained
    /// in x-client-original-example. Unconstrained object placeholders with scalar examples become
    /// object/scalar unions, widening validation; x-stainless-any values retain arbitrary JSON.
    /// Documented 24-hex identifiers incorrectly annotated as UUIDs retain string types; the original
    /// format is retained in x-client-original-format. Enabled by default. Disable to retain the original
    /// example annotations, identifier formats, and type placeholders.
    /// </summary>
    public bool ReconcileSchemaExamples { get; set; } = true;

    /// <summary>
    /// Gives divergent successful JSON responses a shared union return type so Kiota does not discard
    /// alternate statuses or representations. Prefers JSON when every body-bearing success response offers it;
    /// excluded media definitions are retained in x-client-original-non-json-content. Otherwise mixed
    /// JSON and binary/text representations return a stream.
    /// Adds a 200 generation alias when a body schema exists only on other success statuses (such as 207),
    /// retaining the source status in x-client-success-response-status and any replaced response in x-client-original-response.
    /// Enabled by default. This widens per-status validation; original media schemas are retained in
    /// x-client-original-success-schema. Shared component responses remain unchanged.
    /// </summary>
    public bool ProjectDivergentSuccessResponses { get; set; } = true;

    /// <summary>
    /// Projects compatible object unions used by error responses into concrete error models so Kiota can
    /// generate error mappings. Enabled by default. Shared request and success schemas remain unchanged.
    /// This widens validation: required fields become the intersection of branch requirements and oneOf
    /// exclusivity is lost. The original schema is retained in x-client-original-error-schema.
    /// Compatible string enums are combined; conflicting fields retain arbitrary JSON while the exception
    /// remains typed. Closed objects and unsupported branch constraints are not projected.
    /// </summary>
    public bool ProjectCompatibleErrorResponses { get; set; } = true;

    /// <summary>
    /// Projects scalar, array, unrepresentable union, and unstructured JSON or text error responses to exception placeholders. Generated-client
    /// repair replaces their factories and serialization with lossless raw-payload handling. Original schemas
    /// are retained in x-client-raw-error-schema. Enabled by default; requires RepairGeneratedClient.
    /// </summary>
    public bool ProjectRawErrorResponses { get; set; } = true;

    /// <summary>
    /// Moves string formats unsupported by Kiota (including custom formats) to x-client-original-format.
    /// Enabled by default for client generation; validators no longer enforce the moved format. Supported formats such
    /// as uuid and date-time, and example payloads are preserved.
    /// </summary>
    public bool NormalizeUnsupportedStringFormats { get; set; } = true;

    /// <summary>
    /// Maps uint32 to int64 and uint64 to decimal so generated clients can represent the full unsigned range.
    /// Original types and formats are retained in extensions. Enabled by default. Decimal projection retains
    /// integer semantics by intersecting multipleOf with whole-number steps.
    /// </summary>
    public bool NormalizeUnsignedIntegerFormats { get; set; } = true;

    /// <summary>
    /// Maps unsupported integer formats to int64 (byte to int32) without changing their numeric wire values,
    /// and archives unsupported number formats without narrowing the number type. Enabled by default.
    /// </summary>
    public bool NormalizeUnsupportedNumericFormats { get; set; } = true;

    /// <summary>
    /// Archives format annotations on object, array, boolean, and null schemas, for which Kiota has no format codecs.
    /// Preserves schema types and wire values. Enabled by default.
    /// </summary>
    public bool NormalizeUnsupportedContainerFormats { get; set; } = true;

    /// <summary>
    /// Widens integer properties when declared bounds, defaults, examples, or enum values exceed their CLR range.
    /// Uses int64 or decimal with integer constraints, preserving numeric wire values. Enabled by default.
    /// </summary>
    public bool NormalizeIntegerRanges { get; set; } = true;

    /// <summary>
    /// Moves unreachable component schemas to x-client-unused-schemas so Kiota validates only schemas used by
    /// the client. Discriminator mappings and references from reusable components are retained. Enabled by default.
    /// </summary>
    public bool ArchiveUnusedSchemas { get; set; } = true;

    /// <summary>
    /// Selects the first root server, as Kiota does, and preserves alternatives in x-client-original-servers.
    /// When every operation has its own server, supplies a root default without changing those overrides.
    /// Generated clients can still select another server through the request adapter. Enabled by default.
    /// </summary>
    public bool SelectPrimaryServer { get; set; } = true;

    /// <summary>
    /// Presents complex URL-encoded request bodies as JSON models during generation, retaining their form
    /// schema and encoding in x-client-original-form-content. RepairGeneratedClient restores URL-encoded
    /// wire serialization with the generated form serializer. Already-encoded string bodies use a text projection
    /// and are transmitted unchanged. Enabled by default; requires generated-client repair.
    /// </summary>
    public bool ProjectComplexFormRequests { get; set; } = true;

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
    /// Overrides the OpenAPI version used for the fixed document. When unset, Swagger 2.0 is upgraded to
    /// OpenAPI 3.0 so component and response repairs apply consistently; OpenAPI 3.x versions are preserved.
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
