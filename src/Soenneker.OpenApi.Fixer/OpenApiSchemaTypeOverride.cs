namespace Soenneker.OpenApi.Fixer;

/// <summary>
/// An explicit correction to a schema's declared type and format.
/// </summary>
public sealed class OpenApiSchemaTypeOverride
{
    /// <summary>
    /// Replacement JSON Schema type: <c>string</c>, <c>number</c>, <c>integer</c>, <c>boolean</c>,
    /// <c>object</c>, or <c>array</c>. Existing nullability is preserved.
    /// </summary>
    public required string Type { get; set; }

    /// <summary>
    /// Replacement format, for example <c>double</c> or <c>int64</c>.
    /// When null, removes the original format rather than carrying an incompatible format forward.
    /// </summary>
    public string? Format { get; set; }
}
