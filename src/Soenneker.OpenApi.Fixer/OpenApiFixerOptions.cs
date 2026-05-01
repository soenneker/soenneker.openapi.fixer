namespace Soenneker.OpenApi.Fixer;

/// <summary>
/// Optional behaviors for <see cref="OpenApiFixer"/>.
/// </summary>
public sealed class OpenApiFixerOptions
{
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
}
