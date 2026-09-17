namespace Soenneker.OpenApi.Fixer.Fixers.Abstract;

/// <summary>
/// Fixes raw OpenAPI JSON before it is read by the OpenAPI parser.
/// </summary>
public interface IOpenApiPreprocessingFixer
{
    /// <summary>
    /// Normalizes raw OpenAPI JSON so the parser can read malformed-but-recoverable specs.
    /// </summary>
    /// <remarks>
    /// Recovers loose JSON syntax, duplicate properties (last occurrence wins, with a warning), metadata,
    /// schema field shapes, and path parameter declarations. Schema traversal distinguishes schema maps from
    /// examples, defaults, constants, and extension payloads. Unparseable input is returned unchanged for the
    /// document loader to report; recovery does not guarantee a valid contract for arbitrary input.
    /// </remarks>
    /// <param name="json">The raw OpenAPI JSON.</param>
    /// <returns>The normalized JSON.</returns>
    /// <param name="options">Optional preprocessing and logging behavior. Standalone calls honor the logging settings; calls made during a fix use that operation's captured settings.</param>
    string Fix(string json, OpenApiFixerOptions? options = null);
}
