namespace Soenneker.OpenApi.Fixer.Fixers.Abstract;

/// <summary>
/// Fixes raw OpenAPI JSON before it is read by the OpenAPI parser.
/// </summary>
public interface IOpenApiPreprocessingFixer
{
    /// <summary>
    /// Normalizes raw OpenAPI JSON so the parser can read malformed-but-recoverable specs.
    /// </summary>
    /// <param name="json">The raw OpenAPI JSON.</param>
    /// <returns>The normalized JSON.</returns>
    string Fix(string json);
}
