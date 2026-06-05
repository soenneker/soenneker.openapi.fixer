using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer.Fixers.Abstract;

/// <summary>
/// Transforms integer properties and parameters whose names end with <c>Id</c> from <c>int32</c> to <c>int64</c>.
/// </summary>
public interface IOpenApiInt32IdFixer
{
    /// <summary>
    /// Executes the transform operation.
    /// </summary>
    /// <param name="document">The document.</param>
    void Transform(OpenApiDocument document);
}
