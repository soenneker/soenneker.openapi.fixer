using Soenneker.OpenApi.Fixer;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Abstract;

/// <summary>
/// Normalizes OpenAPI JSON documents for generated-client compatibility and repairs generated Kiota enum members.
/// </summary>
public interface IOpenApiFixer
{
    /// <summary>
    /// Reads an OpenAPI JSON file, applies the default normalization rules, and replaces the target only after the result parses successfully.
    /// </summary>
    /// <param name="sourceFilePath">The source OpenAPI JSON file.</param>
    /// <param name="targetFilePath">The destination for the normalized JSON. An existing file is replaced after validation succeeds.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the operation.</returns>
    ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads an OpenAPI JSON file, applies the selected normalization rules, and replaces the target only after the result parses successfully.
    /// </summary>
    /// <param name="sourceFilePath">The source OpenAPI JSON file.</param>
    /// <param name="targetFilePath">The destination for the normalized JSON. An existing file is replaced after validation succeeds.</param>
    /// <param name="options">Optional transformations to apply in addition to the default normalization.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the operation.</returns>
    ValueTask Fix(string sourceFilePath, string targetFilePath, OpenApiFixerOptions? options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rewrites invalid member identifiers in Kiota-generated C# enum files while preserving their wire values.
    /// </summary>
    /// <param name="generatedRoot">The root directory containing generated C# files. A missing directory is ignored.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the operation.</returns>
    ValueTask SanitizeGeneratedEnumMembers(string generatedRoot, CancellationToken cancellationToken = default);
}
