using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Abstract;

/// <summary>
/// Normalizes OpenAPI JSON documents for generated-client compatibility and repairs generated Kiota enum members.
/// </summary>
public interface IOpenApiFixer
{
    /// <summary>
    /// Repairs inline response operation-ID links in OpenAPI JSON files beneath a directory before merging them, updating changed files in place.
    /// </summary>
    /// <remarks>
    /// Local operation IDs take precedence. Unique cross-document IDs become relative operation references.
    /// Unresolved or ambiguous operation-ID links are removed with a warning; operation references, API operations,
    /// schemas, reusable link components, and example payloads are preserved. Files without an OpenAPI declaration are skipped.
    /// </remarks>
    /// <param name="directoryPath">The directory containing all JSON documents to be merged.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the operation.</returns>
    ValueTask FixOperationLinks(string directoryPath, CancellationToken cancellationToken = default);

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
    /// <param name="options">Optional normalization and logging settings. Logging includes file paths, major processing stages, completion timing, warnings, and errors by default, with settings captured independently for each call.</param>
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
