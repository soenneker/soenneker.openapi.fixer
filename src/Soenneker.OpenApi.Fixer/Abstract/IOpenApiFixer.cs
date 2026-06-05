using Soenneker.OpenApi.Fixer;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Abstract;

/// <summary>
/// Defines the open api fixer contract.
/// </summary>
public interface IOpenApiFixer
{
    /// <summary>
    /// Executes the fix operation.
    /// </summary>
    /// <param name="sourceFilePath">The source file path.</param>
    /// <param name="targetFilePath">The target file path.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes the fix operation.
    /// </summary>
    /// <param name="sourceFilePath">The source file path.</param>
    /// <param name="targetFilePath">The target file path.</param>
    /// <param name="options">The options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    ValueTask Fix(string sourceFilePath, string targetFilePath, OpenApiFixerOptions? options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes the sanitize generated enum members operation.
    /// </summary>
    /// <param name="generatedRoot">The generated root.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    ValueTask SanitizeGeneratedEnumMembers(string generatedRoot, CancellationToken cancellationToken = default);
}