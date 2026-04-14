using Soenneker.OpenApi.Fixer;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Abstract;

public interface IOpenApiFixer
{
    ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default);

    ValueTask Fix(string sourceFilePath, string targetFilePath, OpenApiFixerOptions? options, CancellationToken cancellationToken = default);

    ValueTask SanitizeGeneratedEnumMembers(string generatedRoot, CancellationToken cancellationToken = default);
}