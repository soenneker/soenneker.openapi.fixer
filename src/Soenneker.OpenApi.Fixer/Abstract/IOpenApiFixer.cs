using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Abstract;

public interface IOpenApiFixer
{
    ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default);

    ValueTask GenerateKiota(string fixedPath, string clientName, string libraryName, string targetDir,
        CancellationToken cancellationToken = default);
}