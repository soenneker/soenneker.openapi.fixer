using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Abstract;

public interface IOpenApiFixer
{
    ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default);
}