using Soenneker.Facts.Local;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.FixturedUnit;
using System.IO;
using System.Threading.Tasks;
using Soenneker.Utils.Directory.Abstract;
using Xunit;

namespace Soenneker.OpenApi.Fixer.Tests;

[Collection("Collection")]
public sealed class OpenApiFixerTests : FixturedUnitTest
{
    private readonly IOpenApiFixer _util;
    private readonly IDirectoryUtil _directoryUtil;

    public OpenApiFixerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<IOpenApiFixer>(true);
        _directoryUtil = Resolve<IDirectoryUtil>(true);
    }

    [Fact]
    public void Default()
    {

    }

    [LocalFact]
    public async ValueTask ProcessTelnyx()
    {
        const string fixedPath = @"c:\telnyx\spec3fixed.json";
        const string targetDir = @"c:\telnyx\src";

        File.Delete(fixedPath);

        await _util.Fix(@"c:\telnyx\spec3.json", fixedPath, CancellationToken);

        _directoryUtil.DeleteIfExists(targetDir);
        _directoryUtil.CreateIfDoesNotExist(targetDir);

        await _util.GenerateKiota(fixedPath, "TelnyxOpenApiClient", "Soenneker.Telnyx.OpenApiClient", targetDir, CancellationToken);
    }

    [LocalFact]
    public async ValueTask ProcessCloudflare()
    {
        const string fixedPath = @"c:\cloudflare\spec3fixed.json";
        const string targetDir = @"c:\cloudflare\src";

        File.Delete(fixedPath);

        await _util.Fix(@"c:\cloudflare\spec3.json", fixedPath, CancellationToken);

        _directoryUtil.DeleteIfExists(targetDir);
        _directoryUtil.CreateIfDoesNotExist(targetDir);

        await _util.GenerateKiota(fixedPath, "CloudflareOpenApiClient", "Soenneker.Cloudflare.OpenApiClient", targetDir, CancellationToken);
    }
}
