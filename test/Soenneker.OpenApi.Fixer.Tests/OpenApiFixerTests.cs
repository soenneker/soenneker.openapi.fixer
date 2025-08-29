using Soenneker.Facts.Local;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.FixturedUnit;
using System.Threading.Tasks;
using Xunit;

namespace Soenneker.OpenApi.Fixer.Tests;

[Collection("Collection")]
public sealed class OpenApiFixerTests : FixturedUnitTest
{
    private readonly IOpenApiFixer _util;

    public OpenApiFixerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<IOpenApiFixer>(true);
    }

    [Fact]
    public void Default()
    {

    }

    [LocalFact]
    public async ValueTask ProcessTelnyx()
    {
        const string fixedPath = @"c:\telnyx\spec3fixed.json";

        await _util.Fix(@"c:\telnyx\spec3.json", fixedPath, CancellationToken);

       // await _util.ProcessKiota(fixedPath, "TelnyxOpenApiClient", "Soenneker.Telnyx.OpenApiClient", @"c:\telnyx\src", CancellationToken);
    }

    //[LocalFact]
    //public async ValueTask ProcessCloudflare()
    //{
    //    const string fixedPath = @"c:\cloudflare\spec3fixed.json";

    //    await _util.Fix(@"c:\cloudflare\spec3.json", fixedPath, CancellationToken);

    //    await _util.ProcessKiota(fixedPath, "CloudflareOpenApiClient", "Soenneker.Cloudflare.OpenApiClient", @"c:\cloudflare\src", CancellationToken);
    //}
}
