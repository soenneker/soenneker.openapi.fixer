using Soenneker.Facts.Local;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.FixturedUnit;
using System.Threading.Tasks;
using Soenneker.Extensions.ValueTask;
using Soenneker.Utils.Process.Abstract;
using Xunit;

namespace Soenneker.OpenApi.Fixer.Tests;

[Collection("Collection")]
public sealed class OpenApiFixerTests : FixturedUnitTest
{
    private readonly IOpenApiFixer _util;
    private readonly IProcessUtil _processUtil;

    public OpenApiFixerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<IOpenApiFixer>(true);
        _processUtil = Resolve<IProcessUtil>(true);
    }

    [Fact]
    public void Default()
    {

    }

    [LocalFact]
    public async Task Fix()
    {
        var path = @"c:\cloudflare\spec3.json";
        var fixedPath = @"c:\cloudflare\spec3fixed.json";

        await _util.Fix(path, fixedPath, CancellationToken);

        await _processUtil.Start("kiota", "c:\\cloudflare\\src", $"kiota generate -l CSharp -d \"{fixedPath}\" -o src -c CloudflareOpenApiClient -n Soenneker.Cloudflare.OpenApiClient --ebc --cc",
                              waitForExit: true, cancellationToken: CancellationToken)
                          .NoSync();
    }
}
