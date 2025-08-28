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
    public async Task Fix()
    {
        var path = @"c:\telnyx\spec3.json";

        await _util.Fix(path, @"c:\telnyx\spec3-fixed.json", CancellationToken);
    }
}
