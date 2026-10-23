using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiGeneratedFormTests : HostedUnitTest
{
    private readonly IOpenApiFixer _fixer;
    public OpenApiGeneratedFormTests(Host host) : base(host) => _fixer = Resolve<IOpenApiFixer>(true);

    [Test]
    public async ValueTask Repairs_match_route_and_method_preserve_json_and_are_idempotent(CancellationToken ct)
    {
        string root = Path.Combine(Path.GetTempPath(), System.Guid.NewGuid().ToString());
        Directory.CreateDirectory(root);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "fixed.json"), """
            {"paths":{"/things/{id}":{"post":{"requestBody":{"content":{"application/x-www-form-urlencoded":{"schema":{"type":"object"},"encoding":{"data":{"style":"deepObject","explode":true}}}}}},"patch":{"requestBody":{"content":{"application/json":{"schema":{"type":"object"}}}}}}}}
            """, ct);
            await File.WriteAllTextAsync(Path.Combine(root, "kiota-lock.json"), """{"descriptionLocation":"fixed.json"}""", ct);
            const string source = """
            [GeneratedCode("Kiota", "1.0.0")]
            public class ItemRequestBuilder
            {
                public ItemRequestBuilder() : base(requestAdapter, "{+baseurl}/things/{thing%2Did}{?q}", pathParameters) { }
                public void Post() {
                    var requestInfo = new RequestInformation(Method.POST, UrlTemplate, PathParameters);
                    requestInfo.SetContentFromParsable(RequestAdapter, "application/x-www-form-urlencoded", body);
                    return requestInfo;
                }
                public void Patch() {
                    var requestInfo = new RequestInformation(Method.PATCH, UrlTemplate, PathParameters);
                    requestInfo.SetContentFromParsable(RequestAdapter, "application/json", body);
                    return requestInfo;
                }
            }
            """;
            string file = Path.Combine(root, "ItemRequestBuilder.cs");
            await File.WriteAllTextAsync(file, source, ct);
            await File.WriteAllTextAsync(Path.Combine(root, "OtherRequestBuilder.cs"), source.Replace("/things/", "/other/"), ct);
            await File.WriteAllTextAsync(Path.Combine(root, "HandwrittenRequestBuilder.cs"), source.Replace("[GeneratedCode(\"Kiota\", \"1.0.0\")]", ""), ct);
            await _fixer.SanitizeGeneratedEnumMembers(root, ct);
            string result = await File.ReadAllTextAsync(file, ct);
            await Assert.That(result).Contains("OpenApiFormSerializer.SetContent(requestInfo, body,");
            await Assert.That(result).Contains("deepObject");
            await Assert.That(result).Contains("SetContentFromParsable(RequestAdapter, \"application/json\", body)");
            await Assert.That(await File.ReadAllTextAsync(Path.Combine(root, "OtherRequestBuilder.cs"), ct)).IsEqualTo(source.Replace("/things/", "/other/"));
            await Assert.That((await File.ReadAllTextAsync(Path.Combine(root, "HandwrittenRequestBuilder.cs"), ct)).Contains("OpenApiFormSerializer")).IsFalse();
            await Assert.That(File.Exists(Path.Combine(root, "Serialization", "OpenApiFormSerializer.cs"))).IsTrue();
            await _fixer.SanitizeGeneratedEnumMembers(root, ct);
            await Assert.That(await File.ReadAllTextAsync(file, ct)).IsEqualTo(result);
        }
        finally { Directory.Delete(root, true); }
    }
}
