using Microsoft.OpenApi;
using Soenneker.Facts.Local;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using Soenneker.Tests.FixturedUnit;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Soenneker.Facts.Manual;
using Xunit;

namespace Soenneker.OpenApi.Fixer.Tests;

[Collection("Collection")]
public sealed class OpenApiFixerTests : FixturedUnitTest
{
    private readonly IOpenApiFixer _util;
    private readonly IOpenApiNamingFixer _namingFixer;

    public OpenApiFixerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<IOpenApiFixer>(true);
        _namingFixer = Resolve<IOpenApiNamingFixer>(true);
    }

    [Fact]
    public void Default()
    {
    }

    [Fact]
    public void RenameInvalidComponentSchemas_should_pascalize_separator_based_schema_names_and_update_refs()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["git-tag"] = new OpenApiSchema
                    {
                        Title = "Git Tag",
                        Type = JsonSchemaType.Object
                    }
                }
            },
            Paths = new OpenApiPaths
            {
                ["/git/tags/{sha}"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new OpenApiOperation
                        {
                            Responses = new OpenApiResponses
                            {
                                ["200"] = new OpenApiResponse
                                {
                                    Content = new Dictionary<string, IOpenApiMediaType>
                                    {
                                        ["application/json"] = new OpenApiMediaType
                                        {
                                            Schema = new OpenApiSchemaReference("git-tag")
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        _namingFixer.RenameInvalidComponentSchemas(document);

        Assert.True(document.Components.Schemas.ContainsKey("GitTag"));
        Assert.False(document.Components.Schemas.ContainsKey("git-tag"));

        var pathItem = Assert.IsType<OpenApiPathItem>(document.Paths["/git/tags/{sha}"]);
        var operations = Assert.IsAssignableFrom<IDictionary<HttpMethod, OpenApiOperation>>(pathItem.Operations);
        var operation = Assert.IsType<OpenApiOperation>(operations[HttpMethod.Get]);
        var responses = Assert.IsAssignableFrom<OpenApiResponses>(operation.Responses);
        var response = Assert.IsType<OpenApiResponse>(responses["200"]);
        var content = Assert.IsAssignableFrom<IDictionary<string, IOpenApiMediaType>>(response.Content);
        var mediaType = Assert.IsType<OpenApiMediaType>(content["application/json"]);
        var schemaReference = Assert.IsType<OpenApiSchemaReference>(mediaType.Schema);

        Assert.Equal("GitTag", schemaReference.Reference.Id);
    }

    [Fact]
    public async ValueTask Fix_should_promote_inline_object_properties_to_pascalized_component_names()
    {
        string sourcePath = Path.GetTempFileName();
        string targetPath = Path.GetTempFileName();

        try
        {
            File.Delete(targetPath);

            const string spec = """
                                {
                                  "openapi": "3.0.1",
                                  "info": {
                                    "title": "Test",
                                    "version": "1.0.0"
                                  },
                                  "paths": {},
                                  "components": {
                                    "schemas": {
                                      "code-scanning-variant-analysis": {
                                        "type": "object",
                                        "properties": {
                                          "skipped_repositories": {
                                            "type": "object",
                                            "properties": {
                                              "not_found_repos": {
                                                "type": "object",
                                                "properties": {
                                                  "repository_count": {
                                                    "type": "integer"
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, CancellationToken);

            await _util.Fix(sourcePath, targetPath, CancellationToken);

            string fixedSpec = await File.ReadAllTextAsync(targetPath, CancellationToken);

            Assert.Contains("\"CodeScanningVariantAnalysisSkippedRepositories\": {", fixedSpec);
            Assert.Contains("\"CodeScanningVariantAnalysisSkippedRepositoriesNotFoundRepos\": {", fixedSpec);
            Assert.Contains("\"$ref\": \"#/components/schemas/CodeScanningVariantAnalysisSkippedRepositories\"", fixedSpec);
            Assert.Contains("\"$ref\": \"#/components/schemas/CodeScanningVariantAnalysisSkippedRepositoriesNotFoundRepos\"", fixedSpec);
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [ManualFact]
    // [LocalFact]
    public async ValueTask ProcessHubSpot()
    {
        const string sourcePath = @"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\merged.json";
        const string fixedPath = @"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\fixed.json";
        const string targetDir = @"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\hubspot-src";

        File.Delete(fixedPath);

        await _util.Fix(sourcePath, fixedPath, CancellationToken);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "HubSpotOpenApiClient", "Soenneker.HubSpot.OpenApiClient", targetDir, CancellationToken);
    }

    [ManualFact]
    //[LocalFact]
    public async ValueTask ProcessCoinbase()
    {
        const string fixedPath = @"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\spec3fixed.json";
        const string targetDir = @"c:\cloudflare\src";

        File.Delete(fixedPath);

        await _util.Fix(@"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\coinbase.json", fixedPath, CancellationToken);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "CoinbaseOpenApiClient", "Soenneker.Coinbase.OpenApiClient", targetDir, CancellationToken);
    }

    [ManualFact]
    //[LocalFact]
    public async ValueTask ProcessTelnyx()
    {
        const string fixedPath = @"c:\telnyx\spec3fixed.json";
        const string targetDir = @"c:\telnyx\src";

        File.Delete(fixedPath);

        await _util.Fix(@"c:\telnyx\spec3.json", fixedPath, CancellationToken);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "TelnyxOpenApiClient", "Soenneker.Telnyx.OpenApiClient", targetDir, CancellationToken);
    }

    [ManualFact]
    //[LocalFact]
    public async ValueTask ProcessCloudflare()
    {
        const string fixedPath = @"c:\cloudflare\spec3fixed.json";
        const string targetDir = @"c:\cloudflare\src";

        File.Delete(fixedPath);

        await _util.Fix(@"c:\cloudflare\spec3.json", fixedPath, CancellationToken);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "CloudflareOpenApiClient", "Soenneker.Cloudflare.OpenApiClient", targetDir, CancellationToken);
    }
}