using Microsoft.OpenApi;
using Soenneker.Tests.Attributes.Local;
using Soenneker.OpenApi.Fixer;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using Soenneker.Tests.HostedUnit;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Soenneker.Facts.Manual;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiFixerTests : HostedUnitTest
{
    private readonly IOpenApiFixer _util;
    private readonly IOpenApiNamingFixer _namingFixer;

    public OpenApiFixerTests(Host host) : base(host)
    {
        _util = Resolve<IOpenApiFixer>(true);
        _namingFixer = Resolve<IOpenApiNamingFixer>(true);
    }

    [Test]
    public void Default()
    {
    }

    [Test]
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

    [Test]
    public void RenameInvalidComponentSchemas_should_not_suffix_when_only_conflict_is_same_key_different_case()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["artifact"] = new OpenApiSchema
                    {
                        Title = "Artifact",
                        Type = JsonSchemaType.Object
                    },
                    ["repository"] = new OpenApiSchema
                    {
                        Title = "Repository",
                        Type = JsonSchemaType.Object
                    }
                }
            }
        };

        _namingFixer.RenameInvalidComponentSchemas(document);

        Assert.True(document.Components.Schemas.ContainsKey("Artifact"));
        Assert.True(document.Components.Schemas.ContainsKey("Repository"));
        Assert.False(document.Components.Schemas.ContainsKey("Artifact_1"));
        Assert.False(document.Components.Schemas.ContainsKey("Repository_1"));
        Assert.False(document.Components.Schemas.ContainsKey("artifact"));
        Assert.False(document.Components.Schemas.ContainsKey("repository"));
    }

    [Test]
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

    [Test]
    public async ValueTask Fix_should_not_transform_int32_id_fields_by_default()
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
                                  "paths": {
                                    "/widgets/{widgetId}": {
                                      "get": {
                                        "operationId": "GetWidget",
                                        "parameters": [
                                          {
                                            "name": "widgetId",
                                            "in": "path",
                                            "required": true,
                                            "schema": {
                                              "type": "integer",
                                              "format": "int32"
                                            }
                                          },
                                          {
                                            "name": "page",
                                            "in": "query",
                                            "schema": {
                                              "type": "integer",
                                              "format": "int32"
                                            }
                                          }
                                        ],
                                        "responses": {
                                          "200": {
                                            "description": "Success",
                                            "content": {
                                              "application/json": {
                                                "schema": {
                                                  "$ref": "#/components/schemas/Widget"
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  },
                                  "components": {
                                    "schemas": {
                                      "Widget": {
                                        "type": "object",
                                        "properties": {
                                          "id": {
                                            "type": "integer",
                                            "format": "int32"
                                          },
                                          "organizationId": {
                                            "type": "integer",
                                            "format": "int32"
                                          },
                                          "count": {
                                            "type": "integer",
                                            "format": "int32"
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, CancellationToken);

            await _util.Fix(sourcePath, targetPath, CancellationToken);

            JsonNode root = await ReadJsonNode(targetPath);

            Assert.Equal("int32", GetComponentPropertyFormat(root, "Widget", "id"));
            Assert.Equal("int32", GetComponentPropertyFormat(root, "Widget", "organizationId"));
            Assert.Equal("int32", GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "widgetId"));
            Assert.Equal("int32", GetComponentPropertyFormat(root, "Widget", "count"));
            Assert.Equal("int32", GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "page"));
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_transform_int32_id_fields_when_option_enabled()
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
                                  "paths": {
                                    "/widgets/{widgetId}": {
                                      "get": {
                                        "operationId": "GetWidget",
                                        "parameters": [
                                          {
                                            "name": "widgetId",
                                            "in": "path",
                                            "required": true,
                                            "schema": {
                                              "type": "integer",
                                              "format": "int32"
                                            }
                                          },
                                          {
                                            "name": "page",
                                            "in": "query",
                                            "schema": {
                                              "type": "integer",
                                              "format": "int32"
                                            }
                                          }
                                        ],
                                        "responses": {
                                          "200": {
                                            "description": "Success",
                                            "content": {
                                              "application/json": {
                                                "schema": {
                                                  "$ref": "#/components/schemas/Widget"
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  },
                                  "components": {
                                    "schemas": {
                                      "Widget": {
                                        "type": "object",
                                        "properties": {
                                          "id": {
                                            "type": "integer",
                                            "format": "int32"
                                          },
                                          "organizationId": {
                                            "type": "integer",
                                            "format": "int32"
                                          },
                                          "count": {
                                            "type": "integer",
                                            "format": "int32"
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, CancellationToken);

            await _util.Fix(sourcePath, targetPath, new OpenApiFixerOptions
            {
                Int32IdTransform = true
            }, CancellationToken);

            JsonNode root = await ReadJsonNode(targetPath);

            Assert.Equal("int64", GetComponentPropertyFormat(root, "Widget", "id"));
            Assert.Equal("int64", GetComponentPropertyFormat(root, "Widget", "organizationId"));
            Assert.Equal("int64", GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "widgetId"));
            Assert.Equal("int32", GetComponentPropertyFormat(root, "Widget", "count"));
            Assert.Equal("int32", GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "page"));
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public void ExtractInlineSchemas_should_preserve_semantic_response_schema_titles()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>()
            },
            Paths = new OpenApiPaths
            {
                ["/repos/{owner}/{repo}/actions/secrets"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new OpenApiOperation
                        {
                            OperationId = "ActionsListRepoSecrets",
                            Responses = new OpenApiResponses
                            {
                                ["200"] = new OpenApiResponse
                                {
                                    Description = "Success",
                                    Content = new Dictionary<string, IOpenApiMediaType>
                                    {
                                        ["application/json"] = new OpenApiMediaType
                                        {
                                            Schema = new OpenApiSchema
                                            {
                                                Title = "Repository Secrets",
                                                Type = JsonSchemaType.Object,
                                                Properties = new Dictionary<string, IOpenApiSchema>
                                                {
                                                    ["total_count"] = new OpenApiSchema
                                                    {
                                                        Type = JsonSchemaType.Integer
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
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineSchemas", document, CancellationToken);

        Assert.True(document.Components.Schemas.ContainsKey("RepositorySecrets"));
        Assert.False(document.Components.Schemas.ContainsKey("ActionsListRepoSecrets_200"));

        var pathItem = Assert.IsType<OpenApiPathItem>(document.Paths["/repos/{owner}/{repo}/actions/secrets"]);
        var operation = Assert.IsType<OpenApiOperation>(pathItem.Operations[HttpMethod.Get]);
        var response = Assert.IsType<OpenApiResponse>(operation.Responses["200"]);
        var mediaType = Assert.IsType<OpenApiMediaType>(response.Content["application/json"]);
        var schemaReference = Assert.IsType<OpenApiSchemaReference>(mediaType.Schema);

        Assert.Equal("RepositorySecrets", schemaReference.Reference.Id);
    }

    [Test]
    public void ExtractInlineSchemas_should_not_promote_simple_collection_envelopes()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["ActionsSecret"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object
                    }
                }
            },
            Paths = new OpenApiPaths
            {
                ["/repos/{owner}/{repo}/actions/secrets"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new OpenApiOperation
                        {
                            OperationId = "ActionsListRepoSecrets",
                            Responses = new OpenApiResponses
                            {
                                ["200"] = new OpenApiResponse
                                {
                                    Description = "Success",
                                    Content = new Dictionary<string, IOpenApiMediaType>
                                    {
                                        ["application/json"] = new OpenApiMediaType
                                        {
                                            Schema = new OpenApiSchema
                                            {
                                                Type = JsonSchemaType.Object,
                                                Required = new HashSet<string>
                                                {
                                                    "total_count",
                                                    "secrets"
                                                },
                                                Properties = new Dictionary<string, IOpenApiSchema>
                                                {
                                                    ["total_count"] = new OpenApiSchema
                                                    {
                                                        Type = JsonSchemaType.Integer
                                                    },
                                                    ["secrets"] = new OpenApiSchema
                                                    {
                                                        Type = JsonSchemaType.Array,
                                                        Items = new OpenApiSchemaReference("ActionsSecret")
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
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineSchemas", document, CancellationToken);

        Assert.False(document.Components.Schemas.ContainsKey("ActionsListRepoSecrets200"));

        var pathItem = Assert.IsType<OpenApiPathItem>(document.Paths["/repos/{owner}/{repo}/actions/secrets"]);
        var operation = Assert.IsType<OpenApiOperation>(pathItem.Operations[HttpMethod.Get]);
        var response = Assert.IsType<OpenApiResponse>(operation.Responses["200"]);
        var mediaType = Assert.IsType<OpenApiMediaType>(response.Content["application/json"]);
        var inlineSchema = Assert.IsType<OpenApiSchema>(mediaType.Schema);

        Assert.Equal(JsonSchemaType.Object, inlineSchema.Type);
        Assert.True(inlineSchema.Properties.ContainsKey("secrets"));
    }

    [Test]
    public void ExtractInlineObjectPropertySchemas_should_not_promote_composed_collection_properties()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["WorkersBindings"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Array,
                        Items = new OpenApiSchema
                        {
                            Type = JsonSchemaType.Object
                        }
                    },
                    ["WorkersNamespaceScriptAndVersionSettingsItem"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["bindings"] = new OpenApiSchema
                            {
                                AllOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchemaReference("WorkersBindings"),
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.Array,
                                        Items = new OpenApiSchema
                                        {
                                            Type = JsonSchemaType.Object
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineObjectPropertySchemas", document);

        Assert.False(document.Components.Schemas.ContainsKey("WorkersNamespaceScriptAndVersionSettingsItemBindings"));

        var container = Assert.IsType<OpenApiSchema>(document.Components.Schemas["WorkersNamespaceScriptAndVersionSettingsItem"]);
        Assert.NotNull(container.Properties);
        Assert.IsType<OpenApiSchema>(container.Properties["bindings"]);
    }

    [Test]
    public void FixContentTypeWrapperCollisions_should_rename_normalized_component_keys_and_update_request_refs()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["CreateWidgetapplicationJson"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["name"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.String
                            }
                        }
                    }
                }
            },
            Paths = new OpenApiPaths
            {
                ["/widgets"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Post] = new OpenApiOperation
                        {
                            OperationId = "CreateWidget",
                            RequestBody = new OpenApiRequestBody
                            {
                                Content = new Dictionary<string, IOpenApiMediaType>
                                {
                                    ["application/json"] = new OpenApiMediaType
                                    {
                                        Schema = new OpenApiSchemaReference("CreateWidgetapplicationJson")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "FixContentTypeWrapperCollisions", document);

        Assert.False(document.Components.Schemas.ContainsKey("CreateWidgetapplicationJson"));
        Assert.True(document.Components.Schemas.ContainsKey("CreateWidgetapplicationJson_Body"));

        var pathItem = Assert.IsType<OpenApiPathItem>(document.Paths["/widgets"]);
        var operation = Assert.IsType<OpenApiOperation>(pathItem.Operations[HttpMethod.Post]);
        var mediaType = Assert.IsType<OpenApiMediaType>(operation.RequestBody.Content["application/json"]);
        var schemaReference = Assert.IsType<OpenApiSchemaReference>(mediaType.Schema);

        Assert.Equal("CreateWidgetapplicationJson_Body", schemaReference.Reference.Id);
    }

    private static void InvokePrivateVoidMethod(object target, string methodName, params object[] args)
    {
        MethodInfo method = typeof(OpenApiFixer).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)!;
        Assert.NotNull(method);
        method.Invoke(target, args);
    }

    private static async ValueTask<JsonNode> ReadJsonNode(string path)
    {
        string contents = await File.ReadAllTextAsync(path);
        return JsonNode.Parse(contents)!;
    }

    private static string? GetComponentPropertyFormat(JsonNode root, string schemaName, string propertyName)
    {
        return root["components"]?["schemas"]?[schemaName]?["properties"]?[propertyName]?["format"]?.GetValue<string>();
    }

    private static string? GetOperationParameterFormat(JsonNode root, string path, string httpMethod, string parameterName)
    {
        JsonArray? parameters = root["paths"]?[path]?[httpMethod]?["parameters"] as JsonArray;

        JsonObject? parameter = parameters?
                                .OfType<JsonNode>()
                                .Select(node => node as JsonObject)
                                .FirstOrDefault(node => string.Equals(node?["name"]?.GetValue<string>(), parameterName, System.StringComparison.Ordinal));

        return parameter?["schema"]?["format"]?.GetValue<string>();
    }

    [ManualFact]
    // [LocalOnly]
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
    //[LocalOnly]
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
    //[LocalOnly]
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
    //[LocalOnly]
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