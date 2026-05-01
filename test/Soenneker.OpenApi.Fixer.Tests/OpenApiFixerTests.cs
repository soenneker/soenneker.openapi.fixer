using System;
using Microsoft.OpenApi;
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
    public async ValueTask RenameInvalidComponentSchemas_should_pascalize_separator_based_schema_names_and_update_refs()
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

        await Assert.That(document.Components.Schemas.ContainsKey("GitTag")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("git-tag")).IsFalse();

        IOpenApiPathItem pathItem = document.Paths["/git/tags/{sha}"]!;
        OpenApiOperation operation = pathItem.Operations[HttpMethod.Get]!;
        IOpenApiResponse response = operation.Responses["200"]!;
        IOpenApiMediaType mediaType = response.Content["application/json"]!;
        var schemaReference = mediaType.Schema as OpenApiSchemaReference;

        await Assert.That(schemaReference).IsNotNull();
        await Assert.That(schemaReference!.Reference.Id).IsEqualTo("GitTag");
    }

    [Test]
    public async ValueTask RenameInvalidComponentSchemas_should_not_suffix_when_only_conflict_is_same_key_different_case()
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

        await Assert.That(document.Components.Schemas.ContainsKey("Artifact")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("Repository")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("Artifact_1")).IsFalse();
        await Assert.That(document.Components.Schemas.ContainsKey("Repository_1")).IsFalse();
        await Assert.That(document.Components.Schemas.ContainsKey("artifact")).IsFalse();
        await Assert.That(document.Components.Schemas.ContainsKey("repository")).IsFalse();
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

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            string fixedSpec = await File.ReadAllTextAsync(targetPath, System.Threading.CancellationToken.None);

            await Assert.That(fixedSpec).Contains("\"CodeScanningVariantAnalysisSkippedRepositories\": {");
            await Assert.That(fixedSpec).Contains("\"CodeScanningVariantAnalysisSkippedRepositoriesNotFoundRepos\": {");
            await Assert.That(fixedSpec).Contains("\"$ref\": \"#/components/schemas/CodeScanningVariantAnalysisSkippedRepositories\"");
            await Assert.That(fixedSpec).Contains("\"$ref\": \"#/components/schemas/CodeScanningVariantAnalysisSkippedRepositoriesNotFoundRepos\"");
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

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);

            await Assert.That(GetComponentPropertyFormat(root, "Widget", "id")).IsEqualTo("int32");
            await Assert.That(GetComponentPropertyFormat(root, "Widget", "organizationId")).IsEqualTo("int32");
            await Assert.That(GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "widgetId")).IsEqualTo("int32");
            await Assert.That(GetComponentPropertyFormat(root, "Widget", "count")).IsEqualTo("int32");
            await Assert.That(GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "page")).IsEqualTo("int32");
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

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, new OpenApiFixerOptions
            {
                Int32IdTransform = true
            }, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);

            await Assert.That(GetComponentPropertyFormat(root, "Widget", "id")).IsEqualTo("int64");
            await Assert.That(GetComponentPropertyFormat(root, "Widget", "organizationId")).IsEqualTo("int64");
            await Assert.That(GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "widgetId")).IsEqualTo("int64");
            await Assert.That(GetComponentPropertyFormat(root, "Widget", "count")).IsEqualTo("int32");
            await Assert.That(GetOperationParameterFormat(root, "/widgets/{widgetId}", "get", "page")).IsEqualTo("int32");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_strip_date_suffixes_from_generated_names_when_option_enabled()
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
                                    "/assistant_control_2026-04/assistants": {
                                      "get": {
                                        "operationId": "assistant-control-2026-04-list-assistants",
                                        "responses": {
                                          "500": {
                                            "description": "Error",
                                            "content": {
                                              "application/json": {
                                                "schema": {
                                                  "$ref": "#/components/schemas/AssistantControl202604ErrorResponse"
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
                                      "AssistantControl202604ErrorResponse": {
                                        "type": "object",
                                        "properties": {
                                          "message": {
                                            "type": "string"
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, new OpenApiFixerOptions
            {
                StripDateSuffixesFromGeneratedNames = true
            }, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);

            await Assert.That(root["paths"]?["/assistant_control/assistants"]).IsNotNull();
            await Assert.That(root["paths"]?["/assistant_control_2026-04/assistants"]).IsNull();
            await Assert.That(root["paths"]?["/assistant_control/assistants"]?["get"]?["operationId"]?.GetValue<string>())
                        .IsEqualTo("assistant-control-list-assistants");
            await Assert.That(root["components"]?["schemas"]?["AssistantControlErrorResponse"]).IsNotNull();
            await Assert.That(root["components"]?["schemas"]?["AssistantControl202604ErrorResponse"]).IsNull();
            await Assert.That(root["paths"]?["/assistant_control/assistants"]?["get"]?["responses"]?["500"]?["content"]?["application/json"]?["schema"]?["$ref"]
                                  ?.GetValue<string>())
                        .IsEqualTo("#/components/schemas/AssistantControlErrorResponse");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask ExtractInlineSchemas_should_preserve_semantic_response_schema_titles()
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

        InvokePrivateVoidMethod(_util, "ExtractInlineSchemas", document, System.Threading.CancellationToken.None);

        await Assert.That(document.Components.Schemas.ContainsKey("RepositorySecrets")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("ActionsListRepoSecrets_200")).IsFalse();

        IOpenApiPathItem pathItem = document.Paths["/repos/{owner}/{repo}/actions/secrets"]!;
        OpenApiOperation operation = pathItem.Operations[HttpMethod.Get]!;
        IOpenApiResponse response = operation.Responses["200"]!;
        IOpenApiMediaType mediaType = response.Content["application/json"]!;
        var schemaReference = mediaType.Schema as OpenApiSchemaReference;

        await Assert.That(schemaReference).IsNotNull();
        await Assert.That(schemaReference!.Reference.Id).IsEqualTo("RepositorySecrets");
    }

    [Test]
    public async ValueTask ExtractInlineSchemas_should_not_promote_simple_collection_envelopes()
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

        InvokePrivateVoidMethod(_util, "ExtractInlineSchemas", document, System.Threading.CancellationToken.None);

        await Assert.That(document.Components.Schemas.ContainsKey("ActionsListRepoSecrets200")).IsFalse();

        IOpenApiPathItem pathItem = document.Paths["/repos/{owner}/{repo}/actions/secrets"]!;
        OpenApiOperation operation = pathItem.Operations[HttpMethod.Get]!;
        IOpenApiResponse response = operation.Responses["200"]!;
        IOpenApiMediaType mediaType = response.Content["application/json"]!;
        var inlineSchema = mediaType.Schema as OpenApiSchema;

        await Assert.That(inlineSchema).IsNotNull();
        await Assert.That(inlineSchema!.Type).IsEqualTo(JsonSchemaType.Object);
        await Assert.That(inlineSchema!.Properties!.ContainsKey("secrets")).IsTrue();
    }

    [Test]
    public async ValueTask ExtractInlineObjectPropertySchemas_should_not_promote_composed_collection_properties()
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

        await Assert.That(document.Components.Schemas.ContainsKey("WorkersNamespaceScriptAndVersionSettingsItemBindings")).IsFalse();

        var container = document.Components.Schemas["WorkersNamespaceScriptAndVersionSettingsItem"] as OpenApiSchema;
        await Assert.That(container).IsNotNull();
        await Assert.That(container!.Properties).IsNotNull();
        await Assert.That(container!.Properties!["bindings"] as OpenApiSchema).IsNotNull();
    }

    [Test]
    public async ValueTask FixContentTypeWrapperCollisions_should_rename_normalized_component_keys_and_update_request_refs()
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

        await Assert.That(document.Components.Schemas.ContainsKey("CreateWidgetapplicationJson")).IsFalse();
        await Assert.That(document.Components.Schemas.ContainsKey("CreateWidgetapplicationJson_Body")).IsTrue();

        IOpenApiPathItem pathItem = document.Paths["/widgets"]!;
        OpenApiOperation operation = pathItem.Operations[HttpMethod.Post]!;
        IOpenApiMediaType mediaType = operation.RequestBody!.Content["application/json"];
        var schemaReference = mediaType.Schema as OpenApiSchemaReference;

        await Assert.That(schemaReference).IsNotNull();
        await Assert.That(schemaReference!.Reference.Id).IsEqualTo("CreateWidgetapplicationJson_Body");
    }

    private static void InvokePrivateVoidMethod(object target, string methodName, params object[] args)
    {
        MethodInfo? method = typeof(OpenApiFixer).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);

        if (method is null)
            throw new InvalidOperationException($"Could not find private method '{methodName}'.");

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

    [Test]
    [Skip("Manual")]
    // [LocalOnly]
    public async ValueTask ProcessHubSpot()
    {
        const string sourcePath = @"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\merged.json";
        const string fixedPath = @"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\fixed.json";
        File.Delete(fixedPath);

        await _util.Fix(sourcePath, fixedPath, System.Threading.CancellationToken.None);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "HubSpotOpenApiClient", "Soenneker.HubSpot.OpenApiClient", targetDir, System.Threading.CancellationToken.None);
    }

    [Test]
    [Skip("Manual")]
    //[LocalOnly]
    public async ValueTask ProcessCoinbase()
    {
        const string fixedPath = @"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\spec3fixed.json";
        File.Delete(fixedPath);

        await _util.Fix(@"C:\git\Soenneker\OpenApi\soenneker.openapi.fixer\coinbase.json", fixedPath, System.Threading.CancellationToken.None);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "CoinbaseOpenApiClient", "Soenneker.Coinbase.OpenApiClient", targetDir, System.Threading.CancellationToken.None);
    }

    [Test]
    [Skip("Manual")]
    //[LocalOnly]
    public async ValueTask ProcessTelnyx()
    {
        const string fixedPath = @"c:\telnyx\spec3fixed.json";
        File.Delete(fixedPath);

        await _util.Fix(@"c:\telnyx\spec3.json", fixedPath, System.Threading.CancellationToken.None);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "TelnyxOpenApiClient", "Soenneker.Telnyx.OpenApiClient", targetDir, System.Threading.CancellationToken.None);
    }

    [Test]
    [Skip("Manual")]
    //[LocalOnly]
    public async ValueTask ProcessCloudflare()
    {
        const string fixedPath = @"c:\cloudflare\spec3fixed.json";
        File.Delete(fixedPath);

        await _util.Fix(@"c:\cloudflare\spec3.json", fixedPath, System.Threading.CancellationToken.None);

        //await _directoryUtil.DeleteIfExists(targetDir);
        //await _directoryUtil.Create(targetDir);

        //await _util.GenerateKiota(fixedPath, "CloudflareOpenApiClient", "Soenneker.Cloudflare.OpenApiClient", targetDir, System.Threading.CancellationToken.None);
    }
}
