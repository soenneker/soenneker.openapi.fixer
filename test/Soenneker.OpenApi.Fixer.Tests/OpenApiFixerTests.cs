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
    private readonly IOpenApiSchemaFixer _schemaFixer;

    public OpenApiFixerTests(Host host) : base(host)
    {
        _util = Resolve<IOpenApiFixer>(true);
        _namingFixer = Resolve<IOpenApiNamingFixer>(true);
        _schemaFixer = Resolve<IOpenApiSchemaFixer>(true);
    }

    [Test]
    public void Default()
    {
    }

    [Test]
    public async ValueTask Fix_should_normalize_loose_boolean_schema_fields_before_reading()
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
                                    "/widgets": {
                                      "get": {
                                        "operationId": "GetWidgets",
                                        "parameters": [
                                          {
                                            "name": "cursor",
                                            "in": "query",
                                            "schema": {
                                              "type": "string",
                                              "nullable": "0"
                                            }
                                          }
                                        ],
                                        "responses": {
                                          "200": {
                                            "description": "Success",
                                            "content": {
                                              "application/json": {
                                                "schema": {
                                                  "type": "object",
                                                  "readOnly": 0,
                                                  "properties": {
                                                    "id": {
                                                      "type": "string"
                                                    }
                                                  }
                                                },
                                                "example": {
                                                  "deprecated": "0"
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

            await Assert.That(fixedSpec).DoesNotContain("\"nullable\": \"0\"");
            await Assert.That(fixedSpec).DoesNotContain("\"readOnly\": 0");
            await Assert.That(fixedSpec).Contains("\"deprecated\": \"0\"");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_preserve_primitive_only_oneof_unions()
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
                                      "SendEmailRequest": {
                                        "type": "object",
                                        "properties": {
                                          "to": {
                                            "description": "Recipient email address. For multiple addresses, send as an array of strings.",
                                            "oneOf": [
                                              {
                                                "type": "string"
                                              },
                                              {
                                                "type": "array",
                                                "items": {
                                                  "type": "string"
                                                }
                                              }
                                            ]
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
            JsonNode? to = root["components"]?["schemas"]?["SendEmailRequest"]?["properties"]?["to"];
            JsonNode? toSchema = root["components"]?["schemas"]?["SendEmailRequestTo"];

            await Assert.That(to?["$ref"]?.GetValue<string>()).IsEqualTo("#/components/schemas/SendEmailRequestTo");
            await Assert.That(toSchema?["oneOf"]?.AsArray().Count).IsEqualTo(2);
            await Assert.That(toSchema?["oneOf"]?[0]?["type"]?.GetValue<string>()).IsEqualTo("string");
            await Assert.That(toSchema?["oneOf"]?[1]?["type"]?.GetValue<string>()).IsEqualTo("array");
            await Assert.That(toSchema?["oneOf"]?[1]?["items"]?["type"]?.GetValue<string>()).IsEqualTo("string");
            await Assert.That(root.ToJsonString()).DoesNotContain("#/components/schemas/UnionBranch");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_fold_metadata_only_allof_branches_into_promoted_components()
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
                                      "purchase_unit_request": {
                                        "type": "object",
                                        "required": [
                                          "amount"
                                        ],
                                        "properties": {
                                          "amount": {
                                            "type": "string"
                                          }
                                        }
                                      },
                                      "order_request": {
                                        "type": "object",
                                        "properties": {
                                          "purchase_units": {
                                            "type": "array",
                                            "items": {
                                              "allOf": [
                                                {
                                                  "$ref": "#/components/schemas/purchase_unit_request"
                                                },
                                                {
                                                  "title": "purchase_unit",
                                                  "description": "The purchase unit."
                                                }
                                              ]
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

            JsonNode root = await ReadJsonNode(targetPath);
            JsonNode? itemSchema = root["components"]?["schemas"]?["OrderRequestPurchaseUnitsItem"];

            await Assert.That(itemSchema).IsNotNull();
            await Assert.That(itemSchema?["allOf"]).IsNull();
            await Assert.That(itemSchema?["title"]?.GetValue<string>()).IsEqualTo("purchase_unit");
            await Assert.That(itemSchema?["description"]?.GetValue<string>()).IsEqualTo("The purchase unit.");
            await Assert.That(itemSchema?["properties"]?["amount"]?["type"]?.GetValue<string>()).IsEqualTo("string");
            await Assert.That(itemSchema?["required"]?.AsArray().Any(value => value?.GetValue<string>() == "amount") ?? false).IsTrue();
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask WrapNonObjectUnionBranchesEverywhere_should_use_contextual_wrapper_names_for_inline_branches()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["Widget"] = new OpenApiSchema
                    {
                        OneOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                Properties = new Dictionary<string, IOpenApiSchema>
                                {
                                    ["id"] = new OpenApiSchema { Type = JsonSchemaType.String }
                                }
                            },
                            new OpenApiSchema
                            {
                                Type = JsonSchemaType.String
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "WrapNonObjectUnionBranchesEverywhere", document);

        await Assert.That(document.Components.Schemas.ContainsKey("UnionBranch")).IsFalse();
        await Assert.That(document.Components.Schemas.ContainsKey("WidgetBranch2")).IsTrue();

        var widget = document.Components.Schemas["Widget"] as OpenApiSchema;
        var wrapperReference = widget!.OneOf![1] as OpenApiSchemaReference;

        await Assert.That(wrapperReference).IsNotNull();
        await Assert.That(wrapperReference!.Reference.Id).IsEqualTo("WidgetBranch2");
    }

    [Test]
    public async ValueTask ApplySchemaNormalizations_should_not_add_discriminator_to_primitive_only_union()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["CcInstanceType"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        AnyOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchema { Type = JsonSchemaType.String }
                        }
                    }
                }
            },
            Paths = new OpenApiPaths()
        };

        InvokePrivateVoidMethod(_util, "ApplySchemaNormalizations", document, System.Threading.CancellationToken.None);

        var ccInstanceType = document.Components.Schemas["CcInstanceType"] as OpenApiSchema;

        await Assert.That(ccInstanceType).IsNotNull();
        await Assert.That(ccInstanceType!.Discriminator).IsNull();
        await Assert.That(ccInstanceType.Properties?.ContainsKey("type") ?? false).IsFalse();
        await Assert.That(ccInstanceType.Required?.Contains("type") ?? false).IsFalse();
    }

    [Test]
    public async ValueTask Fix_should_handle_recursive_composition_refs()
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
                                      "RecursiveA": {
                                        "anyOf": [
                                          {
                                            "$ref": "#/components/schemas/RecursiveB"
                                          }
                                        ]
                                      },
                                      "RecursiveB": {
                                        "anyOf": [
                                          {
                                            "$ref": "#/components/schemas/RecursiveA"
                                          },
                                          {
                                            "type": "object",
                                            "properties": {
                                              "id": {
                                                "type": "string"
                                              }
                                            }
                                          }
                                        ]
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);

            await Assert.That(root["components"]?["schemas"]?["RecursiveA"]).IsNotNull();
            await Assert.That(root["components"]?["schemas"]?["RecursiveB"]).IsNotNull();
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask RemoveDiscriminatorsFromNonObjectSchemas_should_remove_synthetic_discriminator_from_primitive_union()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["CcInstanceType"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        AnyOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchemaReference("CcInstanceType_Wrapper")
                        },
                        Discriminator = new OpenApiDiscriminator { PropertyName = "type" },
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["type"] = new OpenApiSchema { Type = JsonSchemaType.String, Title = "type", Description = "Union discriminator" }
                        },
                        Required = new HashSet<string> { "type" }
                    },
                    ["CcInstanceType_Wrapper"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["value"] = new OpenApiSchema { Type = JsonSchemaType.String }
                        },
                        Required = new HashSet<string> { "value" }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "RemoveDiscriminatorsFromNonObjectSchemas", document);

        var ccInstanceType = document.Components.Schemas["CcInstanceType"] as OpenApiSchema;

        await Assert.That(ccInstanceType).IsNotNull();
        await Assert.That(ccInstanceType!.Discriminator).IsNull();
        await Assert.That(ccInstanceType.Properties?.ContainsKey("type") ?? false).IsFalse();
        await Assert.That(ccInstanceType.Required?.Contains("type") ?? false).IsFalse();
    }

    [Test]
    public async ValueTask WrapNonObjectUnionBranchesEverywhere_should_wrap_primitive_branches_after_inlining()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["PrimitiveValue"] = new OpenApiSchema { Type = JsonSchemaType.String },
                    ["ObjectBranch"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["id"] = new OpenApiSchema { Type = JsonSchemaType.String }
                        }
                    },
                    ["Parent"] = new OpenApiSchema
                    {
                        OneOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchemaReference("ObjectBranch"),
                            new OpenApiSchemaReference("PrimitiveValue")
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "InlinePrimitivePropertyRefs", document);

        var parent = document.Components.Schemas["Parent"] as OpenApiSchema;
        await Assert.That(parent!.OneOf![1] is OpenApiSchema).IsTrue();

        InvokePrivateVoidMethod(_util, "WrapNonObjectUnionBranchesEverywhere", document);

        var wrapperReference = parent.OneOf[1] as OpenApiSchemaReference;

        await Assert.That(wrapperReference).IsNotNull();
        await Assert.That(wrapperReference!.Reference.Id).IsEqualTo("ParentBranch2");
        await Assert.That(document.Components.Schemas.ContainsKey("ParentBranch2")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("UnionBranch")).IsFalse();
    }

    [Test]
    public async ValueTask WrapNonObjectUnionBranchesEverywhere_should_treat_nullable_object_branches_as_object_like()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["Parent"] = new OpenApiSchema
                    {
                        AnyOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchema { Type = JsonSchemaType.String },
                            new OpenApiSchema { Type = JsonSchemaType.Object | JsonSchemaType.Null }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "WrapNonObjectUnionBranchesEverywhere", document);

        var parent = document.Components.Schemas["Parent"] as OpenApiSchema;
        var wrapperReference = parent!.AnyOf![0] as OpenApiSchemaReference;

        await Assert.That(wrapperReference).IsNotNull();
        await Assert.That(wrapperReference!.Reference.Id).IsEqualTo("ParentBranch1");
        await Assert.That(document.Components.Schemas.ContainsKey("ParentBranch1")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("UnionBranch")).IsFalse();
    }

    [Test]
    public async ValueTask CollapseNonDiscriminatedInlineObjectUnions_should_merge_inline_object_branches()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["DlpEntry"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        OneOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                Required = new HashSet<string> { "id", "type", "pattern" },
                                Properties = new Dictionary<string, IOpenApiSchema>
                                {
                                    ["id"] = new OpenApiSchema { Type = JsonSchemaType.String },
                                    ["pattern"] = new OpenApiSchema { Type = JsonSchemaType.String },
                                    ["type"] = new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.String,
                                        Enum = [JsonValue.Create("custom")!]
                                    }
                                }
                            },
                            new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                Required = new HashSet<string> { "id", "type", "confidence" },
                                Properties = new Dictionary<string, IOpenApiSchema>
                                {
                                    ["id"] = new OpenApiSchema { Type = JsonSchemaType.String },
                                    ["confidence"] = new OpenApiSchema { Type = JsonSchemaType.String },
                                    ["type"] = new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.String,
                                        Enum = [JsonValue.Create("predefined")!]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "CollapseNonDiscriminatedInlineObjectUnions", document);

        var dlpEntry = document.Components.Schemas["DlpEntry"] as OpenApiSchema;
        var typeProperty = dlpEntry!.Properties!["type"] as OpenApiSchema;

        await Assert.That(dlpEntry.OneOf).IsNull();
        await Assert.That(dlpEntry.Type).IsEqualTo(JsonSchemaType.Object);
        await Assert.That(dlpEntry.Properties!.ContainsKey("pattern")).IsTrue();
        await Assert.That(dlpEntry.Properties.ContainsKey("confidence")).IsTrue();
        await Assert.That(dlpEntry.Required).IsEquivalentTo(["id", "type"]);
        await Assert.That(typeProperty!.Enum!.Count).IsEqualTo(2);
    }

    [Test]
    public async ValueTask FixEnumAllOfObjectPropertyMismatch_should_replace_object_allof_enum_property_with_enum_ref()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["DlpDatasetUploadStatus"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        Enum = [JsonValue.Create("complete")!]
                    },
                    ["DlpEntryWithUploadStatus"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["upload_status"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                AllOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchemaReference("DlpDatasetUploadStatus")
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "FixEnumAllOfObjectPropertyMismatch", document);

        var entry = document.Components.Schemas["DlpEntryWithUploadStatus"] as OpenApiSchema;
        var uploadStatus = entry!.Properties!["upload_status"] as OpenApiSchemaReference;

        await Assert.That(uploadStatus).IsNotNull();
        await Assert.That(uploadStatus!.Reference.Id).IsEqualTo("DlpDatasetUploadStatus");
    }

    [Test]
    public async ValueTask NormalizeNullablePrimitiveCompositions_should_collapse_anyof_primitive_null()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["SharedConversation"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["created_by_user_id"] = new OpenApiSchema
                            {
                                Title = "Created By User Id",
                                AnyOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchema { Type = JsonSchemaType.String },
                                    new OpenApiSchema { Type = JsonSchemaType.Null }
                                }
                            }
                        }
                    }
                }
            }
        };

        _schemaFixer.NormalizeNullablePrimitiveCompositions(document);

        var sharedConversation = document.Components.Schemas["SharedConversation"] as OpenApiSchema;
        var property = sharedConversation!.Properties!["created_by_user_id"] as OpenApiSchema;

        await Assert.That(property).IsNotNull();
        await Assert.That(property!.AnyOf).IsNull();
        await Assert.That(property.Type!.Value.HasFlag(JsonSchemaType.String)).IsTrue();
        await Assert.That(property.Type!.Value.HasFlag(JsonSchemaType.Null)).IsTrue();
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
    public async ValueTask RenameInvalidComponentSchemas_should_update_discriminator_mapping_refs()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["email-sending_EmailAttachment"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object
                    },
                    ["EmailSendingEmailBuilder"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Discriminator = new OpenApiDiscriminator
                        {
                            PropertyName = "type",
                            Mapping = new Dictionary<string, OpenApiSchemaReference>
                            {
                                ["attachment"] = new("email-sending_EmailAttachment")
                            }
                        },
                        OneOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchemaReference("email-sending_EmailAttachment")
                        }
                    }
                }
            }
        };

        _namingFixer.RenameInvalidComponentSchemas(document);

        var builder = document.Components.Schemas["EmailSendingEmailBuilder"] as OpenApiSchema;

        await Assert.That(document.Components.Schemas.ContainsKey("EmailSendingEmailAttachment")).IsTrue();
        await Assert.That(builder).IsNotNull();
        await Assert.That(builder!.Discriminator!.Mapping!["attachment"].Reference.Id).IsEqualTo("EmailSendingEmailAttachment");
        await Assert.That((builder.OneOf![0] as OpenApiSchemaReference)!.Reference.Id).IsEqualTo("EmailSendingEmailAttachment");
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
    public async ValueTask Naming_should_create_dotnet_safe_component_and_operation_names()
    {
        await Assert.That(_namingFixer.ValidateComponentName("api.dns-record[id]")).IsEqualTo("ApiDnsRecordId");
        await Assert.That(_namingFixer.ValidateComponentName("123-response")).IsEqualTo("Value123Response");
        await Assert.That(_namingFixer.ValidateComponentName("class")).IsEqualTo("ClassType");
        await Assert.That(_namingFixer.ValidateComponentName("schema")).IsEqualTo("SchemaValue");
        await Assert.That(_namingFixer.NormalizeOperationId("cloudflare.dns-records:list")).IsEqualTo("CloudflareDnsRecordsList");
    }

    [Test]
    public async ValueTask EnsureUniqueOperationIds_should_generate_route_derived_pascal_case_ids_and_stable_suffixes()
    {
        var document = new OpenApiDocument
        {
            Paths = new OpenApiPaths
            {
                ["/zones/{zone_id}/dns_records"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new()
                    }
                },
                ["/zones/{zone_id}/dns_records/{record_id}"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new()
                        {
                            OperationId = "list dns-records"
                        }
                    }
                },
                ["/accounts/{account_id}/dns-records"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new()
                        {
                            OperationId = "list_dns_records"
                        }
                    }
                }
            }
        };

        _namingFixer.NormalizeOperationIds(document);
        _namingFixer.EnsureUniqueOperationIds(document);

        await Assert.That(document.Paths["/zones/{zone_id}/dns_records"]!.Operations![HttpMethod.Get].OperationId)
                    .IsEqualTo("GetZonesByZoneIdDnsRecords");
        await Assert.That(document.Paths["/zones/{zone_id}/dns_records/{record_id}"]!.Operations![HttpMethod.Get].OperationId)
                    .IsEqualTo("ListDnsRecords");
        await Assert.That(document.Paths["/accounts/{account_id}/dns-records"]!.Operations![HttpMethod.Get].OperationId)
                    .IsEqualTo("ListDnsRecords2");
    }

    [Test]
    public async ValueTask RenameInvalidComponentSchemas_should_resolve_normalized_collisions_stably()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["dns-record"] = new OpenApiSchema { Type = JsonSchemaType.Object },
                    ["DNS Record"] = new OpenApiSchema { Type = JsonSchemaType.Object },
                    ["class"] = new OpenApiSchema { Type = JsonSchemaType.Object }
                }
            }
        };

        _namingFixer.RenameInvalidComponentSchemas(document);

        await Assert.That(document.Components.Schemas.ContainsKey("DnsRecord")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("DnsRecord2")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("ClassType")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("dns-record")).IsFalse();
        await Assert.That(document.Components.Schemas.ContainsKey("DNS Record")).IsFalse();
        await Assert.That(document.Components.Schemas.ContainsKey("class")).IsFalse();
    }

    [Test]
    public async ValueTask RenameConflictingPaths_should_remove_empty_path_segments_from_trailing_slashes()
    {
        var document = new OpenApiDocument
        {
            Paths = new OpenApiPaths
            {
                ["/api/0/seer/models/"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new OpenApiOperation
                        {
                            OperationId = "ListModels"
                        }
                    }
                },
                ["/api/0/seer//models/{model_id}/"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation>
                    {
                        [HttpMethod.Get] = new OpenApiOperation
                        {
                            OperationId = "RetrieveModel"
                        }
                    }
                }
            }
        };

        _namingFixer.RenameConflictingPaths(document);

        await Assert.That(document.Paths.ContainsKey("/api/0/seer/models")).IsTrue();
        await Assert.That(document.Paths.ContainsKey("/api/0/seer/models/{modelId}")).IsTrue();
        await Assert.That(document.Paths.Keys.Any(path => path.Length > 1 && path.EndsWith("/", StringComparison.Ordinal))).IsFalse();
        await Assert.That(document.Paths.Keys.Any(path => path.Contains("//", StringComparison.Ordinal))).IsFalse();
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
    public async ValueTask Fix_should_promote_inline_request_and_response_schemas_to_contextual_names()
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
                                    "/zones/{zone_id}/dns_records": {
                                      "post": {
                                        "operationId": "create-dns-record",
                                        "parameters": [
                                          {
                                            "name": "zone_id",
                                            "in": "path",
                                            "required": true,
                                            "schema": {
                                              "type": "string"
                                            }
                                          }
                                        ],
                                        "requestBody": {
                                          "content": {
                                            "application/json": {
                                              "schema": {
                                                "type": "object",
                                                "properties": {
                                                  "name": {
                                                    "type": "string"
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        },
                                        "responses": {
                                          "201": {
                                            "description": "Created",
                                            "content": {
                                              "application/json": {
                                                "schema": {
                                                  "title": "response",
                                                  "type": "object",
                                                  "properties": {
                                                    "id": {
                                                      "type": "string"
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  },
                                  "components": {
                                    "schemas": {}
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);

            const string normalizedPath = "/zones/{zoneId}/dns_records";

            await Assert.That(root["paths"]?[normalizedPath]?["post"]?["operationId"]?.GetValue<string>())
                        .IsEqualTo("CreateDnsRecord");
            await Assert.That(root["components"]?["schemas"]?["CreateDnsRecordRequest"]).IsNotNull();
            await Assert.That(root["components"]?["schemas"]?["CreateDnsRecord201Response"]).IsNotNull();
            await Assert.That(root["paths"]?[normalizedPath]?["post"]?["requestBody"]?["content"]?["application/json"]?["schema"]?["$ref"]
                              ?.GetValue<string>())
                        .IsEqualTo("#/components/schemas/CreateDnsRecordRequest");
            await Assert.That(root["paths"]?[normalizedPath]?["post"]?["responses"]?["201"]?["content"]?["application/json"]?["schema"]?["$ref"]
                              ?.GetValue<string>())
                        .IsEqualTo("#/components/schemas/CreateDnsRecord201Response");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_normalize_path_parameter_placeholders_without_changing_literal_segments()
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
                                    "/zones/{zone_identifier}/dns_records/{record_id}": {
                                      "get": {
                                        "operationId": "get-zone-record",
                                        "parameters": [
                                          {
                                            "name": "zone_identifier",
                                            "in": "path",
                                            "required": true,
                                            "schema": {
                                              "type": "string"
                                            }
                                          },
                                          {
                                            "name": "record_id",
                                            "in": "path",
                                            "required": true,
                                            "schema": {
                                              "type": "string"
                                            }
                                          }
                                        ],
                                        "responses": {
                                          "200": {
                                            "description": "OK"
                                          }
                                        }
                                      }
                                    }
                                  },
                                  "components": {
                                    "schemas": {}
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);
            JsonObject paths = root["paths"]!.AsObject();
            const string normalizedPath = "/zones/{zoneIdentifier}/dns_records/{recordId}";

            await Assert.That(paths.ContainsKey(normalizedPath)).IsTrue();
            await Assert.That(paths.ContainsKey("/zones/{zone_identifier}/dns_records/{record_id}")).IsFalse();

            JsonArray parameters = paths[normalizedPath]?["get"]?["parameters"]?.AsArray() ?? [];
            await Assert.That(parameters.Select(parameter => parameter?["name"]?.GetValue<string>()).OfType<string>().ToArray())
                        .IsEquivalentTo(["zoneIdentifier", "recordId"]);
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_promote_inline_enum_properties_and_parameters_to_pascalized_component_names()
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
                                    "/widgets": {
                                      "get": {
                                        "operationId": "list-widgets",
                                        "parameters": [
                                          {
                                            "name": "status_type",
                                            "in": "query",
                                            "schema": {
                                              "type": "string",
                                              "enum": [
                                                "created_at",
                                                "COUNTRY_CODE"
                                              ]
                                            }
                                          }
                                        ],
                                        "responses": {
                                          "200": {
                                            "description": "OK"
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
                                          "status_type": {
                                            "type": "string",
                                            "enum": [
                                              "active",
                                              "paused"
                                            ]
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
            JsonNode? schemas = root["components"]?["schemas"];

            await Assert.That(schemas?["WidgetStatusType"]).IsNotNull();
            await Assert.That(schemas?["ListWidgetsStatusTypeParameter"]).IsNotNull();
            await Assert.That(schemas?["Widget"]?["properties"]?["status_type"]?["$ref"]?.GetValue<string>())
                        .IsEqualTo("#/components/schemas/WidgetStatusType");
            await Assert.That(root["paths"]?["/widgets"]?["get"]?["parameters"]?[0]?["schema"]?["$ref"]?.GetValue<string>())
                        .IsEqualTo("#/components/schemas/ListWidgetsStatusTypeParameter");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_inject_safe_enum_member_names_for_nonstandard_wire_values()
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
                                      "Status": {
                                        "type": "string",
                                        "enum": [
                                          "1",
                                          "in-progress",
                                          "class",
                                          "!=",
                                          "pending"
                                        ]
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);
            JsonArray values = root["components"]?["schemas"]?["Status"]?["x-ms-enum"]?["values"]?.AsArray() ?? [];

            await Assert.That(GetEnumInjectedName(values, "1")).IsEqualTo("Value1");
            await Assert.That(GetEnumInjectedName(values, "in-progress")).IsEqualTo("InProgress");
            await Assert.That(GetEnumInjectedName(values, "class")).IsEqualTo("ClassValue");
            await Assert.That(GetEnumInjectedName(values, "!=")).IsEqualTo("ExclamationEqual");
            await Assert.That(GetEnumInjectedName(values, "pending")).IsEqualTo("Pending");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_inject_pascal_case_enum_member_names_for_common_openapi_values()
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
                                      "AuditField": {
                                        "type": "string",
                                        "enum": [
                                          "created_at",
                                          "COUNTRY_CODE",
                                          "openapi_v3",
                                          "ssl_status",
                                          "active"
                                        ]
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);
            JsonArray values = root["components"]?["schemas"]?["AuditField"]?["x-ms-enum"]?["values"]?.AsArray() ?? [];

            await Assert.That(GetEnumInjectedName(values, "created_at")).IsEqualTo("CreatedAt");
            await Assert.That(GetEnumInjectedName(values, "COUNTRY_CODE")).IsEqualTo("CountryCode");
            await Assert.That(GetEnumInjectedName(values, "openapi_v3")).IsEqualTo("OpenApiV3");
            await Assert.That(GetEnumInjectedName(values, "ssl_status")).IsEqualTo("SslStatus");
            await Assert.That(GetEnumInjectedName(values, "active")).IsEqualTo("Active");
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_remove_string_defaults_from_enum_schemas()
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
                                      "GrossWeightUnit": {
                                        "type": "string",
                                        "enum": [
                                          "KG",
                                          "LBS"
                                        ],
                                        "default": "KG"
                                      },
                                      "TranslationLocale": {
                                        "type": "string",
                                        "enum": [
                                          "EN",
                                          "ES"
                                        ],
                                        "default": "EN"
                                      },
                                      "ManifestFormat": {
                                        "type": "string",
                                        "enum": [
                                          "PDF",
                                          "PNG"
                                        ],
                                        "default": "PDF"
                                      }
                                    }
                                  }
                                }
                                """;

            await File.WriteAllTextAsync(sourcePath, spec, System.Threading.CancellationToken.None);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);
            JsonNode? schemas = root["components"]?["schemas"];

            await Assert.That(schemas?["GrossWeightUnit"]?["default"]).IsNull();
            await Assert.That(schemas?["TranslationLocale"]?["default"]).IsNull();
            await Assert.That(schemas?["ManifestFormat"]?["default"]).IsNull();
        }
        finally
        {
            File.Delete(sourcePath);
            File.Delete(targetPath);
        }
    }

    [Test]
    public async ValueTask Fix_should_process_cloudflare_unfixed_fixture()
    {
        string sourcePath = FindFixtureFile("cloudflare_unfixed.json");
        string targetPath = Path.GetTempFileName();

        try
        {
            File.Delete(targetPath);

            await _util.Fix(sourcePath, targetPath, System.Threading.CancellationToken.None);

            JsonNode root = await ReadJsonNode(targetPath);

            await Assert.That(root["openapi"]).IsNotNull();
            await Assert.That(root["paths"]?.AsObject().Count > 0).IsTrue();
            await Assert.That(root["components"]?["schemas"]?.AsObject().Count > 0).IsTrue();
        }
        finally
        {
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
                        .IsEqualTo("AssistantControlListAssistants");
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
    public async ValueTask ExtractInlineComposedSchemas_should_promote_inline_composed_properties()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["AbuseReportsErrorMessage"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["code"] = new OpenApiSchema
                            {
                                OneOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.Number
                                    },
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.String
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineComposedSchemas", document);

        await Assert.That(document.Components.Schemas.ContainsKey("AbuseReportsErrorMessageCode")).IsTrue();

        var container = document.Components.Schemas["AbuseReportsErrorMessage"] as OpenApiSchema;
        var codeReference = container!.Properties!["code"] as OpenApiSchemaReference;
        var codeSchema = document.Components.Schemas["AbuseReportsErrorMessageCode"] as OpenApiSchema;

        await Assert.That(codeReference).IsNotNull();
        await Assert.That(codeReference!.Reference.Id).IsEqualTo("AbuseReportsErrorMessageCode");
        await Assert.That(codeSchema!.OneOf!.Count).IsEqualTo(2);
    }

    [Test]
    public async ValueTask ExtractInlineComposedSchemas_should_promote_inline_composition_branches_with_contextual_titles()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["AccessAppRequest"] = new OpenApiSchema
                    {
                        AnyOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchema
                            {
                                Title = "Self Hosted Application",
                                Type = JsonSchemaType.Object,
                                Properties = new Dictionary<string, IOpenApiSchema>
                                {
                                    ["domain"] = new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.String
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineComposedSchemas", document);

        await Assert.That(document.Components.Schemas.ContainsKey("AccessAppRequestSelfHostedApplication")).IsTrue();

        var container = document.Components.Schemas["AccessAppRequest"] as OpenApiSchema;
        var branchReference = container!.AnyOf![0] as OpenApiSchemaReference;

        await Assert.That(branchReference).IsNotNull();
        await Assert.That(branchReference!.Reference.Id).IsEqualTo("AccessAppRequestSelfHostedApplication");
    }

    [Test]
    public async ValueTask ExtractInlineComponentContentSchemas_should_promote_component_response_content_schemas()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>(),
                Responses = new Dictionary<string, IOpenApiResponse>
                {
                    ["rulesets_Failure"] = new OpenApiResponse
                    {
                        Description = "Failure",
                        Content = new Dictionary<string, IOpenApiMediaType>
                        {
                            ["application/json"] = new OpenApiMediaType
                            {
                                Schema = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.Object,
                                    Properties = new Dictionary<string, IOpenApiSchema>
                                    {
                                        ["result"] = new OpenApiSchema
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

        InvokePrivateVoidMethod(_util, "ExtractInlineComponentContentSchemas", document);

        await Assert.That(document.Components.Schemas.ContainsKey("RulesetsFailureResponse")).IsTrue();

        var response = document.Components.Responses["rulesets_Failure"] as OpenApiResponse;
        var reference = response!.Content!["application/json"].Schema as OpenApiSchemaReference;

        await Assert.That(reference).IsNotNull();
        await Assert.That(reference!.Reference.Id).IsEqualTo("RulesetsFailureResponse");
    }

    [Test]
    public async ValueTask ExtractInlineObjectPropertySchemas_should_promote_inline_array_item_objects()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["AaaMechanisms"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["email"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Array,
                                Items = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.Object,
                                    Properties = new Dictionary<string, IOpenApiSchema>
                                    {
                                        ["id"] = new OpenApiSchema
                                        {
                                            Type = JsonSchemaType.String
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

        await Assert.That(document.Components.Schemas.ContainsKey("AaaMechanismsEmailItem")).IsTrue();

        var container = document.Components.Schemas["AaaMechanisms"] as OpenApiSchema;
        var email = container!.Properties!["email"] as OpenApiSchema;
        var itemReference = email!.Items as OpenApiSchemaReference;

        await Assert.That(itemReference).IsNotNull();
        await Assert.That(itemReference!.Reference.Id).IsEqualTo("AaaMechanismsEmailItem");
    }

    [Test]
    public async ValueTask ExtractInlineObjectPropertySchemas_should_promote_top_level_component_array_item_objects()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["GetCountryRead200ResponseResponseJson"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Array,
                        Items = new OpenApiSchema
                        {
                            Type = JsonSchemaType.Object,
                            Properties = new Dictionary<string, IOpenApiSchema>
                            {
                                ["success"] = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.Boolean
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineObjectPropertySchemas", document);

        await Assert.That(document.Components.Schemas.ContainsKey("GetCountryRead200ResponseResponseJsonItem")).IsTrue();

        var response = document.Components.Schemas["GetCountryRead200ResponseResponseJson"] as OpenApiSchema;
        var itemReference = response!.Items as OpenApiSchemaReference;

        await Assert.That(itemReference).IsNotNull();
        await Assert.That(itemReference!.Reference.Id).IsEqualTo("GetCountryRead200ResponseResponseJsonItem");
    }

    [Test]
    public async ValueTask ExtractInlineObjectPropertySchemas_should_promote_explicit_empty_object_properties()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["AaaAuditLogsV2OrgResource"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["request"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineObjectPropertySchemas", document);

        await Assert.That(document.Components.Schemas.ContainsKey("AaaAuditLogsV2OrgResourceRequest")).IsTrue();

        var container = document.Components.Schemas["AaaAuditLogsV2OrgResource"] as OpenApiSchema;
        var requestReference = container!.Properties!["request"] as OpenApiSchemaReference;

        await Assert.That(requestReference).IsNotNull();
        await Assert.That(requestReference!.Reference.Id).IsEqualTo("AaaAuditLogsV2OrgResourceRequest");
    }

    [Test]
    public async ValueTask ExtractInlineObjectPropertySchemas_should_promote_schemas_with_object_members_even_when_type_is_wrong()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["LoadBalancerPoolsPatchPoolsRequest"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["value"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.String,
                                Properties = new Dictionary<string, IOpenApiSchema>
                                {
                                    ["notification_email"] = new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.String
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineObjectPropertySchemas", document);

        await Assert.That(document.Components.Schemas.ContainsKey("LoadBalancerPoolsPatchPoolsRequestValue")).IsTrue();

        var container = document.Components.Schemas["LoadBalancerPoolsPatchPoolsRequest"] as OpenApiSchema;
        var valueReference = container!.Properties!["value"] as OpenApiSchemaReference;
        var valueSchema = document.Components.Schemas["LoadBalancerPoolsPatchPoolsRequestValue"] as OpenApiSchema;

        await Assert.That(valueReference).IsNotNull();
        await Assert.That(valueReference!.Reference.Id).IsEqualTo("LoadBalancerPoolsPatchPoolsRequestValue");
        await Assert.That(valueSchema!.Type).IsEqualTo(JsonSchemaType.Object);
    }

    [Test]
    public async ValueTask InlinePrimitivePropertyRefs_should_inline_array_component_property_refs()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["Messages"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Array,
                        Items = new OpenApiSchema
                        {
                            Type = JsonSchemaType.String
                        }
                    },
                    ["Response"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["errors"] = new OpenApiSchemaReference("Messages")
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "InlinePrimitivePropertyRefs", document);

        var response = document.Components.Schemas["Response"] as OpenApiSchema;
        var errors = response!.Properties!["errors"] as OpenApiSchema;

        await Assert.That(errors).IsNotNull();
        await Assert.That(errors!.Type).IsEqualTo(JsonSchemaType.Array);
        await Assert.That(errors.Items).IsNotNull();
    }

    [Test]
    public async ValueTask FlattenObjectAllOfCompositions_should_merge_plain_object_refs()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["Envelope"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Required = new HashSet<string> { "success" },
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["success"] = new OpenApiSchema { Type = JsonSchemaType.Boolean }
                        }
                    },
                    ["Response"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        AllOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchemaReference("Envelope"),
                            new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                Properties = new Dictionary<string, IOpenApiSchema>
                                {
                                    ["result"] = new OpenApiSchema { Type = JsonSchemaType.String }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "FlattenObjectAllOfCompositions", document);

        var response = document.Components.Schemas["Response"] as OpenApiSchema;

        await Assert.That(response!.AllOf).IsNull();
        await Assert.That(response.Properties!.ContainsKey("success")).IsTrue();
        await Assert.That(response.Properties!.ContainsKey("result")).IsTrue();
        await Assert.That(response.Required!.Contains("success")).IsTrue();
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
        await Assert.That(document.Components.Schemas.ContainsKey("CreateWidgetapplicationJsonBody")).IsTrue();

        IOpenApiPathItem pathItem = document.Paths["/widgets"]!;
        OpenApiOperation operation = pathItem.Operations[HttpMethod.Post]!;
        IOpenApiMediaType mediaType = operation.RequestBody!.Content["application/json"];
        var schemaReference = mediaType.Schema as OpenApiSchemaReference;

        await Assert.That(schemaReference).IsNotNull();
        await Assert.That(schemaReference!.Reference.Id).IsEqualTo("CreateWidgetapplicationJsonBody");
    }

    [Test]
    public async ValueTask NormalizeAllOfWrappers_should_not_wrap_existing_wrapper_value_into_itself()
    {
        var wrapper = new OpenApiSchema
        {
            Type = JsonSchemaType.Object,
            Required = new HashSet<string> { "value" },
            Properties = new Dictionary<string, IOpenApiSchema>
            {
                ["value"] = new OpenApiSchema
                {
                    AllOf = new List<IOpenApiSchema>
                    {
                        new OpenApiSchemaReference("WorkersBindings"),
                        new OpenApiSchema
                        {
                            Type = JsonSchemaType.Array,
                            Items = new OpenApiSchemaReference("WorkersBindingItem")
                        }
                    }
                }
            }
        };

        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["WorkersBindings"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Array,
                        Items = new OpenApiSchemaReference("WorkersBindingItem")
                    },
                    ["WorkersBindingItem"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object
                    },
                    ["WorkersBindings_Wrapper"] = wrapper
                }
            }
        };

        InvokePrivateVoidMethod(_util, "NormalizeAllOfWrappers", document);

        var valueSchema = wrapper.Properties!["value"] as OpenApiSchema;
        bool selfWrapped = valueSchema?.AllOf?.OfType<OpenApiSchemaReference>()
                                      .Any(reference => reference.Reference.Id == "WorkersBindings_Wrapper") == true;

        await Assert.That(valueSchema).IsNotNull();
        await Assert.That(selfWrapped).IsFalse();
    }

    [Test]
    public async ValueTask NormalizeNonObjectAllOfCompositions_should_collapse_cloudflare_style_value_wrappers()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["ErrorMessage"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object
                    },
                    ["Region"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        Enum = new List<JsonNode>
                        {
                            JsonValue.Create("WNAM")!
                        }
                    },
                    ["Uuid"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        AllOf = new List<IOpenApiSchema>
                        {
                            new OpenApiSchema
                            {
                                Type = JsonSchemaType.String,
                                Format = "uuid"
                            }
                        }
                    },
                    ["Response"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["enabled"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                AllOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.Boolean,
                                        Description = "Whether the rule should run."
                                    },
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.Object,
                                        Default = JsonValue.Create(true)
                                    }
                                }
                            },
                            ["errors"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                AllOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.Array,
                                        Items = new OpenApiSchemaReference("ErrorMessage")
                                    }
                                }
                            },
                            ["region"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                AllOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchemaReference("Region"),
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.String,
                                        Default = JsonValue.Create("WNAM")
                                    }
                                }
                            },
                            ["schema_id"] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                AllOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchemaReference("Uuid"),
                                    new OpenApiSchema
                                    {
                                        Type = JsonSchemaType.String,
                                        Description = "Schema identifier."
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "NormalizeNonObjectAllOfCompositions", document);

        var response = document.Components.Schemas["Response"] as OpenApiSchema;
        var enabled = response!.Properties!["enabled"] as OpenApiSchema;
        var errors = response.Properties["errors"] as OpenApiSchema;
        var region = response.Properties["region"] as OpenApiSchema;
        var schemaId = response.Properties["schema_id"] as OpenApiSchema;
        var uuid = document.Components.Schemas["Uuid"] as OpenApiSchema;

        await Assert.That(enabled!.Type).IsEqualTo(JsonSchemaType.Boolean);
        await Assert.That(enabled.AllOf).IsNull();
        await Assert.That(enabled.Default).IsNotNull();
        await Assert.That(enabled.Description).IsEqualTo("Whether the rule should run.");

        await Assert.That(errors!.Type).IsEqualTo(JsonSchemaType.Array);
        await Assert.That(errors.AllOf).IsNull();
        await Assert.That(errors.Items).IsNotNull();

        await Assert.That(region!.Type).IsEqualTo(JsonSchemaType.String);
        await Assert.That(region.AllOf).IsNull();
        await Assert.That(region.Enum!.Count).IsEqualTo(1);
        await Assert.That(region.Default).IsNotNull();

        await Assert.That(uuid!.Type).IsEqualTo(JsonSchemaType.String);
        await Assert.That(uuid.AllOf).IsNull();
        await Assert.That(schemaId!.Type).IsEqualTo(JsonSchemaType.String);
        await Assert.That(schemaId.Format).IsEqualTo("uuid");
        await Assert.That(schemaId.Description).IsEqualTo("Schema identifier.");
    }

    [Test]
    public async ValueTask RemoveDeprecatedOperationsAndSchemas_should_preserve_referenced_deprecated_schemas()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["LegacyEnum"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        Deprecated = true,
                        Enum = new List<JsonNode> { JsonValue.Create("legacy")! }
                    },
                    ["UnusedDeprecated"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        Deprecated = true
                    },
                    ["Container"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["legacy"] = new OpenApiSchemaReference("LegacyEnum")
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "RemoveDeprecatedOperationsAndSchemas", document);

        await Assert.That(document.Components.Schemas.ContainsKey("LegacyEnum")).IsTrue();
        await Assert.That(document.Components.Schemas.ContainsKey("UnusedDeprecated")).IsFalse();
    }

    [Test]
    public async ValueTask ExtractInlineComposedSchemas_should_not_create_self_reference_when_context_name_matches_ref()
    {
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>
                {
                    ["TransferAuthorization"] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["guarantee_decision"] = new OpenApiSchema
                            {
                                Deprecated = true,
                                AllOf = new List<IOpenApiSchema>
                                {
                                    new OpenApiSchemaReference("TransferAuthorizationGuaranteeDecision")
                                }
                            }
                        }
                    }
                }
            }
        };

        InvokePrivateVoidMethod(_util, "ExtractInlineComposedSchemas", document);

        var transferAuthorization = document.Components.Schemas["TransferAuthorization"] as OpenApiSchema;
        var propertyReference = transferAuthorization!.Properties!["guarantee_decision"] as OpenApiSchemaReference;
        var wrapper = document.Components.Schemas["TransferAuthorizationGuaranteeDecisionWrapper"] as OpenApiSchema;

        await Assert.That(document.Components.Schemas.ContainsKey("TransferAuthorizationGuaranteeDecision")).IsFalse();
        await Assert.That(propertyReference).IsNotNull();
        await Assert.That(propertyReference!.Reference.Id).IsEqualTo("TransferAuthorizationGuaranteeDecisionWrapper");
        await Assert.That(wrapper).IsNotNull();
        await Assert.That((wrapper!.AllOf![0] as OpenApiSchemaReference)!.Reference.Id).IsEqualTo("TransferAuthorizationGuaranteeDecision");
    }

    private static void InvokePrivateVoidMethod(object target, string methodName, params object[] args)
    {
        MethodInfo? method = typeof(OpenApiFixer).GetMethod(methodName, BindingFlags.Instance | BindingFlags.Static | BindingFlags.NonPublic);

        if (method is null)
            throw new InvalidOperationException($"Could not find private method '{methodName}'.");

        method.Invoke(method.IsStatic ? null : target, args);
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

    private static string? GetEnumInjectedName(JsonArray values, string enumValue)
    {
        JsonObject? valueObject = values.OfType<JsonNode>()
                                        .Select(node => node as JsonObject)
                                        .FirstOrDefault(node => string.Equals(node?["value"]?.GetValue<string>(), enumValue, StringComparison.Ordinal));

        return valueObject?["name"]?.GetValue<string>();
    }

    private static string FindFixtureFile(string fileName)
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory != null)
        {
            string candidate = Path.Combine(directory.FullName, fileName);

            if (File.Exists(candidate))
                return candidate;

            directory = directory.Parent;
        }

        throw new FileNotFoundException($"Could not locate fixture '{fileName}' from '{AppContext.BaseDirectory}'.", fileName);
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
