using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Kiota.Abstractions;
using Microsoft.Kiota.Abstractions.Serialization;
using Microsoft.Kiota.Serialization.Json;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiProviderCompatibilityTests
{
    [Test]
    public async ValueTask Unresolved_server_defaults_remain_explicit_and_payloads_are_untouched()
    {
        JsonNode input = JsonNode.Parse("""
            {"servers":[{"url":"{baseUrl}","variables":{"baseUrl":{"default":""}}}],
             "components":{"schemas":{"Payload":{"example":{"servers":[{"url":"{baseUrl}","variables":{"baseUrl":{"default":""}}}]}}}}}
            """)!;
        JsonNode result = Run("NormalizeUnresolvedServerVariables", input);
        await Assert.That(result["servers"]![0]!["variables"]!["baseUrl"]!["default"]!.GetValue<string>()).IsEqualTo("{baseUrl}");
        await Assert.That(JsonNode.DeepEquals(result["components"], input["components"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeUnresolvedServerVariables", result))).IsTrue();
    }

    [Test]
    public async ValueTask Nonnullable_enum_discards_only_impossible_null_alternative()
    {
        JsonNode input = JsonNode.Parse("""{"components":{"schemas":{"Level":{"type":"string","enum":[null,"info","error"]}}}}""")!;
        JsonNode result = Run("NormalizeNullableEnumValues", input);
        await Assert.That(result["components"]!["schemas"]!["Level"]!["enum"]!.ToJsonString()).IsEqualTo("[\"info\",\"error\"]");
        await Assert.That(result["components"]!["schemas"]!["Level"]!["x-client-original-enum"]!.AsArray().Count).IsEqualTo(3);
    }

    [Test]
    [Arguments("string", "url", null)]
    [Arguments("string", "currency", null)]
    [Arguments("string", "decimal", null)]
    [Arguments("string", "integer", null)]
    [Arguments("string", "repo.nwo", null)]
    [Arguments("string", "vendor-specific-id", null)]
    [Arguments("string", "double", null)]
    [Arguments("number", "integer", null)]
    [Arguments("integer", "unix-time", "int64")]
    [Arguments("integer", "timestamp", "int64")]
    [Arguments("integer", "byte", "int32")]
    [Arguments("integer", "custom-integer", "int64")]
    [Arguments("number", "float64", null)]
    [Arguments("object", "phone-number-capabilities", null)]
    [Arguments("array", "vendor-list", null)]
    [Arguments("boolean", "vendor-flag", null)]
    public async ValueTask Provider_formats_preserve_wire_types(string type, string format, string? expectedFormat)
    {
        var schema = new JsonObject { ["type"] = type, ["format"] = format };
        var document = new JsonObject { ["components"] = new JsonObject { ["schemas"] = new JsonObject { ["Value"] = schema } } };
        JsonNode result = Run("NormalizeKiotaResponseCompatibility", document, new OpenApiFixerOptions());
        JsonNode value = result["components"]!["schemas"]!["Value"]!;
        await Assert.That(value["type"]!.GetValue<string>()).IsEqualTo(type);
        await Assert.That(value["format"]?.GetValue<string>()).IsEqualTo(expectedFormat);
        await Assert.That(value["x-client-original-format"]!.GetValue<string>()).IsEqualTo(format);
    }

    [Test]
    [Arguments("3.0.3")]
    [Arguments("3.1.0")]
    public async ValueTask Nullable_object_references_become_nullable_models_without_losing_constraints(string version)
    {
        JsonNode input = JsonNode.Parse("""{"openapi":"3.1.0","components":{"schemas":{"Role":{"type":"object","required":["id"],"properties":{"id":{"type":"string"}},"additionalProperties":false},"NullableRole":{"oneOf":[{"$ref":"#/components/schemas/Role"},{"type":"null"}],"description":"Nullable role"}}}}""")!;
        input["openapi"] = version;
        JsonNode result = Run("NormalizeNullableObjectUnions", input);
        JsonNode value = result["components"]!["schemas"]!["NullableRole"]!;
        await Assert.That(value["oneOf"]).IsNull();
        await Assert.That(value["properties"]!["id"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(value["required"]!.ToJsonString()).IsEqualTo("[\"id\"]");
        await Assert.That(value["additionalProperties"]!.GetValue<bool>()).IsFalse();
        if (version.StartsWith("3.0")) await Assert.That(value["nullable"]!.GetValue<bool>()).IsTrue();
        else await Assert.That(value["type"]!.ToJsonString()).IsEqualTo("[\"object\",\"null\"]");
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeNullableObjectUnions", result))).IsTrue();
        input["components"]!["schemas"]!["Role"]!["enum"] = new JsonArray(new JsonObject { ["id"] = "a" });
        await Assert.That(JsonNode.DeepEquals(input, Run("NormalizeNullableObjectUnions", input))).IsTrue();
    }

    [Test]
    public async ValueTask Unused_schemas_are_archived_but_reachable_cycles_and_discriminator_targets_remain()
    {
        JsonNode input = JsonNode.Parse("""
            {"servers":[{"url":"https://one.example"},{"url":"https://two.example"}],"paths":{"/x":{"get":{"responses":{"200":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/Root"}}}}}}}},
            "components":{"schemas":{"Root":{"properties":{"child":{"$ref":"#/components/schemas/Child"}},"discriminator":{"propertyName":"kind","mapping":{"leaf":"#/components/schemas/Leaf"}}},"Child":{"properties":{"parent":{"$ref":"#/components/schemas/Root"}}},"Leaf":{"type":"object"},"Unused":{"oneOf":[{"type":"string"},{"type":"object"}]}}}}
            """)!;
        JsonNode result = Run("NormalizeClientSchemaCleanup", input, new OpenApiFixerOptions());
        await Assert.That(result["components"]!["schemas"]!["Unused"]).IsNull();
        await Assert.That(result["x-client-unused-schemas"]!["Unused"]).IsNotNull();
        foreach (string name in new[] { "Root", "Child", "Leaf" }) await Assert.That(result["components"]!["schemas"]![name]).IsNotNull();
        await Assert.That(result["servers"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(result["x-client-original-servers"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeClientSchemaCleanup", result, new OpenApiFixerOptions()))).IsTrue();
        await Assert.That(JsonNode.DeepEquals(input, Run("NormalizeClientSchemaCleanup", input, new OpenApiFixerOptions { ArchiveUnusedSchemas = false, SelectPrimaryServer = false }))).IsTrue();
        input["paths"]!["/x"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["schema"] = new JsonObject { ["$dynamicRef"] = "#tree" };
        input["components"]!["schemas"]!["Unused"]!["$anchor"] = "tree";
        await Assert.That(JsonNode.DeepEquals(input, Run("NormalizeClientSchemaCleanup", input, new OpenApiFixerOptions { SelectPrimaryServer = false }))).IsTrue();
    }

    [Test]
    public async ValueTask Annotated_response_references_project_errors_without_changing_reusable_responses()
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/x":{"get":{"responses":{"429":{"$ref":"#/components/responses/Limit","description":"Local description"}}}}},
            "components":{"responses":{"Limit":{"description":"Shared","content":{"application/json":{"schema":{"anyOf":[{"type":"object","properties":{"reason":{"type":"string"}}},{"type":"object","properties":{"code":{"type":"string"}}}]}}}}},"schemas":{}}}
            """)!;
        JsonNode result = Run("NormalizeKiotaResponseCompatibility", input, new OpenApiFixerOptions());
        JsonNode response = result["paths"]!["/x"]!["get"]!["responses"]!["429"]!;
        await Assert.That(response["$ref"]).IsNull();
        await Assert.That(response["description"]!.GetValue<string>()).IsEqualTo("Local description");
        await Assert.That(JsonNode.DeepEquals(input["components"]!["responses"], result["components"]!["responses"])).IsTrue();
    }

    [Test]
    public async ValueTask Complex_forms_preserve_encoding_and_skip_multiple_media_choices()
    {
        JsonNode input = JsonNode.Parse("""{"paths":{"/x":{"post":{"requestBody":{"required":true,"content":{"application/x-www-form-urlencoded":{"schema":{"type":"object","properties":{"metadata":{"type":"object","additionalProperties":{"type":"string"}}}},"encoding":{"metadata":{"style":"deepObject","explode":true}}}}}}}}}""")!;
        JsonNode result = Run("ProjectComplexFormRequests", input);
        JsonNode body = result["paths"]!["/x"]!["post"]!["requestBody"]!;
        await Assert.That(body["content"]!["application/x-www-form-urlencoded"]).IsNull();
        await Assert.That(body["content"]!["application/json"]!["schema"]!["properties"]!["metadata"]).IsNotNull();
        await Assert.That(body["x-client-original-form-content"]!["encoding"]!["metadata"]!["style"]!.GetValue<string>()).IsEqualTo("deepObject");
        await Assert.That(JsonNode.DeepEquals(result, Run("ProjectComplexFormRequests", result))).IsTrue();
        input["paths"]!["/x"]!["post"]!["requestBody"]!["content"]!["application/json"] = new JsonObject();
        await Assert.That(JsonNode.DeepEquals(input, Run("ProjectComplexFormRequests", input))).IsTrue();
    }

    [Test]
    public async ValueTask Nullable_collection_forms_use_equivalent_json_representation()
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/x":{"patch":{"requestBody":{"content":{
                "application/json":{"schema":{"type":"object","properties":{
                    "items":{"type":["array","null"],"items":{"type":"string"}}
                }}}
            }}}}}}
            """)!;
        JsonObject content = input["paths"]!["/x"]!["patch"]!["requestBody"]!["content"]!.AsObject();
        content["application/x-www-form-urlencoded"] = content["application/json"]!.DeepClone();
        JsonNode result = Run("ProjectComplexFormRequests", input);
        JsonNode body = result["paths"]!["/x"]!["patch"]!["requestBody"]!;
        await Assert.That(body["content"]!["application/x-www-form-urlencoded"]).IsNull();
        await Assert.That(JsonNode.DeepEquals(body["content"]!["application/json"], content["application/json"])).IsTrue();
        await Assert.That(body["x-client-original-alternative-form-content"]).IsNotNull();
    }

    [Test]
    public async ValueTask Raw_error_placeholders_round_trip_scalar_array_and_unknown_object_payloads()
    {
        JsonNode input = JsonNode.Parse("""{"paths":{"/x":{"get":{"responses":{"429":{"description":"Rate limited","content":{"application/json":{"schema":{"type":"string","enum":["Ratelimited"]}}}}}}}},"components":{"schemas":{}}}""")!;
        JsonNode result = Run("NormalizeKiotaResponseCompatibility", input, new OpenApiFixerOptions());
        JsonObject schemas = result["components"]!["schemas"]!.AsObject();
        string name = schemas.First().Key;
        string source = (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedRawError", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null,
            ["namespace Models { public partial class " + name + " {} }", schemas])!;
        string directory = Path.Combine(Path.GetTempPath(), "raw-error-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        try
        {
            await (ValueTask)typeof(OpenApiFixer).GetMethod("WriteGeneratedErrorSupport", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [directory, CancellationToken.None])!;
            Type type = GeneratedUnionRuntimeCompiler.Compile([source, await File.ReadAllTextAsync(Path.Combine(directory, "OpenApiRawError.cs"))]).GetType("Models." + name)!;
            foreach (string payload in new[] { "\"Ratelimited\"", "[{\"code\":1}]", "{\"future\":true}" })
            {
                using JsonDocument document = JsonDocument.Parse(payload);
                var node = new JsonParseNode(document.RootElement);
                IParsable instance = node.GetObjectValue(n => (IParsable)type.GetMethod("CreateFromDiscriminatorValue", BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)!.Invoke(null, [n])!)!;
                await Assert.That(instance is ApiException).IsTrue();
                using var writer = new JsonSerializationWriter();
                writer.WriteObjectValue(null, instance);
                using var reader = new StreamReader(writer.GetSerializedContent());
                await Assert.That(JsonNode.DeepEquals(JsonNode.Parse(payload), JsonNode.Parse(await reader.ReadToEndAsync()))).IsTrue();
            }
        }
        finally { Directory.Delete(directory, true); }
    }

    [Test]
    public async ValueTask Plain_text_error_factory_preserves_payload_and_success_primitives()
    {
        string directory = Path.Combine(Path.GetTempPath(), "text-error-" + Guid.NewGuid());
        Directory.CreateDirectory(directory);
        try
        {
            var schemas = JsonNode.Parse("""{"Failure":{"x-client-raw-error-schema":{"type":"string"},"x-client-raw-error-media-type":"text/plain"}}""")!.AsObject();
            string model = (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedRawError", BindingFlags.Static | BindingFlags.NonPublic)!
                .Invoke(null, ["namespace Models { public partial class Failure {} }", schemas])!;
            string client = Path.Combine(directory, "ApiClient.cs");
            await File.WriteAllTextAsync(client, "[GeneratedCode(\"Kiota\", \"1\")] ApiClientBuilder.RegisterDefaultDeserializer<TextParseNodeFactory>();");
            await (ValueTask)typeof(OpenApiFixer).GetMethod("WriteGeneratedErrorSupport", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [directory, CancellationToken.None])!;
            await (ValueTask)typeof(OpenApiFixer).GetMethod("RepairGeneratedTextErrors", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [directory, schemas, CancellationToken.None])!;
            string repaired = await File.ReadAllTextAsync(client);
            await Assert.That(repaired).Contains("OpenApiTextParseNodeFactory.Register()");
            await (ValueTask)typeof(OpenApiFixer).GetMethod("RepairGeneratedTextErrors", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [directory, schemas, CancellationToken.None])!;
            await Assert.That(await File.ReadAllTextAsync(client)).IsEqualTo(repaired);
            Assembly assembly = GeneratedUnionRuntimeCompiler.Compile(Directory.GetFiles(directory, "*.cs").Where(f => f != client).Select(File.ReadAllText).Append(model).ToArray());
            var factory = (IParseNodeFactory)Activator.CreateInstance(assembly.GetType("Soenneker.OpenApi.Generated.OpenApiTextParseNodeFactory")!, true)!;
            const string text = "\"Authorization error\"\n";
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(text));
            IParseNode node = await factory.GetRootParseNodeAsync("text/plain", stream);
            Type failure = assembly.GetType("Models.Failure")!;
            IParsable error = node.GetObjectValue(n => (IParsable)failure.GetMethod("CreateFromDiscriminatorValue", BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)!.Invoke(null, [n])!);
            await Assert.That(error is ApiException).IsTrue();
            await Assert.That(((UntypedString)failure.GetProperty("Payload")!.GetValue(error)!).GetValue()).IsEqualTo(text);
            using var valueStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("123"));
            await Assert.That((await factory.GetRootParseNodeAsync("text/plain", valueStream)).GetIntValue()).IsEqualTo(123);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Test]
    public async ValueTask Scoped_servers_supply_a_root_only_when_every_operation_overrides_it()
    {
        JsonNode input = JsonNode.Parse("""{"paths":{"/a":{"get":{"servers":[{"url":"https://a.example"}]}},"/b":{"servers":[{"url":"https://b.example"}],"post":{}}}}""")!;
        JsonNode result = Run("NormalizeClientSchemaCleanup", input, new OpenApiFixerOptions());
        await Assert.That(result["servers"]![0]!["url"]!.GetValue<string>()).IsEqualTo("https://a.example");
        await Assert.That(JsonNode.DeepEquals(input["paths"], result["paths"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeClientSchemaCleanup", result, new OpenApiFixerOptions()))).IsTrue();
        input["paths"]!["/unscoped"] = new JsonObject { ["get"] = new JsonObject() };
        await Assert.That(Run("NormalizeClientSchemaCleanup", input, new OpenApiFixerOptions())["servers"]).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask Github_response_examples_are_not_a_get_request_body(bool fallbackSchema)
    {
        JsonNode input = JsonNode.Parse("""{"servers":[{"url":"https://api.github.com"}],"paths":{"/repos/{owner}/{repo}/contents/{path}":{"get":{"parameters":[{"name":"ref","in":"query"}],"requestBody":{"content":{"application/json":{"examples":{"response":{"summary":"file"}}}}}}}}}""")!;
        if (fallbackSchema) input["paths"]!["/repos/{owner}/{repo}/contents/{path}"]!["get"]!["requestBody"]!["content"]!["application/json"]!["schema"] = new JsonObject { ["description"] = "Fallback media schema" };
        JsonNode result = Run("CorrectKnownProviderOperations", input);
        JsonNode operation = result["paths"]!["/repos/{owner}/{repo}/contents/{path}"]!["get"]!;
        await Assert.That(operation["requestBody"]).IsNull();
        await Assert.That(operation["x-client-original-request-body"]).IsNotNull();
        await Assert.That(operation["parameters"]![0]!["name"]!.GetValue<string>()).IsEqualTo("ref");
        input["servers"]![0]!["url"] = "https://another.example";
        await Assert.That(JsonNode.DeepEquals(input, Run("CorrectKnownProviderOperations", input))).IsTrue();
    }

    [Test]
    [Arguments("3.0.3")]
    [Arguments("3.1.0")]
    public async ValueTask Nullable_enum_projection_keeps_null_and_numeric_wire_values(string version)
    {
        JsonNode input = JsonNode.Parse("""{"openapi":"3.1.0","components":{"schemas":{"Flag":{"type":["integer","null"],"enum":[1,null],"minimum":1}}}}""")!;
        input["openapi"] = version;
        if (version == "3.0.3")
        {
            input["components"]!["schemas"]!["Flag"]!["type"] = "integer";
            input["components"]!["schemas"]!["Flag"]!["nullable"] = true;
        }
        JsonNode result = Run("NormalizeNullableEnumValues", input);
        JsonNode value = result["components"]!["schemas"]!["Flag"]!;
        await Assert.That(value["enum"]).IsNull();
        await Assert.That(value["anyOf"]![0]!["enum"]!.ToJsonString()).IsEqualTo("[1]");
        await Assert.That(value["anyOf"]![0]!["minimum"]!.GetValue<int>()).IsEqualTo(1);
        if (version == "3.1.0") await Assert.That(value["anyOf"]![1]!["type"]!.GetValue<string>()).IsEqualTo("null");
        else
        {
            await Assert.That(value["anyOf"]![1]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/OpenApiNullValue");
            JsonNode nullSchema = result["components"]!["schemas"]!["OpenApiNullValue"]!;
            await Assert.That(nullSchema["nullable"]!.GetValue<bool>()).IsTrue();
            await Assert.That(nullSchema["not"]!["type"]!.GetValue<string>()).IsEqualTo("object");
        }
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeNullableEnumValues", result))).IsTrue();
    }

    [Test]
    public async ValueTask Array_discriminators_require_agreeing_subtype_examples_before_repair()
    {
        JsonNode input = JsonNode.Parse("""{"components":{"schemas":{"Base":{"properties":{"kind":{"type":"array","items":{"type":"string","enum":["a"]}}},"discriminator":{"propertyName":"kind","mapping":{"a":"#/components/schemas/Child"}}},"Child":{"allOf":[{"$ref":"#/components/schemas/Base"}],"properties":{"kind":{"type":"string","enum":["a"]}},"example":{"kind":"a"}}}}}""")!;
        JsonNode result = Run("NormalizeMalformedDiscriminatorTags", input);
        await Assert.That(result["components"]!["schemas"]!["Base"]!["properties"]!["kind"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeMalformedDiscriminatorTags", result))).IsTrue();
        input["components"]!["schemas"]!["Child"]!.AsObject().Remove("example");
        await Assert.That(JsonNode.DeepEquals(input, Run("NormalizeMalformedDiscriminatorTags", input))).IsTrue();
    }

    private static JsonNode Run(string method, JsonNode input, OpenApiFixerOptions? options = null) => JsonNode.Parse((string)typeof(OpenApiFixer)
        .GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, options == null ? [input.ToJsonString()] : [input.ToJsonString(), options])!)!;
}
