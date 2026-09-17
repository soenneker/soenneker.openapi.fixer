using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiRecoveryTests : HostedUnitTest
{
    private readonly IOpenApiFixer _fixer;
    private readonly IOpenApiPreprocessingFixer _preprocessor;

    public OpenApiRecoveryTests(Host host) : base(host)
    {
        _fixer = Resolve<IOpenApiFixer>(true);
        _preprocessor = Resolve<IOpenApiPreprocessingFixer>(true);
    }

    [Test]
    public async ValueTask Fix_repairs_loose_literals_before_detecting_version(CancellationToken cancellationToken)
    {
        const string spec = """
            {"openapi":"3.1.0","info":{"title":"True / False / None","version":"1"},
             // A comment containing an unmatched quote: "
             "paths":{},"components":{"schemas":{"Record":{"type":"object","properties":{
               "active":{"type":"boolean","default":True},
               "label":{"type":"string","nullable":True,"default":None},
               "disabled":{"type":"boolean","default":False}
             }}}}}
            """;
        JsonNode result = await Fix(spec, cancellationToken);
        await Assert.That(result["openapi"]!.GetValue<string>()).StartsWith("3.1");
        await Assert.That(result["info"]!["title"]!.GetValue<string>()).IsEqualTo("True / False / None");
        await Assert.That(result["components"]!["schemas"]!["Record"]!["properties"]!["active"]!["default"]!.GetValue<bool>()).IsTrue();
    }

    [Test]
    public async ValueTask Fix_accepts_single_line_control_characters(CancellationToken cancellationToken)
    {
        JsonNode result = await Fix("{\"openapi\":\"3.1.0\",\"info\":{\"title\":\"A\u000bB\",\"version\":\"1\"},\"paths\":{}}", cancellationToken);
        await Assert.That(result["openapi"]!.GetValue<string>()).StartsWith("3.1");
        await Assert.That(result["info"]!["title"]!.GetValue<string>()).IsEqualTo("A\u000bB");
    }

    [Test]
    public async ValueTask Fix_recovers_numeric_metadata(CancellationToken cancellationToken)
    {
        JsonNode result = await Fix("""{"openapi":3.1,"info":{"title":123,"version":2},"paths":{}}""", cancellationToken);
        await Assert.That(result["openapi"]!.GetValue<string>()).StartsWith("3.1");
        await Assert.That(result["info"]!["version"]!.GetValue<string>()).IsEqualTo("2");
        await Assert.That(result["info"]!["title"]!.GetValue<string>()).IsEqualTo("123");
    }

    [Test]
    public async ValueTask Preprocessing_preserves_31_numeric_exclusive_bounds()
    {
        JsonNode result = PreprocessSchema("""{"type":"number","minimum":-5,"maximum":10,"exclusiveMinimum":0,"exclusiveMaximum":1}""");
        await Assert.That(result["exclusiveMinimum"]!.GetValue<int>()).IsEqualTo(0);
        await Assert.That(result["exclusiveMaximum"]!.GetValue<int>()).IsEqualTo(1);
        await Assert.That(result["minimum"]!.GetValue<int>()).IsEqualTo(-5);
        await Assert.That(result["maximum"]!.GetValue<int>()).IsEqualTo(10);
    }

    [Test]
    public async ValueTask Fix_preserves_31_numeric_exclusive_bounds(CancellationToken cancellationToken)
    {
        JsonNode result = await Fix(Spec("""{"type":"object","properties":{"amount":{"type":"number","exclusiveMinimum":0,"exclusiveMaximum":1}}}"""), cancellationToken);
        JsonNode amount = result["components"]!["schemas"]!["Record"]!["properties"]!["amount"]!;
        await Assert.That(amount["exclusiveMinimum"]!.GetValue<int>()).IsEqualTo(0);
        await Assert.That(amount["exclusiveMaximum"]!.GetValue<int>()).IsEqualTo(1);
    }

    [Test]
    [Arguments("3.0.4")]
    [Arguments("3.1.0")]
    public async ValueTask Preprocessing_interprets_exclusive_flags_in_the_source_dialect(string version)
    {
        JsonNode result = PreprocessSchema("""{"type":"number","minimum":2,"maximum":10,"exclusiveMinimum":"true","exclusiveMaximum":"false"}""", version);
        if (version.StartsWith("3.0", StringComparison.Ordinal))
        {
            await Assert.That(result["exclusiveMinimum"]!.GetValue<bool>()).IsTrue();
            await Assert.That(result["exclusiveMaximum"]!.GetValue<bool>()).IsFalse();
            await Assert.That(result["minimum"]!.GetValue<int>()).IsEqualTo(2);
        }
        else
        {
            await Assert.That(result["exclusiveMinimum"]!.GetValue<int>()).IsEqualTo(2);
            await Assert.That(result["exclusiveMaximum"]).IsNull();
            await Assert.That(result["minimum"]).IsNull();
        }
        await Assert.That(result["maximum"]!.GetValue<int>()).IsEqualTo(10);
    }

    [Test]
    [Arguments("example")]
    [Arguments("examples")]
    [Arguments("x-payload")]
    [Arguments("type")]
    [Arguments("nullable")]
    [Arguments("properties")]
    public async ValueTask Preprocessing_repairs_schema_properties_named_like_keywords(string name)
    {
        var schema = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject { [name] = new JsonObject { ["type"] = "string", ["nullable"] = "true" } }
        };
        JsonNode result = PreprocessSchema(schema.ToJsonString());
        JsonNode property = result["properties"]![name]!;
        await Assert.That(property["nullable"]).IsNull();
        await Assert.That(property["type"]!.AsArray().Any(node => node?.GetValue<string>() == "null")).IsTrue();
        await Assert.That(result["properties"]!.AsObject().Count).IsEqualTo(1);
    }

    [Test]
    [Arguments("patternProperties", true)]
    [Arguments("$defs", true)]
    [Arguments("dependentSchemas", true)]
    [Arguments("unevaluatedProperties", false)]
    [Arguments("contains", false)]
    [Arguments("if", false)]
    [Arguments("then", false)]
    [Arguments("else", false)]
    public async ValueTask Preprocessing_repairs_nested_json_schema_keywords(string keyword, bool map)
    {
        var child = new JsonObject { ["type"] = "string", ["nullable"] = "true" };
        var schema = new JsonObject { [keyword] = map ? new JsonObject { ["example"] = child } : child };
        JsonNode result = PreprocessSchema(schema.ToJsonString());
        JsonNode nested = map ? result[keyword]!["example"]! : result[keyword]!;
        await Assert.That(nested["nullable"]).IsNull();
        await Assert.That(nested["type"]!.AsArray().Count).IsEqualTo(2);
    }

    [Test]
    public async ValueTask Json_passes_preserve_payloads_and_extensions()
    {
        const string payload = """{"schema":{"type":["string","number"],"nullable":"true","enum":["a-b","+"]},"schemas":{"properties":{"nullable":"true"}}}""";
        var schema = new JsonObject { ["type"] = "object", ["properties"] = new JsonObject() };
        foreach (string key in new[] { "default", "example", "const", "x-payload" })
            schema[key] = JsonNode.Parse(payload);
        schema["examples"] = new JsonArray(JsonNode.Parse(payload));
        schema["enum"] = new JsonArray(JsonNode.Parse(payload));
        string spec = Spec(schema.ToJsonString());
        string processed = _preprocessor.Fix(spec);
        processed = InvokeJsonPass("NormalizeKiotaIncompatibleMultiTypes", processed);
        processed = InvokeJsonPass("InjectKiotaEnumValueNames", processed);
        JsonNode result = JsonNode.Parse(processed)!["components"]!["schemas"]!["Record"]!;
        await Assert.That(JsonNode.DeepEquals(schema, result)).IsTrue();
    }

    [Test]
    public async ValueTask Json_passes_process_keyword_named_schema_properties()
    {
        string spec = Spec("""{"type":"object","properties":{"example":{"type":["string","number"]},"x-mode":{"type":"string","enum":["a-b"]}}}""");
        string result = InvokeJsonPass("NormalizeKiotaIncompatibleMultiTypes", spec);
        result = InvokeJsonPass("InjectKiotaEnumValueNames", result);
        JsonNode properties = JsonNode.Parse(result)!["components"]!["schemas"]!["Record"]!["properties"]!;
        await Assert.That(properties["example"]!["anyOf"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(properties["x-mode"]!["x-ms-enum"]).IsNotNull();
    }

    [Test]
    public async ValueTask Preprocessing_recovers_collection_shapes_and_quoted_constraints()
    {
        JsonNode result = PreprocessSchema("""{"type":"object","required":"id","allOf":{"type":"object"},"properties":{"id":{"type":"string","minLength":"2","enum":"a-b"},"count":{"type":"number","minimum":"-1.25","exclusiveMaximum":"1e2"}}}""");
        await Assert.That(result["required"]![0]!.GetValue<string>()).IsEqualTo("id");
        await Assert.That(result["allOf"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(result["properties"]!["id"]!["minLength"]!.GetValue<int>()).IsEqualTo(2);
        await Assert.That(result["properties"]!["id"]!["enum"]![0]!.GetValue<string>()).IsEqualTo("a-b");
        await Assert.That(result["properties"]!["count"]!["minimum"]!.GetValue<decimal>()).IsEqualTo(-1.25m);
        await Assert.That(result["properties"]!["count"]!["exclusiveMaximum"]!.GetValue<decimal>()).IsEqualTo(100m);
    }

    [Test]
    public async ValueTask Fix_completes_path_parameters_using_effective_operation_definitions(CancellationToken cancellationToken)
    {
        const string spec = """
            {"openapi":"3.0.4","info":{"title":"Parameters","version":"1"},"paths":{
              "/items/{id}/{revision}":{"parameters":[{"$ref":"#/components/parameters/Id"}],
                "get":{"parameters":{"name":"revision","in":"path","required":false,"schema":{"type":"integer","minimum":1}},"responses":{"200":{"description":"OK"}}},
                "delete":{"responses":{"204":{"description":"Deleted"}}}
              }},"components":{"parameters":{"Id":{"name":"id","in":"path","schema":{"type":"integer","format":"int64"}}}}}
            """;
        JsonNode result = await Fix(spec, cancellationToken);
        JsonNode path = result["paths"]!["/items/{id}/{revision}"]!;
        await Assert.That(path["parameters"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(path["get"]!["parameters"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(path["get"]!["parameters"]![0]!["schema"]!["type"]!.GetValue<string>()).IsEqualTo("integer");
        await Assert.That(path["get"]!["parameters"]![0]!["required"]!.GetValue<bool>()).IsTrue();
        await Assert.That(path["delete"]!["parameters"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(path["delete"]!["parameters"]![0]!["name"]!.GetValue<string>()).IsEqualTo("revision");
        await Assert.That(result["components"]!["parameters"]!["Id"]!["required"]!.GetValue<bool>()).IsTrue();
    }

    [Test]
    public async ValueTask Preprocessing_handles_callbacks_without_treating_expressions_as_path_templates()
    {
        const string spec = """
            {"openapi":"3.1.0","info":{"title":"Callbacks","version":"1"},"paths":{
              "/subscribe":{"post":{"callbacks":{"onEvent":{"{$request.body#/callbackUrl}":{
                "post":{"parameters":[{"name":"id","in":"path","required":"false","schema":{"type":"string","nullable":"true"}}],
                  "requestBody":{"content":{"application/json":{"schema":{"type":"string","nullable":"true"}}}},
                  "responses":{"200":{"description":"OK"}}}
              }}},"responses":{"200":{"description":"OK"}}}}}}
            """;
        JsonNode result = JsonNode.Parse(_preprocessor.Fix(spec))!;
        JsonNode operation = result["paths"]!["/subscribe"]!["post"]!["callbacks"]!["onEvent"]!["{$request.body#/callbackUrl}"]!["post"]!;
        await Assert.That(operation["parameters"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(operation["parameters"]![0]!["required"]!.GetValue<bool>()).IsTrue();
        await Assert.That(operation["requestBody"]!["content"]!["application/json"]!["schema"]!["type"]!.AsArray().Count).IsEqualTo(2);
    }

    [Test]
    public async ValueTask Preprocessing_is_idempotent_for_recovered_input()
    {
        const string spec = """
            {"openapi":3.1,"info":{"title":"Recovery","version":1},"paths":{
              "/items/{id}":{"get":{"responses":{"200":{"description":"OK"}}}}},
             "components":{"schemas":{"Record":{"type":"object","required":"id","properties":{
               "id":{"type":"string","nullable":True,"minLength":"1"},
               "count":{"type":"number","minimum":0,"exclusiveMinimum":True}
             }}}},}
            """;
        string once = _preprocessor.Fix(spec);
        await Assert.That(_preprocessor.Fix(once)).IsEqualTo(once);
    }

    [Test]
    public async ValueTask Fix_preserves_media_ranges_and_parameterized_variants(CancellationToken cancellationToken)
    {
        const string spec = """
            {"openapi":"3.1.0","info":{"title":"Media variants","version":"1"},"paths":{
              "/items":{"get":{"responses":{"200":{"description":"OK","content":{
                "application/json; profile=Upper":{"schema":{"type":"string"},"example":"upper"},
                "application/json; profile=upper":{"schema":{"type":"integer"},"example":42},
                "application/*":{"schema":{"type":"string"},"example":"fallback"},
                "*/*":{"schema":{"type":"string"},"example":"anything"}
              }}}}}}}
            """;
        JsonNode result = await Fix(spec, cancellationToken);
        JsonObject content = result["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"]!.AsObject();
        await Assert.That(content.Count).IsEqualTo(4);
        await Assert.That(content["application/json; profile=Upper"]!["example"]!.GetValue<string>()).IsEqualTo("upper");
        await Assert.That(content["application/json; profile=upper"]!["example"]!.GetValue<int>()).IsEqualTo(42);
        await Assert.That(content["application/*"]).IsNotNull();
        await Assert.That(content["*/*"]).IsNotNull();
    }

    [Test]
    public async ValueTask Fix_recovers_duplicate_properties_deterministically(CancellationToken cancellationToken)
    {
        const string spec = """
            {"openapi":"3.1.0","info":{"title":"Earlier","title":"Final","version":"1"},
             "paths":{},"components":{"schemas":{"Record":{"type":"object","properties":{
               "active":{"type":"boolean","default":False,"default":True},
               "message":{"type":"string","example":"before","example":"after"}
             }}}}}
            """;
        JsonNode result = await Fix(spec, cancellationToken);
        await Assert.That(result["info"]!["title"]!.GetValue<string>()).IsEqualTo("Final");
        await Assert.That(result["components"]!["schemas"]!["Record"]!["properties"]!["active"]!["default"]!.GetValue<bool>()).IsTrue();
    }

    [Test]
    public async ValueTask Preprocessing_preserves_literal_text_and_unrelated_duplicate_payload_values()
    {
        const string spec = """
            {"openapi":"3.1.0", /* a quote: " */ "info":{"title":"True, False, None and \\","version":"1"},
             "paths":{},"x-example":{"schema":{"enum":["+"],"nullable":"true","type":"integer","type":"string"}},
             "components":{"schemas":{"Record":{"type":"boolean","default":True}}}}
            """;
        JsonNode result = JsonNode.Parse(_preprocessor.Fix(spec))!;
        await Assert.That(result["info"]!["title"]!.GetValue<string>()).IsEqualTo("True, False, None and \\");
        await Assert.That(result["x-example"]!["schema"]!["nullable"]!.GetValue<string>()).IsEqualTo("true");
        await Assert.That(result["x-example"]!["schema"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(result["components"]!["schemas"]!["Record"]!["default"]!.GetValue<bool>()).IsTrue();
    }

    [Test]
    public async ValueTask Preprocessing_handles_swagger_definitions_and_path_parameters()
    {
        const string spec = """
            {"swagger":2.0,"info":{"title":"Legacy","version":1},"paths":{
              "/items/{id}":{"get":{"parameters":[{"name":"limit","in":"query","type":"integer","minimum":"1"}],"responses":{"200":{"description":"OK"}}}}},
             "definitions":{"Record":{"type":"object","properties":{"example":{"type":"string","readOnly":"true","minLength":"2"}}}}}
            """;
        JsonNode result = JsonNode.Parse(_preprocessor.Fix(spec))!;
        await Assert.That(result["swagger"]!.GetValue<string>()).IsEqualTo("2.0");
        JsonNode operation = result["paths"]!["/items/{id}"]!["get"]!;
        await Assert.That(operation["parameters"]![0]!["minimum"]!.GetValue<int>()).IsEqualTo(1);
        await Assert.That(operation["parameters"]![1]!["name"]!.GetValue<string>()).IsEqualTo("id");
        await Assert.That(operation["parameters"]![1]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(operation["parameters"]![1]!["schema"]).IsNull();
        await Assert.That(result["definitions"]!["Record"]!["properties"]!["example"]!["readOnly"]!.GetValue<bool>()).IsTrue();
    }

    [Test]
    public async ValueTask Fix_recovers_single_parameter_objects_before_walking_their_schemas(CancellationToken cancellationToken)
    {
        const string spec = """
            {"openapi":"3.1.0","info":{"title":"Single parameter","version":"1"},"paths":{
              "/items":{"get":{"parameters":{"name":"limit","in":"query","required":"false","explode":"true","schema":{"type":"integer","minimum":"1"}},
                "responses":{"200":{"description":"OK"}}}}}}
            """;
        JsonNode result = await Fix(spec, cancellationToken);
        JsonNode parameter = result["paths"]!["/items"]!["get"]!["parameters"]![0]!;
        await Assert.That(parameter["required"]?.GetValue<bool>() ?? false).IsFalse();
        // Query parameters use form style with explode=true by default, which the serializer may omit.
        await Assert.That(parameter["explode"]?.GetValue<bool>() ?? true).IsTrue();
        await Assert.That(parameter["schema"]!["minimum"]!.GetValue<int>()).IsEqualTo(1);
    }

    [Test]
    public async ValueTask Preprocessing_leaves_callback_extensions_untouched()
    {
        const string spec = """
            {"openapi":"3.1.0","paths":{},"components":{"callbacks":{"Event":{
              "x-sample":{"post":{"parameters":[{"name":"id","in":"path","required":"false"}]}},
              "{$request.body#/url}":{"post":{"responses":{"200":{"description":"OK"}}}}
            }}}}
            """;
        JsonNode result = JsonNode.Parse(_preprocessor.Fix(spec))!;
        await Assert.That(result["components"]!["callbacks"]!["Event"]!["x-sample"]!["post"]!["parameters"]![0]!["required"]!.GetValue<string>()).IsEqualTo("false");
    }

    [Test]
    public async ValueTask Preprocessing_preserves_numeric_constraint_precision()
    {
        JsonNode result = PreprocessSchema("""{"type":"number","minimum":"1e-100","maximum":"123456789012345678901234567890.123456789"}""");
        await Assert.That(result["minimum"]!.ToJsonString()).IsEqualTo("1e-100");
        await Assert.That(result["maximum"]!.ToJsonString()).IsEqualTo("123456789012345678901234567890.123456789");
    }

    [Test]
    public async ValueTask Fix_keeps_existing_target_on_unrecoverable_input(CancellationToken cancellationToken)
    {
        string source = Path.GetTempFileName();
        string target = Path.GetTempFileName();
        const string existing = "Existing target";
        try
        {
            await File.WriteAllTextAsync(source, "{ definitely not a specification", cancellationToken);
            await File.WriteAllTextAsync(target, existing, cancellationToken);
            Exception? failure = null;
            try { await _fixer.Fix(source, target, cancellationToken); }
            catch (Exception ex) { failure = ex; }
            await Assert.That(failure).IsNotNull();
            await Assert.That(await File.ReadAllTextAsync(target, cancellationToken)).IsEqualTo(existing);
            await Assert.That(Directory.GetFiles(Path.GetDirectoryName(target)!, Path.GetFileName(target) + ".*.tmp").Length).IsEqualTo(0);
        }
        finally
        {
            File.Delete(source);
            File.Delete(target);
        }
    }

    private JsonNode PreprocessSchema(string schema, string version = "3.1.0") =>
        JsonNode.Parse(_preprocessor.Fix(Spec(schema, version)))!["components"]!["schemas"]!["Record"]!;

    private static string Spec(string schema, string version = "3.1.0") =>
        new JsonObject
        {
            ["openapi"] = version,
            ["info"] = new JsonObject { ["title"] = "Recovery", ["version"] = "1" },
            ["paths"] = new JsonObject(),
            ["components"] = new JsonObject { ["schemas"] = new JsonObject { ["Record"] = JsonNode.Parse(schema) } }
        }.ToJsonString();

    private string InvokeJsonPass(string name, string json) =>
        (string)typeof(OpenApiFixer).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic, [typeof(string)])!.Invoke(_fixer, [json])!;

    private async ValueTask<JsonNode> Fix(string spec, CancellationToken cancellationToken)
    {
        string source = Path.GetTempFileName();
        string target = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(source, spec, cancellationToken);
            await _fixer.Fix(source, target, cancellationToken);
            return JsonNode.Parse(await File.ReadAllTextAsync(target, cancellationToken))!;
        }
        finally
        {
            File.Delete(source);
            File.Delete(target);
        }
    }
}
