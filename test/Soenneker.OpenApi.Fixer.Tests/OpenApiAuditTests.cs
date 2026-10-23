using System.Linq;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiAuditTests : HostedUnitTest
{
    private readonly IOpenApiFixer _fixer;
    public OpenApiAuditTests(Host host) : base(host) => _fixer = Resolve<IOpenApiFixer>(true);

    [Test]
    public async ValueTask Object_allOf_preserves_closed_shape(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Record":{"allOf":[{"type":"object","properties":{"name":{"type":"string"}},"additionalProperties":false,"maxProperties":1}]}}""", ct);
        await Assert.That(Contains(schemas, schemas["Record"]!, "additionalProperties", "false")).IsTrue();
        await Assert.That(Contains(schemas, schemas["Record"]!, "maxProperties", "1")).IsTrue();
    }

    [Test]
    public async ValueTask Constraint_only_allOf_is_not_metadata(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Record":{"type":"object","properties":{"name":{"type":"string"}},"allOf":[{"maxProperties":1}]}}""", ct);
        await Assert.That(Contains(schemas, schemas["Record"]!, "maxProperties", "1")).IsTrue();
    }

    [Test]
    public async ValueTask Primitive_reference_preserves_constraints(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Code":{"type":"string","minLength":3},"Amount":{"type":"number","multipleOf":2,"minimum":0,"exclusiveMinimum":true},"Record":{"type":"object","properties":{"code":{"$ref":"#/components/schemas/Code"},"amount":{"$ref":"#/components/schemas/Amount"}}}}""", ct);
        await Assert.That(Contains(schemas, schemas["Record"]!["properties"]!["code"]!, "minLength", "3")).IsTrue();
        await Assert.That(Contains(schemas, schemas["Record"]!["properties"]!["amount"]!, "multipleOf", "2")).IsTrue();
    }

    [Test]
    public async ValueTask Error_message_array_keeps_wire_type(CancellationToken ct)
    {
        const string paths = """{"/test":{"get":{"responses":{"400":{"description":"Bad request","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Problem"}}}}}}}}""";
        JsonObject schemas = await Fix("""{"Problem":{"type":"object","properties":{"message":{"type":"array","items":{"type":"string"}}}}}""", ct, paths);
        await Assert.That(schemas["Problem"]!["properties"]!["message"]!["type"]!.GetValue<string>()).IsEqualTo("array");
    }

    [Test]
    public async ValueTask Malformed_date_default_does_not_abort_fix(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"DateValue":{"type":"string","format":"date-time","default":123}}""", ct);
        await Assert.That(schemas["DateValue"]!["default"]).IsNull();
    }

    [Test]
    public async ValueTask Map_reference_preserves_validation(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Lookup":{"type":"object","additionalProperties":{"type":"string"},"minProperties":2},"Record":{"type":"object","properties":{"lookup":{"$ref":"#/components/schemas/Lookup"}}}}""", ct);
        await Assert.That(Contains(schemas, schemas["Record"]!["properties"]!["lookup"]!, "minProperties", "2")).IsTrue();
    }

    [Test]
    public async ValueTask Nullable_union_preserves_parent_constraints(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Code":{"maxLength":5,"anyOf":[{"type":"string","minLength":2},{"type":"null"}]}}""", ct, version:"3.1.0");
        await Assert.That(Contains(schemas, schemas["Code"]!, "maxLength", "5")).IsTrue();
    }

    [Test]
    public async ValueTask String_enum_values_are_not_reinterpreted(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Literal":{"type":"string","enum":["{example}","\"quoted\"","a\u0001b"]}}""", ct);
        await Assert.That(schemas["Literal"]!["enum"]!.ToJsonString()).IsEqualTo(JsonNode.Parse("""["{example}","\"quoted\"","a\u0001b"]""")!.ToJsonString());
    }

    [Test]
    public async ValueTask Array_examples_and_empty_defaults_are_preserved(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"ListValue":{"type":"array","items":{"type":"string"},"example":["a","b"]},"TextValue":{"type":"string","default":""}}""", ct);
        await Assert.That(schemas["ListValue"]!["example"]!.ToJsonString()).IsEqualTo("[\"a\",\"b\"]");
        await Assert.That(schemas["TextValue"]!["default"]!.GetValue<string>()).IsEqualTo("");
    }

    [Test]
    public async ValueTask Fractional_integer_default_is_removed(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"CountValue":{"type":"integer","default":1.5}}""", ct);
        await Assert.That(schemas["CountValue"]!["default"]).IsNull();
    }


    [Test]
    public async ValueTask Primitive_allOf_keeps_each_constraint(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Code":{"allOf":[{"type":"string","minLength":2},{"type":"string","minLength":5}]}}""", ct);
        await Assert.That(Contains(schemas, schemas["Code"]!, "minLength", "5")).IsTrue();
    }

    [Test]
    public async ValueTask Map_allOf_keeps_map_value_constraint(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Lookup":{"allOf":[{"type":"object","additionalProperties":{"type":"string","minLength":3}},{"type":"object","properties":{"name":{"type":"string"}}}]}}""", ct);
        await Assert.That(Contains(schemas, schemas["Lookup"]!, "additionalProperties", "{\"minLength\":3,\"type\":\"string\"}") || schemas["Lookup"]!.ToJsonString().Contains("minLength")).IsTrue();
    }

    [Test]
    public async ValueTask External_reference_is_not_replaced_with_local_primitive(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Code":{"type":"string"},"Record":{"type":"object","properties":{"remote":{"$ref":"https://example.com/schema.json#/components/schemas/Code"}}}}""", ct);
        await Assert.That(schemas["Record"]!["properties"]!["remote"]!["$ref"]!.GetValue<string>()).IsEqualTo("https://example.com/schema.json#/components/schemas/Code");
    }

    [Test]
    public async ValueTask Not_constraint_is_not_removed_as_empty(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Record":{"allOf":[{"not":{"type":"string"}}]}}""", ct, version:"3.1.0");
        await Assert.That(Contains(schemas, schemas["Record"]!, "not", "{\"type\":\"string\"}")).IsTrue();
    }

    [Test]
    public async ValueTask Recursive_map_stays_finite(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Lookup":{"type":"object","additionalProperties":{"$ref":"#/components/schemas/Lookup"}},"Record":{"type":"object","properties":{"lookup":{"$ref":"#/components/schemas/Lookup"}}}}""", ct);
        await Assert.That(schemas["Lookup"]!["additionalProperties"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Lookup");
    }


    [Test]
    public async ValueTask Mixed_union_keeps_scalar_wire_shape(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Choice":{"anyOf":[{"type":"string"},{"type":"object","properties":{"name":{"type":"string"}}}]}}""", ct);
        await Assert.That(schemas["Choice"]!["type"]).IsNull();
        await Assert.That(schemas["Choice"]!["anyOf"]!.AsArray().Any(branch => branch?["type"]?.GetValue<string>() == "string")).IsTrue();
    }

    [Test]
    public async ValueTask Empty_closed_object_stays_closed(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Closed":{"type":"object","additionalProperties":false}}""", ct);
        await Assert.That(schemas["Closed"]!["additionalProperties"]!.GetValue<bool>()).IsFalse();
    }

    [Test]
    public async ValueTask References_in_not_follow_schema_renames(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"old_name":{"type":"object","properties":{"name":{"type":"string"}}},"Other":{"not":{"$ref":"#/components/schemas/old_name"}}}""", ct, version:"3.1.0");
        await Assert.That(schemas["Other"]!["not"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/OldName");
    }

    [Test]
    public async ValueTask Literal_route_segments_are_preserved()
    {
        var paths = new Microsoft.OpenApi.OpenApiPaths();
        foreach(string path in new[]{"/things/item", "/things/-/current", "/things/"})
            paths[path] = new Microsoft.OpenApi.OpenApiPathItem();
        var document = new Microsoft.OpenApi.OpenApiDocument { Paths = paths };
        Resolve<Soenneker.OpenApi.Fixer.Fixers.Abstract.IOpenApiNamingFixer>(true).RenameConflictingPaths(document);
        foreach(string path in paths.Keys)
            await Assert.That(document.Paths.ContainsKey(path)).IsTrue();
    }


    [Test]
    public async ValueTask Parameter_content_encoding_is_preserved(CancellationToken ct)
    {
        const string paths = """{"/test":{"get":{"parameters":[{"name":"filter","in":"query","content":{"application/json":{"schema":{"type":"object","properties":{"active":{"type":"boolean"}}}}}}],"responses":{"200":{"description":"OK"}}}}}""";
        JsonObject doc = await Fix("{}", ct, paths, wholeDocument:true);
        JsonNode parameter = doc["paths"]!["/test"]!["get"]!["parameters"]![0]!;
        await Assert.That(parameter["content"]?["application/json"]).IsNotNull();
        await Assert.That(parameter["schema"]).IsNull();
    }

    [Test]
    public async ValueTask Primitive_request_body_keeps_scalar_wire_shape(CancellationToken ct)
    {
        const string paths = """{"/test":{"post":{"requestBody":{"content":{"application/json":{"schema":{"type":"string"}}}},"responses":{"200":{"description":"OK"}}}}}""";
        JsonObject doc = await Fix("{}", ct, paths, wholeDocument:true);
        JsonNode request = doc["paths"]!["/test"]!["post"]!["requestBody"]!["content"]!["application/json"]!["schema"]!;
        await Assert.That(Contains(doc["components"]!["schemas"]!.AsObject(), request, "type", "\"string\"")).IsTrue();
    }

    [Test]
    public async ValueTask Structured_and_null_enum_members_survive(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"ObjectValue":{"type":"object","enum":[{"name":"x"}]},"NullableValue":{"type":["string","null"],"enum":["x",null]},"MixedValue":{"enum":["x",2]}}""", ct, version:"3.1.0");
        await Assert.That(schemas["ObjectValue"]!["type"]!.GetValue<string>()).IsEqualTo("object");
        await Assert.That(schemas["ObjectValue"]!["enum"]![0]!["name"]!.GetValue<string>()).IsEqualTo("x");
        await Assert.That(schemas["NullableValue"]!["enum"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(schemas["MixedValue"]!["type"]).IsNull();
        await Assert.That(schemas["MixedValue"]!["anyOf"]).IsNotNull();
    }


    [Test]
    public async ValueTask Derived_value_constraints_do_not_mutate_shared_base(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"ParentRecord":{"type":"object","required":["value"],"properties":{"value":{"minLength":2}}},"Record":{"allOf":[{"$ref":"#/components/schemas/ParentRecord"},{"type":"object","properties":{"value":{"type":"string","maxLength":5}}}]}}""", ct);
        await Assert.That(Contains(schemas,schemas["Record"]!,"properties", "{}")).IsFalse();
        await Assert.That(schemas["ParentRecord"]!["properties"]!["value"]!["minLength"]!.GetValue<int>()).IsEqualTo(2);
        await Assert.That(schemas["ParentRecord"]!["required"]!.AsArray().Count).IsEqualTo(1);
        JsonNode value = schemas["Record"]!["properties"]!["value"]!;
        await Assert.That(Contains(schemas, value, "minLength", "2")).IsTrue();
        await Assert.That(Contains(schemas, value, "maxLength", "5")).IsTrue();
    }

    [Test]
    public async ValueTask Map_allOf_preserves_closed_overlay(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Lookup":{"allOf":[{"type":"object","additionalProperties":{"type":"string"}},{"type":"object","additionalProperties":false,"properties":{"name":{"type":"string"}}}]}}""", ct);
        await Assert.That(Contains(schemas,schemas["Lookup"]!,"additionalProperties","false")).IsTrue();
    }

    [Test]
    public async ValueTask Referenced_path_parameter_is_renamed_without_mutating_component()
    {
        var parameter = new Microsoft.OpenApi.OpenApiParameter { Name="user_id", In=Microsoft.OpenApi.ParameterLocation.Path, Required=true, Schema=new Microsoft.OpenApi.OpenApiSchema { Type=Microsoft.OpenApi.JsonSchemaType.String } };
        var document = new Microsoft.OpenApi.OpenApiDocument { Components = new Microsoft.OpenApi.OpenApiComponents { Parameters=new System.Collections.Generic.Dictionary<string,Microsoft.OpenApi.IOpenApiParameter> { ["UserId"]=parameter } }, Paths=new Microsoft.OpenApi.OpenApiPaths() };
        document.Paths["/users/{user_id}"] = new Microsoft.OpenApi.OpenApiPathItem { Parameters=[new Microsoft.OpenApi.OpenApiParameterReference("UserId",document)] };
        Resolve<Soenneker.OpenApi.Fixer.Fixers.Abstract.IOpenApiNamingFixer>(true).RenameConflictingPaths(document);
        await Assert.That(document.Paths["/users/{userId}"].Parameters![0].Name).IsEqualTo("userId");
        await Assert.That(parameter.Name).IsEqualTo("user_id");
    }


    [Test]
    public async ValueTask References_in_prefix_items_follow_schema_renames(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"old_name":{"type":"object","properties":{"name":{"type":"string"}}},"TupleValue":{"type":"array","prefixItems":[{"$ref":"#/components/schemas/old_name"}]}}""", ct, version:"3.1.0");
        await Assert.That(schemas["TupleValue"]!["prefixItems"]![0]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/OldName");
    }


    [Test]
    public async ValueTask Required_only_object_keeps_required_and_untyped_properties(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Record":{"type":"object","required":["name"]}}""", ct);
        await Assert.That(schemas["Record"]!["required"]![0]!.GetValue<string>()).IsEqualTo("name");
        await Assert.That(schemas["Record"]!["properties"]?["name"]?["type"]).IsNull();
    }


    [Test]
    public async ValueTask Nullable_enum_keeps_null_as_a_valid_member(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Choice":{"anyOf":[{"type":"string","enum":["x"]},{"type":"null"}]}}""", ct, version:"3.1.0");
        await Assert.That(schemas["Choice"]!["enum"]!.AsArray().Any(value => value == null)).IsTrue();
    }

    [Test]
    public async ValueTask Discriminator_does_not_wrap_scalar_alternative(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Choice":{"anyOf":[{"type":"string","enum":["x"]},{"type":"object","properties":{"name":{"type":"string"}}}],"discriminator":{"propertyName":"kind"}}}""", ct);
        await Assert.That(schemas["Choice"]!["discriminator"]).IsNull();
        await Assert.That(schemas.ToJsonString().Contains("Wrapper")).IsFalse();
    }


    [Test]
    public async ValueTask Equivalent_route_aliases_preserve_both_response_alternatives(CancellationToken ct)
    {
        JsonObject document = await Fix("{}", ct, paths: """
            {"/users/{id}":{"get":{"operationId":"byId","parameters":[{"in":"path","name":"id","required":true,"schema":{"type":"string"}}],"responses":{"200":{"description":"First","content":{"application/json":{"schema":{"type":"string"}}}}}}},
             "/users/{user_id}":{"get":{"operationId":"byUserId","parameters":[{"in":"path","name":"user_id","required":true,"schema":{"type":"string"}}],"responses":{"200":{"description":"Second","content":{"application/json":{"schema":{"type":"integer"}}}}}}}}
            """, wholeDocument: true);
        JsonObject paths = document["paths"]!.AsObject();
        await Assert.That(paths.Count).IsEqualTo(1);
        JsonNode operation = paths.First().Value!["get"]!;
        await Assert.That(operation["x-original-operation-ids"]!.AsArray().Count).IsEqualTo(2);
        JsonNode schema = operation["responses"]!["200"]!["content"]!["application/json"]!["schema"]!;
        if (schema["$ref"] is JsonValue reference)
            schema = document["components"]!["schemas"]![reference.GetValue<string>().Split('/')[^1]]!;
        await Assert.That(schema["anyOf"]!.AsArray().Count).IsEqualTo(2);
    }
    [Test]
    public async ValueTask Composition_with_control_character_enum_keeps_literal_value(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Code":{"allOf":[{"type":"string","enum":["a\u0001b"]}]}}""", ct);
        await Assert.That(schemas["Code"]!["enum"]![0]!.GetValue<string>()).IsEqualTo("a\u0001b");
    }


    [Test]
    public async ValueTask Generated_message_array_override_is_repaired_without_changing_wire_model(CancellationToken ct)
    {
        string directory = Path.Combine(Path.GetTempPath(), System.Guid.NewGuid().ToString());
        Directory.CreateDirectory(directory);
        const string source = """
            public class Problem {
                public override string Message { get => MessageEscaped ?? string.Empty; }
                public List<string>? MessageEscaped { get; set; }
            }
            """;
        string generated = Path.Combine(directory, "Generated.cs"), manual = Path.Combine(directory, "Manual.cs");
        try
        {
            await File.WriteAllTextAsync(generated, "// <auto-generated/>\n" + source, ct);
            await File.WriteAllTextAsync(manual, source, ct);
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            string fixedCode = await File.ReadAllTextAsync(generated, ct);
            await Assert.That(fixedCode).Contains("string.Join(\"; \", MessageEscaped)");
            await Assert.That(fixedCode).Contains("public List<string>? MessageEscaped");
            await Assert.That(await File.ReadAllTextAsync(manual, ct)).IsEqualTo(source);
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            await Assert.That(await File.ReadAllTextAsync(generated, ct)).IsEqualTo(fixedCode);
        }
        finally { File.Delete(generated); File.Delete(manual); Directory.Delete(directory); }
    }

    [Test]
    public async ValueTask Generated_enum_defaults_follow_wire_names_without_rewriting_existing_members(CancellationToken ct)
    {
        string directory = Path.Combine(Path.GetTempPath(), System.Guid.NewGuid().ToString());
        Directory.CreateDirectory(directory);
        string enumFile = Path.Combine(directory, "Risk.cs"), modelFile = Path.Combine(directory, "Model.cs");
        try
        {
            await File.WriteAllTextAsync(enumFile, """
                // <auto-generated/>
                namespace Vendor.Models;
                public enum Risk {
                    [EnumMember(Value = "UNKNOWN")]
                    #pragma warning disable CS1591
                    Unknown,
                    #pragma warning restore CS1591
                    [EnumMember(Value = "default")]
                    #pragma warning disable CS1591
                    DefaultValue,
                    #pragma warning restore CS1591
                }
                """, ct);
            await File.WriteAllTextAsync(modelFile, """
                // <auto-generated/>
                namespace Vendor;
                public class Model {
                    public Model() {
                        Risk = Vendor.Models.Risk.UNKNOWN;
                        Fallback = global::Vendor.Models.Risk.Default;
                        Valid = Vendor.Models.Risk.Unknown;
                        Text = "Vendor.Models.Risk.UNKNOWN";
                    }
                }
                """, ct);
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            string result = await File.ReadAllTextAsync(modelFile, ct);
            await Assert.That(result).Contains("Risk = Vendor.Models.Risk.Unknown;");
            await Assert.That(result).Contains("Fallback = global::Vendor.Models.Risk.DefaultValue;");
            await Assert.That(result).Contains("Valid = Vendor.Models.Risk.Unknown;");
            await Assert.That(result).Contains("Text = \"Vendor.Models.Risk.UNKNOWN\";");
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            await Assert.That(await File.ReadAllTextAsync(modelFile, ct)).IsEqualTo(result);
        }
        finally { File.Delete(enumFile); File.Delete(modelFile); Directory.Delete(directory); }
    }

    [Test]
    public async ValueTask Annotation_wrapper_keeps_closed_object_and_constraints(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"Record":{"type":"object","additionalProperties":false,"minProperties":1,"properties":{"name":{"type":"string","minLength":2}}},
             "Alias":{"allOf":[{"$ref":"#/components/schemas/Record"}],"description":"Alias description"},
             "Constrained":{"allOf":[{"$ref":"#/components/schemas/Record"}],"maxProperties":2}}
            """, ct);
        await Assert.That(schemas["Alias"]!["additionalProperties"]!.GetValue<bool>()).IsFalse();
        await Assert.That(schemas["Alias"]!["minProperties"]!.GetValue<int>()).IsEqualTo(1);
        await Assert.That(schemas["Alias"]!["properties"]!["name"]!["minLength"]!.GetValue<int>()).IsEqualTo(2);
        await Assert.That(schemas["Alias"]!["description"]!.GetValue<string>()).IsEqualTo("Alias description");
        await Assert.That(schemas["Alias"]!["allOf"]).IsNull();
        await Assert.That(schemas["Constrained"]!["maxProperties"]!.GetValue<int>()).IsEqualTo(2);
        await Assert.That(schemas["Constrained"]!["allOf"]).IsNotNull();
    }

    [Test]
    public async ValueTask Recursive_annotation_wrapper_is_not_expanded(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"Node":{"type":"object","additionalProperties":false,"properties":{"child":{"allOf":[{"$ref":"#/components/schemas/Node"}],"description":"Child"}}}}
            """, ct);
        await Assert.That(schemas.ToJsonString().Length < 5000).IsTrue();
        await Assert.That(schemas["Node"]!["properties"]!["child"]).IsNotNull();
    }

    [Test]
    public async ValueTask Boolean_alternatives_intersect_without_object_inheritance(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"Flag":{"allOf":[{"type":"boolean"},{"anyOf":[{"type":"boolean","enum":[true]},{"type":"boolean","enum":[false]}]}]}}
            """, ct);
        await Assert.That(schemas["Flag"]!["type"]!.GetValue<string>()).IsEqualTo("boolean");
        await Assert.That(schemas["Flag"]!["allOf"]).IsNull();
        await Assert.That(schemas["Flag"]!["enum"]!.AsArray().Count).IsEqualTo(2);
    }

    [Test]
    public async ValueTask Shared_fields_are_distributed_into_object_alternatives(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"DistributedPayload":{"type":"object","properties":{"shared":{"type":"string"}},"allOf":[{"anyOf":[
             {"type":"object","required":["left"],"properties":{"left":{"type":"string"}}},
             {"type":"object","required":["right"],"properties":{"right":{"type":"number"}}}]}]}}
            """, ct);
        JsonArray alternatives = schemas["DistributedPayload"]!["anyOf"]!.AsArray();
        await Assert.That(alternatives.Count).IsEqualTo(2);
        foreach (JsonNode? alternative in alternatives)
            await Assert.That(alternative!["properties"]!["shared"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(alternatives[0]!["required"]![0]!.GetValue<string>()).IsEqualTo("left");
        await Assert.That(alternatives[1]!["required"]![0]!.GetValue<string>()).IsEqualTo("right");
        await Assert.That(schemas["DistributedPayload"]!["properties"]).IsNull();
    }

    [Test]
    public async ValueTask Contradictory_enum_intersection_remains_impossible(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""{"Impossible":{"type":"string","enum":["x"],"allOf":[{"type":"string","enum":["y"]}]}}""", ct);
        await Assert.That(schemas["Impossible"]!["not"]!.AsObject().Count).IsEqualTo(0);
        await Assert.That(schemas["Impossible"]!["allOf"]).IsNull();
    }

    [Test]
    public async ValueTask Dictionary_union_preserves_map_constraints_and_empty_string(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"Metadata":{"anyOf":[{"type":"object","additionalProperties":{"type":"string","maxLength":500},"maxProperties":50},{"type":"string","enum":[""]}]}}
            """, ct);
        JsonArray branches = schemas["Metadata"]!["anyOf"]!.AsArray();
        JsonNode map = branches.First(branch => branch?["$ref"] != null)!;
        map = schemas[map["$ref"]!.GetValue<string>().Split('/')[^1]]!;
        await Assert.That(map["additionalProperties"]!["maxLength"]!.GetValue<int>()).IsEqualTo(500);
        await Assert.That(map["maxProperties"]!.GetValue<int>()).IsEqualTo(50);
        await Assert.That(branches.Any(branch => branch?["enum"] is JsonArray values && values.Any(value => value?.GetValue<string>() == ""))).IsTrue();
    }

    [Test]
    public async ValueTask Generated_route_repairs_preserve_slashes_and_literal_item(CancellationToken ct)
    {
        string directory = Path.Combine(Path.GetTempPath(), System.Guid.NewGuid().ToString());
        Directory.CreateDirectory(directory);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(directory, "fixed.json"), """{"paths":{"/items":{"get":{"x-original-trailing-slashes":1},"post":{}}}}""", ct);
            await File.WriteAllTextAsync(Path.Combine(directory, "kiota-lock.json"), """{"descriptionLocation":"fixed.json"}""", ct);
            string path = Path.Combine(directory, "ItemsRequestBuilder.cs");
            await File.WriteAllTextAsync(path, """
                // <auto-generated/>
                namespace Vendor;
                public partial class ItemsRequestBuilder : BaseRequestBuilder
                {
                    public ItemRequestBuilder Item { get; }
                    public ItemRequestBuilder this[string key] { get => new(); }
                    public ItemsRequestBuilder(IRequestAdapter requestAdapter) : base(requestAdapter, "{+baseurl}/items{?page}") {}
                    public object Get() => new RequestInformation(Method.GET, UrlTemplate, PathParameters);
                    public object Post() => new RequestInformation(Method.POST, UrlTemplate, PathParameters);
                }
                """, ct);
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            string result = await File.ReadAllTextAsync(path, ct);
            await Assert.That(result).Contains("IndexerName(\"ItemByKey\")");
            await Assert.That(result).Contains("UrlTemplate.Insert");
            await Assert.That(result).Contains("Method.POST, UrlTemplate, PathParameters");
            await Assert.That(result).Contains("{+baseurl}/items{?page}");
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            await Assert.That(await File.ReadAllTextAsync(path, ct)).IsEqualTo(result);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Test]
    public async ValueTask Generated_inherited_field_shares_storage_and_is_parsed_once(CancellationToken ct)
    {
        string directory = Path.Combine(Path.GetTempPath(), System.Guid.NewGuid().ToString());
        Directory.CreateDirectory(directory);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(directory, "Base.cs"), """
                // <auto-generated/>
                namespace Vendor.Models;
                public partial class Base
                {
                    public string Name { get; set; }
                    public void Serialize(ISerializationWriter writer)
                    {
                        writer.WriteStringValue("name", Name);
                    }
                }
                """, ct);
            string path = Path.Combine(directory, "Derived.cs");
            await File.WriteAllTextAsync(path, """
                // <auto-generated/>
                namespace Vendor.Models;
                public partial class Derived : Vendor.Models.Base
                {
                    public string Name { get; set; }
                    public void Serialize(ISerializationWriter writer)
                    {
                        base.Serialize(writer);
                        writer.WriteStringValue("name", Name);
                    }
                    public object GetFieldDeserializers() => new Dictionary<string, Action<IParseNode>>(base.GetFieldDeserializers())
                    {
                        { "name", n => { Name = n.GetStringValue(); } },
                    };
                }
                """, ct);
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            string result = await File.ReadAllTextAsync(path, ct);
            await Assert.That(result).Contains("get => base.Name; set => base.Name = value;");
            await Assert.That(result.Contains("writer.WriteStringValue")).IsFalse();
            await Assert.That(result.Contains("n.GetStringValue")).IsFalse();
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            await Assert.That(await File.ReadAllTextAsync(path, ct)).IsEqualTo(result);
        }
        finally { Directory.Delete(directory, true); }
    }
    [Test]
    public async ValueTask Nested_tagged_union_keeps_typed_variants_and_wire_discriminator(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"Tagged":{"anyOf":[{"oneOf":[
              {"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["first"]},"a":{"type":"string"}}},
              {"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["second"]},"b":{"type":"integer"}}}]}]}}
            """, ct);
        JsonNode tagged = schemas["Tagged"]!;
        await Assert.That(tagged["anyOf"]).IsNull();
        await Assert.That(tagged["oneOf"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(tagged["discriminator"]!["propertyName"]!.GetValue<string>()).IsEqualTo("kind");
        foreach (string label in new[] { "first", "second" })
        {
            string reference = tagged["discriminator"]!["mapping"]![label]!.GetValue<string>();
            await Assert.That(schemas[reference.Split('/')[^1]]).IsNotNull();
        }
    }

    [Test]
    public async ValueTask Nullable_type_array_in_union_is_preserved(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"NullableMapChoice":{"anyOf":[{"type":["object","null"],"additionalProperties":{"type":"string"}},{"type":"string"}]}}
            """, ct, version: "3.1.0");
        await Assert.That(schemas["NullableMapChoice"]!.ToJsonString()).Contains("null");
    }
    [Test]
    public async ValueTask Optional_tags_do_not_become_required_or_make_anyof_exclusive(CancellationToken ct)
    {
        const string alternatives = """[{"type":"object","properties":{"kind":{"type":"string","enum":["a"]}}},{"type":"object","properties":{"kind":{"type":"string","enum":["b"]}}}]""";
        JsonObject schemas = await Fix("{\"OptionalTagged\":{\"type\":\"object\",\"oneOf\":" + alternatives + "},\"OptionalUnion\":{\"anyOf\":" + alternatives + "}}", ct);
        await Assert.That(schemas["OptionalTagged"]!["discriminator"]!["propertyName"]!.GetValue<string>()).IsEqualTo("kind");
        await Assert.That(schemas["OptionalTagged"]!["required"]).IsNull();
        await Assert.That(schemas["OptionalUnion"]!["oneOf"]).IsNull();
        await Assert.That(schemas["OptionalUnion"]!["discriminator"]).IsNull();
    }

    [Test]
    public async ValueTask Nested_disjoint_anyof_becomes_exclusive_without_losing_variants(CancellationToken ct)
    {
        JsonObject schemas = await Fix("""
            {"NestedTags":{"anyOf":[{"oneOf":[
              {"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["a"]}}},
              {"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["b"]}}}]},
              {"oneOf":[{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["c"]}}}]}]}}
            """, ct);
        await Assert.That(schemas["NestedTags"]!["anyOf"]).IsNull();
        await Assert.That(schemas["NestedTags"]!["oneOf"]!.AsArray().Count).IsEqualTo(3);
        await Assert.That(schemas["NestedTags"]!["discriminator"]!["mapping"]!.AsObject().Count).IsEqualTo(3);
    }

    [Test]
    public async ValueTask Missing_plain_model_factory_is_repaired_without_overriding_discriminators(CancellationToken ct)
    {
        string directory = Path.Combine(Path.GetTempPath(), System.Guid.NewGuid().ToString());
        Directory.CreateDirectory(directory);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(directory, "fixed.json"), """{"components":{"schemas":{"Plain":{"type":"object"},"Polymorphic":{"discriminator":{"propertyName":"kind"}}}}}""", ct);
            await File.WriteAllTextAsync(Path.Combine(directory, "kiota-lock.json"), """{"descriptionLocation":"fixed.json"}""", ct);
            foreach (string name in new[] { "Plain", "Polymorphic" })
                await File.WriteAllTextAsync(Path.Combine(directory, name + ".cs"), "// <auto-generated/>\nnamespace Vendor;\npublic partial class " + name + " : IAdditionalDataHolder, IParsable\n#pragma warning restore CS1591\n{\n}\n", ct);
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            string result = await File.ReadAllTextAsync(Path.Combine(directory, "Plain.cs"), ct);
            await Assert.That(result).Contains("return new Plain();");
            await Assert.That((await File.ReadAllTextAsync(Path.Combine(directory, "Polymorphic.cs"), ct)).Contains("CreateFromDiscriminatorValue")).IsFalse();
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            await Assert.That(await File.ReadAllTextAsync(Path.Combine(directory, "Plain.cs"), ct)).IsEqualTo(result);
        }
        finally { Directory.Delete(directory, true); }
    }
    [Test]
    public async ValueTask Raw_yaml_generation_can_run_source_repairs_without_json_metadata(CancellationToken ct)
    {
        string directory = Path.Combine(Path.GetTempPath(), System.Guid.NewGuid().ToString());
        Directory.CreateDirectory(directory);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(directory, "spec.yaml"), "openapi: 3.0.3\ninfo:\n  title: Raw\n  version: '1'\npaths: {}\n", ct);
            await File.WriteAllTextAsync(Path.Combine(directory, "kiota-lock.json"), """{"descriptionLocation":"spec.yaml"}""", ct);
            const string source = "// <auto-generated/>\nnamespace Vendor;\npublic partial class Plain : IParsable { }\n";
            await File.WriteAllTextAsync(Path.Combine(directory, "Plain.cs"), source, ct);
            await _fixer.SanitizeGeneratedEnumMembers(directory, ct);
            await Assert.That(await File.ReadAllTextAsync(Path.Combine(directory, "Plain.cs"), ct)).IsEqualTo(source);
        }
        finally { Directory.Delete(directory, true); }
    }
    private async ValueTask<JsonObject> Fix(string schemas, CancellationToken ct, string paths = "{}", string version = "3.0.3", bool wholeDocument = false)
    {
        string source = Path.GetTempFileName(), target = Path.GetTempFileName();
        try
        {
            var doc = new JsonObject { ["openapi"]=version, ["info"]=new JsonObject { ["title"]="Audit", ["version"]="1" }, ["paths"]=JsonNode.Parse(paths), ["components"]=new JsonObject { ["schemas"]=JsonNode.Parse(schemas) } };
            await File.WriteAllTextAsync(source, doc.ToJsonString(), ct);
            await _fixer.Fix(source, target, ct);
            JsonNode result = JsonNode.Parse(await File.ReadAllTextAsync(target, ct))!;
            return wholeDocument ? result.AsObject() : result["components"]!["schemas"]!.AsObject();
        }
        finally { File.Delete(source); File.Delete(target); }
    }

    private static bool Contains(JsonObject schemas, JsonNode node, string key, string value, int depth=0)
    {
        if(depth>30) return false;
        if(node["$ref"] is JsonValue reference)
            return Contains(schemas, schemas[reference.GetValue<string>().Split('/')[^1]]!, key, value, depth+1);
        if(node[key]?.ToJsonString()==value) return true;
        if(node["allOf"] is JsonArray branches)
            foreach(JsonNode? branch in branches)
                if(branch!=null && Contains(schemas,branch,key,value,depth+1)) return true;
        return false;
    }
}
