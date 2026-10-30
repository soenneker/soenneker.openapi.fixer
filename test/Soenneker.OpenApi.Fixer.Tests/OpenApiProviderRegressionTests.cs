using System.Collections.Generic;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiProviderRegressionTests
{
    [Test]
    public async ValueTask Explicit_form_encodings_correct_request_media_types_only()
    {
        const string json = """
            {"openapi":"3.0.3","paths":{"/token":{"post":{"requestBody":{"content":{
              "text/plain":{"schema":{"type":"object"},"encoding":{"grant_type":{"style":"form"}}}}}}},
              "/text":{"post":{"requestBody":{"content":{"text/plain":{"schema":{"type":"string"}}}}}}}}
            """;
        string result = Run("NormalizeExplicitFormMediaTypes", json);
        JsonNode paths = JsonNode.Parse(result)!["paths"]!;
        await Assert.That(paths["/token"]!["post"]!["requestBody"]!["content"]!["application/x-www-form-urlencoded"]).IsNotNull();
        await Assert.That(paths["/text"]!["post"]!["requestBody"]!["content"]!["text/plain"]).IsNotNull();
        await Assert.That(Run("NormalizeExplicitFormMediaTypes", result)).IsEqualTo(result);
    }

    [Test]
    public async ValueTask Documented_hex_identifiers_retain_strings_without_weakening_valid_uuid_fields()
    {
        const string json = """
            {"openapi":"3.0.3","components":{"schemas":{"Item":{"type":"object",
              "properties":{"id":{"type":"string","format":"uuid"},"uuid":{"type":"string","format":"uuid"}},
              "example":{"id":"580e64008c9a982ac9b8b754","uuid":"12345678-1234-1234-1234-123456789abc"}}}}}
            """;
        JsonNode fields = JsonNode.Parse(Run("ReconcileSchemaExamples", json))!["components"]!["schemas"]!["Item"]!["properties"]!;
        await Assert.That(fields["id"]!["format"]).IsNull();
        await Assert.That(fields["id"]!["x-client-original-format"]!.GetValue<string>()).IsEqualTo("uuid");
        await Assert.That(fields["uuid"]!["format"]!.GetValue<string>()).IsEqualTo("uuid");
    }

    [Test]
    public async ValueTask Declared_empty_enum_values_are_available_as_typed_members()
    {
        const string source = "namespace Models { public enum Choice { Other, } }";
        JsonObject schemas = JsonNode.Parse("""{"Choice":{"type":"string","enum":["","other"]}}""")!.AsObject();
        var members = new Dictionary<string, string>();
        string result = Run("RepairGeneratedEmptyEnum", source, schemas, members);
        await Assert.That(result).Contains("[EnumMember(Value = \"\")]");
        await Assert.That(members["global::Models.Choice"]).IsEqualTo("EmptyString");
        await Assert.That(Run("RepairGeneratedEmptyEnum", result, schemas, members)).IsEqualTo(result);
        string read = Run("RepairGeneratedEmptyEnumReads", "n.GetEnumValue<global::Models.Choice>()", members);
        await Assert.That(read).Contains("global::Models.Choice.EmptyString");
        await Assert.That(Run("RepairGeneratedEmptyEnumReads", read, members)).IsEqualTo(read);
    }

    [Test]
    public async ValueTask Scalar_aliases_inline_without_changing_enums_or_multitype_schemas()
    {
        const string json = """
            {"openapi":"3.0.3","components":{"schemas":{
              "ModelId":{"type":"string","example":"model-id","x-client-inline-scalar":true},
              "Mode":{"type":"string","enum":["auto"]},
              "Mixed":{"type":["string","null"]},
              "Request":{"type":"object","properties":{
                "model":{"$ref":"#/components/schemas/ModelId"},
                "mode":{"$ref":"#/components/schemas/Mode"},
                "mixed":{"$ref":"#/components/schemas/Mixed"}}}}}}
            """;
        string result = Run("InlineScalarReferences", json);
        JsonNode fields = JsonNode.Parse(result)!["components"]!["schemas"]!["Request"]!["properties"]!;
        await Assert.That(fields["model"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(fields["model"]!["example"]!.GetValue<string>()).IsEqualTo("model-id");
        await Assert.That(fields["mode"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Mode");
        await Assert.That(fields["mixed"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Mixed");
        await Assert.That(Run("InlineScalarReferences", result)).IsEqualTo(result);
    }

    [Test]
    public async ValueTask Binary_collections_use_base64_strings_and_retain_null_tracking()
    {
        const string source = """
            public partial class Certificate : IParsable {
              public List<byte[]> CaChain { get; set; }
              public void Serialize(ISerializationWriter writer) {
                writer.WriteCollectionOfPrimitiveValues<byte[]>("caChain", CaChain);
              }
              void Read(IParseNode n) { CaChain = n.GetCollectionOfPrimitiveValues<byte[]>()?.AsList(); }
            }
            """;
        string result = Run("RepairGeneratedUntypedCollections", source);
        await Assert.That(result).Contains("Convert.FromBase64String(value)");
        await Assert.That(result).Contains("Convert.ToBase64String(value)");
        result = Run("RepairGeneratedExplicitNulls", result, new Dictionary<string, string>());
        await Assert.That(result).Contains("_explicitNullValueCaChain");
        await Assert.That(result).Contains("_explicitNullProperties?.Remove(\"caChain\")");
    }

    private static string Run(string method, params object[] args) =>
        (string)typeof(OpenApiFixer).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, args)!;
}
