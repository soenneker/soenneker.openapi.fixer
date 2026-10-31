using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedUnionTests
{
    [Test]
    public async ValueTask Nested_wrapper_does_not_turn_its_parent_into_a_wrapper()
    {
        const string source = """
            public partial class Parent : IParsable {
                public string Name { get; set; }
                public partial class Value : IComposedTypeWrapper, IParsable {
                    public string String { get; set; }
                    public static Value CreateFromDiscriminatorValue(IParseNode parseNode) { var result = new Value(); return result; }
                    public virtual void Serialize(ISerializationWriter writer) { writer.WriteStringValue(null, String); }
                }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Parent":{"anyOf":[{"type":"object"},{"type":"object"}]}}""")!.AsObject();
        await Assert.That(Repair(source, schemas)).IsEqualTo(source);
    }

    [Test]
    public async ValueTask Empty_nested_union_wrappers_emit_json_null_instead_of_an_empty_document()
    {
        const string source = """
            public partial class Parent : IParsable
            {
                public void Serialize(ISerializationWriter writer) { writer.WriteStringValue("name", Name); }
                public partial class Value : IComposedTypeWrapper, IParsable
                {
                    public string? String { get; set; }
                    public int? Integer { get; set; }
                    public static Value CreateFromDiscriminatorValue(IParseNode parseNode) { return new Value(); }
                    public virtual void Serialize(ISerializationWriter writer) { if (String != null) writer.WriteStringValue(null, String); else if (Integer != null) writer.WriteIntValue(null, Integer); }
                }
            }
            """;
        string Run(string text) => (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedEmptyUnionSerialization", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [text])!;
        string repaired = Run(source);
        await Assert.That(repaired).Contains("String == null && Integer == null");
        await Assert.That(repaired).Contains("writer.WriteNullValue(null); return;");
        await Assert.That(repaired).Contains("UntypedNull) return new Value();");
        await Assert.That(repaired).Contains("public void Serialize(ISerializationWriter writer) { writer.WriteStringValue(\"name\", Name); }");
        await Assert.That(Run(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Union_boolean_and_number_getters_require_matching_json_shapes()
    {
        const string source = "IComposedTypeWrapper if(parseNode.GetBoolValue() is bool b) {} else if(parseNode.GetIntValue() is int i) {}";
        string repaired = RepairShapes(source);
        await Assert.That(repaired).Contains("UntypedBoolean && parseNode.GetBoolValue()");
        await Assert.That(repaired).Contains("UntypedInteger && parseNode.GetIntValue()");
        await Assert.That(RepairShapes(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Multipart_operations_accept_the_runtime_multipart_body_without_changing_json_operations()
    {
        const string source = """
            public Task PostAsync(global::Models.Body body) {}
            public RequestInformation ToPostRequestInformation(global::Models.Body body) {
                requestInfo.SetContentFromParsable(RequestAdapter, "multipart/form-data", body);
                return requestInfo;
            }
            public Task PutAsync(global::Models.Body body) {}
            """;
        string repaired = (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedMultipartBodies", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [source])!;
        await Assert.That(repaired).Contains("PostAsync(global::Microsoft.Kiota.Abstractions.MultipartBody body)");
        await Assert.That(repaired).Contains("ToPostRequestInformation(global::Microsoft.Kiota.Abstractions.MultipartBody body)");
        await Assert.That(repaired).Contains("PutAsync(global::Models.Body body)");
    }

    [Test]
    public async ValueTask Union_string_getters_require_a_string_node_and_repairs_are_idempotent()
    {
        const string source = "IComposedTypeWrapper if(parseNode.GetStringValue() is string value) { result.String = value; }";
        string repaired = RepairShapes(source);
        await Assert.That(repaired).Contains("UntypedString && parseNode.GetStringValue()");
        await Assert.That(RepairShapes(repaired)).IsEqualTo(repaired);
        await Assert.That(RepairShapes(source.Replace("IComposedTypeWrapper", "IParsable"))).IsEqualTo(source.Replace("IComposedTypeWrapper", "IParsable"));
    }

    [Test]
    public async ValueTask Ambiguous_objects_preserve_payloads_instead_of_initializing_every_branch()
    {
        JsonObject schemas = JsonNode.Parse("""
            {"Choice":{"anyOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
             "Left":{"type":"object","properties":{"left":{"type":"string"}}},
             "Right":{"type":"object","properties":{"right":{"type":"string"}}}}
            """)!.AsObject();
        string repaired = Repair(Source, schemas);
        await Assert.That(repaired).Contains("result.UnrecognizedValue = unionPayload");
        await Assert.That(repaired).DoesNotContain("result.Left = new global::Models.Left();");
        await Assert.That(repaired).DoesNotContain("result.Right = new global::Models.Right();");
        await Assert.That(repaired).Contains("IAdditionalDataHolder");
        await Assert.That(repaired).Contains("writer.WriteNullValue(null)");
        await Assert.That(repaired).Contains("OpenApiUnionAdditionalData");
        await Assert.That(Repair(repaired, schemas)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Required_literal_tags_select_only_a_unique_matching_branch()
    {
        JsonObject schemas = JsonNode.Parse("""
            {"Choice":{"anyOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
             "Left":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["left"]}}},
             "Right":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["right"]}}}}
            """)!.AsObject();
        string repaired = Repair(Source, schemas);
        await Assert.That(repaired).Contains("\"left\".Equals(");
        await Assert.That(repaired).Contains("\"right\".Equals(");
        await Assert.That(repaired).Contains("matchesUnionBranch0 && matchingUnionBranches == 1");
        await Assert.That(repaired).Contains("matchesUnionBranch1 && matchingUnionBranches == 1");
        await Assert.That(repaired).DoesNotContain("GetChildNode(\"type\")");
    }

    [Test]
    public async ValueTask Multiple_object_arrays_do_not_duplicate_factory_locals_before_early_returns()
    {
        JsonObject schemas = JsonNode.Parse("""
            {"Choice":{"oneOf":[{"type":"array","items":{"$ref":"#/components/schemas/Left"}},
              {"type":"array","items":{"$ref":"#/components/schemas/Right"}}]}}
            """)!.AsObject();
        string source = Source.Replace("public global::Models.Left Left", "public List<global::Models.Left> Left")
            .Replace("public global::Models.Right Right", "public List<global::Models.Right> Right");
        string repaired = Repair(source, schemas);
        await Assert.That(repaired.Split("var unionPayload =").Length).IsEqualTo(2);
        await Assert.That(repaired).Contains("result.UnrecognizedValue = ambiguousArray; return result;");
    }

    [Test]
    public async ValueTask Inherited_required_fields_and_tags_participate_in_union_selection()
    {
        JsonObject schemas = JsonNode.Parse("""
            {"Choice":{"oneOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
             "Left":{"type":"object","allOf":[{"$ref":"#/components/schemas/LeftBase"}],"required":["common"],"properties":{"common":{"type":"string"}}},
             "Right":{"type":"object","allOf":[{"$ref":"#/components/schemas/RightBase"}],"required":["common"],"properties":{"common":{"type":"string"}}},
             "LeftBase":{"type":"object","required":["resource_id","kind"],"properties":{"resource_id":{"type":"string"},"kind":{"type":"string","enum":["left"]}}},
             "RightBase":{"type":"object","required":["external_id"],"properties":{"external_id":{"type":"string"}}}}
            """)!.AsObject();
        string repaired = Repair(Source, schemas);
        await Assert.That(repaired).Contains("ContainsKey(\"resource_id\")");
        await Assert.That(repaired).Contains("ContainsKey(\"external_id\")");
        await Assert.That(repaired).Contains("\"left\".Equals(");
        await Assert.That(repaired).Contains("matchesUnionBranch0 && matchingUnionBranches == 1");
        await Assert.That(Repair(repaired, schemas)).IsEqualTo(repaired);
    }

    private static string Repair(string source, JsonObject schemas) => (string)typeof(OpenApiFixer)
        .GetMethod("RepairGeneratedUnionFactories", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [source, schemas])!;

    private static string RepairShapes(string source) => (string)typeof(OpenApiFixer)
        .GetMethod("RepairGeneratedUnionCollectionChecks", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [source])!;

    private const string Source = """
        public partial class Choice : IParsable, IComposedTypeWrapper
        {
            public global::Models.Left Left { get; set; }
            public global::Models.Right Right { get; set; }
            public static global::Models.Choice CreateFromDiscriminatorValue(IParseNode parseNode)
            {
                var result = new global::Models.Choice();
                result.Left = new global::Models.Left();
                result.Right = new global::Models.Right();
                return result;
            }
            public virtual void Serialize(ISerializationWriter writer)
            {
                writer.WriteObjectValue<global::Models.Left>(null, Left, Right);
            }
        }
        """;
}
