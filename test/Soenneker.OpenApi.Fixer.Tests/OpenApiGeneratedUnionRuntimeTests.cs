using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Kiota.Abstractions.Serialization;
using Microsoft.Kiota.Serialization.Json;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedUnionRuntimeTests
{
    [Test]
    public async ValueTask Generated_wrapper_selects_only_a_known_unique_match_and_round_trips_unknown_payloads()
    {
        JsonObject schemas = JsonNode.Parse("""
            {"Choice":{"anyOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
             "Left":{"type":"object","required":["id","created"],"properties":{"id":{"type":"string"},"created":{"type":"integer"}}},
             "Right":{"type":"object","required":["id","deleted"],"properties":{"id":{"type":"string"},"deleted":{"const":true}}}}
            """)!.AsObject();
        Type known = await CompileWrapper(schemas);
        await Assert.That(known.GetProperty("Left")!.GetValue(Parse(known, """{"id":"a","created":1}"""))).IsNotNull();
        await Assert.That(known.GetProperty("Right")!.GetValue(Parse(known, """{"id":"d","deleted":true}"""))).IsNotNull();
        foreach (string payload in new[] { """{"id":"a","created":1,"deleted":true}""", """{"id":123}""" })
            await AssertRaw(known, payload);

        schemas["Right"]!["properties"]!["deleted"] = new JsonObject { ["unsupportedAssertion"] = true };
        Type uncertain = await CompileWrapper(schemas);
        await AssertRaw(uncertain, """{"id":"a","created":1,"deleted":true}""");
        await AssertRaw(uncertain, """{"id":"a","deleted":true}""");
    }

    private static IParsable Parse(Type wrapper, string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);
        var node = new JsonParseNode(document.RootElement.Clone());
        return node.GetObjectValue(parseNode => (IParsable)wrapper.GetMethod("CreateFromDiscriminatorValue")!.Invoke(null, [parseNode])!)!;
    }

    private static async ValueTask AssertRaw(Type wrapper, string json)
    {
        IParsable instance = Parse(wrapper, json);
        await Assert.That(wrapper.GetProperty("Left")!.GetValue(instance)).IsNull();
        await Assert.That(wrapper.GetProperty("Right")!.GetValue(instance)).IsNull();
        using var writer = new JsonSerializationWriter();
        writer.WriteObjectValue(null, instance);
        using var reader = new StreamReader(writer.GetSerializedContent());
        await Assert.That(JsonNode.DeepEquals(JsonNode.Parse(json), JsonNode.Parse(await reader.ReadToEndAsync()))).IsTrue();
    }

    private static async ValueTask<Type> CompileWrapper(JsonObject schemas)
    {
        var matchers = new GeneratedUnionMatcherWriter(schemas);
        string source = (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedUnionFactories", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [Source, schemas, matchers])!;
        string directory = Path.Combine(Path.GetTempPath(), $"union-runtime-{Guid.NewGuid()}");
        Directory.CreateDirectory(directory);
        try
        {
            await (ValueTask)typeof(OpenApiFixer).GetMethod("WriteGeneratedUnionSupport", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [directory, CancellationToken.None])!;
            string[] support = Directory.GetFiles(directory, "*.cs").Select(File.ReadAllText).ToArray();
            return GeneratedUnionRuntimeCompiler.Compile(support.Append(source).Append(matchers.BuildSource()).ToArray()).GetType("Models.Choice")!;
        }
        finally { Directory.Delete(directory, true); }
    }

    private const string Source = """
        using System;
        using System.Collections.Generic;
        using Microsoft.Kiota.Abstractions.Serialization;
        namespace Models {
        public partial class Choice : IParsable, IComposedTypeWrapper {
            public global::Models.Left Left { get; set; }
            public global::Models.Right Right { get; set; }
            public static global::Models.Choice CreateFromDiscriminatorValue(IParseNode parseNode) {
                var result = new global::Models.Choice();
                result.Left = new global::Models.Left();
                result.Right = new global::Models.Right();
                return result;
            }
            public virtual IDictionary<string, Action<IParseNode>> GetFieldDeserializers() { return new Dictionary<string, Action<IParseNode>>(); }
            public virtual void Serialize(ISerializationWriter writer) { writer.WriteObjectValue<global::Models.Left>(null, Left, Right); }
        }
        public class Left : IParsable {
            public IDictionary<string, Action<IParseNode>> GetFieldDeserializers() => new Dictionary<string, Action<IParseNode>>();
            public void Serialize(ISerializationWriter writer) { }
        }
        public class Right : Left { }
        }
        """;
}
