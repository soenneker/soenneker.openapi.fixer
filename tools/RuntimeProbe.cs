using System.Reflection;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Kiota.Abstractions.Serialization;
using Microsoft.Kiota.Serialization.Json;

internal static class RuntimeProbe
{
    public static async Task Run(string specification)
    {
        JsonNode document = JsonNode.Parse(await File.ReadAllTextAsync(specification))!;
        var failures = new List<string>();
        int models = 0, payloads = 0;
        var exercisedContracts = new HashSet<string>();
        foreach (Type type in Assembly.GetExecutingAssembly().GetTypes().Where(t => t.Namespace == "Verification.Client.Models" &&
            !t.IsAbstract && typeof(IParsable).IsAssignableFrom(t) && !t.IsNested))
        {
            if (document["components"]?["schemas"]?[type.Name] is not JsonObject schema) continue;
            models++;
            try
            {
                var model = (IParsable)Activator.CreateInstance(type)!;
                HashSet<string> fields = FieldNames(model, new HashSet<Type>());
                foreach (string field in Properties(schema, document, new HashSet<string>()))
                    if (!fields.Contains(field)) failures.Add(type.Name + ": missing deserializer for " + field);
                if (schema["x-verification-payloads"] is JsonArray examples)
                    foreach (JsonNode? example in examples)
                    {
                        await (Task)typeof(RuntimeProbe).GetMethod(nameof(RoundTrip), BindingFlags.NonPublic | BindingFlags.Static)!
                            .MakeGenericMethod(type).Invoke(null, [example!.ToJsonString(), true])!;
                        payloads++;
                    }
                if (schema["x-verification-provider-payloads"] is JsonArray providerExamples)
                {
                    exercisedContracts.Add(type.Name);
                    foreach (JsonNode? example in providerExamples)
                    {
                        await (Task)typeof(RuntimeProbe).GetMethod(nameof(RoundTrip), BindingFlags.NonPublic | BindingFlags.Static)!
                            .MakeGenericMethod(type).Invoke(null, [example!.ToJsonString(), false])!;
                        payloads++;
                    }
                }
            }
            catch (Exception error) { failures.Add(type.Name + ": " + error.GetBaseException().Message); }
        }
        if (document["components"]?["schemas"] is JsonObject schemas)
            foreach ((string name, JsonNode? schema) in schemas)
                if (schema?["x-verification-provider-payloads"] != null && !exercisedContracts.Contains(name))
                    failures.Add(name + ": required provider contract was not exercised");
        var report = new { models, payloads, failures };
        await File.WriteAllTextAsync(Path.Combine(Path.GetDirectoryName(specification)!, "runtime-report.json"),
            System.Text.Json.JsonSerializer.Serialize(report, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
        if (failures.Count > 0) throw new InvalidOperationException(string.Join(Environment.NewLine, failures.Take(30)));
        Console.WriteLine($"Verified {models} model field maps and {payloads} payload round trips.");
    }

    private static HashSet<string> FieldNames(IParsable model, HashSet<Type> seen)
    {
        var names = new HashSet<string>(model.GetFieldDeserializers().Keys);
        if (!seen.Add(model.GetType()) || model is not IComposedTypeWrapper) return names;
        foreach (PropertyInfo property in model.GetType().GetProperties().Where(p => typeof(IParsable).IsAssignableFrom(p.PropertyType) &&
            !p.PropertyType.IsAbstract && p.PropertyType.GetConstructor(Type.EmptyTypes) != null))
            names.UnionWith(FieldNames((IParsable)Activator.CreateInstance(property.PropertyType)!, seen));
        return names;
    }

    private static IEnumerable<string> Properties(JsonObject schema, JsonNode document, HashSet<string> seen)
    {
        if (schema["$ref"] is JsonValue reference)
        {
            string path = reference.GetValue<string>();
            if (path.StartsWith("#/components/schemas/") && seen.Add(path) &&
                document["components"]?["schemas"]?[path[21..].Replace("~1", "/").Replace("~0", "~")] is JsonObject target)
                foreach (string name in Properties(target, document, seen)) yield return name;
        }
        if (schema["properties"] is JsonObject properties)
            foreach (string name in properties.Select(p => p.Key)) yield return name;
        if (schema["allOf"] is JsonArray all)
            foreach (JsonObject item in all.OfType<JsonObject>())
                foreach (string name in Properties(item, document, seen)) yield return name;
    }

    private static async Task RoundTrip<T>(string json, bool exact) where T : IParsable
    {
        var factory = (ParsableFactory<T>)typeof(T).GetMethod("CreateFromDiscriminatorValue", BindingFlags.Static | BindingFlags.Public)!
            .CreateDelegate(typeof(ParsableFactory<T>));
        using var input = new MemoryStream(Encoding.UTF8.GetBytes(json));
        IParseNode node = await new JsonParseNodeFactory().GetRootParseNodeAsync("application/json", input);
        T? value = node.GetObjectValue(factory);
        using var writer = new JsonSerializationWriter();
        writer.WriteObjectValue(null, value);
        using Stream output = writer.GetSerializedContent();
        JsonNode? actual = await JsonNode.ParseAsync(output);
        JsonNode? expected = JsonNode.Parse(json);
        if (exact ? !JsonNode.DeepEquals(expected, actual) : !Preserves(expected, actual))
            throw new InvalidOperationException($"Payload changed: {json} -> {actual}");
    }

    private static bool Preserves(JsonNode? expected, JsonNode? actual)
    {
        if (expected is JsonObject fields)
            return actual is JsonObject result && fields.All(p => result.ContainsKey(p.Key) && Preserves(p.Value, result[p.Key]));
        if (expected is JsonArray values)
            return actual is JsonArray result && values.Count == result.Count && values.Select((value, index) => Preserves(value, result[index])).All(v => v);
        return JsonNode.DeepEquals(expected, actual);
    }
}
