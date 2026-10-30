using System;
using System.Collections.Generic;
using System.Reflection;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedServiceRouteTests
{
    [Test]
    [Arguments("?start_date={start_date}{&limit*}", "")]
    [Arguments("{?limit*}", "")]
    [Arguments("", "")]
    [Arguments("", ".json")]
    public async ValueTask Namespaced_routes_with_required_queries_restore_wire_paths(string query, string suffix)
    {
        Type metadataType = typeof(OpenApiFixer).Assembly.GetType("Soenneker.OpenApi.Fixer.OpenApiGeneratedRouteMetadata")!;
        object metadata = Activator.CreateInstance(metadataType, true)!;
        var routes = (Dictionary<string, Dictionary<string, (string Path, string Server)>>)metadataType.GetProperty("Routes", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)!;
        routes["/source/items/{}" + suffix] = new() { ["GET"] = ("/items/{id}" + suffix, "https://api.example.com/v1") };
        string source = "public Builder(IRequestAdapter requestAdapter) : base(requestAdapter, \"{+baseurl}/source/items/{itemId}" + query + "\") {}\nvoid Get() { var info = new RequestInformation(Method.GET, UrlTemplate, PathParameters); }";
        MethodInfo repair = typeof(OpenApiFixer).GetMethod("RepairGeneratedServiceRoutes", BindingFlags.Static | BindingFlags.NonPublic)!;
        string result = (string)repair.Invoke(null, [source, metadata])!;
        await Assert.That(result).Contains(".Resolve(UrlTemplate, RequestAdapter.BaseUrl, \"https://api.example.com/v1\", \"/items/{itemId}" + suffix + "\", PathParameters.ContainsKey");
        await Assert.That((string)repair.Invoke(null, [result, metadata])!).IsEqualTo(result);
    }
    [Test]
    [Arguments("{baseUrl}")]
    [Arguments("https://api.example.com/{tenant}")]
    public async ValueTask Unresolved_service_variables_remain_explicit_in_route_metadata(string server)
    {
        string root = Path.Combine(Path.GetTempPath(), "route-metadata-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            JsonObject document = JsonNode.Parse("""{"paths":{"/namespace/items":{"get":{"x-client-original-path":"/items"}}}}""")!.AsObject();
            document["servers"] = new JsonArray(new JsonObject { ["url"] = server });
            await File.WriteAllTextAsync(Path.Combine(root, "spec.json"), document.ToJsonString());
            await File.WriteAllTextAsync(Path.Combine(root, "kiota-lock.json"), """{"descriptionLocation":"spec.json"}""");
            object pending = typeof(OpenApiFixer).GetMethod("ReadGeneratedServiceRoutes", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [root, CancellationToken.None])!;
            Task task = (Task)pending.GetType().GetMethod("AsTask")!.Invoke(pending, null)!;
            await task;
            object metadata = task.GetType().GetProperty("Result")!.GetValue(task)!;
            Type type = metadata.GetType();
            var routes = (Dictionary<string, Dictionary<string, (string Path, string Server)>>)type.GetProperty("Routes", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)!;
            await Assert.That(routes["/namespace/items"]["GET"].Server).IsEqualTo(server);
            await Assert.That(type.GetProperty("DefaultServer", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)).IsNull();
        }
        finally { Directory.Delete(root, true); }
    }
}
