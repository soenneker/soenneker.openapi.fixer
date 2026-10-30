using System.Reflection;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedErrorTests
{
    [Test]
    public async ValueTask Missing_error_ranges_preserve_specific_typed_mappings_and_are_idempotent()
    {
        const string source = """
            var errorMapping = new Dictionary<string, ParsableFactory<IParsable>>
            {
                { "422", Models.ValidationError.CreateFromDiscriminatorValue },
            };
            return await RequestAdapter.SendAsync<Model>(requestInfo, Model.CreateFromDiscriminatorValue, errorMapping, cancellationToken);
            """;
        string repaired = Repair(source);
        await Assert.That(repaired).Contains("{ \"422\", Models.ValidationError.CreateFromDiscriminatorValue }");
        await Assert.That(repaired).Contains("{ \"4XX\", global::Soenneker.OpenApi.Generated.OpenApiRawError.CreateFromDiscriminatorValue }");
        await Assert.That(repaired).Contains("{ \"5XX\", global::Soenneker.OpenApi.Generated.OpenApiRawError.CreateFromDiscriminatorValue }");
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
        await Assert.That(Repair(source.Replace("\"422\"", "\"XXX\""))).IsEqualTo(source.Replace("\"422\"", "\"XXX\""));
    }

    [Test]
    public async ValueTask An_operation_without_an_error_dictionary_gets_payload_preserving_fallbacks()
    {
        const string source = "return await RequestAdapter.SendPrimitiveAsync<string>(requestInfo, default, cancellationToken).ConfigureAwait(false);";
        string repaired = Repair(source);
        await Assert.That(repaired).DoesNotContain(", default, cancellationToken)");
        await Assert.That(repaired).Contains("OpenApiRawError.CreateFromDiscriminatorValue");
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }

    private static string Repair(string source) => (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedErrorFallbacks", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [source])!;
}
