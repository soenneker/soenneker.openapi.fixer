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

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask Array_errors_do_not_downgrade_another_operations_typed_mapping(bool defaultError)
    {
        const string source = """
            /// <exception cref="List<Models.Error>">When receiving a 400 status code</exception>
            public async Task First() {
                var errorMapping = new Dictionary<string, ParsableFactory<IParsable>> {
                    { "400", Models.Error.CreateFromDiscriminatorValue },
                };
                await RequestAdapter.SendNoContentAsync(requestInfo, errorMapping, cancellationToken);
            }
            /// <exception cref="Models.Error">When receiving a 400 status code</exception>
            public async Task Second() {
                var errorMapping = new Dictionary<string, ParsableFactory<IParsable>> {
                    { "400", Models.Error.CreateFromDiscriminatorValue },
                };
                await RequestAdapter.SendNoContentAsync(requestInfo, errorMapping, cancellationToken);
            }
            """;
        string input = defaultError ? source.Replace("400", "XXX").Replace("receiving a XXX", "receiving a 4XX or 5XX") : source;
        string repaired = Repair(input);
        string status = defaultError ? "XXX" : "400";
        await Assert.That(repaired).DoesNotContain("cref=\"List<");
        await Assert.That(repaired).Contains("{ \"" + status + "\", global::Soenneker.OpenApi.Generated.OpenApiRawError.CreateFromDiscriminatorValue }");
        await Assert.That(repaired).Contains("{ \"" + status + "\", Models.Error.CreateFromDiscriminatorValue }");
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }

    private static string Repair(string source) => (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedErrorFallbacks", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [source])!;
}
