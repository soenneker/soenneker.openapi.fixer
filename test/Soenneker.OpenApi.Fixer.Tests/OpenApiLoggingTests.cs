using Soenneker.Utils.MemoryStream;
using Microsoft.Extensions.Logging.Abstractions;
using Soenneker.Utils.File.Abstract;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using Soenneker.OpenApi.Fixer.Registrars;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiLoggingTests
{
    private static readonly IFileUtil _fileUtil = new Soenneker.Utils.File.FileUtil(NullLogger<Soenneker.Utils.File.FileUtil>.Instance, new MemoryStreamUtil());

    private const string Spec = """
        {"openapi":"3.1.0","info":{"title":"Logging","version":"1"},"paths":{
          "/records":{"get":{"operationId":"get-record","responses":{"200":{"description":"OK","content":{
            "application/json":{"schema":{"$ref":"#/components/schemas/record-model"}}
          }}}}}},"components":{"schemas":{"record-model":{"type":"object","properties":{
            "child":{"type":"object","properties":{"text":{"type":"string"}}}
          }}}}}
        """;

    private static string DuplicateSpec => Spec.Replace("\"title\":\"Logging\"", "\"title\":\"Earlier\",\"title\":\"Logging\"", StringComparison.Ordinal);

    [Test]
    public async ValueTask Defaults_include_file_progress_without_individual_repair_details(CancellationToken cancellationToken)
    {
        var logs = new RecordingProvider();
        await using ServiceProvider services = CreateServices(logs);
        await Fix(services.GetRequiredService<IOpenApiFixer>(), Spec, null, cancellationToken);

        await Assert.That(logs.FixerEntries.Length).IsEqualTo(0);
        Entry[] progress = logs.Entries.Where(entry => entry.EventId.Name == "OpenApiFixProgress").ToArray();
        await Assert.That(progress.Any(entry => entry.Message.StartsWith("Starting OpenAPI fix:", StringComparison.Ordinal))).IsTrue();
        await Assert.That(progress.Any(entry => entry.Message.StartsWith("Validating temporary OpenAPI output", StringComparison.Ordinal))).IsTrue();
        await Assert.That(progress.Last().Message.StartsWith("Cleaned OpenAPI spec saved", StringComparison.Ordinal)).IsTrue();
        await Assert.That(progress.All(entry => entry.Level == LogLevel.Information)).IsTrue();
        await Assert.That(logs.Entries.Any(entry => entry.Category.StartsWith("Soenneker.Utils.File", StringComparison.Ordinal))).IsFalse();
    }

    [Test]
    public async ValueTask Verbose_logging_includes_progress_details_and_completion_without_changing_output(CancellationToken cancellationToken)
    {
        var logs = new RecordingProvider();
        await using ServiceProvider services = CreateServices(logs, LogLevel.Information);
        IOpenApiFixer fixer = services.GetRequiredService<IOpenApiFixer>();
        string baseline = await Fix(fixer, Spec, null, cancellationToken);
        string result = await Fix(fixer, Spec, new OpenApiFixerOptions { VerboseLogging = true }, cancellationToken);
        Entry[] entries = logs.FixerEntries;

        await Assert.That(result).IsEqualTo(baseline);
        await Assert.That(logs.Entries.Any(entry => entry.Message.StartsWith("Running initial cleanup", StringComparison.Ordinal))).IsTrue();
        await Assert.That(logs.Entries.Any(entry => entry.Message.StartsWith("Cleaned OpenAPI spec saved", StringComparison.Ordinal))).IsTrue();
        await Assert.That(entries.Any(entry => entry.Category.EndsWith("OpenApiNamingFixer", StringComparison.Ordinal))).IsTrue();
        await Assert.That(entries.All(entry => entry.Level == LogLevel.Information)).IsTrue();

        await Fix(fixer, Spec, null, cancellationToken);
        await Assert.That(logs.FixerEntries.Length).IsEqualTo(entries.Length);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask Warnings_and_errors_remain_available_regardless_of_verbosity(bool verbose, CancellationToken cancellationToken)
    {
        var logs = new RecordingProvider();
        await using ServiceProvider services = CreateServices(logs);
        IOpenApiFixer fixer = services.GetRequiredService<IOpenApiFixer>();
        var options = new OpenApiFixerOptions { VerboseLogging = verbose };
        await Fix(fixer, DuplicateSpec, options, cancellationToken);
        Exception? failure = null;
        try { await Fix(fixer, "invalid JSON", options, cancellationToken); }
        catch (Exception ex) { failure = ex; }

        await Assert.That(failure).IsNotNull();
        await Assert.That(logs.FixerEntries.Any(entry => entry.Level == LogLevel.Warning)).IsTrue();
        await Assert.That(logs.FixerEntries.Count(entry => entry.Level == LogLevel.Error)).IsEqualTo(1);
        int afterFailure = logs.FixerEntries.Length;
        await Fix(fixer, Spec, null, cancellationToken);
        await Assert.That(logs.FixerEntries.Length).IsEqualTo(afterFailure);
    }

    [Test]
    public async ValueTask Host_severity_filters_still_apply(CancellationToken cancellationToken)
    {
        var logs = new RecordingProvider();
        await using ServiceProvider services = CreateServices(logs, LogLevel.Warning);
        await Fix(services.GetRequiredService<IOpenApiFixer>(), DuplicateSpec,
            new OpenApiFixerOptions { VerboseLogging = true }, cancellationToken);

        await Assert.That(logs.FixerEntries.Length).IsGreaterThan(0);
        await Assert.That(logs.FixerEntries.All(entry => entry.Level == LogLevel.Warning)).IsTrue();
    }

    [Test]
    public async ValueTask Standalone_preprocessing_honors_verbosity_and_restores_defaults()
    {
        var logs = new RecordingProvider();
        await using ServiceProvider services = CreateServices(logs);
        IOpenApiPreprocessingFixer fixer = services.GetRequiredService<IOpenApiPreprocessingFixer>();
        string result = fixer.Fix("invalid JSON", new OpenApiFixerOptions { VerboseLogging = true });
        await Assert.That(result).IsEqualTo("invalid JSON");
        await Assert.That(logs.FixerEntries.Length).IsGreaterThan(0);
        int verboseCount = logs.FixerEntries.Length;
        fixer.Fix("invalid JSON");
        await Assert.That(logs.FixerEntries.Length).IsEqualTo(verboseCount);
        fixer.Fix(DuplicateSpec);
        await Assert.That(logs.FixerEntries.Count(entry => entry.Level == LogLevel.Warning)).IsEqualTo(1);
    }

    [Test]
    public async ValueTask Cancellation_propagates_and_restores_defaults(CancellationToken cancellationToken)
    {
        var logs = new RecordingProvider();
        await using ServiceProvider services = CreateServices(logs);
        IOpenApiFixer fixer = services.GetRequiredService<IOpenApiFixer>();
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        Exception? failure = null;
        try
        {
            await fixer.Fix("unused-source.json", "unused-target.json",
                new OpenApiFixerOptions { VerboseLogging = true }, canceled.Token);
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        await Assert.That(failure is OperationCanceledException).IsTrue();
        await Assert.That(logs.Entries.Any(entry => entry.Message.StartsWith("OpenAPI fix was canceled:", StringComparison.Ordinal))).IsTrue();
        await Assert.That(logs.FixerEntries.Any(entry => entry.Level == LogLevel.Error)).IsFalse();
        int afterCancellation = logs.FixerEntries.Length;
        await Fix(fixer, Spec, null, cancellationToken);
        await Assert.That(logs.FixerEntries.Length).IsEqualTo(afterCancellation);
    }

    [Test]
    public async ValueTask Concurrent_singleton_calls_use_isolated_snapshots(CancellationToken cancellationToken)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int blocked = 0;
        var logs = new RecordingProvider
        {
            OnEntry = entry =>
            {
                if (entry.Message.StartsWith("Normalizing operation IDs", StringComparison.Ordinal) && Interlocked.CompareExchange(ref blocked, 1, 0) == 0)
                {
                    entered.TrySetResult();
                    release.Task.WaitAsync(TimeSpan.FromSeconds(15), cancellationToken).GetAwaiter().GetResult();
                }
            }
        };
        await using ServiceProvider services = CreateServices(logs);
        IOpenApiFixer fixer = services.GetRequiredService<IOpenApiFixer>();
        var options = new OpenApiFixerOptions { VerboseLogging = true };
        Task<string> verbose = Task.Run(async () => await Fix(fixer, Spec, options, cancellationToken), cancellationToken);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(15), cancellationToken);
            int beforeQuietCall = logs.FixerEntries.Length;
            options.VerboseLogging = false;
            await Fix(fixer, Spec, options, cancellationToken);
            await Assert.That(logs.FixerEntries.Length).IsEqualTo(beforeQuietCall);
        }
        finally
        {
            release.TrySetResult();
            await verbose;
        }

        await Assert.That(logs.Entries.Any(entry => entry.Message.StartsWith("Cleaned OpenAPI spec saved", StringComparison.Ordinal))).IsTrue();
        await Assert.That(logs.FixerEntries.Any(entry => entry.Category.EndsWith("OpenApiNamingFixer", StringComparison.Ordinal))).IsTrue();
    }

    private static ServiceProvider CreateServices(RecordingProvider logs, LogLevel level = LogLevel.Trace)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddLogging(builder => builder.SetMinimumLevel(level).AddProvider(logs));
        services.AddOpenApiFixerAsSingleton();
        return services.BuildServiceProvider();
    }

    private static async Task<string> Fix(IOpenApiFixer fixer, string spec, OpenApiFixerOptions? options, CancellationToken cancellationToken)
    {
        string source = Path.GetTempFileName();
        string target = Path.GetTempFileName();
        try
        {
            await _fileUtil.Write(source, spec, cancellationToken: cancellationToken, log: false);
            await fixer.Fix(source, target, options, cancellationToken);
            return await _fileUtil.Read(target, cancellationToken: cancellationToken, log: false);
        }
        finally
        {
            await _fileUtil.Delete(source, log: false);
            await _fileUtil.Delete(target, log: false);
        }
    }

    private sealed record Entry(string Category, LogLevel Level, string Message, EventId EventId);

    private sealed class RecordingProvider : ILoggerProvider
    {
        internal ConcurrentQueue<Entry> Entries { get; } = new();
        internal Action<Entry>? OnEntry { get; init; }
        internal Entry[] FixerEntries => Entries.Where(entry => entry.Category.StartsWith("Soenneker.OpenApi.Fixer", StringComparison.Ordinal) && entry.EventId.Name != "OpenApiFixProgress").ToArray();

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(this, categoryName);
        public void Dispose() { }

        private sealed class RecordingLogger(RecordingProvider provider, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                var entry = new Entry(category, logLevel, formatter(state, exception), eventId);
                provider.Entries.Enqueue(entry);
                provider.OnEntry?.Invoke(entry);
            }
        }
    }
}
