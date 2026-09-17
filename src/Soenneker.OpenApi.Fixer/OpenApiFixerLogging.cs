using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Soenneker.OpenApi.Fixer;

internal static class OpenApiFixerLogging
{
    // Carry the per-call switch through singleton helpers without sharing mutable state between calls.
    private static readonly AsyncLocal<bool?> _verbose = new();

    internal static IDisposable Begin(bool verbose)
    {
        var scope = new Scope(_verbose.Value);
        _verbose.Value = verbose;
        return scope;
    }

    internal static IDisposable? BeginIfNeeded(bool verbose) => _verbose.Value is null ? Begin(verbose) : null;

    internal static void LogProgress(this ILogger logger, string message, params object?[] args) =>
        logger.LogInformation(new EventId(1000, "OpenApiFixProgress"), message, args);

    internal static bool IsVerboseEnabled(this ILogger logger) =>
        _verbose.Value == true && logger.IsEnabled(LogLevel.Information);

    internal static void LogVerbose(this ILogger logger, string message, params object?[] args)
    {
        if (logger.IsVerboseEnabled())
            logger.LogInformation(message, args);
    }

    internal static void LogVerbose(this ILogger logger, Exception exception, string message, params object?[] args)
    {
        if (logger.IsVerboseEnabled())
            logger.LogInformation(exception, message, args);
    }

    private sealed class Scope(bool? previous) : IDisposable
    {
        public void Dispose() => _verbose.Value = previous;
    }
}
