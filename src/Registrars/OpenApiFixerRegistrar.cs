using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.OpenApi.Fixer.Abstract;

namespace Soenneker.OpenApi.Fixer.Registrars;

/// <summary>
/// A utility that fixes problem OpenApi specs being converted into clients
/// </summary>
public static class OpenApiFixerRegistrar
{
    /// <summary>
    /// Adds <see cref="IOpenApiFixer"/> as a singleton service. <para/>
    /// </summary>
    public static IServiceCollection AddOpenApiFixerAsSingleton(this IServiceCollection services)
    {
        services.TryAddSingleton<IOpenApiFixer, OpenApiFixer>();

        return services;
    }

    /// <summary>
    /// Adds <see cref="IOpenApiFixer"/> as a scoped service. <para/>
    /// </summary>
    public static IServiceCollection AddOpenApiFixerAsScoped(this IServiceCollection services)
    {
        services.TryAddScoped<IOpenApiFixer, OpenApiFixer>();

        return services;
    }
}
