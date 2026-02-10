using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Fixer.Fixers;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using Soenneker.Utils.Directory;
using Soenneker.Utils.Directory.Abstract;
using Soenneker.Utils.Process.Registrars;

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
        services.AddProcessUtilAsSingleton();
        
        // Register fixer interfaces and implementations
        services.TryAddSingleton<IOpenApiDescriptionFixer, OpenApiDescriptionFixer>();
        services.TryAddSingleton<IOpenApiReferenceFixer, OpenApiReferenceFixer>();
        services.TryAddSingleton<IOpenApiNamingFixer, OpenApiNamingFixer>();
        services.TryAddSingleton<IOpenApiSchemaFixer, OpenApiSchemaFixer>();
        services.AddDirectoryUtilAsSingleton();

        // Register main fixer
        services.TryAddSingleton<IOpenApiFixer, OpenApiFixer>();

        return services;
    }

    /// <summary>
    /// Adds <see cref="IOpenApiFixer"/> as a scoped service. <para/>
    /// </summary>
    public static IServiceCollection AddOpenApiFixerAsScoped(this IServiceCollection services)
    {
        services.AddProcessUtilAsScoped();
        
        // Register fixer interfaces and implementations
        services.TryAddScoped<IOpenApiDescriptionFixer, OpenApiDescriptionFixer>();
        services.TryAddScoped<IOpenApiReferenceFixer, OpenApiReferenceFixer>();
        services.TryAddScoped<IOpenApiNamingFixer, OpenApiNamingFixer>();
        services.TryAddScoped<IOpenApiSchemaFixer, OpenApiSchemaFixer>();
        services.AddDirectoryUtilAsScoped();
        
        // Register main fixer
        services.TryAddScoped<IOpenApiFixer, OpenApiFixer>();

        return services;
    }
}