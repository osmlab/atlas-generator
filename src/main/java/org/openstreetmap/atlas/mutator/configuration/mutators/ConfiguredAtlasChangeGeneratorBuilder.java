package org.openstreetmap.atlas.mutator.configuration.mutators;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.ConfiguredObjectBuilder;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matthieun
 */
public class ConfiguredAtlasChangeGeneratorBuilder extends ConfiguredObjectBuilder
{
    private static final Logger logger = LoggerFactory
            .getLogger(ConfiguredAtlasChangeGeneratorBuilder.class);

    public ConfiguredAtlasChangeGeneratorBuilder(final Configuration configuration,
            final Map<String, String> sparkConfiguration)
    {
        super(configuration, sparkConfiguration);
    }

    public Optional<ConfiguredAtlasChangeGenerator> create(final String country,
            final String mutatorName, final Configuration configuration)
    {
        try
        {
            final String className = getClassName(mutatorName);
            final Class<?> clazz = getClass(mutatorName);
            final Optional<ConfiguredAtlasChangeGenerator> result = temporaryConfiguredAtlasChangeGenerator(
                    clazz, className, mutatorName, configuration, country);
            return result.filter(candidate -> isUsable(candidate, country));
        }
        catch (final Exception e)
        {
            throw new CoreException("Cannot instantiate AtlasChangeGenerator {} for {}",
                    mutatorName, country, e);
        }
    }

    private boolean isUsable(final ConfiguredAtlasChangeGenerator candidate, final String country)
    {
        candidate.setSparkConfiguration(this.getSparkConfiguration());
        final Set<String> candidateCountries = candidate.getCountries();
        final Set<String> candidateExcludedCountries = candidate.getExcludedCountries();
        if (candidateCountries.isEmpty())
        {
            if (candidateExcludedCountries.contains(country))
            {
                return false;
            }
        }
        else
        {
            if (!candidateCountries.contains(country)
                    || candidateExcludedCountries.contains(country))
            {
                return false;
            }
        }
        return candidate.isEnabled();
    }

    private Optional<ConfiguredAtlasChangeGenerator> temporaryConfiguredAtlasChangeGenerator(
            final Class<?> clazz, final String className, final String mutatorName,
            final Configuration configuration, final String country) throws NoSuchMethodException,
            IllegalAccessException, InstantiationException, InvocationTargetException
    {
        ConfiguredAtlasChangeGenerator result = null;
        if (clazz == null)
        {
            logger.error("Unable to recognize mutation. Class {} not found for {}.", className,
                    mutatorName);
        }
        else
        {
            if (ConfiguredAtlasChangeGenerator.class.isAssignableFrom(clazz))
            {
                // If configured, try to construct it with the Configuration
                result = (ConfiguredAtlasChangeGenerator) clazz
                        .getConstructor(String.class, Configuration.class)
                        .newInstance(mutatorName, configuration);
            }
            else if (AtlasChangeGenerator.class.isAssignableFrom(clazz))
            {
                // Otherwise use the default constructor and the general interface.
                final AtlasChangeGenerator source = (AtlasChangeGenerator) clazz.getConstructor()
                        .newInstance();
                result = new BasicConfiguredAtlasChangeGenerator(className, source, configuration);
            }
            else
            {
                verifyDisabled(mutatorName, country, configuration);
            }
        }
        return Optional.ofNullable(result);
    }

    private void verifyDisabled(final String mutatorName, final String country,
            final Configuration configuration)
    {
        final BasicConfiguredAtlasChangeGenerator noOp = BasicConfiguredAtlasChangeGenerator
                .noOp(mutatorName, configuration);
        final String message = "Mutator {} was defined in the configuration, "
                + "but could not be created for {}.{}";
        if (noOp.isEnabled())
        {
            throw new CoreException(message, mutatorName, country, "");
        }
        else
        {
            logger.warn(message, mutatorName, country,
                    " It is not enabled=true so it will be ignored.");
        }
    }
}
