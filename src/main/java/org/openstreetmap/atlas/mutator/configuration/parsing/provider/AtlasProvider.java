package org.openstreetmap.atlas.mutator.configuration.parsing.provider;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.mutator.configuration.parsing.provider.file.AtlasFileAtlasProvider;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * A specific interface for a function that takes a country code and shard and makes it an Atlas
 * object optional. This type can be inferred at runtime from the class name in a configuration file
 * provided that the implementation has a default constructor which takes a {@link Configuration}
 * object.
 * 
 * @author matthieun
 */
public interface AtlasProvider extends BiFunction<String, Shard, Optional<Atlas>>, Serializable
{
    /**
     * @author matthieun
     */
    class AtlasProviderConstants
    {
        public static final String SPARK_CONFIGURATION_KEY = "sparkConfiguration";
        public static final String FILE_PATH_KEY = "filePath";

        private AtlasProviderConstants()
        {
        }
    }

    // The default provider assumes that the resource is a PackedAtlas and just loads it directly.
    static AtlasProvider defaultProvider()
    {
        return new AtlasFileAtlasProvider();
    }

    static AtlasProvider from(final String className, final Configuration configuration)
    {
        try
        {
            final Class<?> clazz = Class.forName(className);
            if (!AtlasProvider.class.isAssignableFrom(clazz))
            {
                throw new CoreException("Class {} is not an AtlasProvider.", clazz.getName());
            }
            return (AtlasProvider) clazz.getDeclaredConstructor(Configuration.class)
                    .newInstance(configuration);
        }
        catch (final Exception e)
        {
            throw new CoreException("Cannot instantiate AtlasProvider {}", className, e);
        }
    }

    /**
     * Provide more generic runtime context to the AtlasProvider, this method to be used right
     * before running the dependent mutation.
     * 
     * @param context
     *            Generic context that is needed by the provider
     */
    default void setAtlasProviderContext(final Map<String, Object> context)
    {
    }
}
