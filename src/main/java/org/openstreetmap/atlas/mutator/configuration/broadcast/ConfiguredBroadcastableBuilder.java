package org.openstreetmap.atlas.mutator.configuration.broadcast;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.mutator.configuration.ConfiguredObjectBuilder;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * @author matthieun
 */
public class ConfiguredBroadcastableBuilder extends ConfiguredObjectBuilder
{
    public ConfiguredBroadcastableBuilder(final Configuration configuration)
    {
        super(configuration, Maps.hashMap());
    }

    public ConfiguredBroadcastable create(final String name)
    {
        final String path = ConfiguredBroadcastable.CONFIGURATION_ROOT + "." + name;
        final Class<?> clazz = getClass(path);
        if (clazz == null)
        {
            throw new CoreException("Could not find a {} matching configuration path {}",
                    ConfiguredBroadcastable.class.getSimpleName(), path);
        }
        if (ConfiguredBroadcastable.class.isAssignableFrom(clazz))
        {
            try
            {
                return (ConfiguredBroadcastable) clazz
                        .getConstructor(String.class, Configuration.class)
                        .newInstance(name, getConfiguration());
            }
            catch (final Exception e)
            {
                throw new CoreException("Unable to create {}", name, e);
            }
        }
        else
        {
            throw new CoreException("{} is not a {}", name,
                    ConfiguredBroadcastable.class.getSimpleName());
        }
    }
}
