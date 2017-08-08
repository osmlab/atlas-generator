package org.openstreetmap.atlas.generator.tools.spark.context;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;

/**
 * Find a {@link SparkContextProvider} using its class name
 *
 * @author matthieun
 */
public class SparkContextProviderFinder implements StringConverter<SparkContextProvider>
{
    @Override
    public SparkContextProvider convert(final String value)
    {
        try
        {
            final Class<?> clazz = Class.forName(value);
            if (!SparkContextProvider.class.isAssignableFrom(clazz))
            {
                throw new CoreException("Class {} is not a SparkContextProvider.", clazz.getName());
            }
            return (SparkContextProvider) clazz.newInstance();
        }
        catch (final Exception e)
        {
            throw new CoreException("Cannot instantiate SparkContextProvider", e);
        }
    }
}
