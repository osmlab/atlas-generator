package org.openstreetmap.atlas.generator.sharding;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.spark.converters.ConfigurationConverter;
import org.openstreetmap.atlas.geography.sharding.DynamicTileSharding;
import org.openstreetmap.atlas.geography.sharding.GeoHashSharding;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTileSharding;
import org.openstreetmap.atlas.streaming.resource.ResourceCloseable;
import org.openstreetmap.atlas.utilities.collections.StringList;

/**
 * Convenience methods to get a sharding for the {@link AtlasGenerator}
 *
 * @author matthieun
 */
public final class AtlasSharding
{
    private static final int SHARDING_STRING_SPLIT = 2;
    private static final int SLIPPY_ZOOM_MAXIMUM = 18;

    /**
     * Parse a sharding definition string
     *
     * @param sharding
     *            The definition string
     * @param configuration
     *            configuration for the filesystem
     * @return The corresponding {@link AtlasSharding} instance.
     */
    public static Sharding forString(final String sharding, final Configuration configuration)
    {
        final StringList split;
        split = StringList.split(sharding, "@");
        if (split.size() != SHARDING_STRING_SPLIT)
        {
            throw new CoreException("Invalid sharding string: {}", sharding);
        }
        if ("slippy".equals(split.get(0)))
        {
            final int zoom;
            zoom = Integer.valueOf(split.get(1));
            if (zoom > SLIPPY_ZOOM_MAXIMUM)
            {
                throw new CoreException("Slippy Sharding zoom too high : {}, max is {}", zoom,
                        SLIPPY_ZOOM_MAXIMUM);
            }
            return new SlippyTileSharding(zoom);
        }
        if ("geohash".equals(split.get(0)))
        {
            final int precision;
            precision = Integer.valueOf(split.get(1));
            return new GeoHashSharding(precision);
        }
        if ("dynamic".equals(split.get(0)))
        {
            try (ResourceCloseable shardingResource = SparkJob.resource(split.get(1),
                    ConfigurationConverter.hadoopToMapConfiguration(configuration)))
            {
                if (shardingResource == null)
                {
                    throw new CoreException("Sharding resource does not exist: {}", sharding);
                }
                return new DynamicTileSharding(shardingResource);
            }
            catch (final Exception e)
            {
                throw new CoreException("Unable to parse sharding {}", sharding, e);
            }
        }
        throw new CoreException("Sharding type {} is not recognized.", split.get(0));
    }

    /**
     * Parse a sharding definition string
     *
     * @param sharding
     *            The definition string
     * @param configuration
     *            configuration for the filesystem
     * @return The corresponding {@link AtlasSharding} instance.
     */
    public static Sharding forString(final String sharding, final Map<String, String> configuration)
    {
        final Configuration hadoopConfiguration = new Configuration();
        configuration.forEach(hadoopConfiguration::set);
        return AtlasSharding.forString(sharding, hadoopConfiguration);
    }

    private AtlasSharding()
    {
    }
}
