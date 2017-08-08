package org.openstreetmap.atlas.generator.sharding;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.geography.sharding.DynamicTileSharding;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTileSharding;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
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
        if ("dynamic".equals(split.get(0)))
        {
            final Path definition = new Path(split.get(1));
            try
            {
                final FileSystem fileSystem = definition.getFileSystem(configuration);
                return new DynamicTileSharding(new InputStreamResource(() ->
                {
                    try
                    {
                        return fileSystem.open(definition);
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException("Failed opening {}", definition, e);
                    }
                }));
            }
            catch (final IOException e)
            {
                throw new CoreException("Failed reading {}", definition, e);
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
