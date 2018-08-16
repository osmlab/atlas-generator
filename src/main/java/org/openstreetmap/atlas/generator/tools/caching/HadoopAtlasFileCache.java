package org.openstreetmap.atlas.generator.tools.caching;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.caching.ConcurrentResourceCache;
import org.openstreetmap.atlas.utilities.caching.strategies.SystemTemporaryFileCachingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache an atlas file stored in the standard way (parentpath/COUNTRY/COUNTRY_z-x-y.atlas) to a
 * system temporary location. This cache is designed to be used with atlases that can be fetched
 * from a hadoop file system. This cache is threadsafe.
 *
 * @author lcram
 */
public class HadoopAtlasFileCache extends ConcurrentResourceCache
{
    private static final Logger logger = LoggerFactory.getLogger(HadoopAtlasFileCache.class);

    private final String parentAtlasPath;

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param configuration
     *            The configuration map
     */
    public HadoopAtlasFileCache(final String parentAtlasPath,
            final Map<String, String> configuration)
    {
        super(new SystemTemporaryFileCachingStrategy(),
                uri -> FileSystemHelper.resource(uri.toString(), configuration));
        this.parentAtlasPath = parentAtlasPath;
    }

    /**
     * Get an {@link Optional} of an atlas resource specified by the given parameters.
     *
     * @param country
     *            The ISO country code of the desired shard
     * @param shard
     *            The {@link Shard} object representing the shard
     * @return an {@link Optional} wrapping the shard
     */
    public Optional<Atlas> get(final String country, final Shard shard)
    {
        final String atlasName = String.format("%s_%s", country, shard.getName());
        final String atlasURIString = this.parentAtlasPath + "/" + country + "/" + atlasName
                + FileSuffix.ATLAS.toString();
        final URI atlasURI;

        try
        {
            atlasURI = new URI(atlasURIString);
        }
        catch (final URISyntaxException exception)
        {
            throw new CoreException("Bad URI syntax: {}", atlasURIString, exception);
        }

        final Optional<Resource> atlasResourceOptional = this.get(atlasURI);

        if (!atlasResourceOptional.isPresent())
        {
            return Optional.empty();
        }

        return Optional.ofNullable(new AtlasResourceLoader().load(atlasResourceOptional.get()));
    }
}
