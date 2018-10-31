package org.openstreetmap.atlas.generator.tools.caching;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.caching.ConcurrentResourceCache;
import org.openstreetmap.atlas.utilities.caching.strategies.NamespaceCachingStrategy;
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
    private static final String GLOBAL_HADOOP_FILECACHE_NAMESPACE = "__HadoopAtlasFileCache_global_namespace__";

    private final String parentAtlasPath;
    private final SlippyTilePersistenceScheme atlasScheme;

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
        this(parentAtlasPath, GLOBAL_HADOOP_FILECACHE_NAMESPACE,
                AtlasGeneratorParameters.ATLAS_SCHEME.getDefault(), configuration);
    }

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param atlasScheme
     *            The scheme used to locate atlas files based on slippy tiles
     * @param configuration
     *            The configuration map
     */
    public HadoopAtlasFileCache(final String parentAtlasPath,
            final SlippyTilePersistenceScheme atlasScheme, final Map<String, String> configuration)
    {
        this(parentAtlasPath, GLOBAL_HADOOP_FILECACHE_NAMESPACE, atlasScheme, configuration);
    }

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param namespace
     *            The namespace for this cache's resoures
     * @param configuration
     *            The configuration map
     */
    public HadoopAtlasFileCache(final String parentAtlasPath, final String namespace,
            final Map<String, String> configuration)
    {
        this(parentAtlasPath, namespace, AtlasGeneratorParameters.ATLAS_SCHEME.getDefault(),
                configuration);
    }

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param namespace
     *            The namespace for this cache's resoures
     * @param atlasScheme
     *            The scheme used to locate atlas files based on slippy tiles
     * @param configuration
     *            The configuration map
     */
    public HadoopAtlasFileCache(final String parentAtlasPath, final String namespace,
            final SlippyTilePersistenceScheme atlasScheme, final Map<String, String> configuration)
    {
        super(new NamespaceCachingStrategy(namespace), uri ->
        {
            if (!FileSystemHelper.exists(uri.toString(), configuration))
            {
                logger.warn("Fetcher: resource {} does not exist!", uri);
                return Optional.empty();
            }
            return Optional.ofNullable(FileSystemHelper.resource(uri.toString(), configuration));
        });
        this.parentAtlasPath = parentAtlasPath;
        this.atlasScheme = atlasScheme;
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
    public Optional<Resource> get(final String country, final Shard shard)
    {
        String compiledAtlasScheme = "";
        if (shard instanceof SlippyTile)
        {
            compiledAtlasScheme = this.atlasScheme.compile((SlippyTile) shard);
        }
        final String atlasName = String.format("%s_%s", country, shard.getName());
        final String atlasURIString = this.parentAtlasPath + "/" + country + "/"
                + compiledAtlasScheme + atlasName + FileSuffix.ATLAS.toString();
        final URI atlasURI;

        try
        {
            atlasURI = new URI(atlasURIString);
        }
        catch (final URISyntaxException exception)
        {
            throw new CoreException("Bad URI syntax: {}", atlasURIString, exception);
        }

        return this.get(atlasURI);
    }
}
