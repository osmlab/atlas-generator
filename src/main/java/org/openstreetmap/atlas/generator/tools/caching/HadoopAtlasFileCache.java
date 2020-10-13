package org.openstreetmap.atlas.generator.tools.caching;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.caching.ConcurrentResourceCache;
import org.openstreetmap.atlas.utilities.caching.strategies.CachingStrategy;
import org.openstreetmap.atlas.utilities.caching.strategies.NamespaceCachingStrategy;
import org.openstreetmap.atlas.utilities.runtime.Retry;
import org.openstreetmap.atlas.utilities.scalars.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache an atlas file stored in the standard way (parentpath/COUNTRY/COUNTRY_z-x-y.atlas) to a
 * system temporary location. This cache is designed to be used with atlases that can be fetched
 * from a hadoop file system. This cache is threadsafe. You can optionally associate a namespace
 * with a {@link HadoopAtlasFileCache} at creation time. This will create a unique container
 * per-namespace for cached content.
 * <p>
 * For example, consider the following code:
 * <p>
 * <code>
 * ResourceCache cache1 = new HadoopAtlasFileCache(parentPath, "namespace1", config);<br>
 * ResourceCache cache2 = new HadoopAtlasFileCache(parentPath, "namespace2", config);<br>
 * <br>
 * // We will be fetching resource behind URI "parentPath/AAA/AAA_1-1-1.atlas"<br> Resource r1 =
 * cache1.get("AAA", new SlippyTile(1, 1, 1)).get();<br> // Assume some event changes the contents
 * behind the URI between get() calls<br> Resource r2 = cache1.get("AAA", new SlippyTile(1, 1,
 * 1)).get();<br>
 * <br>
 * // This fails since the caches have different namespaces and the resource contents changed<br>
 * Assert.assertEquals(r1.all(), r2.all());<br>
 * <br>
 * // Now we invalidate cache1's copy of the resource<br> cache1.invalidate(getURIForResource(r1));<br>
 * // This call to cache1.get() will re-fetch since we invalidated<br> r1 = cache1.get("AAA", new
 * SlippyTile(1, 1, 1)).get();<br>
 * <br>
 * // Now this passes since cache1 was refreshed<br> Assert.assertEquals(r1.all(), r2.all());<br>
 * </code>
 * <p>
 * The key takeaway here is that <code>r1</code> and <code>r2</code> are not guaranteed to have the
 * same contents, even though their URIs are the same. Since the two cache instances have different
 * namespaces, their underlying stores do not intersect. It is also worth noting that the
 * {@link HadoopAtlasFileCache} cannot detect if the resource behind a URI has changed. You must
 * call {@link HadoopAtlasFileCache#invalidate()} or {@link HadoopAtlasFileCache#invalidate(URI)} to
 * force an update.
 * <p>
 *
 * @author lcram
 * @author sbhalekar
 */
public class HadoopAtlasFileCache extends ConcurrentResourceCache
{
    private static final Logger logger = LoggerFactory.getLogger(HadoopAtlasFileCache.class);
    private static final String GLOBAL_HADOOP_FILE_CACHE_NAMESPACE = "__HadoopAtlasFileCache_global_namespace__";
    private static final int RETRY_ATTEMPTS = 5;

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
        this(parentAtlasPath, GLOBAL_HADOOP_FILE_CACHE_NAMESPACE,
                AtlasGeneratorParameters.ATLAS_SCHEME.getDefault(), configuration);
    }

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param fetcher
     *            Function to fetch an atlas resource in case of cache miss
     */
    public HadoopAtlasFileCache(final String parentAtlasPath,
            final Function<URI, Optional<Resource>> fetcher)
    {
        this(parentAtlasPath, GLOBAL_HADOOP_FILE_CACHE_NAMESPACE,
                AtlasGeneratorParameters.ATLAS_SCHEME.getDefault(), fetcher);
    }

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param cachingStrategy
     *            Strategy to cache an atlas resource
     * @param fetcher
     *            Function to fetch an atlas resource in case of cache miss
     */
    public HadoopAtlasFileCache(final String parentAtlasPath, final CachingStrategy cachingStrategy,
            final Function<URI, Optional<Resource>> fetcher)
    {
        this(parentAtlasPath, AtlasGeneratorParameters.ATLAS_SCHEME.getDefault(), cachingStrategy,
                fetcher);
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
        this(parentAtlasPath, GLOBAL_HADOOP_FILE_CACHE_NAMESPACE, atlasScheme, configuration);
    }

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param namespace
     *            The namespace for this cache's resources
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
        this(parentAtlasPath, atlasScheme, new NamespaceCachingStrategy(namespace)
        {
            @Override
            protected void validateLocalFile(final File localFile)
            {
                // Make sure that the file is not corrupt by loading it.
                PackedAtlas.load(localFile);
            }
        }, uri ->
        {
            final Retry retry = new Retry(RETRY_ATTEMPTS, Duration.ONE_SECOND).withQuadratic(true);
            final boolean exists = retry.run(() ->
            {
                try (InputStream ignored = FileSystemHelper.resource(uri.toString(), configuration)
                        .read())
                {
                    return true;
                }
                catch (final Exception exception)
                {
                    if (exception.getMessage().contains(FileSystemHelper.FILE_NOT_FOUND))
                    {
                        return false;
                    }
                    else
                    {
                        throw new CoreException("Unable to test existence of {}", uri, exception);
                    }
                }
            });
            if (!exists)
            {
                logger.warn("Fetcher: resource {} does not exist!", uri);
                return Optional.empty();
            }
            return Optional.ofNullable(FileSystemHelper.resource(uri.toString(), configuration));
        });
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
     * @param fetcher
     *            Function to fetch an atlas resource in case of cache miss
     */
    public HadoopAtlasFileCache(final String parentAtlasPath, final String namespace,
            final SlippyTilePersistenceScheme atlasScheme,
            final Function<URI, Optional<Resource>> fetcher)
    {
        this(parentAtlasPath, atlasScheme, new NamespaceCachingStrategy(namespace)
        {
            @Override
            protected void validateLocalFile(final File localFile)
            {
                // Make sure that the file is not corrupt by loading it.
                PackedAtlas.load(localFile);
            }
        }, fetcher);
    }

    /**
     * Create a new cache.
     *
     * @param parentAtlasPath
     *            The parent path to the atlas files. This might look like hdfs://some/path/to/files
     * @param atlasScheme
     *            The scheme used to locate atlas files based on slippy tiles
     * @param cachingStrategy
     *            Strategy to cache an atlas resource
     * @param fetcher
     *            Function to fetch an atlas resource in case of cache miss
     */
    public HadoopAtlasFileCache(final String parentAtlasPath,
            final SlippyTilePersistenceScheme atlasScheme, final CachingStrategy cachingStrategy,
            final Function<URI, Optional<Resource>> fetcher)
    {
        super(cachingStrategy, fetcher);
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
        return this.get(getURIFromCountryAndShard(country, shard));
    }

    /**
     * Invalidate a given shard for a given country. It is highly recommended to use this
     * implementation over {@link HadoopAtlasFileCache#invalidate(URI)}.
     *
     * @param country
     *            the country
     * @param shard
     *            the shard to invalidate
     */
    public void invalidate(final String country, final Shard shard)
    {
        this.invalidate(getURIFromCountryAndShard(country, shard));
    }

    private URI getURIFromCountryAndShard(final String country, final Shard shard)
    {
        String compiledAtlasScheme = "";
        if (shard instanceof SlippyTile)
        {
            compiledAtlasScheme = this.atlasScheme.compile((SlippyTile) shard);
        }
        final String atlasName = String.format("%s_%s", country, shard.getName());

        final String atlasURIString = SparkFileHelper.combine(this.parentAtlasPath, country,
                compiledAtlasScheme, atlasName + FileSuffix.ATLAS.toString());

        final URI atlasURI;
        try
        {
            atlasURI = new URI(atlasURIString);
        }
        catch (final URISyntaxException exception)
        {
            throw new CoreException("Bad URI syntax: {}", atlasURIString, exception);
        }
        return atlasURI;
    }
}
