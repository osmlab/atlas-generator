package org.openstreetmap.atlas.mutator.configuration.parsing.provider.file;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.caching.ConcurrentResourceCache;
import org.openstreetmap.atlas.utilities.caching.strategies.CachingStrategy;
import org.openstreetmap.atlas.utilities.caching.strategies.NamespaceCachingStrategy;
import org.openstreetmap.atlas.utilities.runtime.Retry;
import org.openstreetmap.atlas.utilities.scalars.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lots of inspiration from {@link HadoopAtlasFileCache}
 * 
 * @author matthieun
 */
public class HadoopPbfFileCache extends ConcurrentResourceCache
{
    private static final Logger logger = LoggerFactory.getLogger(HadoopPbfFileCache.class);
    private static final String GLOBAL_HADOOP_FILE_CACHE_NAMESPACE = "__HadoopPbfFileCache_global_namespace__";
    private static final int RETRY_ATTEMPTS = 5;

    private final String parentPbfPath;
    private final SlippyTilePersistenceScheme pbfScheme;

    public HadoopPbfFileCache(final String parentPbfPath,
            final SlippyTilePersistenceScheme pbfScheme, final Map<String, String> configuration)
    {
        this(parentPbfPath, pbfScheme,
                new NamespaceCachingStrategy(GLOBAL_HADOOP_FILE_CACHE_NAMESPACE)
                {
                    @Override
                    protected void validateLocalFile(final File localFile)
                    {
                        // No inexpensive way to make it happen here...
                    }
                }, uri ->
                {
                    final Retry retry = new Retry(RETRY_ATTEMPTS, Duration.ONE_SECOND)
                            .withQuadratic(true);
                    final boolean exists = retry.run(() ->
                    {
                        try (InputStream ignored = FileSystemHelper
                                .resource(uri.toString(), configuration).read())
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
                                throw new CoreException("Unable to test existence of {}", uri,
                                        exception);
                            }
                        }
                    });
                    if (!exists)
                    {
                        logger.warn("Fetcher: resource {} does not exist!", uri);
                        return Optional.empty();
                    }
                    return Optional
                            .ofNullable(FileSystemHelper.resource(uri.toString(), configuration));
                });
    }

    public HadoopPbfFileCache(final String parentAtlasPath,
            final SlippyTilePersistenceScheme atlasScheme, final CachingStrategy cachingStrategy,
            final Function<URI, Optional<Resource>> fetcher)
    {
        super(cachingStrategy, fetcher);
        this.parentPbfPath = parentAtlasPath;
        this.pbfScheme = atlasScheme;
    }

    public Optional<Resource> get(final Shard shard)
    {
        return this.get(getURIFromShard(shard));
    }

    public void invalidate(final Shard shard)
    {
        this.invalidate(getURIFromShard(shard));
    }

    private URI getURIFromShard(final Shard shard)
    {
        String compiledPbfScheme = "";
        if (shard instanceof SlippyTile)
        {
            compiledPbfScheme = this.pbfScheme.compile((SlippyTile) shard);
        }
        final String pbfURIString = SparkFileHelper.combine(this.parentPbfPath, compiledPbfScheme);

        final URI pbfURI;
        try
        {
            pbfURI = new URI(pbfURIString);
        }
        catch (final URISyntaxException exception)
        {
            throw new CoreException("Bad URI syntax: {}", pbfURIString, exception);
        }
        return pbfURI;
    }
}
