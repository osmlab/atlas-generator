package org.openstreetmap.atlas.mutator.configuration.parsing.provider.file;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.mutator.configuration.parsing.provider.AtlasProvider;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.caching.ConcurrentResourceCache;
import org.openstreetmap.atlas.utilities.caching.ResourceCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract AtlasProvider that focuses on accessing Atlas data from files/resources using
 * {@link ResourceCache}s.
 * 
 * @author matthieun
 */
public abstract class AbstractFileAtlasProvider implements AtlasProvider
{
    private static final long serialVersionUID = -6878724137563447659L;
    private static final Logger logger = LoggerFactory.getLogger(AbstractFileAtlasProvider.class);

    private String atlasPath = null;
    private Map<String, String> sparkConfiguration = null;

    @Override
    public Optional<Atlas> apply(final String country, final Shard shard)
    {
        Optional<Resource> resourceOption = getResourceFromCache(country, shard);
        if (resourceOption.isPresent())
        {
            final CountryShard countryShard = new CountryShard(country, shard);
            Resource namedAtlasResource = namedAtlasResource(countryShard, resourceOption.get());
            Optional<Atlas> namedAtlas = null;
            try
            {
                namedAtlas = resourceToAtlas(namedAtlasResource, country, shard);
                if (namedAtlas.isPresent())
                {
                    // Trigger a load
                    namedAtlas.get().numberOfAreas();
                }
                else
                {
                    return Optional.empty();
                }
            }
            catch (final Exception e)
            {
                // It is possible that the cache had a collision, and the file is broken, in
                // that case, retry.
                logger.warn(
                        "Invalidating cache for {} at {}, to attempt a re-fetch. (Source path was {})",
                        country, shard.getName(), this.atlasPath);
                invalidateCache(country, shard);
                resourceOption = getResourceFromCache(country, shard);
                if (resourceOption.isPresent())
                {
                    namedAtlasResource = namedAtlasResource(countryShard, resourceOption.get());
                    namedAtlas = resourceToAtlas(namedAtlasResource, country, shard);
                    if (namedAtlas.isPresent())
                    {
                        // Trigger a load
                        namedAtlas.get().numberOfAreas();
                    }
                    else
                    {
                        return Optional.empty();
                    }
                }
            }
            return namedAtlas;
        }
        return Optional.empty();
    }

    @Override
    public void setAtlasProviderContext(final Map<String, Object> context)
    {
        this.atlasPath = (String) context.get(AtlasProviderConstants.FILE_PATH_KEY);
        if (this.atlasPath == null)
        {
            throw new CoreException("AtlasProvider {} is missing atlasPath.",
                    this.getClass().getCanonicalName());
        }
        this.sparkConfiguration = (Map<String, String>) context
                .get(AtlasProviderConstants.SPARK_CONFIGURATION_KEY);
        if (this.sparkConfiguration == null)
        {
            throw new CoreException("AtlasProvider {} is missing sparkConfiguration.",
                    this.getClass().getCanonicalName());
        }
    }

    protected String getAtlasPath()
    {
        return this.atlasPath;
    }

    /**
     * Use a {@link ConcurrentResourceCache} to get a remote resource from the country and shard.
     *
     * @param country
     *            The country code
     * @param shard
     *            The provided shard
     * @return If it exists, the resource associated with the country and shard.
     */
    protected abstract Optional<Resource> getResourceFromCache(String country, Shard shard);

    protected Map<String, String> getSparkConfiguration()
    {
        return this.sparkConfiguration;
    }

    /**
     * Invalidate the cache for a specific country and shard, allowing for a retry.
     * 
     * @param country
     *            The country code
     * @param shard
     *            The provided shard
     */
    protected abstract void invalidateCache(String country, Shard shard);

    /**
     * The core of the {@link AbstractFileAtlasProvider}, in which a provided file becomes a
     * resource which is then translated into an Atlas.
     *
     * @param resource
     *            The resource to translate
     * @param country
     *            The country code
     * @param shard
     *            The provided shard
     * @return The resulting Atlas.
     */
    protected abstract Optional<Atlas> resourceToAtlas(Resource resource, String country,
            Shard shard);

    private Resource namedAtlasResource(final CountryShard countryShard,
            final Resource atlasResource)
    {
        return new Resource()
        {
            @Override
            public String getName()
            {
                return countryShard.getName() + FileSuffix.ATLAS;
            }

            @Override
            public long length()
            {
                return atlasResource.length();
            }

            @Override
            public InputStream read()
            {
                return atlasResource.read();
            }
        };
    }
}
