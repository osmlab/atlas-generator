package org.openstreetmap.atlas.generator.tools.caching;

import java.net.URI;
import java.util.function.Function;

import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.caching.ConcurrentResourceCache;
import org.openstreetmap.atlas.utilities.caching.strategies.CachingStrategy;

/**
 * Cache an atlas file stored in the standard way (parentpath/COUNTRY/COUNTRY_z-x-y.atlas) to a
 * system temporary location. This cache is designed to be used with atlases that can be fetched
 * from a hadoop file system.
 *
 * @author lcram
 */
public class HadoopAtlasFileCache extends ConcurrentResourceCache
{

    public HadoopAtlasFileCache(final CachingStrategy cachingStrategy,
            final Function<URI, Resource> fetcher)
    {
        super(cachingStrategy, fetcher);
        // TODO Auto-generated constructor stub
    }
}
