package org.openstreetmap.atlas.mutator.configuration.parsing.provider.file;

import java.lang.module.Configuration;
import java.util.Map;
import java.util.Optional;

import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.streaming.resource.Resource;

/**
 * The simplest default Atlas provider which takes a repository of serialized Atlas files to provide
 * the Atlas objects.
 * 
 * @author matthieun
 */
public class AtlasFileAtlasProvider extends AbstractFileAtlasProvider
{
    private static final long serialVersionUID = -8571516671628110381L;

    // This cache can be transient since it is configured on the executor, right when the
    // ConfiguredAtlasFetcher generates the custom Fetcher function to be used by the
    // DynamicAtlasPolicy
    private transient HadoopAtlasFileCache cache;

    public AtlasFileAtlasProvider(final Configuration configuration)
    {
    }

    public AtlasFileAtlasProvider()
    {
    }

    @Override
    public void setAtlasProviderContext(final Map<String, Object> context)
    {
        super.setAtlasProviderContext(context);
        this.cache = new HadoopAtlasFileCache(this.getAtlasPath(), this.getSparkConfiguration());
    }

    @Override
    protected Optional<Resource> getResourceFromCache(final String country, final Shard shard)
    {
        return this.cache.get(country, shard);
    }

    @Override
    protected void invalidateCache(final String country, final Shard shard)
    {
        this.cache.invalidate(country, shard);
    }

    @Override
    protected Optional<Atlas> resourceToAtlas(final Resource resource, final String country,
            final Shard shard)
    {
        return Optional.ofNullable(PackedAtlas.load(resource));
    }
}
