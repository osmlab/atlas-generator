package org.openstreetmap.atlas.generator;

import java.util.Map;
import java.util.Optional;

import org.openstreetmap.atlas.generator.tools.spark.DataLocator;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.MultiIterable;

/**
 * Locate Atlas Files.
 *
 * @author matthieun
 */
public class AtlasLocator extends DataLocator<Atlas>
{
    private static final long serialVersionUID = 245229416973321209L;

    public AtlasLocator(final Map<String, String> sparkContext)
    {
        super(sparkContext);
    }

    public Iterable<Atlas> atlasForShard(final String atlasPath,
            final Iterable<String> countryShards)
    {
        final Iterable<Atlas> clear = retrieve(Iterables.stream(countryShards)
                .map(countryShard -> fullPath(atlasPath, countryShard)).collect());
        final Iterable<Atlas> gzipped = retrieve(Iterables.stream(countryShards)
                .map(countryShard -> fullPathGzipped(atlasPath, countryShard)).collect());
        return new MultiIterable<>(clear, gzipped);
    }

    public Optional<Atlas> atlasForShard(final String atlasPath, final String countryShard)
    {
        final Optional<Atlas> clear = retrieve(fullPath(atlasPath, countryShard));
        if (clear.isPresent())
        {
            return clear;
        }
        else
        {
            return retrieve(fullPathGzipped(atlasPath, countryShard));
        }
    }

    @Override
    protected Optional<Atlas> readFrom(final Resource resource)
    {
        return Optional.ofNullable(new AtlasResourceLoader().load(resource));
    }

    private String fullPath(final String atlasPath, final String countryShard)
    {
        return SparkFileHelper.combine(atlasPath, countryShard + FileSuffix.ATLAS);
    }

    private String fullPathGzipped(final String atlasPath, final String countryShard)
    {
        return SparkFileHelper.combine(atlasPath,
                countryShard + FileSuffix.ATLAS + FileSuffix.GZIP);
    }
}
