package org.openstreetmap.atlas.generator;

import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasCountryStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasProtoOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.delta.RemovedMultipleAtlasDeltaOutputFormat;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;

/**
 * @author samgass
 */
public enum AtlasGeneratorJobGroup
{
    RAW(0, "Raw Atlas Creation", "rawAtlas", Atlas.class, MultipleAtlasOutputFormat.class),
    LINE_SLICED(
            1,
            "Line Sliced Atlas Creation",
            "lineSlicedAtlas",
            Atlas.class,
            MultipleAtlasOutputFormat.class),
    LINE_SLICED_SUB(
            2,
            "Line Sliced Sub Atlas Creation",
            "lineSlicedSubAtlas",
            Atlas.class,
            MultipleAtlasOutputFormat.class),
    FULLY_SLICED(
            3,
            "Fully Sliced Atlas Creation",
            "fullySlicedAtlas",
            Atlas.class,
            MultipleAtlasOutputFormat.class),
    WAY_SECTIONED(
            4,
            "Way Sectioned Atlas Creation",
            "atlas",
            Atlas.class,
            MultipleAtlasOutputFormat.class),
    WAY_SECTIONED_PBF(
            4,
            "Way Sectioned Atlas Creation",
            "atlas",
            Atlas.class,
            MultipleAtlasProtoOutputFormat.class),
    SHARD_STATISTICS(
            5,
            "Shard Statistics Creation",
            "shardStats",
            AtlasStatistics.class,
            MultipleAtlasStatisticsOutputFormat.class),
    COUNTRY_STATISTICS(
            6,
            "Country Statistics Creations",
            "countryStats",
            AtlasStatistics.class,
            MultipleAtlasCountryStatisticsOutputFormat.class),
    DELTAS(
            7,
            "Atlas Deltas Creation",
            "deltas",
            AtlasDelta.class,
            RemovedMultipleAtlasDeltaOutputFormat.class);

    private final String description;
    private final Integer identifier;

    private final String cacheFolder;
    private final Class<?> keyClass;
    private final Class<? extends MultipleOutputFormat<?, ?>> outputClass;

    AtlasGeneratorJobGroup(final Integer identifier, final String description,
            final String cacheFolder, final Class<?> keyClass,
            final Class<? extends MultipleOutputFormat<?, ?>> outputClass)
    {
        this.identifier = identifier;
        this.description = description;
        this.cacheFolder = cacheFolder;
        this.outputClass = outputClass;
        this.keyClass = keyClass;
    }

    public String getCacheFolder()
    {
        return this.cacheFolder;
    }

    public String getDescription()
    {
        return this.description;
    }

    public Integer getId()
    {
        return this.identifier;
    }

    public Class<?> getKeyClass()
    {
        return this.keyClass;
    }

    public Class<?> getOutputClass()
    {
        return this.outputClass;
    }
}
