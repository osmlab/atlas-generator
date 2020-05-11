package org.openstreetmap.atlas.generator;

import java.util.List;

import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasCountryStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasFeatureChangeOutput;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasProtoOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasStatisticsOutputFormat;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;

/**
 * @author samgass
 */
public enum AtlasGeneratorJobGroup
{
    RAW(0, "Raw Atlas Creation", "rawAtlas", Atlas.class, MultipleAtlasOutputFormat.class),
    SLICED(1, "Sliced Atlas Creation", "slicedAtlas", Atlas.class, MultipleAtlasOutputFormat.class),
    SLICED_SUB(
            2,
            "Multipolygon Relation Sub Atlas Creation",
            "multipolygonRelationSubAtlas",
            Atlas.class,
            MultipleAtlasOutputFormat.class),
    EDGE_SUB(
            4,
            "Edge-only Sub Atlas Creation",
            "edgeOnlySubAtlas",
            Atlas.class,
            MultipleAtlasOutputFormat.class),
    WAY_SECTIONED_PBF(
            5,
            "Way Sectioned Atlas Creation",
            "atlas",
            Atlas.class,
            MultipleAtlasProtoOutputFormat.class),
    SHARD_STATISTICS(
            6,
            "Shard Statistics Creation",
            "shardStats",
            AtlasStatistics.class,
            MultipleAtlasStatisticsOutputFormat.class),
    COUNTRY_STATISTICS(
            7,
            "Country Statistics Creation",
            "countryStats",
            AtlasStatistics.class,
            MultipleAtlasCountryStatisticsOutputFormat.class),
    DIFFS(8, "Atlas Diff Creation", "diffs", List.class, MultipleAtlasFeatureChangeOutput.class),
    TAGGABLE_FILTERED_OUTPUT(
            9,
            "Taggable Filtered SubAtlas Creation",
            "filteredOutput",
            Atlas.class,
            MultipleAtlasOutputFormat.class),
    CONFIGURED_FILTERED_OUTPUT(
            10,
            "Configured Filtered SubAtlas Creation",
            "configuredOutput",
            Atlas.class,
            MultipleAtlasOutputFormat.class);

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

    public Class<? extends MultipleOutputFormat<?, ?>> getOutputClass()
    {
        return this.outputClass;
    }
}
