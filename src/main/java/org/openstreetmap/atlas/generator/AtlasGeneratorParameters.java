package org.openstreetmap.atlas.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceSchemeType;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.spark.persistence.PersistenceTools;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.pbf.BridgeConfiguredFilter;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.tags.filters.ConfiguredTaggableFilter;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.Command.Optionality;
import org.openstreetmap.atlas.utilities.runtime.Command.Switch;
import org.openstreetmap.atlas.utilities.runtime.Command.SwitchList;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * @author matthieun
 */
public final class AtlasGeneratorParameters
{
    public static final Switch<StringList> COUNTRIES = new Switch<>("countries",
            "Comma separated list of countries to be included in the final Atlas",
            value -> StringList.split(value, ","), Optionality.REQUIRED);
    public static final Switch<String> COUNTRY_SHAPES = new Switch<>("countryShapes",
            "Shape file containing the countries", StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> PBF_PATH = new Switch<>("pbfs", "The path to PBFs",
            StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<SlippyTilePersistenceScheme> PBF_SCHEME = new Switch<>("pbfScheme",
            "The folder structure of the PBF",
            SlippyTilePersistenceScheme::getSchemeInstanceFromString, Optionality.OPTIONAL,
            PbfLocator.DEFAULT_SCHEME);
    public static final Switch<String> PBF_SHARDING = new Switch<>("pbfSharding",
            "The sharding tree of the pbf files. If not specified, this will default to the general Atlas sharding.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<SlippyTilePersistenceScheme> ATLAS_SCHEME = new Switch<>(
            "atlasScheme",
            "The folder structure of the output Atlas. Example: \"zz/xx/yy/\" or \"\""
                    + " (everything under the same folder)",
            SlippyTilePersistenceScheme::getSchemeInstanceFromString, Optionality.OPTIONAL,
            SlippyTilePersistenceSchemeType.EMPTY.getValue());
    public static final Switch<String> PREVIOUS_OUTPUT_FOR_DELTA = new Switch<>(
            "previousOutputForDelta",
            "The path of the output of the previous job that can be used for delta computation",
            StringConverter.IDENTITY, Optionality.OPTIONAL, "");
    public static final Switch<String> CODE_VERSION = new Switch<>("codeVersion",
            "The code version", StringConverter.IDENTITY, Optionality.OPTIONAL, "unknown");
    public static final Switch<String> DATA_VERSION = new Switch<>("dataVersion",
            "The data version", StringConverter.IDENTITY, Optionality.OPTIONAL, "unknown");
    public static final Switch<String> EDGE_CONFIGURATION = new Switch<>("edgeConfiguration",
            "The path to the configuration file that defines what OSM Way becomes an Edge",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> WAY_SECTIONING_CONFIGURATION = new Switch<>(
            "waySectioningConfiguration",
            "The path to the configuration file that defines where to section Ways to make Edges.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> SLICING_CONFIGURATION = new Switch<>("slicingConfiguration",
            "The path to the configuration file that defines which relations to dynamically expand when slicing.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> PBF_WAY_CONFIGURATION = new Switch<>(
            "osmPbfWayConfiguration",
            "The path to the configuration file that defines which PBF Ways becomes an Atlas Entity.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> PBF_NODE_CONFIGURATION = new Switch<>(
            "osmPbfNodeConfiguration",
            "The path to the configuration file that defines which PBF Nodes becomes an Atlas Entity.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> PBF_RELATION_CONFIGURATION = new Switch<>(
            "osmPbfRelationConfiguration",
            "The path to the configuration file that defines which PBF Relations becomes an Atlas Entity",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> SHOULD_INCLUDE_FILTERED_OUTPUT_CONFIGURATION = new Switch<>(
            "shouldIncludeFilteredOutputConfiguration",
            "The path to the configuration file that defines which will be included in filtered output."
                    + " Filtered output will only be generated if this switch is specified, and will be"
                    + " stored in a separate subdirectory.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<Boolean> LINE_DELIMITED_GEOJSON_OUTPUT = new Switch<>(
            "lineDelimitedGeojsonOutput",
            "Output each shard as a line delimited geojson output file in the ldgeojson folder.",
            Boolean::parseBoolean, Optionality.OPTIONAL, "false");
    public static final Switch<String> SHARDING_TYPE = new Switch<>("sharding",
            "The sharding definition.", StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<StringList> CONFIGURED_FILTER_OUTPUT = new Switch<>(
            "configuredOutputFilter", "Path to configuration file for filtered output",
            value -> StringList.split(value, ","), Optionality.OPTIONAL);
    public static final Switch<StringList> CONFIGURED_FILTER_NAME = new Switch<>(
            "configuredFilterName", "Name of the filter to be used for configured output",
            value -> StringList.split(value, ","), Optionality.OPTIONAL);
    public static final Switch<Boolean> STATISTICS = new Switch<>("statistics",
            "Whether to run the shard statistics and country statistics", Boolean::parseBoolean,
            Optionality.OPTIONAL, "false");
    public static final Switch<Boolean> KEEP_ALL = new Command.Flag("keepAll",
            "Keep all objects, overrides any filters!");

    public static ConfiguredFilter getConfiguredFilterFrom(final String name,
            final Resource configurationResource)
    {
        return ConfiguredFilter.from(name, getStandardConfigurationFrom(configurationResource));
    }

    public static ConfiguredFilter getConfiguredFilterFrom(final String name, final String path,
            final Map<String, String> configurationMap)
    {
        return getConfiguredFilterFrom(name, SparkJob.resource(path, configurationMap));
    }

    public static BridgeConfiguredFilter getConfiguredFilterFrom(final String root,
            final String name, final Resource configurationResource)
    {
        return new BridgeConfiguredFilter(root, name,
                getStandardConfigurationFrom(configurationResource));
    }

    public static List<ConfiguredFilter> getConfiguredFilterListFrom(final StringList name,
            final Resource configurationResource)
    {
        final List<ConfiguredFilter> filters = new ArrayList<>();
        name.forEach(n -> filters.add(
                ConfiguredFilter.from(n, getStandardConfigurationFrom(configurationResource))));
        return filters;
    }

    public static List<ConfiguredFilter> getConfiguredFilterListFrom(final StringList names,
            final StringList paths, final Map<String, String> configurationMap)
    {
        final List<ConfiguredFilter> filters = new ArrayList<>();
        // loop through paths
        for (final String path : paths)
        {
            // loop through filter names, check each path for filter with that name, if it exists
            // add it to the list
            for (final String name : names)
            {
                if (ConfiguredFilter.isPresent(name,
                        getStandardConfigurationFrom(SparkJob.resource(path, configurationMap))))
                {
                    filters.add(getConfiguredFilterFrom(name,
                            SparkJob.resource(path, configurationMap)));
                }
            }
        }

        return filters;
    }

    public static StandardConfiguration getStandardConfigurationFrom(
            final Resource configurationResource)
    {
        return new StandardConfiguration(configurationResource);
    }

    public static StandardConfiguration getStandardConfigurationFrom(final String path,
            final Map<String, String> configurationMap)
    {
        return getStandardConfigurationFrom(SparkJob.resource(path, configurationMap));
    }

    public static ConfiguredTaggableFilter getTaggableFilterFrom(
            final Resource configurationResource)
    {
        return new ConfiguredTaggableFilter(getStandardConfigurationFrom(configurationResource));
    }

    public static ConfiguredTaggableFilter getTaggableFilterFrom(final String path,
            final Map<String, String> configurationMap)
    {
        return getTaggableFilterFrom(SparkJob.resource(path, configurationMap));
    }

    public static boolean runStatistics(final CommandMap command)
    {
        return (boolean) command.get(STATISTICS);
    }

    protected static AtlasLoadingOption buildAtlasLoadingOption(final CountryBoundaryMap boundaries,
            final Map<String, String> properties)
    {
        final AtlasLoadingOption atlasLoadingOption = AtlasLoadingOption
                .createOptionWithAllEnabled(boundaries);

        // Apply all configurations
        final String edgeConfiguration = properties.get(EDGE_CONFIGURATION.getName());
        if (edgeConfiguration != null)
        {
            atlasLoadingOption.setEdgeFilter(
                    getConfiguredFilterFrom("", AtlasLoadingOption.ATLAS_EDGE_FILTER_NAME,
                            new StringResource(edgeConfiguration)));
        }

        final String waySectioningConfiguration = properties
                .get(WAY_SECTIONING_CONFIGURATION.getName());
        if (waySectioningConfiguration != null)
        {
            atlasLoadingOption.setWaySectionFilter(
                    getConfiguredFilterFrom("", AtlasLoadingOption.ATLAS_WAY_SECTION_FILTER_NAME,
                            new StringResource(waySectioningConfiguration)));
        }

        final String pbfNodeConfiguration = properties.get(PBF_NODE_CONFIGURATION.getName());
        if (pbfNodeConfiguration != null)
        {
            atlasLoadingOption.setOsmPbfNodeFilter(
                    getTaggableFilterFrom(new StringResource(pbfNodeConfiguration)));
        }

        final String pbfWayConfiguration = properties.get(PBF_WAY_CONFIGURATION.getName());
        if (pbfWayConfiguration != null)
        {
            atlasLoadingOption.setOsmPbfWayFilter(
                    getTaggableFilterFrom(new StringResource(pbfWayConfiguration)));
        }

        if (Boolean.TRUE.equals(Boolean.parseBoolean(properties.get(KEEP_ALL.getName()))))
        {
            atlasLoadingOption.setKeepAll(true);
        }
        final String pbfRelationConfiguration = properties
                .get(PBF_RELATION_CONFIGURATION.getName());
        if (pbfRelationConfiguration != null)
        {
            atlasLoadingOption.setOsmPbfRelationFilter(
                    getTaggableFilterFrom(new StringResource(pbfRelationConfiguration)));
        }

        final String slicingConfiguration = properties.get(SLICING_CONFIGURATION.getName());
        if (slicingConfiguration != null)
        {
            atlasLoadingOption.setRelationSlicingFilter(getConfiguredFilterFrom("",
                    AtlasLoadingOption.ATLAS_RELATION_SLICING_FILTER_NAME,
                    new StringResource(slicingConfiguration)));
            atlasLoadingOption.setRelationSlicingConsolidateFilter(getConfiguredFilterFrom("",
                    AtlasLoadingOption.ATLAS_RELATION_SLICING_CONSOLIDATE_FILTER_NAME,
                    new StringResource(slicingConfiguration)));
        }

        return atlasLoadingOption;
    }

    protected static Map<String, String> extractAtlasLoadingProperties(final CommandMap command,
            final Map<String, String> sparkContext)
    {
        final Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put(CODE_VERSION.getName(), (String) command.get(CODE_VERSION));
        propertyMap.put(DATA_VERSION.getName(), (String) command.get(DATA_VERSION));

        final String edgeConfiguration = (String) command.get(EDGE_CONFIGURATION);
        propertyMap.put(EDGE_CONFIGURATION.getName(), edgeConfiguration == null ? null
                : SparkJob.resource(edgeConfiguration, sparkContext).all());

        final String waySectioningConfiguration = (String) command
                .get(WAY_SECTIONING_CONFIGURATION);
        propertyMap.put(WAY_SECTIONING_CONFIGURATION.getName(),
                waySectioningConfiguration == null ? null
                        : SparkJob.resource(waySectioningConfiguration, sparkContext).all());

        final String pbfNodeConfiguration = (String) command.get(PBF_NODE_CONFIGURATION);
        propertyMap.put(PBF_NODE_CONFIGURATION.getName(), pbfNodeConfiguration == null ? null
                : SparkJob.resource(pbfNodeConfiguration, sparkContext).all());

        final String pbfWayConfiguration = (String) command.get(PBF_WAY_CONFIGURATION);
        propertyMap.put(PBF_WAY_CONFIGURATION.getName(), pbfWayConfiguration == null ? null
                : SparkJob.resource(pbfWayConfiguration, sparkContext).all());

        final String pbfRelationConfiguration = (String) command.get(PBF_RELATION_CONFIGURATION);
        propertyMap.put(PBF_RELATION_CONFIGURATION.getName(),
                pbfRelationConfiguration == null ? null
                        : SparkJob.resource(pbfRelationConfiguration, sparkContext).all());

        final String slicingConfiguration = (String) command.get(SLICING_CONFIGURATION);
        propertyMap.put(SLICING_CONFIGURATION.getName(), slicingConfiguration == null ? null
                : SparkJob.resource(slicingConfiguration, sparkContext).all());

        final Boolean keepAll = (Boolean) command.get(KEEP_ALL);
        propertyMap.put(KEEP_ALL.getName(),
                keepAll == null ? KEEP_ALL.getDefault().toString() : keepAll.toString());

        return propertyMap;
    }

    protected static SwitchList switches()
    {
        return new SwitchList().with(COUNTRIES, COUNTRY_SHAPES, SHARDING_TYPE, PBF_PATH, PBF_SCHEME,
                PBF_SHARDING, PREVIOUS_OUTPUT_FOR_DELTA, CODE_VERSION, DATA_VERSION,
                EDGE_CONFIGURATION, WAY_SECTIONING_CONFIGURATION, PBF_NODE_CONFIGURATION,
                PBF_WAY_CONFIGURATION, PBF_RELATION_CONFIGURATION, SLICING_CONFIGURATION,
                ATLAS_SCHEME, LINE_DELIMITED_GEOJSON_OUTPUT,
                SHOULD_INCLUDE_FILTERED_OUTPUT_CONFIGURATION,
                PersistenceTools.COPY_SHARDING_AND_BOUNDARIES, CONFIGURED_FILTER_OUTPUT,
                CONFIGURED_FILTER_NAME, STATISTICS, KEEP_ALL);
    }

    private AtlasGeneratorParameters()
    {
    }
}
