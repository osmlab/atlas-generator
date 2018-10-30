package org.openstreetmap.atlas.generator;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.atlas.generator.persistence.AbstractMultipleAtlasBasedOutputFormat;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.tags.filters.ConfiguredTaggableFilter;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command.Optionality;
import org.openstreetmap.atlas.utilities.runtime.Command.Switch;
import org.openstreetmap.atlas.utilities.runtime.Command.SwitchList;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * @author matthieun
 */
public final class AtlasGeneratorParameters
{
    private static final String SHARDING_DEFAULT = "slippy@10";

    public static final Switch<StringList> COUNTRIES = new Switch<>("countries",
            "Comma separated list of countries to be included in the final Atlas",
            value -> StringList.split(value, ","), Optionality.REQUIRED);
    public static final Switch<String> COUNTRY_SHAPES = new Switch<>("countryShapes",
            "Shape file containing the countries", StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<String> PBF_PATH = new Switch<>("pbfs", "The path to PBFs",
            StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<SlippyTilePersistenceScheme> PBF_SCHEME = new Switch<>("pbfScheme",
            "The folder structure of the PBF", SlippyTilePersistenceScheme::new,
            Optionality.OPTIONAL, PbfLocator.DEFAULT_SCHEME);
    public static final Switch<String> PBF_SHARDING = new Switch<>("pbfSharding",
            "The sharding tree of the pbf files. If not specified, this will default to the general Atlas sharding.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<SlippyTilePersistenceScheme> ATLAS_SCHEME = new Switch<>(
            "atlasScheme",
            "The folder structure of the output Atlas. Example: \"zz/xx/yy/\" or \"\""
                    + " (everything under the same folder)",
            SlippyTilePersistenceScheme::new, Optionality.OPTIONAL,
            AbstractMultipleAtlasBasedOutputFormat.DEFAULT_SCHEME);
    public static final Switch<String> SHARDING_TYPE = new Switch<>("sharding",
            "The sharding definition.", StringConverter.IDENTITY, Optionality.OPTIONAL,
            SHARDING_DEFAULT);
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
    public static final Switch<String> SHOULD_ALWAYS_SLICE_CONFIGURATION = new Switch<>(
            "shouldAlwaysSliceConfiguration",
            "The path to the configuration file that defines which entities on which country slicing will"
                    + " always be attempted regardless of the number of countries it intersects according to the"
                    + " country boundary map's grid index.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<Boolean> USE_JAVA_FORMAT = new Switch<>("useJavaFormat",
            "Generate the atlas files using the java serialization atlas format (as opposed to the protobuf format).",
            Boolean::parseBoolean, Optionality.OPTIONAL, "false");

    public static StandardConfiguration getStandardConfigurationFrom(
            final Resource configurationResource)
    {
        return new StandardConfiguration(configurationResource);
    }

    public static ConfiguredTaggableFilter getTaggableFilterFrom(
            final Resource configurationResource)
    {
        return new ConfiguredTaggableFilter(getStandardConfigurationFrom(configurationResource));
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
            atlasLoadingOption
                    .setEdgeFilter(getTaggableFilterFrom(new StringResource(edgeConfiguration)));
        }

        final String waySectioningConfiguration = properties
                .get(WAY_SECTIONING_CONFIGURATION.getName());
        if (waySectioningConfiguration != null)
        {
            atlasLoadingOption.setWaySectionFilter(
                    getTaggableFilterFrom(new StringResource(waySectioningConfiguration)));
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

        final String pbfRelationConfiguration = properties
                .get(PBF_RELATION_CONFIGURATION.getName());
        if (pbfRelationConfiguration != null)
        {
            atlasLoadingOption.setOsmPbfRelationFilter(
                    getTaggableFilterFrom(new StringResource(pbfRelationConfiguration)));
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
                : FileSystemHelper.resource(edgeConfiguration, sparkContext).all());

        final String waySectioningConfiguration = (String) command
                .get(WAY_SECTIONING_CONFIGURATION);
        propertyMap.put(WAY_SECTIONING_CONFIGURATION.getName(), waySectioningConfiguration == null
                ? null : FileSystemHelper.resource(waySectioningConfiguration, sparkContext).all());

        final String pbfNodeConfiguration = (String) command.get(PBF_NODE_CONFIGURATION);
        propertyMap.put(PBF_NODE_CONFIGURATION.getName(), pbfNodeConfiguration == null ? null
                : FileSystemHelper.resource(pbfNodeConfiguration, sparkContext).all());

        final String pbfWayConfiguration = (String) command.get(PBF_WAY_CONFIGURATION);
        propertyMap.put(PBF_WAY_CONFIGURATION.getName(), pbfWayConfiguration == null ? null
                : FileSystemHelper.resource(pbfWayConfiguration, sparkContext).all());

        final String pbfRelationConfiguration = (String) command.get(PBF_RELATION_CONFIGURATION);
        propertyMap.put(PBF_RELATION_CONFIGURATION.getName(), pbfRelationConfiguration == null
                ? null : FileSystemHelper.resource(pbfRelationConfiguration, sparkContext).all());

        return propertyMap;
    }

    protected static SwitchList switches()
    {
        return new SwitchList().with(COUNTRIES, COUNTRY_SHAPES, SHARDING_TYPE, PBF_PATH, PBF_SCHEME,
                PBF_SHARDING, PREVIOUS_OUTPUT_FOR_DELTA, CODE_VERSION, DATA_VERSION,
                EDGE_CONFIGURATION, WAY_SECTIONING_CONFIGURATION, PBF_NODE_CONFIGURATION,
                PBF_WAY_CONFIGURATION, PBF_RELATION_CONFIGURATION, ATLAS_SCHEME,
                SHOULD_ALWAYS_SLICE_CONFIGURATION, USE_JAVA_FORMAT);
    }

    private AtlasGeneratorParameters()
    {
    }
}
