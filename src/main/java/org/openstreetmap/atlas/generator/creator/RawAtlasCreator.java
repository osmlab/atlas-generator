package org.openstreetmap.atlas.generator.creator;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Function;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.generator.PbfLoader;
import org.openstreetmap.atlas.generator.PbfLocator;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasMetaData;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.atlas.dynamic.DynamicAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas.AtlasSerializationFormat;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.raw.creation.RawAtlasGenerator;
import org.openstreetmap.atlas.geography.atlas.raw.sectioning.WaySectionProcessor;
import org.openstreetmap.atlas.geography.atlas.raw.slicing.RawAtlasCountrySlicer;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tool allows the user to replicate raw flow {@link AtlasGenerator} conditions on a certain
 * shard. It is {@link DynamicAtlas} enabled, and will create sliced shards for expansion where
 * necessary.
 *
 * @author lcram
 */
public class RawAtlasCreator extends Command
{
    /**
     * Represents the different possible stages (or flavors) in the raw atlas flow.
     *
     * @author lcram
     */
    private enum RawAtlasFlavor
    {
        RAW_ATLAS("raw"),
        LINE_SLICED_ATLAS("lineSliced"),
        RELATION_SLICED_ATLAS("relationSliced"),
        SLICED_ATLAS("sliced"),
        SECTIONED_ATLAS("sectioned");

        private final String flavorString;

        public static RawAtlasFlavor flavorStringToRawAtlasFlavor(final String string)
        {
            for (final RawAtlasFlavor flavor : RawAtlasFlavor.values())
            {
                if (flavor.toString().equalsIgnoreCase(string))
                {
                    return flavor;
                }
            }
            throw new CoreException("Invalid RawAtlasFlavor {}", string);
        }

        RawAtlasFlavor(final String flavorString)
        {
            this.flavorString = flavorString;
        }

        @Override
        public String toString()
        {
            return this.flavorString;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(RawAtlasCreator.class);
    private static final String DEFAULT_RAW_ATLAS_CACHE_NAME = "__RawAtlasCreator_rawAtlasCache__";
    private static final String DEFAULT_LINE_SLICED_ATLAS_CACHE_NAME = "__RawAtlasCreator_lineSlicedAtlasCache__";
    private static final String DEFAULT_FULLY_SLICED_ATLAS_CACHE_NAME = "__RawAtlasCreator_fullySlicedAtlasCache__";
    private static final String DEFAULT_WATER_RELATION_SUB_ATLAS_CACHE_PATH = "__RawAtlasCreator_waterRelationSubAtlasCache__";

    private static final String USER_HOME = System.getProperty("user.home");
    /*
     * A path to a country boundary map file
     */
    public static final Switch<CountryBoundaryMap> BOUNDARIES = new Switch<>("boundaries",
            "The boundary map to use", value -> CountryBoundaryMap.fromPlainText(new File(value)),
            Optionality.REQUIRED);

    /*
     * The ISO-3 country code of the country's shard to build
     */
    public static final Switch<String> COUNTRY = new Switch<>("country", "The country code",
            StringConverter.IDENTITY, Optionality.REQUIRED);

    /*
     * The path to where the final output will be saved
     */
    public static final Switch<File> OUTPUT = new Switch<>("output",
            "The path where the output will be saved", value ->
            {
                final File result = new File(value);
                result.mkdirs();
                return result;
            }, Optionality.REQUIRED);

    /*
     * The path to the necessary OSM PBF files for this build.
     */
    public static final Switch<String> PBF_PATH = new Switch<>("pbfs",
            "The path to PBF shards needed to build the desired atlas", StringConverter.IDENTITY,
            Optionality.REQUIRED);

    /*
     * The path to the cache of line sliced Atlases. Relation slicing expects a cache of two types
     * of Atlas, one for line sliced Atlases and another for line sliced Atlases containing
     * exclusively lines for water relations.
     */
    public static final Switch<String> LINE_SLICED_ATLAS_CACHE_PATH = new Switch<>(
            "lineSlicedAtlasCache", "The path to the line sliced atlas cache for DynamicAtlas",
            StringConverter.IDENTITY, Optionality.OPTIONAL,
            SparkFileHelper.combine(USER_HOME, DEFAULT_LINE_SLICED_ATLAS_CACHE_NAME));

    public static final Switch<String> WATER_RELATION_SUB_ATLAS_CACHE_PATH = new Switch<>(
            "waterRelationSubAtlasCache",
            "The path to the line-sliced water relation subatlas cache for DynamicAtlas",
            StringConverter.IDENTITY, Optionality.OPTIONAL,
            SparkFileHelper.combine(USER_HOME, DEFAULT_WATER_RELATION_SUB_ATLAS_CACHE_PATH));

    /*
     * The path to the cache of fully sliced atlases. This class uses DynamicAtlas to do way
     * sectioning, and it will create fully sliced atlas for shards it needs on the fly. It will
     * then save them to this location for later use.
     */
    public static final Switch<String> FULLY_SLICED_ATLAS_CACHE_PATH = new Switch<>(
            "fullySlicedAtlasCache", "The path to the fully sliced atlas cache for DynamicAtlas",
            StringConverter.IDENTITY, Optionality.OPTIONAL,
            SparkFileHelper.combine(USER_HOME, DEFAULT_FULLY_SLICED_ATLAS_CACHE_NAME));

    public static final Switch<String> RAW_ATLAS_CACHE_PATH = new Switch<>("rawAtlasCache",
            "The path to the sliced atlas cache for DynamicAtlas", StringConverter.IDENTITY,
            Optionality.OPTIONAL, SparkFileHelper.combine(USER_HOME, DEFAULT_RAW_ATLAS_CACHE_NAME));

    /*
     * If we attempt to populate the sliced atlas cache and still miss, we can optionally fail fast.
     * This type of cache miss will occur if the necessary PBF was not provided to the command.
     * Generally, you will run this command once with this set to "false", then you can browse the
     * logs to see which PBFs you will need. After you acquire them, set this to "true".
     */
    public static final Switch<Boolean> FAIL_FAST_CACHE_MISS = new Switch<>(
            "failFastOnSlicedCacheMiss", "Fail fast on a sliced cache miss", Boolean::parseBoolean,
            Optionality.OPTIONAL, "true");

    /*
     * Optionally provided a PBF scheme. This is useful if you have lots of PBFs available and are
     * storing them with an alternate storage scheme.
     */
    // TODO support alternate PBF schemes. Currently we assume all PBFs are stored directly at the
    // provided path.
    /*
     * public static final Switch<SlippyTilePersistenceScheme> PBF_SCHEME = new
     * Switch<>("pbfScheme", "The folder structure of the PBF", SlippyTilePersistenceScheme::new,
     * Optionality.OPTIONAL, PbfLocator.DEFAULT_SCHEME);
     */

    /*
     * Option to provide if your PBF sharding does not match your atlas sharding. If in doubt,
     * ignore this parameter.
     */
    public static final Switch<String> PBF_SHARDING = new Switch<>("pbfSharding",
            "The sharding tree of the pbf files. If not specified, this will default to the general Atlas sharding.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);

    /*
     * Your atlas sharding.
     */
    public static final Switch<String> SHARDING_TYPE = new Switch<>("sharding",
            "The sharding definition.", StringConverter.IDENTITY, Optionality.REQUIRED);

    /*
     * The shard you are trying to build, in string format (eg. 10-234-125)
     */
    public static final Switch<Shard> TILE = new Switch<>("tile", "The SlippyTile name to use",
            SlippyTile::forName, Optionality.REQUIRED);

    /*
     * The flavor of raw atlas you would like as output (ie. raw, sliced, sectioned)
     */
    public static final Switch<RawAtlasFlavor> ATLAS_FLAVOR = new Switch<>("rawAtlasFlavor",
            "Which flavor of raw atlas - " + RawAtlasFlavor.RAW_ATLAS.toString() + ", "
                    + RawAtlasFlavor.SLICED_ATLAS.toString() + ", or "
                    + RawAtlasFlavor.SECTIONED_ATLAS.toString(),
            RawAtlasFlavor::flavorStringToRawAtlasFlavor, Optionality.OPTIONAL,
            RawAtlasFlavor.SECTIONED_ATLAS.toString());

    /*
     * Change the serialization to legacy Java format if desired.
     */
    public static final Switch<Boolean> USE_JAVA_ATLAS = new Switch<>("useJavaAtlas",
            "Use the Java serialization format.", Boolean::parseBoolean, Optionality.OPTIONAL,
            "false");

    public static void main(final String[] args)
    {
        new RawAtlasCreator().run(args);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final CountryBoundaryMap countryBoundaryMap = (CountryBoundaryMap) command.get(BOUNDARIES);
        final Shard shardToBuild = (Shard) command.get(TILE);
        final String pbfPath = (String) command.get(PBF_PATH);
        final String rawAtlasCachePath = (String) command.get(RAW_ATLAS_CACHE_PATH);
        final String lineSlicedAtlasCachePath = (String) command.get(LINE_SLICED_ATLAS_CACHE_PATH);
        final String fullySlicedAtlasCachePath = (String) command
                .get(FULLY_SLICED_ATLAS_CACHE_PATH);
        final boolean failFastOnSlicedCacheMiss = (boolean) command.get(FAIL_FAST_CACHE_MISS);
        final SlippyTilePersistenceScheme pbfScheme = SlippyTilePersistenceScheme
                .getSchemeInstanceFromString(PbfLocator.DEFAULT_SCHEME);
        final String pbfShardingName = (String) command.get(PBF_SHARDING);
        final String shardingName = (String) command.get(SHARDING_TYPE);
        final Sharding sharding = AtlasSharding.forString(shardingName, Maps.stringMap());
        final Sharding pbfSharding = pbfShardingName != null
                ? AtlasSharding.forString(shardingName, Maps.stringMap())
                : sharding;
        final String countryName = (String) command.get(COUNTRY);
        final File output = (File) command.get(OUTPUT);
        final RawAtlasFlavor atlasFlavor = (RawAtlasFlavor) command.get(ATLAS_FLAVOR);
        final boolean useJavaFormat = (boolean) command.get(USE_JAVA_ATLAS);

        PbfLoader.setAtlasSaveFolder(output);
        final PackedAtlas atlas = runGenerationForFlavor(atlasFlavor, countryBoundaryMap,
                shardToBuild, pbfPath, rawAtlasCachePath, lineSlicedAtlasCachePath,
                fullySlicedAtlasCachePath, failFastOnSlicedCacheMiss, failFastOnSlicedCacheMiss,
                pbfScheme, pbfSharding, sharding, countryName);

        if (useJavaFormat)
        {
            atlas.setSaveSerializationFormat(AtlasSerializationFormat.JAVA);
        }
        else
        {
            atlas.setSaveSerializationFormat(AtlasSerializationFormat.PROTOBUF);
        }
        atlas.save(output.child(countryName + CountryShard.COUNTRY_SHARD_SEPARATOR
                + shardToBuild.getName() + FileSuffix.ATLAS));
        atlas.saveAsGeoJson(
                output.child(countryName + "_" + shardToBuild.getName() + FileSuffix.GEO_JSON));

        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(BOUNDARIES, TILE, SHARDING_TYPE, PBF_PATH,
                RAW_ATLAS_CACHE_PATH, LINE_SLICED_ATLAS_CACHE_PATH, FULLY_SLICED_ATLAS_CACHE_PATH,
                FAIL_FAST_CACHE_MISS, PBF_SHARDING, COUNTRY, OUTPUT, ATLAS_FLAVOR, USE_JAVA_ATLAS);
    }

    private Optional<Atlas> fetchCachedAtlas(final Shard shard, final String countryName,
            final String cachePath)
    {
        final String shardName = shard.getName();
        final String atlasName = countryName + "_" + shardName + FileSuffix.ATLAS;
        final AtlasResourceLoader loader = new AtlasResourceLoader();
        final Path cacheFile = Paths.get(cachePath, atlasName);
        final Optional<Atlas> atlasOption = Optional
                .ofNullable(loader.load(new File(cacheFile.toString())));

        if (!atlasOption.isPresent())
        {
            logger.warn("Sliced cache miss for {}", atlasName);
        }
        else
        {
            logger.info("Sliced cache hit for {}", atlasName);
        }

        return atlasOption;
    }

    @SuppressWarnings("unchecked")
    private Function<Shard, Optional<Atlas>> fullySlicedAtlasFetcher(final String countryName,
            final CountryBoundaryMap countryBoundaryMap, final String cachePath,
            final Sharding sharding, final Function<Shard, Optional<Atlas>> lineSlicedAtlasFetcher)
    {
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            // Check the fully sliced Atlas cache to see if we have a copy of the fully sliced Atlas
            // on the filesystem
            final String filename = countryName + CountryShard.COUNTRY_SHARD_SEPARATOR
                    + shard.getName() + FileSuffix.ATLAS;
            final Optional<Atlas> fetchedAtlas = fetchCachedAtlas(shard, countryName, cachePath);
            if (fetchedAtlas.isPresent())
            {
                return fetchedAtlas;
            }

            // if not, try and generate it, save it, then return it
            final Atlas lineSlicedAtlas = generateFullySlicedAtlas(countryName, shard,
                    countryBoundaryMap, sharding, lineSlicedAtlasFetcher);
            saveAtlas(cachePath, filename, (PackedAtlas) lineSlicedAtlas);
            return Optional.of(lineSlicedAtlas);

        };
    }

    private Atlas generateFullySlicedAtlas(final String countryName, final Shard shardToBuild,
            final CountryBoundaryMap countryBoundaryMap, final Sharding sharding,
            final Function<Shard, Optional<Atlas>> lineSlicedAtlasFetcher)
    {
        return new RawAtlasCountrySlicer(countryName, countryBoundaryMap, sharding,
                lineSlicedAtlasFetcher).sliceRelations(shardToBuild);
    }

    private Atlas generateLineSlicedAtlas(final String countryName,
            final CountryBoundaryMap countryBoundaryMap, final Atlas rawAtlas)
    {
        return new RawAtlasCountrySlicer(countryName, countryBoundaryMap).sliceLines(rawAtlas);
    }

    private Atlas generateRawAtlas(final String pbfPath, final Shard shardToBuild)
    {
        final String unknown = "unknown";
        final RawAtlasGenerator rawAtlasGenerator = new RawAtlasGenerator(
                new File(this.getPBFFilePathFromDirectory(pbfPath, shardToBuild)))
                        .withMetaData(new AtlasMetaData(null, true, unknown, unknown, unknown,
                                shardToBuild.getName(), Maps.hashMap()));
        return rawAtlasGenerator.build();
    }

    private Atlas generateSectionedAtlas(final Shard shardToBuild,
            final CountryBoundaryMap countryBoundaryMap, final Sharding sharding,
            final Function<Shard, Optional<Atlas>> fullySlicedAtlasFetcher)
    {

        final WaySectionProcessor processor = new WaySectionProcessor(shardToBuild,
                AtlasLoadingOption.createOptionWithAllEnabled(countryBoundaryMap), sharding,
                fullySlicedAtlasFetcher);
        return processor.run();
    }

    /*
     * NOTE This method implicitly assumes the PBF scheme is the default scheme. If alternate
     * schemes are to be supported, this needs to change.
     */
    private String getPBFFilePathFromDirectory(final String pbfPath, final Shard shardToBuild)
    {
        final Path pbfPathWithFile = Paths.get(pbfPath, shardToBuild.getName() + FileSuffix.PBF);
        return pbfPathWithFile.toString();
    }

    @SuppressWarnings("unchecked")
    private Function<Shard, Optional<Atlas>> lineSlicedAtlasFetcher(final String countryName,
            final CountryBoundaryMap countryBoundaryMap, final String lineSlicedCachePath,
            final Function<Shard, Optional<Atlas>> rawAtlasFetcher)
    {
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            // Check the line sliced Atlas cache to see if we have a copy of the line sliced Atlas
            // on the filesystem
            final String filename = countryName + CountryShard.COUNTRY_SHARD_SEPARATOR
                    + shard.getName() + FileSuffix.ATLAS;
            final Optional<Atlas> fetchedAtlas = fetchCachedAtlas(shard, countryName,
                    lineSlicedCachePath);
            if (fetchedAtlas.isPresent())
            {
                return fetchedAtlas;
            }

            // If not, try and get the raw Atlas from its cache and line slice it, save it, then
            // return it
            final Optional<Atlas> fetchedRawAtlas = rawAtlasFetcher.apply(shard);
            if (fetchedRawAtlas.isPresent())
            {
                final Atlas lineSlicedAtlas = generateLineSlicedAtlas(countryName,
                        countryBoundaryMap, fetchedRawAtlas.get());
                saveAtlas(lineSlicedCachePath, filename, (PackedAtlas) lineSlicedAtlas);
                return Optional.of(lineSlicedAtlas);
            }
            return Optional.empty();
        };
    }

    @SuppressWarnings("unchecked")
    private Function<Shard, Optional<Atlas>> rawAtlasFetcher(final String countryName,
            final String cachePath, final String pbfPath, final boolean failFastOnRawAtlasCacheMiss)
    {
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            final String filename = countryName + CountryShard.COUNTRY_SHARD_SEPARATOR
                    + shard.getName() + FileSuffix.ATLAS;
            final Optional<Atlas> fetchedAtlas = fetchCachedAtlas(shard, countryName, cachePath);
            if (fetchedAtlas.isPresent())
            {
                return fetchedAtlas;
            }
            else if (failFastOnRawAtlasCacheMiss)
            {
                throw new CoreException("Failed to find raw Atlas file {} in cache!", shard);
            }
            final Atlas rawAtlas = generateRawAtlas(pbfPath, shard);
            saveAtlas(cachePath, filename, (PackedAtlas) rawAtlas);
            return Optional.of(rawAtlas);
        };
    }

    private PackedAtlas runGenerationForFlavor(final RawAtlasFlavor atlasFlavor,
            final CountryBoundaryMap countryBoundaryMap, final Shard shardToBuild,
            final String pbfPath, final String rawAtlasCachePath, final String lineSlicedCachePath,
            final String fullySlicedCachePath, final boolean failFastOnRawAtlasCacheMiss,
            final boolean failFastOnSlicedCacheMiss, final SlippyTilePersistenceScheme pbfScheme,
            final Sharding pbfSharding, final Sharding sharding, final String countryName)
    {
        logger.info("Using raw atlas flavor {}", atlasFlavor);
        final String filename = countryName + CountryShard.COUNTRY_SHARD_SEPARATOR
                + shardToBuild.getName() + FileSuffix.ATLAS;
        if (atlasFlavor == RawAtlasFlavor.RAW_ATLAS)
        {
            final Atlas rawAtlas = generateRawAtlas(pbfPath, shardToBuild);
            saveAtlas(rawAtlasCachePath, filename, (PackedAtlas) rawAtlas);
            return (PackedAtlas) rawAtlas;
        }

        final Function<Shard, Optional<Atlas>> rawAtlasFetcher = rawAtlasFetcher(countryName,
                rawAtlasCachePath, pbfPath, failFastOnRawAtlasCacheMiss);

        final Function<Shard, Optional<Atlas>> lineSlicedAtlasFetcher = lineSlicedAtlasFetcher(
                countryName, countryBoundaryMap, lineSlicedCachePath, rawAtlasFetcher);

        final Atlas fullySlicedAtlas = generateFullySlicedAtlas(countryName, shardToBuild,
                countryBoundaryMap, sharding, lineSlicedAtlasFetcher);
        if (atlasFlavor == RawAtlasFlavor.SLICED_ATLAS)
        {
            saveAtlas(fullySlicedCachePath, filename, (PackedAtlas) fullySlicedAtlas);
            return (PackedAtlas) fullySlicedAtlas;
        }

        final Function<Shard, Optional<Atlas>> fullySlicedAtlasFetcher = fullySlicedAtlasFetcher(
                countryName, countryBoundaryMap, fullySlicedCachePath, sharding,
                lineSlicedAtlasFetcher);

        final Atlas sectionedAtlas = generateSectionedAtlas(shardToBuild, countryBoundaryMap,
                sharding, fullySlicedAtlasFetcher);
        if (atlasFlavor == RawAtlasFlavor.SECTIONED_ATLAS)
        {
            return (PackedAtlas) sectionedAtlas;
        }

        throw new CoreException("RawAtlasFlavor value {} was invalid!", atlasFlavor.toString());
    }

    private void saveAtlas(final String cachePath, final String filename, final PackedAtlas atlas)
    {
        final Path cacheFile = Paths.get(cachePath, filename);
        final File atlasFile = new File(cacheFile.toString());
        atlas.setSaveSerializationFormat(AtlasSerializationFormat.PROTOBUF);
        atlas.save(atlasFile);
    }
}
