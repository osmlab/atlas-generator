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
import org.openstreetmap.atlas.geography.atlas.Atlas;
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
 * A raw atlas compatible version of the {@link AtlasCreator} command. This tool allows the user to
 * replicate raw flow {@link AtlasGenerator} conditions on a certain shard. It is
 * {@link DynamicAtlas} enabled, and will create sliced shards for expansion where necessary.
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
        RawAtlas("raw"),
        SlicedRawAtlas("sliced"),
        SectionedRawAtlas("sectioned");

        private final String flavorString;

        public static RawAtlasFlavor flavorStringToRawAtlasFlavor(final String string)
        {
            for (final RawAtlasFlavor flavor : RawAtlasFlavor.values())
            {
                if (flavor.getFlavorString().equalsIgnoreCase(string))
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

        public String getFlavorString()
        {
            return this.flavorString;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(RawAtlasCreator.class);
    private static final String DEFAULT_CACHE_NAME = "__RawAtlasCreator_slicedAtlasCache__";

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
     * The path to the cache of sliced raw atlases. This class uses DynamicAtlas to do way
     * sectioning, and it will create sliced raw atlas for shards it needs on the fly. It will then
     * save them to this location for later use.
     */
    public static final Switch<String> SLICED_CACHE_PATH = new Switch<>("slicedCache",
            "The path to the sliced atlas cache for DynamicAtlas", StringConverter.IDENTITY,
            Optionality.OPTIONAL, System.getProperty("user.home") + "/" + DEFAULT_CACHE_NAME);

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
            "Which flavor of raw atlas - " + RawAtlasFlavor.RawAtlas.getFlavorString() + ", "
                    + RawAtlasFlavor.SlicedRawAtlas.getFlavorString() + ", or "
                    + RawAtlasFlavor.SectionedRawAtlas.getFlavorString(),
            RawAtlasFlavor::flavorStringToRawAtlasFlavor, Optionality.OPTIONAL,
            RawAtlasFlavor.SectionedRawAtlas.getFlavorString());

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
        final String slicedCachePath = (String) command.get(SLICED_CACHE_PATH);
        final boolean failFastOnSlicedCacheMiss = (boolean) command.get(FAIL_FAST_CACHE_MISS);
        final SlippyTilePersistenceScheme pbfScheme = new SlippyTilePersistenceScheme(
                PbfLocator.DEFAULT_SCHEME);
        final String pbfShardingName = (String) command.get(PBF_SHARDING);
        final String shardingName = (String) command.get(SHARDING_TYPE);
        final Sharding sharding = AtlasSharding.forString(shardingName, Maps.stringMap());
        final Sharding pbfSharding = pbfShardingName != null
                ? AtlasSharding.forString(shardingName, Maps.stringMap()) : sharding;
        final String countryName = (String) command.get(COUNTRY);
        final File output = (File) command.get(OUTPUT);
        final RawAtlasFlavor atlasFlavor = (RawAtlasFlavor) command.get(ATLAS_FLAVOR);
        final boolean useJavaFormat = (boolean) command.get(USE_JAVA_ATLAS);

        PbfLoader.setAtlasSaveFolder(output);
        final PackedAtlas atlas = runGenerationForFlavor(atlasFlavor, countryBoundaryMap,
                shardToBuild, pbfPath, slicedCachePath, failFastOnSlicedCacheMiss, pbfScheme,
                pbfSharding, sharding, countryName);
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
        atlas.saveAsGeoJson(output.child(countryName + "_" + shardToBuild.getName() + ".geojson"));

        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(BOUNDARIES, TILE, SHARDING_TYPE, PBF_PATH, SLICED_CACHE_PATH,
                FAIL_FAST_CACHE_MISS, PBF_SHARDING, COUNTRY, OUTPUT, ATLAS_FLAVOR, USE_JAVA_ATLAS);
    }

    private Function<Shard, Optional<Atlas>> atlasFetcher(final String countryName,
            final String slicedCachePath, final boolean failFastOnSlicedCacheMiss,
            final String pbfPath, final CountryBoundaryMap countryBoundaryMap)
    {
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            final String shardName = shard.getName();
            final String atlasName = countryName + "_" + shardName + FileSuffix.ATLAS;
            final AtlasResourceLoader loader = new AtlasResourceLoader();
            final Path slicedCacheFile = Paths.get(slicedCachePath, atlasName);
            final RawAtlasCreator creator = new RawAtlasCreator();

            Optional<Atlas> atlasOption = Optional
                    .ofNullable(loader.load(new File(slicedCacheFile.toString())));

            if (!atlasOption.isPresent())
            {
                logger.warn("Sliced cache miss for {}, generating...", atlasName);

                try
                {
                    atlasOption = Optional.ofNullable(creator.generateSlicedAtlasFromScratch(
                            pbfPath, countryName, countryBoundaryMap, shard));
                }
                catch (final Exception exception)
                {
                    atlasOption = Optional.empty();
                }

                if (atlasOption.isPresent())
                {
                    final File atlasFile = new File(slicedCacheFile.toString());
                    atlasOption.get().save(atlasFile);
                    logger.info("Successfully saved {} to sliced cache", atlasName);
                }
                else
                {
                    logger.error("Failed to generate sliced atlas {}", atlasName);
                    if (failFastOnSlicedCacheMiss)
                    {
                        logger.error("Failing fast, it is likely the PBF was missing");
                        System.exit(1);
                    }
                    else
                    {
                        throw new CoreException("Failed to generate sliced atlas {}", atlasName);
                    }
                }
            }
            else
            {
                logger.info("Sliced cache hit for {}", atlasName);
            }

            return atlasOption;
        };
    }

    private Atlas generateRawAtlas(final String pbfPath, final Shard shardToBuild)
    {
        final RawAtlasGenerator rawAtlasGenerator = new RawAtlasGenerator(
                new File(this.getPBFFilePathFromDirectory(pbfPath, shardToBuild)));
        final Atlas rawAtlas = rawAtlasGenerator.build();
        return rawAtlas;
    }

    private Atlas generateSectionedAtlasGivenSlicedAtlas(final String countryName,
            final Shard shardToBuild, final CountryBoundaryMap countryBoundaryMap,
            final Sharding sharding, final String slicedCachePath,
            final boolean failFastOnSlicedCacheMiss, final String pbfPath)
    {
        final Function<Shard, Optional<Atlas>> slicedRawAtlasFetcher = this.atlasFetcher(
                countryName, slicedCachePath, failFastOnSlicedCacheMiss, pbfPath,
                countryBoundaryMap);

        final WaySectionProcessor processor = new WaySectionProcessor(shardToBuild,
                AtlasLoadingOption.createOptionWithAllEnabled(countryBoundaryMap), sharding,
                slicedRawAtlasFetcher);
        final Atlas finalAtlas = processor.run();
        return finalAtlas;
    }

    private Atlas generateSlicedAtlasFromScratch(final String pbfPath, final String countryName,
            final CountryBoundaryMap countryBoundaryMap, final Shard shardToBuild)
    {
        return new RawAtlasCountrySlicer(countryName, countryBoundaryMap)
                .slice(new RawAtlasGenerator(
                        new File(this.getPBFFilePathFromDirectory(pbfPath, shardToBuild))).build());
    }

    private Atlas generateSlicedAtlasGivenRawAtlas(final String countryName,
            final CountryBoundaryMap countryBoundaryMap, final Atlas rawAtlas)
    {
        final Atlas slicedRawAtlas = new RawAtlasCountrySlicer(countryName, countryBoundaryMap)
                .slice(rawAtlas);
        return slicedRawAtlas;
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

    private PackedAtlas runGenerationForFlavor(final RawAtlasFlavor atlasFlavor,
            final CountryBoundaryMap countryBoundaryMap, final Shard shardToBuild,
            final String pbfPath, final String slicedCachePath,
            final boolean failFastOnSlicedCacheMiss, final SlippyTilePersistenceScheme pbfScheme,
            final Sharding pbfSharding, final Sharding sharding, final String countryName)
    {
        logger.info("Using raw atlas flavor {}", atlasFlavor);

        final Atlas rawAtlas = generateRawAtlas(pbfPath, shardToBuild);
        if (atlasFlavor == RawAtlasFlavor.RawAtlas)
        {
            return (PackedAtlas) rawAtlas;
        }

        final Atlas slicedAtlas = generateSlicedAtlasGivenRawAtlas(countryName, countryBoundaryMap,
                rawAtlas);
        if (atlasFlavor == RawAtlasFlavor.SlicedRawAtlas)
        {
            return (PackedAtlas) slicedAtlas;
        }

        final Atlas sectionedAtlas = generateSectionedAtlasGivenSlicedAtlas(countryName,
                shardToBuild, countryBoundaryMap, sharding, slicedCachePath,
                failFastOnSlicedCacheMiss, pbfPath);
        if (atlasFlavor == RawAtlasFlavor.SectionedRawAtlas)
        {
            return (PackedAtlas) sectionedAtlas;
        }

        throw new CoreException("RawAtlasFlavor value {} was invalid!", atlasFlavor.toString());
    }
}
