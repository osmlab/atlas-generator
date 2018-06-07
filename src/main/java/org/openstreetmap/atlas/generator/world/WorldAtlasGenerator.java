package org.openstreetmap.atlas.generator.world;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.generator.AtlasGeneratorHelper;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasMetaData;
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlasCloner;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.pbf.OsmPbfLoader;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.geography.atlas.statistics.Counter;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.tags.Taggable;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single threaded process taking an OSM PBF file and making an Atlas from it.
 *
 * @author matthieun
 */
public class WorldAtlasGenerator extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(WorldAtlasGenerator.class);

    private static final Switch<File> PBF = new Switch<>("pbf",
            "The pbf file or folder containing the OSM pbfs", File::new, Optionality.REQUIRED);
    private static final Switch<File> ATLAS = new Switch<>("atlas",
            "The atlas file to which the Atlas will be saved", File::new, Optionality.REQUIRED);
    private static final Switch<File> STATISTICS = new Switch<>("statistics",
            "The file that will contain the statistics", File::new, Optionality.OPTIONAL);
    public static final Switch<CountryBoundaryMap> BOUNDARIES = new Switch<>("boundaries",
            "The boundary map to use", value -> CountryBoundaryMap.fromPlainText(new File(value)),
            Optionality.REQUIRED);
    public static final Switch<String> CODE_VERSION = new Switch<>("codeVersion",
            "The code version", StringConverter.IDENTITY, Optionality.OPTIONAL, "unknown");
    public static final Switch<String> DATA_VERSION = new Switch<>("dataVersion",
            "The data version", StringConverter.IDENTITY, Optionality.OPTIONAL, "unknown");
    public static final Switch<Boolean> USE_RAW_ATLAS = new Switch<>("useRawAtlas",
            "Allow PBF to Atlas process to use Raw Atlas flow", Boolean::parseBoolean,
            Optionality.OPTIONAL, "false");
    public static final Switch<String> FORCE_SLICING_CONFIGURATION = new Switch<>(
            "forceSlicingConfiguration",
            "The path to the configuration file that defines which entities on which country slicing will"
                    + " always be attempted regardless of the number of countries it intersects according to the"
                    + " country boundary map's grid index.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);

    public static void main(final String[] args)
    {
        new WorldAtlasGenerator().run(args);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final CountryBoundaryMap countryBoundaryMap = (CountryBoundaryMap) command.get(BOUNDARIES);
        final File pbf = (File) command.get(PBF);
        final File output = (File) command.get(ATLAS);
        final File statisticsOutput = (File) command.get(STATISTICS);
        final String codeVersion = (String) command.get(CODE_VERSION);
        final String dataVersion = (String) command.get(DATA_VERSION);
        final Shard world = SlippyTile.ROOT;
        final String forceSlicingConfiguration = (String) command.get(FORCE_SLICING_CONFIGURATION);
        final Predicate<Taggable> forceSlicingPredicate = forceSlicingConfiguration == null
                ? taggable -> false
                : AtlasGeneratorHelper.getTaggableFilterFrom(new File(forceSlicingConfiguration));
        countryBoundaryMap.setForceSlicingPredicate(forceSlicingPredicate);

        final Time start = Time.now();

        // Prepare
        final AtlasLoadingOption loadingOptions = AtlasLoadingOption
                .createOptionWithAllEnabled(countryBoundaryMap)
                .setAdditionalCountryCodes(countryBoundaryMap.allCountryNames());
        final AtlasMetaData metaData = new AtlasMetaData(null, true, codeVersion, dataVersion,
                "WORLD", world.getName(), Maps.hashMap());

        final Counter counter = new Counter();
        counter.setCountsDefinition(Counter.POI_COUNTS_DEFINITION.getDefault());

        final Atlas atlas;
        if (pbf.isDirectory())
        {
            final List<Atlas> pieces = pbf.listFilesRecursively().stream()
                    .filter(pbfFile -> pbfFile.getName().endsWith(FileSuffix.PBF.toString()))
                    .map(pbfFile ->
                    {
                        logger.info("Generating pieced atlas from {}", pbfFile);
                        final OsmPbfLoader loader = new OsmPbfLoader(pbfFile, MultiPolygon.MAXIMUM,
                                loadingOptions).withMetaData(metaData);
                        return loader.read();
                    }).filter(piece -> piece != null).collect(Collectors.toList());
            logger.info("Concatenating atlas from {} pieces.", pieces.size());
            atlas = pieces.isEmpty() ? null
                    : new PackedAtlasCloner().cloneFrom(new MultiAtlas(pieces));
        }
        else
        {
            logger.info("Generating world atlas from {}", pbf);
            final OsmPbfLoader loader = new OsmPbfLoader(pbf, MultiPolygon.MAXIMUM, loadingOptions)
                    .withMetaData(metaData);
            atlas = loader.read();
        }

        if (atlas == null)
        {
            logger.warn("There was no data to add to the Atlas.");
            return 0;
        }

        logger.info("Saving world atlas to {}", output);
        atlas.save(output);
        logger.info("Generating world statistics...");
        final AtlasStatistics statistics = counter.processAtlas(atlas);
        logger.info("Saving world statistics to {}", statisticsOutput);
        statistics.save(statisticsOutput);

        logger.info("Finished creating world atlas in {}", start.elapsedSince());

        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(PBF, ATLAS, STATISTICS, BOUNDARIES, CODE_VERSION, DATA_VERSION,
                FORCE_SLICING_CONFIGURATION);
    }

}
