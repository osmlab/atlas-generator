package org.openstreetmap.atlas.generator.sharding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.raw.creation.RawAtlasGenerator;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.maps.MultiMap;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command that verifies all expected PBF files exist and can be converted to atlas.
 *
 * @author jamesgage
 */
public class PbfVerifier extends Command
{
    private static final Switch<File> SLIPPY_TILE_FILE = new Switch<>("slippyTileFile",
            "The slippy tile file that lists all pbf files that should have been generated.",
            File::new, Optionality.REQUIRED);
    private static final Switch<String> PBF_PATH = new Switch<>("pbfPath",
            "The path to the pbf root directory", value -> value, Optionality.REQUIRED);
    private static final int LOGGING_RATE = 1000;
    private static final Logger logger = LoggerFactory.getLogger(PbfVerifier.class);

    public static MultiPolygon forPolygon(final Polygon polygon)
    {
        final MultiMap<Polygon, Polygon> multiMap = new MultiMap<>();
        multiMap.put(polygon, new ArrayList<>());
        return new MultiPolygon(multiMap);
    }

    public static void main(final String[] args)
    {
        new PbfVerifier().run(args);
    }

    public static HashMap<String, Rectangle> parseSlippyTileFile(final File slippyTileFile)
    {
        final Integer size = Integer.parseInt(slippyTileFile.firstLine());
        final HashMap<String, Rectangle> shardToBounds = new HashMap<>(size);
        for (final String line : slippyTileFile.lines())
        {
            final StringList splitLine = StringList.split(line, "|");
            if (splitLine.size() == 2)
            {
                final String boundsString = splitLine.get(0);
                final String name = splitLine.get(1);
                final Rectangle bounds = Rectangle.forString(boundsString);
                shardToBounds.put(name, bounds);
            }
        }
        return shardToBounds;
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final String pbfPath = (String) command.get(PBF_PATH);
        final Map<String, String> configuration = new HashMap<>();
        configuration.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        final List<Resource> pbfFiles = FileSystemHelper.listResourcesRecursively(pbfPath,
                configuration, new PbfFilePathFilter());
        final List<String> pbfFileNames = pbfFiles.stream().map(file -> file.getName())
                .collect(Collectors.toList());
        final Time start = Time.now();

        // get the count of all pbf files generated
        final int pbfFileCount = pbfFiles.size();

        // get the count of pbf tiles that should have been generated
        final File slippyTileFile = (File) command.get(SLIPPY_TILE_FILE);

        final HashMap<String, Rectangle> shardToBounds = parseSlippyTileFile(slippyTileFile);

        final Integer expectedPbfCount;
        try
        {
            expectedPbfCount = Integer.parseInt(slippyTileFile.firstLine());

            // subtract the number of pbfs generated from the expected count
            final Integer missingPbfCount = expectedPbfCount - pbfFileCount;

            // since there can be empty PBF files, any missing file is a problem! fail if there
            // aren't the same # of pbfs as the slippytileFile
            if (missingPbfCount != 0)
            {
                logger.error("There are " + missingPbfCount + " slippy tiles missing!");
                shardToBounds.keySet().forEach(pbfName ->
                {
                    if (!pbfFileNames.contains(pbfName))
                    {
                        logger.error(pbfName + " is missing!");
                    }
                });
                return 1;
            }
            // try to load each pbf, any that fail should be logged
            // create a parallel stream, load each pbf path using RawAtlasGenerator with
            // AtlasLoadingOption set to no slicing
            final AtomicInteger count = new AtomicInteger();
            pbfFiles.parallelStream().forEach(pbfFile ->
            {
                final RawAtlasGenerator rawAtlasGenerator = new RawAtlasGenerator(pbfFile,
                        AtlasLoadingOption.createOptionWithNoSlicing(),
                        forPolygon(shardToBounds.get(pbfFile.getName())));
                try
                {
                    final Atlas atlas = rawAtlasGenerator.buildNoTrim();
                    final int currentCount = count.getAndIncrement();
                    if (currentCount % LOGGING_RATE == 0)
                    {
                        logger.info("Processed " + currentCount + " PBF files.");
                    }
                }
                catch (final Exception e)
                {
                    logger.error("Error while building " + pbfFile.getName() + "!");
                }
            });
        }
        catch (final NumberFormatException e)
        {
            logger.error("Couldn't parse the pbf count from the slippy tile file!");
            return 1;
        }
        logger.info("---------------------------------------------");
        logger.info("All " + expectedPbfCount + " pbfs are present and valid!");
        logger.info("Verification ran in: " + start.elapsedSince().asMinutes() + " minutes.");
        logger.info("---------------------------------------------");
        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(SLIPPY_TILE_FILE, PBF_PATH);
    }

}
