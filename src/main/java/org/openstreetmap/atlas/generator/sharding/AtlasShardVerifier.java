package org.openstreetmap.atlas.generator.sharding;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.converters.SparkOptionsStringConverter;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.openstreetmap.atlas.streaming.writers.SafeBufferedWriter;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matthieun
 */
public class AtlasShardVerifier extends Command
{
    public static final Switch<String> ATLAS_FOLDER = new Switch<>("atlasFolder",
            "Folder containing Atlas Shards named by the AtlasGenerator", StringConverter.IDENTITY,
            Optionality.REQUIRED);
    public static final Switch<String> EXPECTED_SHARDS = new Switch<>("expectedShards",
            "Path to file containing all the expected shards, or to folder containing reference atlas files",
            StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<String> OUTPUT = new Switch<>("output",
            "The file to list all the missing shards", StringConverter.IDENTITY,
            Optionality.REQUIRED);
    public static final Switch<Map<String, String>> SPARK_OPTIONS = new Switch<>("sparkOptions",
            "Comma separated list of Spark options, i.e. key1=value1,key2=value2",
            new SparkOptionsStringConverter(), Optionality.OPTIONAL, "");
    public static final Switch<Integer> LIST_DEPTH = new Switch<>("listDepth",
            "Depth to list recursive folders", Integer::valueOf, Optionality.OPTIONAL, "2");
    public static final Switch<Pattern> PATH_FILTER_REGEX = new Switch<>("pathFilterRegex",
            "Regex to filter paths to list", Pattern::compile, Optionality.OPTIONAL, ".*\\.atlas");

    private static final Logger logger = LoggerFactory.getLogger(AtlasShardVerifier.class);

    public static void main(final String[] args)
    {
        new AtlasShardVerifier().run(args);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final String atlasFolder = (String) command.get(ATLAS_FOLDER);
        final int depth = (int) command.get(LIST_DEPTH);
        final Pattern pattern = (Pattern) command.get(PATH_FILTER_REGEX);
        logger.debug("Using regex filter \"{}\"", pattern);
        final PathFilter filter = path -> pattern.matcher(path.toString()).matches();
        @SuppressWarnings("unchecked")
        final Map<String, String> sparkConfiguration = (Map<String, String>) command
                .get(SPARK_OPTIONS);
        final WritableResource output = FileSystemHelper
                .writableResource((String) command.get(OUTPUT), sparkConfiguration);

        final String expectedShardsPath = (String) command.get(EXPECTED_SHARDS);
        final Set<CountryShard> expectedShards;
        if (FileSystemHelper.isFile(expectedShardsPath, sparkConfiguration))
        {
            logger.trace("isFile: {}", expectedShardsPath);
            expectedShards = FileSystemHelper.resource(expectedShardsPath, sparkConfiguration)
                    .linesList().stream().map(CountryShard::forName).collect(Collectors.toSet());
        }
        else if (FileSystemHelper.isDirectory(expectedShardsPath, sparkConfiguration))
        {
            logger.trace("isDirectory: {}", expectedShardsPath);
            expectedShards = shardsFromFolder(expectedShardsPath, sparkConfiguration, depth,
                    filter);
        }
        else
        {
            throw new CoreException("{} does not exist.", expectedShardsPath);
        }
        final Set<CountryShard> existingShards = shardsFromFolder(atlasFolder, sparkConfiguration,
                depth, filter);
        expectedShards.removeAll(existingShards);
        try (SafeBufferedWriter writer = output.writer())
        {
            expectedShards.stream().map(CountryShard::getName).forEach(writer::writeLine);
        }
        catch (final Exception e)
        {
            throw new CoreException("Verification failed", e);
        }
        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(ATLAS_FOLDER, EXPECTED_SHARDS, OUTPUT, SPARK_OPTIONS,
                LIST_DEPTH, PATH_FILTER_REGEX);
    }

    private Set<CountryShard> shardsFromFolder(final String expectedShardsPath,
            final Map<String, String> sparkConfiguration, final int depth, final PathFilter filter)
    {
        logger.trace("listResourcesRecursively: {}", expectedShardsPath);
        final Time start = Time.now();
        final Set<CountryShard> result = FileSystemHelper
                .streamPathsRecursively(expectedShardsPath, sparkConfiguration, filter, depth)
                .map(Path::getName)
                .map(name -> StringList.split(name, "/").last()
                        .orElseThrow(() -> new CoreException("Path {} not recognized!", name)))
                .map(name -> StringList.split(name, ".").get(0)).map(CountryShard::forName)
                .collect(Collectors.toSet());
        logger.debug("Took {} to find {} countryShards in {}", start.elapsedSince(), result.size(),
                expectedShardsPath);
        return result;
    }
}
