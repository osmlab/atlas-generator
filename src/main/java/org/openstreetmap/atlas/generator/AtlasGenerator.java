package org.openstreetmap.atlas.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.openstreetmap.atlas.generator.AtlasGeneratorHelper.NamedAtlasStatistics;
import org.openstreetmap.atlas.generator.persistence.MultipleLineDelimitedGeojsonOutputFormat;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMapArchiver;
import org.openstreetmap.atlas.geography.boundary.CountryShardListing;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.tags.Taggable;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.maps.MultiMapWithSet;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Generate {@link Atlas} Shards for a specific version and a specific set of countries
 *
 * @author matthieun
 * @author mgostintsev
 * @author samgass
 */
public class AtlasGenerator extends SparkJob
{

    private static final long serialVersionUID = 5985696743749843135L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasGenerator.class);

    private static final String SAVED_MESSAGE = "\n\n********** SAVED FOR STEP: {} **********\n";

    public static final String LINE_DELIMITED_GEOJSON_STATISTICS_FOLDER = "ldgeojson";

    public static void main(final String[] args)
    {
        new AtlasGenerator().run(args);
    }

    /**
     * Generates a {@link List} of {@link AtlasGenerationTask}s for given countries using given
     * {@link CountryBoundaryMap} and {@link Sharding} strategy.
     *
     * @param countries
     *            Countries to generate tasks for
     * @param boundaryMap
     *            {@link CountryBoundaryMap} to read country boundaries
     * @param sharding
     *            {@link Sharding} strategy
     * @return {@link List} of {@link AtlasGenerationTask}s
     */
    protected static List<AtlasGenerationTask> generateTasks(final StringList countries,
            final CountryBoundaryMap boundaryMap, final Sharding sharding)
    {
        final MultiMapWithSet<String, Shard> countryToShardMap = CountryShardListing
                .countryToShardList(countries, boundaryMap, sharding);
        // Generate tasks from country-shard map
        final List<AtlasGenerationTask> tasks = new ArrayList<>();
        countryToShardMap.keySet().forEach(country ->
        {
            final Set<Shard> shards = countryToShardMap.get(country);
            if (!shards.isEmpty())
            {
                shards.forEach(shard -> tasks.add(new AtlasGenerationTask(country, shard, shards)));
            }
            else
            {
                logger.warn("No shards were found for {}. Skipping task generation.", country);
            }
        });

        return tasks;
    }

    @Override
    public String getName()
    {
        return "Atlas Generator";
    }

    @Override
    public void start(final CommandMap command)
    {
        final Map<String, String> sparkContext = configurationMap();

        final StringList countries = (StringList) command.get(AtlasGeneratorParameters.COUNTRIES);
        final String countryShapes = (String) command.get(AtlasGeneratorParameters.COUNTRY_SHAPES);
        final String previousOutputForDelta = (String) command
                .get(AtlasGeneratorParameters.PREVIOUS_OUTPUT_FOR_DELTA);
        final String pbfPath = (String) command.get(AtlasGeneratorParameters.PBF_PATH);
        final SlippyTilePersistenceScheme pbfScheme = (SlippyTilePersistenceScheme) command
                .get(AtlasGeneratorParameters.PBF_SCHEME);
        final SlippyTilePersistenceScheme atlasScheme = (SlippyTilePersistenceScheme) command
                .get(AtlasGeneratorParameters.ATLAS_SCHEME);
        final String pbfShardingName = (String) command.get(AtlasGeneratorParameters.PBF_SHARDING);
        final String shardingName = (String) command.get(AtlasGeneratorParameters.SHARDING_TYPE);
        final Sharding sharding = AtlasSharding.forString(shardingName, configuration());
        final Sharding pbfSharding = pbfShardingName != null
                ? AtlasSharding.forString(pbfShardingName, configuration())
                : sharding;
        final PbfContext pbfContext = new PbfContext(pbfPath, pbfSharding, pbfScheme);
        final String shouldAlwaysSliceConfiguration = (String) command
                .get(AtlasGeneratorParameters.SHOULD_ALWAYS_SLICE_CONFIGURATION);
        final Predicate<Taggable> shouldAlwaysSlicePredicate;
        if (shouldAlwaysSliceConfiguration == null)
        {
            shouldAlwaysSlicePredicate = taggable -> false;
        }
        else
        {
            shouldAlwaysSlicePredicate = AtlasGeneratorParameters.getTaggableFilterFrom(
                    FileSystemHelper.resource(shouldAlwaysSliceConfiguration, sparkContext));
        }
        final String output = output(command);
        final boolean useJavaFormat = (boolean) command
                .get(AtlasGeneratorParameters.USE_JAVA_FORMAT);
        final boolean lineDelimitedGeojsonOutput = (boolean) command
                .get(AtlasGeneratorParameters.LINE_DELIMITED_GEOJSON_OUTPUT);

        // This has to be converted here, as we need the Spark Context
        final Resource countryBoundaries = resource(countryShapes);

        logger.info("Reading country boundaries from {}", countryShapes);
        final CountryBoundaryMap boundaries = new CountryBoundaryMapArchiver()
                .read(countryBoundaries);
        logger.info("Done Reading {} country boundaries from {}", boundaries.size(), countryShapes);
        if (!boundaries.hasGridIndex())
        {
            logger.warn(
                    "Given country boundary file didn't have grid index. Initializing grid index for {}.",
                    countries);
            boundaries.initializeGridIndex(countries.stream().collect(Collectors.toSet()));
        }
        boundaries.setShouldAlwaysSlicePredicate(shouldAlwaysSlicePredicate);

        // Generate country-shard generation tasks
        final Time timer = Time.now();
        final List<AtlasGenerationTask> tasks = generateTasks(countries, boundaries, sharding);
        logger.debug("Generated {} tasks in {}.", tasks.size(), timer.elapsedSince());

        // AtlasLoadingOption isn't serializable, neither is command map. To avoid duplicating
        // boiler-plate code for creating the AtlasLoadingOption, extract the properties we need
        // from the command map and pass those around to create the AtlasLoadingOption
        final Map<String, String> atlasLoadingOptions = AtlasGeneratorParameters
                .extractAtlasLoadingProperties(command, sparkContext);

        // Leverage Spark broadcast to have a read-only variable cached on each machine, instead of
        // shipping a copy with each task. All of these are re-used across tasks and are unchanged.
        final Broadcast<CountryBoundaryMap> broadcastBoundaries = getContext()
                .broadcast(boundaries);
        final Broadcast<Map<String, String>> broadcastLoadingOptions = getContext()
                .broadcast(atlasLoadingOptions);
        final Broadcast<Sharding> broadcastSharding = getContext().broadcast(sharding);

        // Generate the raw Atlas and filter any null atlases
        final JavaPairRDD<String, Atlas> countryRawAtlasRDD = getContext()
                .parallelize(tasks, tasks.size())
                .mapToPair(AtlasGeneratorHelper.generateRawAtlas(broadcastBoundaries, sparkContext,
                        broadcastLoadingOptions, pbfContext, atlasScheme))
                .filter(tuple -> tuple._2() != null).cache();

        // Persist the RDD and save the intermediary state
        saveAsHadoop(countryRawAtlasRDD, AtlasGeneratorJobGroup.RAW, output);

        // Slice the raw Atlas and filter any null atlases
        final JavaPairRDD<String, Atlas> lineSlicedAtlasRDD = countryRawAtlasRDD
                .mapToPair(AtlasGeneratorHelper.sliceRawAtlasLines(broadcastBoundaries))
                .filter(tuple -> tuple._2() != null).cache();
        saveAsHadoop(lineSlicedAtlasRDD, AtlasGeneratorJobGroup.LINE_SLICED, output);

        // Remove the raw atlas RDD from cache since we've cached the sliced RDD
        countryRawAtlasRDD.unpersist();

        // Subatlas the raw shard Atlas files based on water relations
        final JavaPairRDD<String, Atlas> lineSlicedSubAtlasRDD = lineSlicedAtlasRDD
                .mapToPair(AtlasGeneratorHelper.subatlas(AtlasGeneratorHelper.subAtlasFilter,
                        AtlasCutType.SILK_CUT))
                .filter(tuple -> tuple._2() != null).cache();
        saveAsHadoop(lineSlicedSubAtlasRDD, AtlasGeneratorJobGroup.LINE_SLICED_SUB, output);

        // Relation slice the line sliced Atlas and filter any null atlases
        final JavaPairRDD<String, Atlas> fullySlicedRawAtlasShardsRDD = countryRawAtlasRDD
                .mapToPair(AtlasGeneratorHelper.sliceRawAtlasRelations(broadcastBoundaries,
                        broadcastSharding,
                        getAlternateSubFolderOutput(output,
                                AtlasGeneratorJobGroup.LINE_SLICED_SUB.getCacheFolder()),
                        getAlternateSubFolderOutput(output,
                                AtlasGeneratorJobGroup.LINE_SLICED.getCacheFolder()),
                        atlasScheme, sparkContext))
                .filter(tuple -> tuple._2() != null).cache();
        saveAsHadoop(fullySlicedRawAtlasShardsRDD, AtlasGeneratorJobGroup.FULLY_SLICED, output);

        // Remove the line sliced atlas RDD from cache since we've cached the fully sliced RDD
        lineSlicedAtlasRDD.unpersist();
        lineSlicedSubAtlasRDD.unpersist();

        // Section the sliced Atlas
        final JavaPairRDD<String, Atlas> countryAtlasShardsRDD = fullySlicedRawAtlasShardsRDD
                .mapToPair(AtlasGeneratorHelper.sectionRawAtlas(broadcastBoundaries,
                        broadcastSharding, sparkContext, broadcastLoadingOptions,
                        getAlternateSubFolderOutput(output,
                                AtlasGeneratorJobGroup.FULLY_SLICED.getCacheFolder()),
                        atlasScheme, tasks))
                .cache();

        if (useJavaFormat)
        {
            saveAsHadoop(countryAtlasShardsRDD, AtlasGeneratorJobGroup.WAY_SECTIONED, output);
        }
        else
        {
            saveAsHadoop(countryAtlasShardsRDD, AtlasGeneratorJobGroup.WAY_SECTIONED_PBF, output);
        }

        if (lineDelimitedGeojsonOutput)
        {
            countryAtlasShardsRDD.saveAsHadoopFile(
                    getAlternateSubFolderOutput(output, LINE_DELIMITED_GEOJSON_STATISTICS_FOLDER),
                    Text.class, String.class, MultipleLineDelimitedGeojsonOutputFormat.class,
                    new JobConf(configuration()));
            logger.info("\n\n********** SAVED THE LINE DELIMITED GEOJSON ATLAS **********\n");
        }

        // Remove the sliced atlas RDD from cache since we've cached the final RDD
        fullySlicedRawAtlasShardsRDD.unpersist();

        // Create the metrics
        final JavaPairRDD<String, AtlasStatistics> statisticsRDD = countryAtlasShardsRDD
                .mapToPair(AtlasGeneratorHelper.generateAtlasStatistics(broadcastSharding)).cache();
        saveAsHadoop(statisticsRDD, AtlasGeneratorJobGroup.SHARD_STATISTICS, output);

        // Aggregate the metrics
        final JavaPairRDD<String, AtlasStatistics> reducedStatisticsRDD = statisticsRDD
                .mapToPair(tuple ->
                {
                    final String countryShardName = tuple._1();
                    final String countryName = StringList.split(countryShardName, "_").get(0);
                    return new Tuple2<>(countryName,
                            // Here using NamedAtlasStatistics so the reduceByKey function below can
                            // name what statistic merging failed, if any.
                            new NamedAtlasStatistics(countryName, tuple._2()));
                }).reduceByKey(AtlasGeneratorHelper.reduceAtlasStatistics())
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2().getAtlasStatistics()));

        // Save aggregated metrics
        saveAsHadoop(reducedStatisticsRDD, AtlasGeneratorJobGroup.COUNTRY_STATISTICS, output);
        statisticsRDD.unpersist();

        // Compute the deltas, if needed
        if (!previousOutputForDelta.isEmpty())
        {
            final JavaPairRDD<String, AtlasDelta> deltasRDD = countryAtlasShardsRDD.flatMapToPair(
                    AtlasGeneratorHelper.computeAtlasDelta(sparkContext, previousOutputForDelta));
            saveAsHadoop(deltasRDD, AtlasGeneratorJobGroup.DELTAS, output);
        }

        countryAtlasShardsRDD.unpersist();
    }

    @Override
    protected List<String> outputToClean(final CommandMap command)
    {
        final String output = output(command);
        final List<String> staticPaths = super.outputToClean(command);
        for (final AtlasGeneratorJobGroup group : AtlasGeneratorJobGroup.values())
        {
            staticPaths.add(getAlternateSubFolderOutput(output, group.getCacheFolder()));
        }
        return staticPaths;
    }

    @Override
    protected SwitchList switches()
    {
        final SwitchList result = super.switches();
        result.addAll(AtlasGeneratorParameters.switches());
        return result;
    }

    @SuppressWarnings("unchecked")
    private void saveAsHadoop(final JavaPairRDD<?, ?> atlasRDD, final AtlasGeneratorJobGroup group,
            final String output)
    {
        this.getContext().setJobGroup(group.getId().toString(), group.getDescription());
        atlasRDD.saveAsHadoopFile(getAlternateSubFolderOutput(output, group.getCacheFolder()),
                Text.class, group.getKeyClass(),
                (Class<? extends MultipleOutputFormat<?, ?>>) group.getOutputClass(),
                new JobConf(configuration()));
        logger.info(SAVED_MESSAGE, group.getDescription());
    }

}
