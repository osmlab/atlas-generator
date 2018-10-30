package org.openstreetmap.atlas.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasCountryStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasProtoOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.delta.RemovedMultipleAtlasDeltaOutputFormat;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
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
 */
public class AtlasGenerator extends SparkJob
{
    private static final long serialVersionUID = 5985696743749843135L;

    private static final Logger logger = LoggerFactory.getLogger(AtlasGenerator.class);
    public static final String ATLAS_FOLDER = "atlas";
    public static final String RAW_ATLAS_FOLDER = "rawAtlas";
    public static final String SLICED_RAW_ATLAS_FOLDER = "slicedRawAtlas";
    public static final String SHARD_STATISTICS_FOLDER = "shardStats";
    public static final String COUNTRY_STATISTICS_FOLDER = "countryStats";
    public static final String SHARD_DELTAS_FOLDER = "deltas";
    public static final String SHARD_DELTAS_ADDED_FOLDER = "deltasAdded";
    public static final String SHARD_DELTAS_CHANGED_FOLDER = "deltasChanged";
    public static final String SHARD_DELTAS_REMOVED_FOLDER = "deltasRemoved";

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
        final JavaPairRDD<String, Atlas> countryRawAtlasShardsRDD = getContext()
                .parallelize(tasks, tasks.size())
                .mapToPair(AtlasGeneratorHelper.generateRawAtlas(broadcastBoundaries, sparkContext,
                        broadcastLoadingOptions, pbfContext, atlasScheme))
                .filter(tuple -> tuple._2() != null);

        // Persist the RDD and save the intermediary state
        countryRawAtlasShardsRDD.cache();
        this.getContext().setJobGroup("0", "Raw Atlas Extraction From PBF");
        countryRawAtlasShardsRDD.saveAsHadoopFile(
                getAlternateSubFolderOutput(output, RAW_ATLAS_FOLDER), Text.class, Atlas.class,
                MultipleAtlasOutputFormat.class, new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE RAW ATLAS **********\n");

        // Slice the raw Atlas and filter any null atlases
        final JavaPairRDD<String, Atlas> countrySlicedRawAtlasShardsRDD = countryRawAtlasShardsRDD
                .mapToPair(AtlasGeneratorHelper.sliceRawAtlas(broadcastBoundaries))
                .filter(tuple -> tuple._2() != null);

        // Persist the RDD and save the intermediary state
        final String slicedRawAtlasPath = getAlternateSubFolderOutput(output,
                SLICED_RAW_ATLAS_FOLDER);
        countrySlicedRawAtlasShardsRDD.cache();
        this.getContext().setJobGroup("1", "Raw Atlas Country Slicing");
        countrySlicedRawAtlasShardsRDD.saveAsHadoopFile(slicedRawAtlasPath, Text.class, Atlas.class,
                MultipleAtlasOutputFormat.class, new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE SLICED RAW ATLAS **********\n");

        // Remove the raw atlas RDD from cache since we've cached the sliced RDD
        countryRawAtlasShardsRDD.unpersist();

        // Section the sliced raw Atlas
        final JavaPairRDD<String, Atlas> countryAtlasShardsRDD = countrySlicedRawAtlasShardsRDD
                .mapToPair(AtlasGeneratorHelper.sectionRawAtlas(broadcastBoundaries,
                        broadcastSharding, sparkContext, broadcastLoadingOptions,
                        slicedRawAtlasPath, atlasScheme, tasks));

        // Persist the RDD and save the final atlas
        countryAtlasShardsRDD.cache();
        this.getContext().setJobGroup("2", "Raw Atlas Way Sectioning");
        if (useJavaFormat)
        {
            countryAtlasShardsRDD.saveAsHadoopFile(
                    getAlternateSubFolderOutput(output, ATLAS_FOLDER), Text.class, Atlas.class,
                    MultipleAtlasOutputFormat.class, new JobConf(configuration()));
        }
        else
        {
            countryAtlasShardsRDD.saveAsHadoopFile(
                    getAlternateSubFolderOutput(output, ATLAS_FOLDER), Text.class, Atlas.class,
                    MultipleAtlasProtoOutputFormat.class, new JobConf(configuration()));
        }
        logger.info("\n\n********** SAVED THE FINAL ATLAS **********\n");

        // Remove the sliced atlas RDD from cache since we've cached the final RDD
        countrySlicedRawAtlasShardsRDD.unpersist();

        // Create the metrics
        final JavaPairRDD<String, AtlasStatistics> statisticsRDD = countryAtlasShardsRDD
                .mapToPair(AtlasGeneratorHelper.generateAtlasStatistics(broadcastSharding));

        // Persist the RDD and save
        statisticsRDD.cache();
        this.getContext().setJobGroup("3", "Shard Statistics Creation");
        statisticsRDD.saveAsHadoopFile(getAlternateSubFolderOutput(output, SHARD_STATISTICS_FOLDER),
                Text.class, AtlasStatistics.class, MultipleAtlasStatisticsOutputFormat.class,
                new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE SHARD STATISTICS **********\n");

        // Aggregate the metrics
        final JavaPairRDD<String, AtlasStatistics> reducedStatisticsRDD = statisticsRDD
                .mapToPair(tuple ->
                {
                    final String countryShardName = tuple._1();
                    final String countryName = StringList.split(countryShardName, "_").get(0);
                    return new Tuple2<>(countryName, tuple._2());
                }).reduceByKey(AtlasStatistics::merge);

        // Save aggregated metrics
        this.getContext().setJobGroup("4", "Country Statistics Creation");
        reducedStatisticsRDD.saveAsHadoopFile(
                getAlternateSubFolderOutput(output, COUNTRY_STATISTICS_FOLDER), Text.class,
                AtlasStatistics.class, MultipleAtlasCountryStatisticsOutputFormat.class,
                new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE COUNTRY STATISTICS **********\n");

        // Compute the deltas, if needed
        if (!previousOutputForDelta.isEmpty())
        {
            final JavaPairRDD<String, AtlasDelta> deltasRDD = countryAtlasShardsRDD.flatMapToPair(
                    AtlasGeneratorHelper.computeAtlasDelta(sparkContext, previousOutputForDelta));

            // Save the deltas
            this.getContext().setJobGroup("5", "Deltas Creation");
            deltasRDD.saveAsHadoopFile(getAlternateSubFolderOutput(output, SHARD_DELTAS_FOLDER),
                    Text.class, AtlasDelta.class, RemovedMultipleAtlasDeltaOutputFormat.class,
                    new JobConf(configuration()));
            logger.info("\n\n********** SAVED THE DELTAS **********\n");
        }
    }

    @Override
    protected List<String> outputToClean(final CommandMap command)
    {
        final String output = output(command);
        final List<String> staticPaths = super.outputToClean(command);
        staticPaths.add(getAlternateSubFolderOutput(output, COUNTRY_STATISTICS_FOLDER));
        staticPaths.add(getAlternateSubFolderOutput(output, SHARD_STATISTICS_FOLDER));
        staticPaths.add(getAlternateSubFolderOutput(output, SHARD_DELTAS_FOLDER));
        staticPaths.add(getAlternateSubFolderOutput(output, SHARD_DELTAS_ADDED_FOLDER));
        staticPaths.add(getAlternateSubFolderOutput(output, SHARD_DELTAS_CHANGED_FOLDER));
        staticPaths.add(getAlternateSubFolderOutput(output, SHARD_DELTAS_REMOVED_FOLDER));
        staticPaths.add(getAlternateSubFolderOutput(output, ATLAS_FOLDER));
        return staticPaths;
    }

    @Override
    protected SwitchList switches()
    {
        final SwitchList result = super.switches();
        result.addAll(AtlasGeneratorParameters.switches());
        return result;
    }
}
