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
import org.openstreetmap.atlas.generator.AtlasGeneratorHelper.NamedAtlasStatistics;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasCountryStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasProtoOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleLineDelimitedGeojsonOutputFormat;
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
    public static final String LINE_SLICED_ATLAS_FOLDER = "lineSlicedAtlas";
    public static final String LINE_SLICED_SUB_ATLAS_FOLDER = "lineSlicedSubAtlas";
    public static final String FULLY_SLICED_ATLAS_FOLDER = "fullySlicedAtlas";
    public static final String SHARD_STATISTICS_FOLDER = "shardStats";
    public static final String COUNTRY_STATISTICS_FOLDER = "countryStats";
    public static final String SHARD_DELTAS_FOLDER = "deltas";
    public static final String SHARD_DELTAS_ADDED_FOLDER = "deltasAdded";
    public static final String SHARD_DELTAS_CHANGED_FOLDER = "deltasChanged";
    public static final String SHARD_DELTAS_REMOVED_FOLDER = "deltasRemoved";
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
                ? AtlasSharding.forString(pbfShardingName, configuration()) : sharding;
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
                .filter(tuple -> tuple._2() != null);

        // Persist the RDD and save the intermediary state
        countryRawAtlasRDD.cache();
        this.getContext().setJobGroup("0", "Raw Atlas from PBF by country creation");
        countryRawAtlasRDD.saveAsHadoopFile(getAlternateSubFolderOutput(output, RAW_ATLAS_FOLDER),
                Text.class, Atlas.class, MultipleAtlasOutputFormat.class,
                new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE RAW ATLAS **********\n");

        // Slice the raw Atlas and filter any null atlases
        final JavaPairRDD<String, Atlas> lineSlicedAtlasRDD = countryRawAtlasRDD
                .mapToPair(AtlasGeneratorHelper.sliceRawAtlasLines(broadcastBoundaries))
                .filter(tuple -> tuple._2() != null);

        // Persist the RDD and save the intermediary state
        final String lineSlicedAtlasPath = getAlternateSubFolderOutput(output,
                LINE_SLICED_ATLAS_FOLDER);
        lineSlicedAtlasRDD.cache();
        this.getContext().setJobGroup("1", "Raw Atlas Line Slicing");
        lineSlicedAtlasRDD.saveAsHadoopFile(lineSlicedAtlasPath, Text.class, Atlas.class,
                MultipleAtlasOutputFormat.class, new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE SLICED ATLAS **********\n");

        // Remove the raw atlas RDD from cache since we've cached the sliced RDD
        countryRawAtlasRDD.unpersist();

        // Subatlas the raw shard Atlas files based on water relations
        final JavaPairRDD<String, Atlas> lineSlicedSubAtlasRDD = lineSlicedAtlasRDD
                .mapToPair(AtlasGeneratorHelper.subatlasWaterRelations())
                .filter(tuple -> tuple._2() != null);
        final String lineSlicedSubAtlasPath = getAlternateSubFolderOutput(output,
                LINE_SLICED_SUB_ATLAS_FOLDER);
        lineSlicedSubAtlasRDD.cache();
        this.getContext().setJobGroup("2", "Line Sliced SubAtlas creation");
        lineSlicedSubAtlasRDD.saveAsHadoopFile(lineSlicedSubAtlasPath, Text.class, Atlas.class,
                MultipleAtlasOutputFormat.class, new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE LINE SLICED SUB ATLAS **********\n");

        // Slice the raw Atlas and filter any null atlases
        final JavaPairRDD<String, Atlas> fullySlicedRawAtlasShardsRDD = countryRawAtlasRDD
                .mapToPair(AtlasGeneratorHelper.sliceRawAtlasRelations(broadcastBoundaries,
                        broadcastSharding, lineSlicedSubAtlasPath, lineSlicedAtlasPath, atlasScheme,
                        sparkContext, broadcastLoadingOptions, tasks))
                .filter(tuple -> tuple._2() != null);

        // Persist the RDD and save the intermediary state
        final String fullySlicedRawAtlasPath = getAlternateSubFolderOutput(output,
                FULLY_SLICED_ATLAS_FOLDER);
        fullySlicedRawAtlasShardsRDD.cache();
        this.getContext().setJobGroup("3", "Raw Atlas Relation Slicing");
        fullySlicedRawAtlasShardsRDD.saveAsHadoopFile(fullySlicedRawAtlasPath, Text.class,
                Atlas.class, MultipleAtlasOutputFormat.class, new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE SLICED RAW ATLAS **********\n");

        // Remove the partially sliced atlas RDD from cache since we've cached the sliced RDD
        lineSlicedAtlasRDD.unpersist();
        lineSlicedSubAtlasRDD.unpersist();

        // Section the sliced raw Atlas
        final JavaPairRDD<String, Atlas> countryAtlasShardsRDD = fullySlicedRawAtlasShardsRDD
                .mapToPair(AtlasGeneratorHelper.sectionRawAtlas(broadcastBoundaries,
                        broadcastSharding, sparkContext, broadcastLoadingOptions,
                        fullySlicedRawAtlasPath, atlasScheme, tasks));

        // Persist the RDD and save the final atlas
        countryAtlasShardsRDD.cache();
        this.getContext().setJobGroup("4", "Raw Atlas Way Sectioning");
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
                .mapToPair(AtlasGeneratorHelper.generateAtlasStatistics(broadcastSharding));

        // Persist the RDD and save
        statisticsRDD.cache();
        this.getContext().setJobGroup("5", "Shard Statistics Creation");
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
                    return new Tuple2<>(countryName,
                            // Here using NamedAtlasStatistics so the reduceByKey function below can
                            // name what statistic merging failed, if any.
                            new NamedAtlasStatistics(countryName, tuple._2()));
                }).reduceByKey(AtlasGeneratorHelper.reduceAtlasStatistics())
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2().getAtlasStatistics()));

        // Save aggregated metrics
        this.getContext().setJobGroup("6", "Country Statistics Creation");
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
            this.getContext().setJobGroup("7", "Deltas Creation");
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
