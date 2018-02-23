package org.openstreetmap.atlas.generator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasOutputFormat;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasStatisticsOutputFormat;
import org.openstreetmap.atlas.generator.persistence.delta.RemovedMultipleAtlasDeltaOutputFormat;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.geography.atlas.statistics.Counter;
import org.openstreetmap.atlas.geography.boundary.CountryBoundary;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMapArchiver;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.tags.filters.ConfiguredTaggableFilter;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.maps.MultiMap;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.runtime.system.memory.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Generate {@link Atlas} Shards for a specific version and a specific set of Countries
 *
 * @author matthieun
 */
public class AtlasGenerator extends SparkJob
{
    /**
     * @author matthieun
     */
    @SuppressWarnings("unused")
    private static class CountrySplitter implements Serializable, Function<String, String>
    {
        private static final long serialVersionUID = 6984474253285033371L;

        @Override
        public String apply(final String key)
        {
            return StringList.split(key, CountryShard.COUNTRY_SHARD_SEPARATOR).get(0);
        }
    }

    private static final long serialVersionUID = 5985696743749843135L;

    private static final Logger logger = LoggerFactory.getLogger(AtlasGenerator.class);
    public static final String ATLAS_FOLDER = "atlas";
    public static final String SHARD_STATISTICS_FOLDER = "shardStats";
    public static final String COUNTRY_STATISTICS_FOLDER = "countryStats";
    public static final String SHARD_DELTAS_FOLDER = "deltas";
    public static final String SHARD_DELTAS_ADDED_FOLDER = "deltasAdded";
    public static final String SHARD_DELTAS_CHANGED_FOLDER = "deltasChanged";
    public static final String SHARD_DELTAS_REMOVED_FOLDER = "deltasRemoved";

    private static final String SHARDING_DEFAULT = "slippy@10";

    public static final Switch<StringList> COUNTRIES = new Switch<>("countries",
            "Comma separated list of countries to be included in the final Atlas",
            value -> StringList.split(value, ","), Optionality.REQUIRED);
    public static final Switch<String> COUNTRY_SHAPES = new Switch<>("countryShapes",
            "Shape file containing the countries", StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<String> PBFS_URL = new Switch<>("pbfs",
            "The path to the folder that contains the pbf map data", StringConverter.IDENTITY,
            Optionality.REQUIRED);
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

    public static void main(final String[] args)
    {
        new AtlasGenerator().run(args);
    }

    /**
     * @param shard
     *            The {@link Shard} to test
     * @param boundaries
     *            The set of country boundaries to test the shard with
     * @return {@code true} when the shard partially or fully intersects at least one of the country
     *         boundaries in the set.
     */
    protected static boolean filterShards(final Shard shard, final List<CountryBoundary> boundaries)
    {
        boolean result = false;
        for (final CountryBoundary boundary : boundaries)
        {
            final MultiPolygon boundaryShape = boundary.getBoundary();
            for (final Polygon outer : boundaryShape.outers())
            {
                if (outer.overlaps(shard.bounds()))
                {
                    result = true;
                    for (final Polygon inner : boundaryShape.innersOf(outer))
                    {
                        if (inner.fullyGeometricallyEncloses(shard.bounds()))
                        {
                            result = false;
                            break;
                        }
                    }
                    if (result)
                    {
                        break;
                    }
                }
            }
            if (result)
            {
                break;
            }
        }
        return result;
    }

    /**
     * Get a rough idea of all the shards for a specific country, by using the country's bounds to
     * find all the shards. It some cases, this list will contain a lot of false positives.
     *
     * @param sharding
     *            The sharding tree to consider
     * @param countryBoundary
     *            The country boundary for which to find the shards
     * @return The rough tally of shards
     */
    protected static Set<Shard> roughShards(final Sharding sharding,
            final CountryBoundary countryBoundary)
    {
        final Set<Shard> shards = new HashSet<>();
        for (final Polygon subBoundary : countryBoundary.getBoundary().outers())
        {
            sharding.shards(subBoundary.bounds()).forEach(shards::add);
        }
        return shards;
    }

    private static ConfiguredTaggableFilter getTaggableFilterFrom(final String path,
            final Configuration configuration)
    {
        final Path pathified = new Path(path);
        return new ConfiguredTaggableFilter(new StandardConfiguration(new InputStreamResource(() ->
        {
            try
            {
                return pathified.getFileSystem(configuration).open(pathified);
            }
            catch (final IOException e)
            {
                throw new CoreException("Unable to read configuration from {}", path, e);
            }
        })));
    }

    @Override
    public String getName()
    {
        return "Atlas Generator";
    }

    @Override
    public void start(final CommandMap command)
    {
        // logger.info("***********************Classpath:**********************");
        // final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        // final URL[] urls = ((URLClassLoader) classLoader).getURLs();
        // for (final URL url : urls)
        // {
        // logger.info(url.getFile());
        // final File file = new File(url.getFile());
        // if (file.isDirectory())
        // {
        // file.listFilesRecursively()
        // .forEach(sub -> logger.info("\t{}", sub.getAbsolutePath()));
        // }
        // }
        // logger.info("*******************************************************");
        // final Class clazz = DOMParser.class;
        // logger.info("##########$$$$$$$$$--" + clazz.getSimpleName() + ": "
        // + clazz.getProtectionDomain().getCodeSource() + ": "
        // + clazz.getProtectionDomain().getCodeSource().getLocation());
        final StringList countries = (StringList) command.get(COUNTRIES);
        final String countryShapes = (String) command.get(COUNTRY_SHAPES);
        final String previousOutputForDelta = (String) command.get(PREVIOUS_OUTPUT_FOR_DELTA);
        final String pbfsUrl = (String) command.get(PBFS_URL);
        // The PBF URL, including the version
        final String pbfPath = pbfsUrl;
        final String shardingName = (String) command.get(SHARDING_TYPE);
        final Sharding sharding = AtlasSharding.forString(shardingName, configuration());
        final String codeVersion = (String) command.get(CODE_VERSION);
        final String dataVersion = (String) command.get(DATA_VERSION);
        final String edgeConfiguration = (String) command.get(EDGE_CONFIGURATION);
        final String waySectioningConfiguration = (String) command
                .get(WAY_SECTIONING_CONFIGURATION);
        final String pbfNodeConfiguration = (String) command.get(PBF_NODE_CONFIGURATION);
        final String pbfWayConfiguration = (String) command.get(PBF_WAY_CONFIGURATION);
        final String pbfRelationConfiguration = (String) command.get(PBF_RELATION_CONFIGURATION);
        final String output = output(command);
        final Map<String, String> sparkContext = configurationMap();

        // This has to be converted here, as we need the Spark Context
        final Resource countryBoundaries = resource(countryShapes);

        logger.info("Reading country boundaries from {}", countryShapes);
        final CountryBoundaryMap worldBoundaries = new CountryBoundaryMapArchiver()
                .read(countryBoundaries);
        logger.info("Done Reading {} country boundaries from {}", worldBoundaries.size(),
                countryShapes);

        // ****** SPARK CODE BELOW ****** //

        // The code below is as parallel as there are countries...
        final JavaRDD<String> countriesRDD = getContext().parallelize(Iterables.asList(countries),
                countries.size());

        // Get a list of country shard pairs, with a lot of false positives
        // TODO Leverage country boundary map grid index when ready
        final List<Tuple2<String, Shard>> roughCountryShards = countriesRDD
                .flatMapToPair(countryName ->
                {
                    // For each country boundary
                    final List<CountryBoundary> boundaries = worldBoundaries
                            .countryBoundary(countryName);

                    // Handle missing boundaries case
                    if (boundaries == null)
                    {
                        logger.error("No boundaries found for country {}!", countryName);
                        return new ArrayList<>();
                    }

                    logger.info("Generating shards for country {}", countryName);
                    final Set<Shard> shards = new HashSet<>();
                    for (final CountryBoundary boundary : boundaries)
                    {
                        // Identify all the shards in the country's bounding box and filter out
                        // those that do not intersect
                        shards.addAll(roughShards(sharding, boundary));
                    }
                    // Assign the country name / shard couples to the countryShards list to be
                    // parallelized
                    final List<Tuple2<String, Shard>> countryShards = new ArrayList<>();
                    shards.forEach(shard -> countryShards.add(new Tuple2<>(countryName, shard)));
                    return countryShards;
                }).collect();

        // Get a RDD of country shards without any false positives
        // TODO Leverage country boundary map grid index when ready
        final JavaPairRDD<String, Shard> preCountryShardsRDD = getContext()
                .parallelizePairs(roughCountryShards, roughCountryShards.size()).filter(tuple ->
                {
                    final String countryName = tuple._1();
                    final Shard shard = tuple._2();
                    final List<CountryBoundary> boundaries = worldBoundaries
                            .countryBoundary(countryName);
                    return filterShards(shard, boundaries);
                });

        // Collect and re-parallelize so the code below is as parallel as there are countries/shard
        // pairs (many more!)...
        final List<Tuple2<String, Shard>> countryShards = preCountryShardsRDD.collect();
        // Keep it as a MultiMap to pass it to the Atlas meta data.
        final MultiMap<String, Shard> countryToShardMap = new MultiMap<>();
        countryShards.forEach(tuple -> countryToShardMap.add(tuple._1(), tuple._2()));
        final JavaPairRDD<String, Shard> countryShardsRDD = getContext()
                .parallelizePairs(countryShards, countryShards.size());

        // Transform the map country name to shard to country name to Atlas
        // This is not enforced, but it has to be a 1-1 mapping here.
        final Map<String, String> configurationMap = configurationMap();
        final JavaPairRDD<String, Atlas> countryAtlasShardsRDD = countryShardsRDD.mapToPair(tuple ->
        {
            // Get the country name
            final String countryName = tuple._1();
            // Get the country shard
            final Shard shard = tuple._2();
            // Build the AtlasLoadingOption
            final AtlasLoadingOption atlasLoadingOption = AtlasLoadingOption
                    .createOptionWithAllEnabled(worldBoundaries)
                    .setAdditionalCountryCodes(countryName);
            // Create a local hadoop configuration to avoid serializing the whole class
            final Configuration hadoopConfiguration = new Configuration();
            configurationMap.entrySet()
                    .forEach(entry -> hadoopConfiguration.set(entry.getKey(), entry.getValue()));

            // Apply all configurations
            if (edgeConfiguration != null)
            {
                atlasLoadingOption.setEdgeFilter(
                        getTaggableFilterFrom(edgeConfiguration, hadoopConfiguration));
            }
            if (waySectioningConfiguration != null)
            {
                atlasLoadingOption.setWaySectionFilter(
                        getTaggableFilterFrom(waySectioningConfiguration, hadoopConfiguration));
            }
            if (pbfNodeConfiguration != null)
            {
                atlasLoadingOption.setOsmPbfNodeFilter(
                        getTaggableFilterFrom(pbfNodeConfiguration, hadoopConfiguration));
            }
            if (pbfWayConfiguration != null)
            {
                atlasLoadingOption.setOsmPbfWayFilter(
                        getTaggableFilterFrom(pbfWayConfiguration, hadoopConfiguration));
            }
            if (pbfRelationConfiguration != null)
            {
                atlasLoadingOption.setOsmPbfRelationFilter(
                        getTaggableFilterFrom(pbfRelationConfiguration, hadoopConfiguration));
            }

            // Build the appropriate PbfLoader
            final PbfContext pbfContext = new PbfContext(pbfPath, sharding);
            final PbfLoader loader = new PbfLoader(pbfContext, sparkContext, worldBoundaries,
                    atlasLoadingOption, codeVersion, dataVersion, countryToShardMap);
            final String name = countryName + CountryShard.COUNTRY_SHARD_SEPARATOR
                    + shard.getName();
            final Atlas atlas;
            try
            {
                // Generate the Atlas for this shard
                atlas = loader.load(countryName, shard);
            }
            catch (final Throwable e)
            {
                throw new CoreException("Building Atlas {} failed!", name, e);
            }

            // Report on memory usage
            logger.info("Printing memory after loading Atlas {}", name);
            Memory.printCurrentMemory();
            // Output the Name/Atlas couple
            final Tuple2<String, Atlas> result = new Tuple2<>(name, atlas);
            return result;
        });

        // Filter out null Atlas.
        final JavaPairRDD<String, Atlas> countryNonNullAtlasShardsRDD = countryAtlasShardsRDD
                .filter(tuple -> tuple._2() != null);

        // Cache the Atlas
        countryNonNullAtlasShardsRDD.cache();
        logger.info("\n\n********** CACHED THE ATLAS **********\n");

        // Run the metrics
        final JavaPairRDD<String, AtlasStatistics> statisticsRDD = countryNonNullAtlasShardsRDD
                .mapToPair(tuple ->
                {
                    final Counter counter = new Counter().withSharding(sharding);
                    counter.setCountsDefinition(Counter.POI_COUNTS_DEFINITION.getDefault());
                    final AtlasStatistics statistics;
                    try
                    {
                        statistics = counter.processAtlas(tuple._2());
                    }
                    catch (final Exception e)
                    {
                        throw new CoreException("Building Atlas Statistics for {} failed!",
                                tuple._1(), e);
                    }
                    final Tuple2<String, AtlasStatistics> result = new Tuple2<>(tuple._1(),
                            statistics);
                    return result;
                });

        // Cache the statistics
        statisticsRDD.cache();
        logger.info("\n\n********** CACHED THE SHARD STATISTICS **********\n");

        // Save the metrics
        // splitAndSaveAsHadoopFile(statisticsRDD,
        // getAlternateParallelFolderOutput(output, SHARD_STATISTICS_FOLDER),
        // AtlasStatistics.class, MultipleAtlasStatisticsOutputFormat.class,
        // new CountrySplitter());
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

        // Save the aggregated metrics
        // reducedStatisticsRDD.saveAsHadoopFile(
        // getAlternateSubFolderOutput(output, COUNTRY_STATISTICS_FOLDER), Text.class,
        // AtlasStatistics.class, MultipleAtlasStatisticsOutputFormat.class,
        // new JobConf(configuration()));
        reducedStatisticsRDD.saveAsHadoopFile(
                getAlternateSubFolderOutput(output, COUNTRY_STATISTICS_FOLDER), Text.class,
                AtlasStatistics.class, MultipleAtlasStatisticsOutputFormat.class,
                new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE COUNTRY STATISTICS **********\n");

        // Compute the deltas if needed
        if (!previousOutputForDelta.isEmpty())
        {
            // Compute the deltas
            final JavaPairRDD<String, AtlasDelta> deltasRDD = countryNonNullAtlasShardsRDD
                    .flatMapToPair(tuple ->
                    {
                        final String countryShardName = tuple._1();
                        final Atlas current = tuple._2();
                        final List<Tuple2<String, AtlasDelta>> result = new ArrayList<>();
                        try
                        {
                            final Optional<Atlas> alter = new AtlasLocator(sparkContext)
                                    .atlasForShard(previousOutputForDelta + "/"
                                            + StringList
                                                    .split(countryShardName,
                                                            CountryShard.COUNTRY_SHARD_SEPARATOR)
                                                    .get(0),
                                            countryShardName);
                            if (alter.isPresent())
                            {
                                logger.info("Printing memory after other Atlas loaded for Delta {}",
                                        countryShardName);
                                Memory.printCurrentMemory();
                                final AtlasDelta delta = new AtlasDelta(current, alter.get())
                                        .generate();
                                result.add(new Tuple2<>(countryShardName, delta));
                            }
                        }
                        catch (final Exception e)
                        {
                            logger.error("Skipping! Could not generate deltas for {}",
                                    countryShardName, e);
                        }
                        return result;
                    });

            // deltasRDD.cache();
            // logger.info("\n\n********** CACHED THE DELTAS **********\n");

            // Save the deltas
            // splitAndSaveAsHadoopFile(deltasRDD, getAlternateOutput(output, SHARD_DELTAS_FOLDER),
            // AtlasDelta.class, MultipleAtlasDeltaOutputFormat.class,
            // COUNTRY_NAME_FROM_COUNTRY_SHARD);
            // deltasRDD.saveAsHadoopFile(getAlternateOutput(output, SHARD_DELTAS_FOLDER),
            // Text.class,
            // AtlasDelta.class, MultipleAtlasDeltaOutputFormat.class,
            // new JobConf(configuration()));
            // logger.info("\n\n********** SAVED THE DELTAS **********\n");
            //
            // splitAndSaveAsHadoopFile(deltasRDD,
            // getAlternateOutput(output, SHARD_DELTAS_ADDED_FOLDER), AtlasDelta.class,
            // AddedMultipleAtlasDeltaOutputFormat.class, COUNTRY_NAME_FROM_COUNTRY_SHARD);
            // deltasRDD.saveAsHadoopFile(getAlternateOutput(output, SHARD_DELTAS_ADDED_FOLDER),
            // Text.class, AtlasDelta.class, AddedMultipleAtlasDeltaOutputFormat.class,
            // new JobConf(configuration()));
            // logger.info("\n\n********** SAVED THE DELTAS ADDED **********\n");
            //
            // splitAndSaveAsHadoopFile(deltasRDD,
            // getAlternateOutput(output, SHARD_DELTAS_CHANGED_FOLDER), AtlasDelta.class,
            // ChangedMultipleAtlasDeltaOutputFormat.class, COUNTRY_NAME_FROM_COUNTRY_SHARD);
            // deltasRDD.saveAsHadoopFile(getAlternateOutput(output, SHARD_DELTAS_CHANGED_FOLDER),
            // Text.class, AtlasDelta.class, ChangedMultipleAtlasDeltaOutputFormat.class,
            // new JobConf(configuration()));
            // logger.info("\n\n********** SAVED THE DELTAS CHANGED **********\n");

            // splitAndSaveAsHadoopFile(deltasRDD,
            // getAlternateParallelFolderOutput(output, SHARD_DELTAS_REMOVED_FOLDER),
            // AtlasDelta.class, RemovedMultipleAtlasDeltaOutputFormat.class,
            // new CountrySplitter());
            deltasRDD.saveAsHadoopFile(
                    getAlternateSubFolderOutput(output, SHARD_DELTAS_REMOVED_FOLDER), Text.class,
                    AtlasDelta.class, RemovedMultipleAtlasDeltaOutputFormat.class,
                    new JobConf(configuration()));
            logger.info("\n\n********** SAVED THE DELTAS REMOVED **********\n");
        }

        // Save the result as Atlas files, one for each key.
        // splitAndSaveAsHadoopFile(countryNonNullAtlasShardsRDD,
        // getAlternateParallelFolderOutput(output, ATLAS_FOLDER), Atlas.class,
        // MultipleAtlasOutputFormat.class, new CountrySplitter());
        countryNonNullAtlasShardsRDD.saveAsHadoopFile(
                getAlternateSubFolderOutput(output, ATLAS_FOLDER), Text.class, Atlas.class,
                MultipleAtlasOutputFormat.class, new JobConf(configuration()));
        logger.info("\n\n********** SAVED THE ATLAS **********\n");
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
        return super.switches().with(COUNTRIES, COUNTRY_SHAPES, SHARDING_TYPE, PBFS_URL,
                PREVIOUS_OUTPUT_FOR_DELTA, CODE_VERSION, DATA_VERSION, EDGE_CONFIGURATION,
                WAY_SECTIONING_CONFIGURATION, PBF_NODE_CONFIGURATION, PBF_WAY_CONFIGURATION,
                PBF_RELATION_CONFIGURATION);
    }
}
