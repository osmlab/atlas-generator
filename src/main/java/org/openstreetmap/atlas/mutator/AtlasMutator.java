package org.openstreetmap.atlas.mutator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.MultipleAtlasOutputFormat;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.mutator.persistence.FeatureChangeOutputFormat;
import org.openstreetmap.atlas.mutator.persistence.MultipleFeatureChangeOutputFormat;
import org.openstreetmap.atlas.streaming.Streams;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.OutputStreamWritableResource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.maps.MultiMap;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.openstreetmap.atlas.utilities.runtime.Retry;
import org.openstreetmap.atlas.utilities.scalars.Duration;
import org.openstreetmap.atlas.utilities.threads.Pool;
import org.openstreetmap.atlas.utilities.threads.Result;
import org.openstreetmap.atlas.utilities.time.Time;
import org.openstreetmap.atlas.utilities.tuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * {@link SparkJob} that applies mutations to Atlas shards, following a specific configuration and
 * Mutator DAG for each country.
 *
 * @author matthieun
 */
public class AtlasMutator extends SparkJob
{
    public static final String COUNTRY_AND_LEVELS = "countryAndLevels.txt";
    public static final String POTENTIAL_LOG_FILES_PATH = "potentialLogFiles";
    public static final String POTENTIAL_LOG_FILES_NAME = FileSuffix.TEXT.toString()
            + FileSuffix.GZIP.toString();
    public static final String MUTATOR_META_DATA_KEY = "mutator";
    public static final String MUTATOR_META_DATA_SPLIT = ":";
    public static final String LOG_FOLDER = "logs";
    public static final String LOG_GENERATED = "generated";
    public static final String LOG_ASSIGNED = "assigned";
    public static final String LOG_APPLIED = "applied";

    protected static final String LEVEL_MESSAGE = "\n\n********** {} **********\n";

    private static final long serialVersionUID = -7622283643442835682L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasMutator.class);
    private static final int SHARD_BUILDER_THREADS = 10;
    private static final int WRITE_RETRIES = 5;
    // RDDs are expensive to compute here. Allow spill to disk.
    // Re-computing a stage here should be last resort.
    private static final StorageLevel STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK_SER();

    private transient AtlasMutatorConfiguration atlasMutatorConfiguration;
    private transient Map<String, List<AtlasMutationLevel>> countryGroupMutations;
    private transient Map<String, List<CountryShard>> countryGroupShards;
    private transient AtlasMutatorParameters.DebugFeatureChangeOutput debugFeatureChangeOutput;
    private transient int startLevel = 0;
    private transient int stopLevel = Integer.MAX_VALUE;
    private final transient Set<String> debugShards = new HashSet<>();
    private transient Sharding sharding;
    private transient String outputFolder;
    private transient boolean appliedRDDPersistUseSer;

    public static void main(final String[] args)
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
        new AtlasMutator().runWithoutQuitting(args);
    }

    @Override
    public String getName()
    {
        return "Atlas Mutator";
    }

    @SuppressWarnings("unchecked")
    @Override
    public int onRun(final CommandMap command)
    {
        // Here force the spark.hadoop.validateOutputSpecs to be false since it writes many times to
        // the same output folder
        Map<String, String> additionalMap = new HashMap<>();
        if (command.containsKey(SparkJob.ADDITIONAL_SPARK_OPTIONS.getName()))
        {
            additionalMap = (Map<String, String>) command
                    .get(SparkJob.ADDITIONAL_SPARK_OPTIONS.getName());
        }
        additionalMap.put("spark.hadoop.validateOutputSpecs", "false");
        command.put(SparkJob.ADDITIONAL_SPARK_OPTIONS.getName(), additionalMap);
        return super.onRun(command);
    }

    @Override
    public void start(final CommandMap command)
    {
        this.atlasMutatorConfiguration = AtlasMutatorParameters.atlasMutatorConfiguration(command,
                configurationMap());
        this.outputFolder = AtlasMutatorParameters.output(command);
        if (this.countryGroupMutations == null)
        {
            this.countryGroupMutations = this.atlasMutatorConfiguration
                    .getCountryToMutationLevels();
        }
        if (this.countryGroupMutations.isEmpty())
        {
            throw new CoreException(
                    "There was nothing to run! This is usually a configuration issue.");
        }
        if (this.countryGroupShards == null)
        {
            this.countryGroupShards = new ConcurrentHashMap<>();
        }
        this.debugFeatureChangeOutput = AtlasMutatorParameters.debugFeatureChangeOutput(command);
        this.startLevel = AtlasMutatorParameters.startLevel(command);
        this.stopLevel = AtlasMutatorParameters.stopLevel(command);
        this.sharding = AtlasMutatorParameters.sharding(command, configurationMap());
        this.appliedRDDPersistUseSer = AtlasMutatorParameters.isAppliedRDDPersistUseSer(command);

        AtlasMutatorParameters.debugShards(command).forEach(includeListed ->
        {
            this.debugShards.add(includeListed);
            this.countryGroupMutations.values().forEach(levelList -> levelList
                    .forEach(level -> level.addDebugWhiteListedShard(includeListed)));
        });
        AtlasMutatorParameters.debugMutations(command).forEach(includeListed -> amendLevels(
                level -> level.addDebugWhiteListedMutator(includeListed)));
        final boolean addMutationTags = AtlasMutatorParameters.isAddMutationTags(command);
        amendLevels(level -> level.setAddMutationTags(addMutationTags));
        this.saveCountryAndLevels();
        createBroadcastVariables();
        execute(command);
        this.copyToOutput(command, AtlasMutatorParameters.input(command),
                AtlasMutatorParameters.output(command));
        logger.info("Done!");
    }

    @Override
    public SwitchList switches()
    {
        final SwitchList result = super.switches();
        result.addAll(AtlasMutatorParameters.switches());
        return result;
    }

    private void amendLevels(final Consumer<AtlasMutationLevel> levelConsumer)
    {
        this.countryGroupMutations.values().forEach(levelList -> levelList.forEach(levelConsumer));
    }

    /**
     * Asynchronously build the task lists for all the countries. This is to allow smaller countries
     * to start while bigger countries take a while to figure out what shards need to be processed.
     *
     * @return The thread pool used, to be closed later by the method consuming the output.
     */
    private Pool buildShards()
    {
        // No sonar here: The pool is meant to be closed by another private method.
        final Pool pool = new Pool(SHARD_BUILDER_THREADS, "shards-builder"); // NOSONAR

        for (final Entry<String, List<AtlasMutationLevel>> entry : this.countryGroupMutations
                .entrySet())
        {
            if (this.debugShards.isEmpty())
            {
                logger.info("Submitting {} to initial shards discovery step.", entry.getKey());
                // Here, this.countryShards has to be thread safe
                pool.queue(() ->
                {
                    final List<CountryShard> countryShards = Iterables
                            .asList(entry.getValue().get(0).shards());
                    this.countryGroupShards.put(entry.getKey(), countryShards);
                    savePotentialLogFiles(entry.getValue(), countryShards);
                });
            }
            else
            {
                final List<CountryShard> countryShards = this.debugShards.stream()
                        .map(this.sharding::shardForName)
                        .flatMap(shard -> entry.getValue().iterator().next().getCountries().stream()
                                .map(country -> new CountryShard(country, shard)))
                        .collect(Collectors.toList());
                this.countryGroupShards.put(entry.getKey(), countryShards);
                savePotentialLogFiles(entry.getValue(), countryShards);
            }
        }
        return pool;
    }

    private void createBroadcastVariables()
    {
        final Map<String, Broadcast<?>> broadcastVariables = new HashMap<>();
        this.countryGroupMutations.values()
                .forEach(levelList -> levelList.forEach(level -> level.getMutators().forEach(
                        mutator -> mutator.getBroadcastVariablesNeeded().forEach((name, value) ->
                        {
                            if (!broadcastVariables.containsKey(name))
                            {
                                broadcastVariables.put(name,
                                        getContext().broadcast(value.read(configurationMap())));
                            }
                        }))));
        amendLevels(level -> broadcastVariables.forEach(level::addBroadcastVariable));
    }

    /**
     * Run every country's job and collect the final RDDs that contain the aggregated final results
     * of each country. Those final RDDs are aggregated, where all the country's results are merged
     * into one single RDD. Similarly, all the country's feature changes (when debug enabled) are
     * all aggregated into one single RDD.
     *
     * @param command
     *            The command details
     */
    private void execute(final CommandMap command)
    {
        final int numberCountryGroups = this.countryGroupMutations.size();
        final Map<String, Boolean> countryGroupsProcessed = new ConcurrentHashMap<>();
        final Map<String, Boolean> countryGroupsAvailable = new ConcurrentHashMap<>();
        final int countryConcurrency = AtlasMutatorParameters.countryConcurrency(command);
        final Duration submissionStaggering = AtlasMutatorParameters.submissionStaggering(command);
        try (Pool buildShards = buildShards();
                Pool pool = new Pool(countryConcurrency, "country-dag");
                Pool geoJsonPool = new Pool(countryConcurrency, "save-ldgeojson");
                Pool awaitResultsPool = new Pool(countryConcurrency, "await-results"))
        {
            final Map<String, Result<Boolean>> results = new HashMap<>();
            while (countryGroupsProcessed.size() < numberCountryGroups)
            {
                if (countryGroupsAvailable.size() > 0)
                {
                    countryGroupsAvailable.keySet().forEach(countryGroup ->
                    {
                        if (submissionStaggering.isMoreThan(Duration.ZERO))
                        {
                            logger.info("Staggering country {} submission to the driver by {}.",
                                    countryGroup, submissionStaggering);
                            submissionStaggering.sleep();
                        }
                        results.put(countryGroup,
                                pool.queue(mutateCountryGroup(
                                        this.countryGroupMutations.get(countryGroup),
                                        this.countryGroupShards.get(countryGroup), geoJsonPool)));
                    });
                    countryGroupsProcessed.putAll(countryGroupsAvailable);
                    countryGroupsAvailable.clear();
                }
                this.countryGroupShards.keySet().stream()
                        .filter(country -> !countryGroupsProcessed.containsKey(country))
                        .forEach(country ->
                        {
                            logger.info("Found new country shards: {}", country);
                            countryGroupsAvailable.put(country, true);
                        });
                Duration.ONE_SECOND.sleep();
            }
            results.forEach((countryGroup, result) -> awaitResultsPool.queue(() ->
            {
                try
                {
                    result.get();
                    logger.info("Finished Mutator DAG for {}", countryGroup);
                }
                catch (final Exception e)
                {
                    throw new CoreException("Unable to process country {}", countryGroup, e);
                }
            }));
        }
    }

    private String getLogFileName(final AtlasMutationLevel level, final String type)
    {
        return "-" + level.getLevelIndex() + "-" + type;
    }

    /**
     * @param levels
     *            All the levels to mutate a country (group)
     * @param shards
     *            All the shards to process
     * @return A function that processes all the mutation levels for a specific country, in order.
     */
    private Callable<Boolean> mutateCountryGroup(final List<AtlasMutationLevel> levels,
            final List<CountryShard> shards, final Pool geoJsonPool)
    {
        return () ->
        {
            final Time start = Time.now();
            logger.info("Starting Spark Job for country {}", levels.get(0).getCountryGroup());
            if (this.startLevel > 0 && this.startLevel >= levels.size())
            {
                throw new CoreException(
                        "Debug Skip Level specified to {} cannot be applied to {} which has only {} levels.",
                        this.startLevel, levels.get(0).getCountryGroup(), levels.size());
            }
            if (this.stopLevel < this.startLevel)
            {
                throw new CoreException(
                        "Debug Stop Level specified to {} cannot be smaller than the Debug Skip Level {}.",
                        this.stopLevel, this.startLevel);
            }
            JavaPairRDD<CountryShard, PackedAtlas> currentAtlasRDD = null;

            // Do the other levels
            for (int index = this.startLevel; index <= Math.min(levels.size() - 1,
                    this.stopLevel); index++)
            {
                final Time startLocal = Time.now();
                final AtlasMutationLevel level = levels.get(index);
                if (logger.isInfoEnabled())
                {
                    logger.info(LEVEL_MESSAGE.replace("{}", "Starting Level {}"), level);
                }
                currentAtlasRDD = runMutationLevel(level, shards, currentAtlasRDD, geoJsonPool);
                if (logger.isInfoEnabled())
                {
                    logger.info(LEVEL_MESSAGE.replace("{}", "Finished Level {} in {}"), level,
                            startLocal.elapsedSince());
                }
            }
            logger.info("Finished Spark Job for country {} in {}", levels.get(0).getCountryGroup(),
                    start.elapsedSince());
            return true;
        };
    }

    /**
     * This method runs a mutation level as a single independent Spark DAG of execution. It is
     * specific to a country and to a single level of execution (All the mutations that can happen
     * in parallel and do not conflict)
     *
     * @param level
     *            The level configuration to execute the mutation
     * @param shards
     *            The pre-computed list of shards for this level.
     * @param parentAtlasRDD
     *            The atlas RDD from the previous level (Can be null if this level is not RDD Based)
     * @param geoJsonPool
     *            The shared thread pool for saving the geojson logs (this function uses it but does
     *            not close it)
     */
    private JavaPairRDD<CountryShard, PackedAtlas> runMutationLevel(final AtlasMutationLevel level, // NOSONAR
            final List<CountryShard> shards,
            final JavaPairRDD<CountryShard, PackedAtlas> parentAtlasRDD, final Pool geoJsonPool)
    {
        if (shards.isEmpty())
        {
            logger.warn("{}: No shards to process!", level);
            return getContext().parallelizePairs(new ArrayList<>(), 0);
        }

        JavaRDD<CountryShard> shardsRDD = null;
        final int size = shards.size();
        JavaPairRDD<CountryShard, PackedAtlas> sourceAtlasRDD = null;
        // 1. Figure out where to source the origin Atlas files from. Any level that can source
        // Atlas from an existing RDD will do that.
        if (parentAtlasRDD != null)
        {
            // 1.1 If the parent level saved the output RDD to memory, use it
            logger.info("{} using AtlasRDD from previous level directly.", level);
            sourceAtlasRDD = parentAtlasRDD;
        }
        else if (level.canPreloadAtlasRDD())
        {
            // 1.2 If the parent level saved the output RDD to the output fileSystem, retrieve
            // it from there at once. This RDD will contain null values in case an Atlas is not
            // found.
            sourceAtlasRDD = getContext().parallelize(shards, size)
                    .flatMapToPair(AtlasMutatorHelper.shardToAtlas(level));
            sourceAtlasRDD.setName(level + ": sourceAtlasRDD");
            sourceAtlasRDD.persist(STORAGE_LEVEL);
        }
        else
        {
            // 1.3 A level that cannot source Atlas from an existing RDD will have to parallelize
            // the shards only, and go from there. Individual tasks downstream will have to source
            // the Atlas files from the FileSystem, on demand.
            shardsRDD = getContext().parallelize(shards, size);
            shardsRDD.setName(level + ": shardsRDD");
            shardsRDD.persist(STORAGE_LEVEL);
        }

        // 2. Get all the feature changes for a shard and filter out the ones that are too far from
        // the shard (dynamicAtlas side effects)
        final JavaPairRDD<CountryShard, List<FeatureChange>> shardFeatureChangesRDD;
        if (sourceAtlasRDD != null)
        {
            // 2.1 Re-organize the atlas RDD to get the expansion shards and Atlases
            final JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> atlasGroupsRDDGeneration = AtlasMutatorDriver
                    .getAtlasGroupsRDD(level.toString() + ": Generate", size, sourceAtlasRDD,
                            level.getGenerationShardExplorer(),
                            level.getGenerationInputDependencyToRequest());
            atlasGroupsRDDGeneration.setName(level + ": atlasGroupsRDDGeneration");
            // 2.2 Generate the FeatureChanges
            shardFeatureChangesRDD = atlasGroupsRDDGeneration
                    .flatMapToPair(AtlasMutatorHelper.shardToAtlasMapToFeatureChanges(level));
        }
        else
        {
            shardFeatureChangesRDD = shardsRDD
                    .flatMapToPair(AtlasMutatorHelper.shardToFeatureChanges(level));
        }
        shardFeatureChangesRDD.setName(level + ": shardFeatureChangesRDD");

        if (shouldSaveGenerated())
        {
            shardFeatureChangesRDD.persist(STORAGE_LEVEL);
        }

        // 3. Assign feature changes to the shards where the change can be applied
        final JavaPairRDD<CountryShard, List<FeatureChange>> shardAssignedFeatureChangesRDD = shardFeatureChangesRDD
                .flatMapToPair(
                        AtlasMutatorHelper.shardFeatureChangesToAssignedShardFeatureChanges(level))
                // concatenate in a fat list
                .reduceByKey(AtlasMutatorHelper.assignedToConcatenatedFeatureChanges(level));
        shardAssignedFeatureChangesRDD.setName(level + ": shardAssignedFeatureChangesRDD");

        if (shouldSaveAssigned())
        {
            shardAssignedFeatureChangesRDD.persist(STORAGE_LEVEL);
        }

        // 4. Merge all feature changes of each shard into a single list (and resolve conflicts)
        final JavaPairRDD<CountryShard, List<FeatureChange>> shardAppliedFeatureChangesRDD = shardAssignedFeatureChangesRDD
                // Trim by merging colliding featureChanges
                .mapToPair(AtlasMutatorHelper.assignedToShardAppliedFeatureChanges(level));
        shardAppliedFeatureChangesRDD.setName(level + ": shardAppliedFeatureChangesRDD");
        if (this.appliedRDDPersistUseSer)
        {
            shardAppliedFeatureChangesRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
        }
        else
        {
            shardAppliedFeatureChangesRDD.persist(StorageLevel.MEMORY_AND_DISK());
        }
        final String descriptionMerging = level + ": FC Generate, Assign, Merge";
        this.getContext().setJobGroup(level.toString(), descriptionMerging);
        final Set<CountryShard> shardsForApplication = shardsFrom(shardAppliedFeatureChangesRDD);
        logger.info("{}: Found {} shards with feature changes to apply to.", level,
                shardsForApplication.size());

        // 4.1 Repartition (Each task has only one shard to list pair)
        final JavaPairRDD<CountryShard, List<FeatureChange>> shardAppliedRepartitionedFeatureChangesRDD = shardAppliedFeatureChangesRDD
                .repartitionAndSortWithinPartitions(
                        AtlasMutatorParameters.shardPartitioner(shardsForApplication),
                        AtlasMutatorParameters.shardComparator());
        shardAppliedRepartitionedFeatureChangesRDD
                .setName(level + ": shardAppliedRepartitionedFeatureChangesRDD");

        // 4.2 Coalesce (Remove empty tasks)
        final int partitionSize = shardsForApplication.size();
        final JavaPairRDD<CountryShard, List<FeatureChange>> shardAppliedCoalescedFeatureChangesRDD = partitionSize == 0
                ? shardAppliedRepartitionedFeatureChangesRDD
                : shardAppliedRepartitionedFeatureChangesRDD.coalesce(partitionSize);
        shardAppliedCoalescedFeatureChangesRDD
                .setName(level + ": shardAppliedCoalescedFeatureChangesRDD");

        // 5. Make a Change with the List of FeatureChanges
        final JavaPairRDD<CountryShard, Change> shardChangeRDD = shardAppliedCoalescedFeatureChangesRDD
                .mapToPair(AtlasMutatorHelper.featureChangeListToChange(level));
        shardChangeRDD.setName(level + ": shardChangeRDD");

        // 6. Apply the change to the proper DynamicAtlas
        final JavaPairRDD<CountryShard, Tuple<PackedAtlas, Change>> changedAtlasRDD;
        if (sourceAtlasRDD != null)
        {
            // 6.1 Re-organize the atlas RDD to get the expansion shards and Atlases
            final JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> atlasGroupsRDDApplication = AtlasMutatorDriver
                    .getAtlasGroupsRDD(level.toString() + ": Apply", size, sourceAtlasRDD,
                            level.getApplicationShardExplorer(),
                            level.getApplicationInputDependencyToRequest());
            atlasGroupsRDDApplication.setName(level + ": atlasGroupsRDDApplication");
            // 6.2 Merge with the shardChangeRDD. Use leftOuterJoin here to make sure that even if
            // we have a Change for a shard that does not exist in sourceAtlasRDD, we do not lose
            // it.
            final JavaPairRDD<CountryShard, Tuple2<Change, org.apache.spark.api.java.Optional<Map<CountryShard, PackedAtlas>>>> changeAndShardToAtlasMapRDD = shardChangeRDD
                    .leftOuterJoin(atlasGroupsRDDApplication);
            changeAndShardToAtlasMapRDD.setName(level + ": changeAndShardToAtlasMapRDD");
            // 6.3 Apply
            changedAtlasRDD = changeAndShardToAtlasMapRDD
                    .flatMapToPair(AtlasMutatorHelper.changeAndShardToAtlasMapToAtlas(level));
        }
        else
        {
            changedAtlasRDD = shardChangeRDD.flatMapToPair(AtlasMutatorHelper.changeToAtlas(level));
        }

        // 7. Backfill all the shards that have no Change applied
        changedAtlasRDD.setName(level + ": changedAtlasRDD");
        changedAtlasRDD.persist(STORAGE_LEVEL);
        final String descriptionChangeAtlas = level + ": Build ChangeAtlas";
        this.getContext().setJobGroup(level.toString(), descriptionChangeAtlas);
        final Set<CountryShard> populatedShards = shardsFrom(changedAtlasRDD);
        shardAppliedFeatureChangesRDD.unpersist(false);

        final JavaPairRDD<CountryShard, PackedAtlas> unChangedAtlasRDD;
        if (sourceAtlasRDD != null)
        {
            final JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> atlasGroupsRDDBackfill = AtlasMutatorDriver
                    .getAtlasGroupsRDDSimple(level.toString() + ": Backfill", sourceAtlasRDD);
            atlasGroupsRDDBackfill.setName(level + ": atlasGroupsRDDBackfill");
            unChangedAtlasRDD = atlasGroupsRDDBackfill
                    .filter(tuple -> !populatedShards.contains(tuple._1()))
                    .flatMapToPair(AtlasMutatorHelper
                            .untouchedShardAndMapToPotentialSourcePackedAtlas(level));
        }
        else
        {
            unChangedAtlasRDD = shardsRDD.filter(shard -> !populatedShards.contains(shard))
                    .flatMapToPair(
                            AtlasMutatorHelper.untouchedShardToPotentialSourcePackedAtlas(level));
        }
        unChangedAtlasRDD.setName(level + ": unChangedAtlasRDD");
        final JavaPairRDD<CountryShard, PackedAtlas> resultAtlasRDD = changedAtlasRDD
                // First get rid of the change at this point
                .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2().getFirst()))
                .union(unChangedAtlasRDD).coalesce(size);
        resultAtlasRDD.setName(level + ": resultAtlasRDD");
        if (level.isChildNeedsRDDInput() || shouldSaveInputDependency(level))
        {
            resultAtlasRDD.persist(STORAGE_LEVEL);
        }

        // Save stuff

        // 2.1 Asynchronously save the generated LDGeojson feature changes
        saveFeatureChangesMaybe(level, LOG_GENERATED, shardFeatureChangesRDD, geoJsonPool);

        // 3.1 Asynchronously save the assigned LDGeojson feature changes
        saveFeatureChangesMaybe(level, LOG_ASSIGNED, shardAssignedFeatureChangesRDD, geoJsonPool);

        // 4.3 Asynchronously save the applied LDGeojson feature changes
        saveFeatureChangesAppliedMaybe(level, changedAtlasRDD, geoJsonPool);

        // 8. Choose what to do with the cloned ChangeAtlas
        if (level.isChildNeedsRDDInput())
        {
            // 8.1 If the next level will take the RDD, don't save. Use count() here as a trigger to
            // compute the RDD (which will be cached).
            resultAtlasRDD.count();
        }
        else
        {
            // 8.2 If the next level does not expect an AtlasRDD to be passed in, save it to output
            saveAsHadoopAtlas(resultAtlasRDD, level);
        }

        // 9. Save any potential input dependencies to provide
        if (!level.isChildNeedsRDDInput() && !level.isChildCanPreloadRDDInput())
        {
            // Here if the child can read RDD input, it will either load its Atlas RDD itself from
            // the main source folder, or get it directly from this level's output, and then do its
            // own filtering, so this below is not needed.
            for (final InputDependency inputDependency : level.getInputDependenciesToProvide())
            {
                saveAsHadoopAtlas(resultAtlasRDD, level, Optional.of(inputDependency));
            }
        }

        // 10. Cleanup
        shardFeatureChangesRDD.unpersist(false);
        shardAssignedFeatureChangesRDD.unpersist(false);
        changedAtlasRDD.unpersist(false);
        if (sourceAtlasRDD != null)
        {
            sourceAtlasRDD.unpersist(false);
        }
        else
        {
            shardsRDD.unpersist(false);
        }

        if (level.isChildNeedsRDDInput())
        {
            return resultAtlasRDD;
        }
        else
        {
            resultAtlasRDD.unpersist(false);
            return null;
        }
    }

    /**
     * Save a shard-based Atlas RDD
     *
     * @param atlasRDD
     *            The RDD
     * @param level
     *            The level at which that RDD was generated
     */
    private void saveAsHadoopAtlas(final JavaPairRDD<CountryShard, PackedAtlas> atlasRDD,
            final AtlasMutationLevel level)
    {
        saveAsHadoopAtlas(atlasRDD, level, Optional.empty());
    }

    /**
     * Save a shard-based Atlas RDD
     *
     * @param atlasRDD
     *            The RDD
     * @param level
     *            The level at which that RDD was generated
     * @param inputDependencyOption
     *            An optional {@link InputDependency} that defines how each Atlas in that RDD needs
     *            to be filtered prior to saving.
     */
    private void saveAsHadoopAtlas(final JavaPairRDD<CountryShard, PackedAtlas> atlasRDD,
            final AtlasMutationLevel level, final Optional<InputDependency> inputDependencyOption)
    {
        final String description;
        final String outputAtlasPath;
        final JavaPairRDD<CountryShard, PackedAtlas> finalAtlasRDD;

        if (inputDependencyOption.isPresent())
        {
            final InputDependency inputDependency = inputDependencyOption.get();
            finalAtlasRDD = atlasRDD.flatMapToPair(
                    AtlasMutatorHelper.inputDependencyFilteredAtlas(level, inputDependency));
            description = level + ": Save Filtered Mutated Atlas: " + inputDependency;
            outputAtlasPath = level.getOutputAtlasPath(inputDependency);
        }
        else
        {
            finalAtlasRDD = atlasRDD;
            description = level + ": Save Mutated Atlas";
            outputAtlasPath = level.getOutputAtlasPath();
        }
        this.getContext().setJobGroup(level.toString(), description);
        // Make it a JavaPairRDD<String, Atlas> first to be able to use
        // MultipleAtlasOutputFormat, then save it
        saveAsHadoopAtlas(AtlasMutatorHelper.embedCountryNameInKey("", finalAtlasRDD),
                outputAtlasPath);
    }

    /**
     * Save a (country name + shard)-based RDD
     *
     * @param atlasWithCountryNameEmbeddedRDD
     *            The RDD
     * @param outputAtlasPath
     *            The location at which to save that RDD
     */
    private void saveAsHadoopAtlas(
            final JavaPairRDD<String, PackedAtlas> atlasWithCountryNameEmbeddedRDD,
            final String outputAtlasPath)
    {
        atlasWithCountryNameEmbeddedRDD.saveAsHadoopFile(outputAtlasPath, Text.class, Atlas.class,
                MultipleAtlasOutputFormat.class, new JobConf(configuration()));
    }

    /**
     * Save a (country name + shard)-based RDD
     *
     * @param featureChangesWithCountryNameEmbeddedRDD
     *            The RDD
     * @param outputFeatureChangePath
     *            The location at which to save that RDD
     */
    private void saveAsHadoopFeatureChanges(
            final JavaPairRDD<String, List<FeatureChange>> featureChangesWithCountryNameEmbeddedRDD,
            final String outputFeatureChangePath)
    {
        featureChangesWithCountryNameEmbeddedRDD.saveAsHadoopFile(outputFeatureChangePath,
                Text.class, Atlas.class, MultipleFeatureChangeOutputFormat.class,
                new JobConf(configuration()));
    }

    private void saveAsHadoopFeatureChanges(final String type,
            final JavaPairRDD<CountryShard, List<FeatureChange>> featureChangeRDD,
            final AtlasMutationLevel level)
    {
        final String descriptionLineDelimitedGeoJson = level + ": Save LDGeojson - " + type;
        this.getContext().setJobGroup(level.toString(), descriptionLineDelimitedGeoJson);
        saveAsHadoopFeatureChanges(
                AtlasMutatorHelper.embedCountryNameInKey(getLogFileName(level, type),
                        featureChangeRDD),
                SparkFileHelper.combine(this.atlasMutatorConfiguration.getOutput(), LOG_FOLDER));
    }

    private void saveCountryAndLevels()
    {
        if (logger.isInfoEnabled())
        {
            for (final String countryKey : this.atlasMutatorConfiguration
                    .getCountryToMutationLevels().keySet())
            {
                logger.info("{}", this.atlasMutatorConfiguration.detailsString(countryKey));
            }
        }
        saveTextToFile(this.atlasMutatorConfiguration.detailsString(), this.outputFolder,
                COUNTRY_AND_LEVELS);
    }

    private void saveFeatureChangesAppliedMaybe(final AtlasMutationLevel level,
            final JavaPairRDD<CountryShard, Tuple<PackedAtlas, Change>> changedAtlasRDD,
            final Pool geoJsonPool)
    {
        if (shouldSaveApplied())
        {
            final JavaPairRDD<CountryShard, List<FeatureChange>> shardFeatureChangesRDD = changedAtlasRDD
                    .flatMapToPair(tuple ->
                    {
                        final List<Tuple2<CountryShard, List<FeatureChange>>> result = new ArrayList<>();
                        final CountryShard countryShard = tuple._1();
                        final Change change = tuple._2().getSecond();
                        if (change != null)
                        {
                            result.add(new Tuple2<>(countryShard, change.getFeatureChanges()));
                        }
                        return result.iterator();
                    });
            saveFeatureChangesMaybe(level, LOG_APPLIED, shardFeatureChangesRDD, geoJsonPool);
        }
    }

    private void saveFeatureChangesMaybe(final AtlasMutationLevel level, final String type,
            final JavaPairRDD<CountryShard, List<FeatureChange>> shardFeatureChangesRDD,
            final Pool geoJsonPool)
    {
        if ((LOG_GENERATED.equals(type) && shouldSaveGenerated())
                || (LOG_ASSIGNED.equals(type) && shouldSaveAssigned())
                || (LOG_APPLIED.equals(type) && shouldSaveApplied()))
        {
            geoJsonPool
                    .queue(() -> saveAsHadoopFeatureChanges(type, shardFeatureChangesRDD, level));
        }
    }

    private void savePotentialLogFiles(final List<AtlasMutationLevel> levels,
            final List<CountryShard> countryShards)
    {
        if (this.debugFeatureChangeOutput == AtlasMutatorParameters.DebugFeatureChangeOutput.NONE)
        {
            return;
        }
        final MultiMap<String, String> countryToLogFiles = new MultiMap<>();
        for (final AtlasMutationLevel level : levels)
        {
            final String logFileName = getLogFileName(level, LOG_APPLIED);
            for (final CountryShard countryShard : countryShards)
            {
                final String logFileNameWithCountry = countryShard.getCountry() + logFileName;
                // Reproduce the log file path by re-using the code path elements used in their
                // creation
                countryToLogFiles.add(countryShard.getCountry(),
                        SparkFileHelper.combine(LOG_FOLDER, logFileNameWithCountry,
                                new CountryShard(logFileNameWithCountry, countryShard.getShard())
                                        .getName()
                                        + FeatureChangeOutputFormat.getTotalExtension()));
            }
        }
        countryToLogFiles.forEach((country, logFiles) -> saveTextToFile(
                new StringList(logFiles).join(System.lineSeparator()), this.outputFolder,
                POTENTIAL_LOG_FILES_PATH, country + POTENTIAL_LOG_FILES_NAME));
    }

    /**
     * Save some String to a file in the File System
     *
     * @param contents
     *            The contents of the file
     * @param location
     *            The location in the fileSystem
     * @param combinePaths
     *            Optionally, some file path directories and names to combine to get the final path
     */
    private void saveTextToFile(final String contents, final String location,
            final String... combinePaths)
    {
        final String path = SparkFileHelper.combine(location, combinePaths);
        new Retry(WRITE_RETRIES, Duration.ONE_SECOND).withQuadratic(true).run(() ->
        {
            OutputStream outputStream = null;
            try
            {
                outputStream = new FileSystemCreator().get(path, configurationMap())
                        .create(new Path(path));
                final OutputStreamWritableResource outputResource = new OutputStreamWritableResource(
                        outputStream);
                if (path.endsWith(FileSuffix.GZIP.toString()))
                {
                    outputResource.setCompressor(Compressor.GZIP);
                }
                outputResource.writeAndClose(contents);
            }
            catch (final IOException e)
            {
                throw new CoreException("Unable to write to {}", path, e);
            }
            finally
            {
                if (outputStream != null)
                {
                    Streams.close(outputStream);
                }
            }
        });
    }

    /**
     * Utility function to filter down a shard based RDD to the set of individual shards present.
     *
     * @param shardBasedRdd
     *            The shard based RDD
     * @param <T>
     *            The type of the RDD
     * @return The set of shards present.
     */
    private <T> Set<CountryShard> shardsFrom(final JavaPairRDD<CountryShard, T> shardBasedRdd)
    {
        return shardBasedRdd.map(
                // Here we need to map to String then read back to Shard to avoid an un-serializable
                // lambda error.
                tuple -> tuple._1().getName()).distinct().collect().stream()
                .map(CountryShard::forName).collect(Collectors.toSet());
    }

    private boolean shouldSaveApplied()
    {
        return AtlasMutatorParameters.DebugFeatureChangeOutput.NONE != this.debugFeatureChangeOutput;
    }

    private boolean shouldSaveAssigned()
    {
        return AtlasMutatorParameters.DebugFeatureChangeOutput.ALL == this.debugFeatureChangeOutput;
    }

    private boolean shouldSaveGenerated()
    {
        return AtlasMutatorParameters.DebugFeatureChangeOutput.ALL == this.debugFeatureChangeOutput;
    }

    private boolean shouldSaveInputDependency(final AtlasMutationLevel level)
    {
        return !level.isChildCanPreloadRDDInput()
                && !level.getInputDependenciesToProvide().isEmpty();
    }
}
