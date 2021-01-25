package org.openstreetmap.atlas.mutator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.exception.change.FeatureChangeMergeException;
import org.openstreetmap.atlas.generator.tools.json.PersistenceJsonParser;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasMetaData;
import org.openstreetmap.atlas.geography.atlas.change.Change;
import org.openstreetmap.atlas.geography.atlas.change.ChangeAtlas;
import org.openstreetmap.atlas.geography.atlas.change.ChangeBuilder;
import org.openstreetmap.atlas.geography.atlas.change.ChangeType;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteArea;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEntity;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteLineItem;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteLocationItem;
import org.openstreetmap.atlas.geography.atlas.dynamic.DynamicAtlas;
import org.openstreetmap.atlas.geography.atlas.dynamic.policy.DynamicAtlasPolicy;
import org.openstreetmap.atlas.geography.atlas.items.Area;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.ItemType;
import org.openstreetmap.atlas.geography.atlas.items.LineItem;
import org.openstreetmap.atlas.geography.atlas.items.LocationItem;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.mutator.configuration.parsing.ConfiguredSubAtlas;
import org.openstreetmap.atlas.mutator.configuration.parsing.mergeforgiveness.ConfiguredMergeForgivenessPolicy;
import org.openstreetmap.atlas.mutator.filtering.ChangeFilter;
import org.openstreetmap.atlas.tags.ISOCountryTag;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.maps.MultiMap;
import org.openstreetmap.atlas.utilities.scalars.Duration;
import org.openstreetmap.atlas.utilities.threads.Pool;
import org.openstreetmap.atlas.utilities.threads.Result;
import org.openstreetmap.atlas.utilities.time.Time;
import org.openstreetmap.atlas.utilities.tuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * This class contains most of the lambda function executed by Spark slaves when running an
 * {@link AtlasMutator} spark job.
 *
 * @author matthieun
 */
public final class AtlasMutatorHelper implements Serializable
{
    /**
     * A unique key for a FeatureChange
     *
     * @author matthieun
     */
    public static final class FeatureChangeKey implements Serializable
    {
        private static final long serialVersionUID = -9078313497297437479L;

        private final long identifier;
        private final ItemType itemType;

        public FeatureChangeKey(final long identifier, final ItemType itemType)
        {
            this.identifier = identifier;
            this.itemType = itemType;
        }

        @Override
        public boolean equals(final Object other)
        {
            if (other instanceof FeatureChangeKey)
            {
                return ((FeatureChangeKey) other).getIdentifier() == this.getIdentifier()
                        && ((FeatureChangeKey) other).getItemType() == this.getItemType();
            }
            return false;
        }

        public long getIdentifier()
        {
            return this.identifier;
        }

        public ItemType getItemType()
        {
            return this.itemType;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(this.identifier, this.itemType);
        }
    }

    private static final long serialVersionUID = 3761422875454997973L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasMutatorHelper.class);
    private static final String ERROR_IN_MUTATION = "{}: Shard {}: Error generating FeatureChanges for {} after {}";
    private static final int EXECUTE_MUTATIONS_THREADS = 5;

    public static String getAlternateSubFolderOutput(final String output, final String name)
    {
        return SparkFileHelper.combine(SparkFileHelper.parentPath(output), name);
    }

    /**
     * @param level
     *            The level at which the merging is being done
     * @return A function that merges two lists of {@link FeatureChange} into one single list. It
     *         applies merge rules on all the {@link FeatureChange} objects that collide.
     */
    protected static Function2<List<FeatureChange>, List<FeatureChange>, List<FeatureChange>> assignedToConcatenatedFeatureChanges(
            final AtlasMutationLevel level)
    {
        final String message = "Concatenating two FeatureChange lists: [L: {}, R: {}, UUID: {}]";
        return (list1, list2) ->
        {
            final String uuid = UUID.randomUUID().toString();
            final String leftSize = String.valueOf(list1.size());
            final String rightSize = String.valueOf(list2.size());
            final Time start = Time.now();
            try
            {
                // Get the merged collisions
                final List<FeatureChange> result = list1;
                result.addAll(list2);
                return result;
            }
            catch (final Exception e)
            {
                throw exception(start, level, message, e, leftSize, rightSize, uuid).get();
            }
        };
    }

    /**
     * @param level
     *            The level at which the merging is being done
     * @return A function that merges two lists of {@link FeatureChange} into one single list. It
     *         applies merge rules on all the {@link FeatureChange} objects that collide.
     */
    protected static PairFunction<Tuple2<CountryShard, List<FeatureChange>>, CountryShard, List<FeatureChange>> assignedToShardAppliedFeatureChanges(
            final AtlasMutationLevel level)
    {
        final String message = "Shard {}: Merging FeatureChanges.";
        return tuple ->
        {
            final CountryShard countryShard = tuple._1();
            final Time start = startTime(level, message, countryShard.getName());
            try
            {
                final MultiMap<FeatureChangeKey, FeatureChange> indexed = new MultiMap<>();
                tuple._2().forEach(featureChange -> indexed.add(featureChangeKey(featureChange),
                        featureChange));
                final List<FeatureChange> result = new ArrayList<>();
                for (final Entry<FeatureChangeKey, List<FeatureChange>> entry : indexed.entrySet())
                {
                    final List<FeatureChange> value = entry.getValue();
                    if (value.size() > 1)
                    {
                        final LinkedList<FeatureChange> newValue = Lists.newLinkedList(value);
                        while (newValue.size() > 1)
                        {
                            final FeatureChange first = newValue.poll();
                            final FeatureChange second = newValue.poll();
                            final FeatureChange merged = merge(level, first, second);
                            newValue.add(merged);
                        }
                        result.add(newValue.peek());
                    }
                    else
                    {
                        result.add(value.get(0));
                    }
                }
                logTime(start, level, message, countryShard.getName());
                return new Tuple2<>(countryShard, result);
            }
            catch (final Exception e)
            {
                throw exception(start, level, message, e, countryShard.getName()).get();
            }
        };
    }

    /**
     * @param level
     *            The mutation level definition
     * @return A function that takes a tuple of shard and map of expansion atlases, and returns all
     *         the {@link FeatureChange} objects generated by that expanded shard.
     */
    protected static PairFlatMapFunction<Tuple2<CountryShard, Tuple2<Change, org.apache.spark.api.java.Optional<Map<CountryShard, PackedAtlas>>>>, CountryShard, Tuple<PackedAtlas, Change>> changeAndShardToAtlasMapToAtlas(
            final AtlasMutationLevel level)
    {
        return tuple ->
        {
            final CountryShard countryShard = tuple._1();
            final Tuple2<Change, org.apache.spark.api.java.Optional<Map<CountryShard, PackedAtlas>>> changeAndShardToAtlasMap = tuple
                    ._2();
            final Change change = changeAndShardToAtlasMap._1();
            final Map<CountryShard, PackedAtlas> shardToAtlasMap = changeAndShardToAtlasMap._2()
                    // If we have a change that leaked to a CountryShard where we did not have any
                    // Atlas, make it an empty map. The next function, changeToAtlas, will take care
                    // of creating an Atlas from scratch with the change.
                    .orElse(new HashMap<>());
            final BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy> getThePolicyFunction = (Serializable & BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy>) (
                    levelInternal, shardInternal) -> levelInternal
                            .getRDDBasedApplicationPolicy(shardInternal, shardToAtlasMap);
            final Tuple2<CountryShard, Change> source = new Tuple2<>(countryShard, change);
            return changeToAtlas(level, getThePolicyFunction).call(source);
        };

    }

    /**
     * @param level
     *            The level at which the application is being done.
     * @return A function that applies a change to the right Atlas and returns a cloned
     *         {@link PackedAtlas} ready to save.
     */
    protected static PairFlatMapFunction<Tuple2<CountryShard, Change>, CountryShard, Tuple<PackedAtlas, Change>> changeToAtlas(
            final AtlasMutationLevel level)
    {
        // Wrap in another lambda to get full stack traces in case of error
        return tuple -> changeToAtlas(level,
                /* This one needs to be serializable */
                (Serializable & BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy>) AtlasMutationLevel::getApplicationPolicy)
                        .call(tuple);
    }

    /**
     * @param level
     *            The level at which the application is being done.
     * @param levelAndShardToPolicyFunction
     *            The function that decides what policy to get from the level (simple generation, or
     *            generation from RDD)
     * @return A function that applies a change to the right Atlas and returns a cloned
     *         {@link PackedAtlas} ready to save along with the actually used filtered change
     *         object.
     */
    protected static PairFlatMapFunction<Tuple2<CountryShard, Change>, CountryShard, Tuple<PackedAtlas, Change>> changeToAtlas( // NOSONAR
            final AtlasMutationLevel level,
            final BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy> levelAndShardToPolicyFunction)
    {
        final String message1 = "Shard {}: Applying Change to Atlas.";
        final String message2 = message1 + " Loaded {} shards for that which took {}. "
                + "ChangeFilter operation took {}.";
        return tuple ->
        {
            final CountryShard countryShard = tuple._1();
            if (!level.getDebugIncludeListedShards().isEmpty() && !level
                    .getDebugIncludeListedShards().contains(countryShard.getShard().getName()))
            {
                if (logger.isWarnEnabled())
                {
                    final String message = message1 + " It is not in debugShards. Skipping!";
                    logger.warn(message, countryShard.getName());
                }
                return Collections.emptyIterator();
            }
            final Change change = tuple._2();
            String numberOfShards = "0";
            String dynamicAtlasDuration = Duration.ZERO.toString();
            String changeFilterDuration = Duration.ZERO.toString();
            final Time start = startTime(level, message1, countryShard.getName());
            try
            {
                final String changeAtlasName = countryShard.getName();
                final List<Tuple2<CountryShard, Tuple<PackedAtlas, Change>>> result = new ArrayList<>();
                ChangeAtlas changeAtlas = null;
                final DynamicAtlasPolicy policy = levelAndShardToPolicyFunction.apply(level,
                        countryShard);
                final Optional<Change> stripped;
                if (policy.getAtlasFetcher().apply(countryShard.getShard()).isPresent())
                {
                    // Apply only if the initial atlas exists
                    final Time dynamicAtlasStart = Time.now();
                    final DynamicAtlas dynamicAtlas = new DynamicAtlas(policy);
                    dynamicAtlas.preemptiveLoad();
                    dynamicAtlasDuration = dynamicAtlasStart.elapsedSince().toString();
                    numberOfShards = String.valueOf(dynamicAtlas.getNumberOfShardsLoaded());
                    // Add a change filter here to remove the shallow ones that dont have
                    // a proper source in the dynamic atlas.
                    final Time changeFilterStart = Time.now();
                    stripped = new ChangeFilter(dynamicAtlas).apply(change);
                    changeFilterDuration = changeFilterStart.elapsedSince().toString();
                    if (stripped.isPresent())
                    {
                        changeAtlas = createChangeAtlas(Optional.of(dynamicAtlas), changeAtlasName,
                                stripped.get());
                    }
                }
                else
                {
                    // There is no existing shard, try creating a new one with the changes.
                    // This can happen if a mutation created a new FeatureChange that spills out of
                    // a populated shard into an empty shard, at which point that empty shard now
                    // needs to contain some data.
                    numberOfShards = "1";
                    final Time changeFilterStart = Time.now();
                    stripped = new ChangeFilter(countryShard.getShard()).apply(change);
                    changeFilterDuration = changeFilterStart.elapsedSince().toString();
                    if (stripped.isPresent())
                    {
                        changeAtlas = attemptNewChangeAtlas(level, countryShard, changeAtlasName,
                                stripped.get());
                    }
                }
                if (changeAtlas != null)
                {
                    final Optional<Atlas> packedAtlasOption = changeAtlas
                            .subAtlas(countryShard.bounds(), AtlasCutType.SOFT_CUT);
                    if (packedAtlasOption.isPresent())
                    {
                        result.add(new Tuple2<>(countryShard, new Tuple<>(
                                (PackedAtlas) packedAtlasOption.get(),
                                stripped.flatMap(ChangeFilter::stripForSaving).orElse(null))));
                    }
                    else
                    {
                        logger.warn("{}: SubAtlas on ChangeAtlas for {} returned nothing.", level,
                                countryShard.getName());
                    }
                }
                else
                {
                    numberOfShards = "0";
                }
                logTime(start, level, message2, countryShard.getName(), numberOfShards,
                        dynamicAtlasDuration, changeFilterDuration);
                return result.iterator();
            }
            catch (final Exception e)
            {
                throw exception(start, level, message2, e, countryShard.getName(), numberOfShards,
                        dynamicAtlasDuration, changeFilterDuration).get();
            }
        };
    }

    protected static <I> JavaPairRDD<String, I> embedCountryNameInKey(final String logFileName,
            final JavaPairRDD<CountryShard, I> atlasRDD)
    {
        return atlasRDD.mapToPair(tuple -> new Tuple2<>(
                PersistenceJsonParser.createJsonKey(tuple._1().getCountry() + logFileName,
                        tuple._1().getShard().getName(), Maps.hashMap()),
                tuple._2()));
    }

    /**
     * @param level
     *            The level at which the operation is happening
     * @return A function that groups each list of {@link FeatureChange}s into one {@link Change}
     */
    protected static PairFunction<Tuple2<CountryShard, List<FeatureChange>>, CountryShard, Change> featureChangeListToChange(
            final AtlasMutationLevel level)
    {
        final String message = "Shard {}: Translate FeatureChange objects to a Change.";
        return tuple ->
        {
            final CountryShard shard = tuple._1();
            final Time start = startTime(level, message, shard.getName());
            try
            {
                final ChangeBuilder builder = ChangeBuilder.newInstance();
                builder.addAll(tuple._2());
                final Change result = builder.get();
                logTime(start, level, message, shard.getName());
                return new Tuple2<>(tuple._1(), result);
            }
            catch (final Exception e)
            {
                throw exception(start, level, message, e, shard.getName()).get();
            }
        };
    }

    /**
     * Filter Mutated Atlas to provide input dependency
     *
     * @param level
     *            The level at which the operation is happening
     * @param inputDependency
     *            The {@link InputDependency} providing the definition for filtering.
     * @return A function that takes a shard and Atlas pair and filters the Atlas according to the
     *         definition in the {@link InputDependency}
     */
    protected static PairFlatMapFunction<Tuple2<CountryShard, PackedAtlas>, CountryShard, PackedAtlas> inputDependencyFilteredAtlas(
            final AtlasMutationLevel level, final InputDependency inputDependency)
    {
        final String message = "Shard {}: Filter Mutated Atlas to provide input dependency {}";
        return tuple ->
        {
            final CountryShard countryShard = tuple._1();
            final String shardName = countryShard.getName();
            final PackedAtlas atlas = tuple._2();
            final Time start = startTime(level.toString(), message, shardName,
                    inputDependency.toString());
            try
            {
                final List<Tuple2<CountryShard, PackedAtlas>> result = new ArrayList<>();
                final ConfiguredSubAtlas subAtlasFunction = inputDependency.getSubAtlas();
                subAtlasFunction.apply(atlas).ifPresent(
                        subAtlas -> result.add(new Tuple2<>(countryShard, (PackedAtlas) subAtlas)));
                logTime(start, level.toString(), message, shardName, inputDependency.toString());
                return result.iterator();
            }
            catch (final Exception e)
            {
                throw exception(start, level.toString(), message, e, shardName,
                        inputDependency.toString()).get();
            }
        };
    }

    /**
     * @param level
     *            The level which can provide the sharding tree.
     * @return A function that assigns every {@link FeatureChange} to all the shards where it can be
     *         applied.
     */
    protected static PairFlatMapFunction<Tuple2<CountryShard, List<FeatureChange>>, CountryShard, List<FeatureChange>> shardFeatureChangesToAssignedShardFeatureChanges(
            final AtlasMutationLevel level)
    {
        final String message = "Shard {}: Assigning FeatureChanges to application shards.";
        return tuple ->
        {
            final String shardName = tuple._1().getName();
            final String country = tuple._1().getCountry();
            final List<FeatureChange> featureChanges = tuple._2();
            final MultiMap<CountryShard, FeatureChange> shardToApplied = new MultiMap<>();
            final Time start = startTime(level, message, shardName);
            try
            {
                for (final FeatureChange featureChange : featureChanges)
                {
                    final Iterable<? extends Shard> candidateShards = level
                            .getAtlasMutatorConfiguration().getSharding()
                            .shards(featureChange.bounds());
                    candidateShards.forEach(shard -> shardToApplied
                            .add(new CountryShard(country, shard), featureChange));
                }
                final List<Tuple2<CountryShard, List<FeatureChange>>> result = new ArrayList<>();
                shardToApplied.forEach((shard, featureChangeList) -> result
                        .add(new Tuple2<>(shard, featureChangeList)));
                logTime(start, level, message, shardName);
                return result.iterator();
            }
            catch (final Exception e)
            {
                throw exception(start, level, message, e, shardName).get();
            }
        };
    }

    /**
     * @param level
     *            The level of interest
     * @return A function that transforms a {@link CountryShard} into the corresponding
     *         {@link PackedAtlas} from the level's parent's source folder.
     */
    protected static PairFlatMapFunction<CountryShard, CountryShard, PackedAtlas> shardToAtlas(
            final AtlasMutationLevel level)
    {
        final String message1 = "Shard {}: Load Atlas to initial RDD.";
        final String message2 = message1 + " Downloaded {} shards for that.";
        return countryShard ->
        {
            final Time start = startTime(level, message1, countryShard.getName());
            final List<Tuple2<CountryShard, PackedAtlas>> result = new ArrayList<>();
            try
            {
                final Optional<Atlas> atlasOption = level.getSourceFetcher().apply(countryShard);
                atlasOption.ifPresent(atlas ->
                {
                    if (!(atlas instanceof PackedAtlas))
                    {
                        throw new CoreException("Expected fetcher to return a {} for {}",
                                PackedAtlas.class.getSimpleName(), countryShard.getName());
                    }
                    result.add(new Tuple2<>(countryShard, (PackedAtlas) atlas));
                });
                logTime(start, level, message2, countryShard.getName(),
                        String.valueOf(result.size()));
                return result.iterator();
            }
            catch (final Exception e)
            {
                throw exception(start, level, message2, e, countryShard.getName(),
                        String.valueOf(result.size())).get();
            }
        };
    }

    /**
     * @param level
     *            The mutation level definition
     * @return A function that takes a tuple of shard and map of expansion atlases, and returns all
     *         the {@link FeatureChange} objects generated by that expanded shard.
     */
    protected static PairFlatMapFunction<Tuple2<CountryShard, Map<CountryShard, PackedAtlas>>, CountryShard, List<FeatureChange>> shardToAtlasMapToFeatureChanges(
            final AtlasMutationLevel level)
    {
        return tuple ->
        {
            final CountryShard countryShard = tuple._1();
            final Map<CountryShard, PackedAtlas> shardToAtlasMap = tuple._2();
            final BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy> getThePolicyFunction = (Serializable & BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy>) (
                    levelInternal, shardInternal) -> levelInternal
                            .getRDDBasedGenerationPolicy(shardInternal, shardToAtlasMap);
            return shardToFeatureChanges(level, getThePolicyFunction).call(countryShard);
        };

    }

    /**
     * @param level
     *            The mutation level definition
     * @return A function that takes a shard and returns all the {@link FeatureChange} objects
     *         generated by that expanded shard.
     */
    protected static PairFlatMapFunction<CountryShard, CountryShard, List<FeatureChange>> shardToFeatureChanges(
            final AtlasMutationLevel level)
    {
        // Wrap in another lambda to get full stack traces in case of error
        return countryShard -> shardToFeatureChanges(level,
                /* This one needs to be serializable */
                (Serializable & BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy>) AtlasMutationLevel::getGenerationPolicy)
                        .call(countryShard);
    }

    /**
     * @param level
     *            The mutation level definition
     * @param levelAndShardToPolicyFunction
     *            The function that decides what policy to get from the level (simple generation, or
     *            generation from RDD)
     * @return A function that takes a shard and returns all the {@link FeatureChange} objects
     *         generated by that expanded shard.
     */
    protected static PairFlatMapFunction<CountryShard, CountryShard, List<FeatureChange>> shardToFeatureChanges(
            final AtlasMutationLevel level,
            final BiFunction<AtlasMutationLevel, CountryShard, DynamicAtlasPolicy> levelAndShardToPolicyFunction)
    {
        final String message1 = "Shard {}: Generate FeatureChange objects.";
        final String message2 = message1 + " Loaded {} shards for that which took {}.";
        return countryShard ->
        {
            final Time start = startTime(level, message1, countryShard.getName());
            String numberOfShards = "0";
            String dynamicAtlasDuration = Duration.ZERO.toString();
            final String country = countryShard.getCountry();
            try
            {
                List<FeatureChange> result = new ArrayList<>();
                final DynamicAtlasPolicy policy = levelAndShardToPolicyFunction.apply(level,
                        countryShard);
                if (policy.getAtlasFetcher().apply(countryShard.getShard()).isPresent())
                {
                    // Load dynamic atlas only if the initial shard is there
                    final Time dynamicAtlasStart = Time.now();
                    final DynamicAtlas source = new DynamicAtlas(policy)
                    {
                        private static final long serialVersionUID = -1379576156041355921L;

                        @Override
                        public AtlasMetaData metaData()
                        {
                            // Override meta-data here so the country code is properly included.
                            final AtlasMetaData metaData = super.metaData();
                            return new AtlasMetaData(metaData.getSize(), false,
                                    metaData.getCodeVersion().orElse(null),
                                    metaData.getDataVersion().orElse(null), country,
                                    metaData.getShardName().orElse(null), metaData.getTags());
                        }
                    };
                    source.preemptiveLoad();
                    dynamicAtlasDuration = dynamicAtlasStart.elapsedSince().toString();
                    numberOfShards = String.valueOf(source.getNumberOfShardsLoaded());
                    result = executeMutations(level, source, countryShard);
                }
                else
                {
                    logger.warn(
                            "{}: Shard {}: No shard could be loaded when generating FeatureChanges. Skipping!",
                            level, countryShard.getName());
                }
                logTime(start, level, message2, countryShard.getName(), numberOfShards,
                        dynamicAtlasDuration);

                if (result.isEmpty())
                {
                    return new ArrayList<Tuple2<CountryShard, List<FeatureChange>>>().iterator();
                }
                else
                {
                    return Lists.newArrayList(new Tuple2<>(countryShard, result)).iterator();
                }
            }
            catch (final Exception e)
            {
                throw exception(start, level, message2, e, countryShard.getName(), numberOfShards,
                        dynamicAtlasDuration).get();
            }
        };
    }

    protected static PairFlatMapFunction<Tuple2<CountryShard, Map<CountryShard, PackedAtlas>>, CountryShard, PackedAtlas> untouchedShardAndMapToPotentialSourcePackedAtlas(
            final AtlasMutationLevel level)
    {
        return tuple ->
        {
            final CountryShard untouchedShard = tuple._1();
            final Map<CountryShard, PackedAtlas> shardToAtlasMap = tuple._2();
            if (!level.canSourceAtlasObjectsFromRDD())
            {
                throw new CoreException("{} should not pull untouched shards from RDD", level);
            }
            // Here the fetcher does not need to be serializable, since this is inside the executor
            final Function<CountryShard, Optional<Atlas>> fetcher = shard -> Optional
                    .ofNullable(shardToAtlasMap.get(shard));
            return untouchedShardToPotentialSourcePackedAtlas(level, fetcher).call(untouchedShard);
        };
    }

    /**
     * @param level
     *            The level of interest
     * @return Return a function that provides the original source atlas given a shard, or an empty
     *         list if that atlas does not exist.
     */
    protected static PairFlatMapFunction<CountryShard, CountryShard, PackedAtlas> untouchedShardToPotentialSourcePackedAtlas(
            final AtlasMutationLevel level)
    {
        // Wrap in another lambda to get full stack traces in case of error
        return shard -> untouchedShardToPotentialSourcePackedAtlas(level,
                /* This one needs to be serializable */level.getSourceFetcher()).call(shard);
    }

    protected static PairFlatMapFunction<CountryShard, CountryShard, PackedAtlas> untouchedShardToPotentialSourcePackedAtlas(
            final AtlasMutationLevel level, final Function<CountryShard, Optional<Atlas>> fetcher)
    {
        final String message = "Shard {}: Get source PackedAtlas for untouched shards.";
        return countryShard ->
        {
            final Time start = startTime(level, message, countryShard.getName());
            try
            {
                final Optional<Atlas> untouchedSourceOption = fetcher.apply(countryShard);
                final List<Tuple2<CountryShard, PackedAtlas>> result = new ArrayList<>();
                untouchedSourceOption.ifPresent(
                        atlas -> result.add(new Tuple2<>(countryShard, (PackedAtlas) atlas)));
                logTime(start, level, message, countryShard.getName());
                return result.iterator();
            }
            catch (final Exception e)
            {
                throw exception(start, level, message, e, countryShard.getName()).get();
            }
        };
    }

    /**
     * Create a {@link ChangeAtlas} from scratch
     *
     * @param level
     *            The level at which the operation is happening
     * @param shard
     *            The shard to which the {@link ChangeAtlas} is linked
     * @param changeAtlasName
     *            The name of the {@link ChangeAtlas}
     * @param change
     *            The change (including shallow FeatureChanges, which will be stripped out here)
     *            used to build the initial features in the {@link ChangeAtlas}
     * @return The built out {@link ChangeAtlas}, null otherwise.
     */
    private static ChangeAtlas attemptNewChangeAtlas(final AtlasMutationLevel level,
            final CountryShard shard, final String changeAtlasName, final Change change)
    {
        final Optional<Change> stripped = stripShallowFeatureChanges(change);
        // Return a new ChangeAtlas only when there are some non-shallow FCs, and when at least one
        // of those is of type ADD. If they were all REMOVE, then the new ChangeAtlas could not be
        // created.
        if (stripped.isPresent() && stripped.get().changes()
                .anyMatch(featureChange -> featureChange.getChangeType() == ChangeType.ADD))
        {
            logger.warn(
                    "{}: Creating new shard {} that did not exist before! "
                            + "Using {} non-shallow feature changes for that.",
                    level, shard.getName(), stripped.get().getFeatureChanges().size());
            return createChangeAtlas(Optional.empty(), changeAtlasName, stripped.get());
        }
        return null;
    }

    private static Optional<Change> createChange(final Iterable<FeatureChange> featureChanges)
    {
        final ChangeBuilder result = new ChangeBuilder();
        featureChanges.forEach(result::add);
        if (result.peekNumberOfChanges() > 0)
        {
            return Optional.of(result.get());
        }
        else
        {
            return Optional.empty();
        }
    }

    /**
     * Create a {@link ChangeAtlas} from Atlas, Change and name. The main purpose of this method is
     * to control the Atlas' meta-data generation, to make sure we have a reference of all the
     * updates applied to relations. Since updated relations can be inconsistent across shards, we
     * tag them with their mutation and level in the resulting Atlas meta-data, and not directly on
     * the feature itself. However, specifically for relations that are newly created, we tag them
     * directly since we always have full context for created relations.
     * 
     * @param source
     *            The option of a source Atlas. Empty if creating one from scratch
     * @param name
     *            The name of the {@link ChangeAtlas}
     * @param change
     *            The group of {@link FeatureChange}s to apply
     * @return The created ChangeAtlas.
     */
    private static ChangeAtlas createChangeAtlas(final Optional<Atlas> source, final String name,
            final Change change)
    {
        /*
         * This predicate defines entities for which we will move the 'mutator:' tags to the atlas
         * metadata. Basically, anything that is a Relation that was not newly created will have the
         * tags moved (we make an exception for relations that already exist AND already have
         * 'mutator:' tags, since these are generally small relations that were created on a
         * previous level).
         */
        final Predicate<FeatureChange> entitiesToMoveMutatorTagsToAtlasMetaData = featureChange -> ItemType.RELATION == featureChange
                .getItemType() && source.isPresent()
                && source.get().relation(featureChange.getIdentifier()) != null
                && !source.get().relation(featureChange.getIdentifier()).getTags().keySet().stream()
                        .anyMatch(key -> key.startsWith(AtlasMutator.MUTATOR_META_DATA_KEY
                                + AtlasMutator.MUTATOR_META_DATA_SPLIT));

        // Get all the mutator tags for relations
        final Map<String, String> newMetaDataTags = ChangeFilter.mutatorMetaDataFromTags(change,
                entitiesToMoveMutatorTagsToAtlasMetaData);
        final UnaryOperator<AtlasMetaData> metaDataEnhancer = metaData ->
        {
            final Map<String, String> newMetaDataTagsInternal = new HashMap<>(newMetaDataTags);
            // Keep other existing meta-data
            newMetaDataTagsInternal.putAll(metaData.getTags());
            return new AtlasMetaData(metaData.getSize(), false,
                    metaData.getCodeVersion().orElse(null), metaData.getDataVersion().orElse(null),
                    metaData.getCountry().orElse(null), metaData.getShardName().orElse(null),
                    newMetaDataTagsInternal);
        };
        final Change changeWithoutMetaDataTags = ChangeFilter.changeWithoutMutatorTag(change,
                entitiesToMoveMutatorTagsToAtlasMetaData);
        if (source.isPresent())
        {
            return new ChangeAtlas(source.get(), name, changeWithoutMetaDataTags)
            {
                private static final long serialVersionUID = 2368717812777432282L;

                @Override
                public synchronized AtlasMetaData metaData()
                {
                    return metaDataEnhancer.apply(super.metaData());
                }
            };
        }
        else
        {
            return new ChangeAtlas(name, changeWithoutMetaDataTags)
            {
                private static final long serialVersionUID = 2368717812777432282L;

                @Override
                public synchronized AtlasMetaData metaData()
                {
                    return metaDataEnhancer.apply(super.metaData());
                }
            };
        }
    }

    /**
     * Provide a standardized exception with message
     *
     * @param start
     *            The start time
     * @param level
     *            The level at which the operation is happening
     * @param message
     *            The standardized message
     * @param sourceException
     *            The cause
     * @param elements
     *            All the elements to populate the standardized message
     * @return An Exception populated with the standardized message and the time it took to get
     *         there from the start time.
     */
    private static Supplier<CoreException> exception(final Time start,
            final AtlasMutationLevel level, final String message, final Exception sourceException,
            final String... elements)
    {
        return exception(start, level.toString(), message, sourceException, elements);
    }

    /**
     * Provide a standardized exception with message
     *
     * @param start
     *            The start time
     * @param level
     *            The name of the level at which the operation is happening
     * @param message
     *            The standardized message
     * @param sourceException
     *            The cause
     * @param elements
     *            All the elements to populate the standardized message
     * @return An Exception populated with the standardized message and the time it took to get
     *         there from the start time.
     */
    private static Supplier<CoreException> exception(final Time start, final String level,
            final String message, final Exception sourceException, final String... elements)
    {
        final Object[] objects = logObjectsEnd(start, level, elements);
        return () -> new CoreException(MessageFormatter
                .arrayFormat("{}: Failure after {}: " + message, objects).getMessage(),
                sourceException);
    }

    private static List<FeatureChange> executeMutation(final AtlasMutationLevel level,
            final ConfiguredAtlasChangeGenerator mutator, final Atlas source,
            final CountryShard countryShard)
    {
        final List<FeatureChange> result = new ArrayList<>();
        final Set<String> debugIncludeListedMutators = level.getDebugIncludeListedMutators();
        if (!debugIncludeListedMutators.isEmpty()
                && !level.getDebugIncludeListedMutators().contains(mutator.getName()))
        {
            logger.warn("{}: Shard {}: Skipping Mutation {}", level, countryShard.getName(),
                    mutator.getName());
            return result;
        }
        final Time start = Time.now();
        try
        {
            final String mutatorName = mutator.getName();
            logger.info("{}: Shard {}: Starting Mutation {}", level, countryShard.getName(),
                    mutatorName);
            level.getBroadcastVariables().forEach(mutator::addBroadcastVariable);
            final Set<FeatureChange> mutations = mutator.apply(source);
            // Filter and decorate the mutations with context
            mutations.stream()
                    // Only return the mutations that overlap with the initial shard
                    .filter(featureChange -> countryShard.bounds().overlaps(featureChange.bounds()))
                    // Add metadata to the FeatureChange to track the source mutation and shard
                    .map(featureChange -> withMetaData(featureChange, mutatorName, countryShard))
                    // Add tags to each feature that has been touched by a Mutation
                    .map(featureChange -> withTags(level, source, featureChange, mutatorName,
                            countryShard.getCountry()))
                    // By default, ConfiguredChangeAtlasGenerator adds the beforeView data from the
                    // source Atlas. However it does it only for the fields that are updated. Here
                    // we add that beforeView geometry for all FeatureChanges that do not update the
                    // geometry. This is why this is here and specific to AtlasMutator and not done
                    // in FeatureChange
                    .map(featureChange -> withGeometry(source, featureChange))
                    // Collect
                    .forEach(result::add);
            logger.info("{}: Shard {}: Finished Mutation {} in {}", level, countryShard.getName(),
                    mutator.getName(), start.elapsedSince());
            return result;
        }
        catch (final Exception e)
        {
            throw new CoreException(ERROR_IN_MUTATION, level, countryShard.getName(),
                    mutator.getName(), start.elapsedSince(), e);
        }
    }

    private static List<FeatureChange> executeMutations(final AtlasMutationLevel level,
            final Atlas source, final CountryShard shard)
    {
        final List<FeatureChange> result = new ArrayList<>();
        final List<Result<List<FeatureChange>>> poolResults = new ArrayList<>();
        try (Pool mutationsPool = new Pool(EXECUTE_MUTATIONS_THREADS,
                level.toString() + "_executeMutation"))
        {
            for (final ConfiguredAtlasChangeGenerator mutation : level.getMutators())
            {
                poolResults.add(
                        mutationsPool.queue(() -> executeMutation(level, mutation, source, shard)));
            }
            for (final Result<List<FeatureChange>> poolResult : poolResults)
            {
                result.addAll(poolResult.get());
            }
        }
        return result;
    }

    private static FeatureChangeKey featureChangeKey(final FeatureChange featureChange)
    {
        return new FeatureChangeKey(featureChange.getIdentifier(), featureChange.getItemType());
    }

    private static Object[] logObjectsEnd(final Time start, final String level,
            final String[] elements)
    {
        final Object[] objects = new Object[elements.length + 2];
        objects[0] = level;
        objects[1] = start.elapsedSince();
        for (int index = 2; index < objects.length; index++)
        {
            objects[index] = elements[index - 2];
        }
        return objects;
    }

    private static Object[] logObjectsStart(final String level, final String[] elements)
    {
        final Object[] objects = new Object[elements.length + 1];
        objects[0] = level;
        for (int index = 1; index < objects.length; index++)
        {
            objects[index] = elements[index - 1];
        }
        return objects;
    }

    private static void logTime(final Time start, final AtlasMutationLevel level,
            final String message, final String... elements)
    {
        logTime(start, level.toString(), message, elements);
    }

    private static void logTime(final Time start, final String level, final String message,
            final String... elements)
    {
        final Object[] objects = logObjectsEnd(start, level, elements);
        logger.info(MessageFormatter.arrayFormat("{}: Processed in {}: " + message, objects)
                .getMessage());
    }

    private static FeatureChange merge(final AtlasMutationLevel level, final FeatureChange first,
            final FeatureChange second)
    {
        final ConfiguredMergeForgivenessPolicy policy = level.getAtlasMutatorConfiguration()
                .getGlobalMergeForgivenessPolicy();
        try
        {
            return first.merge(second);
        }
        catch (final FeatureChangeMergeException exception)
        {
            final Optional<FeatureChange> featureChangeFromPolicy = policy.applyPolicy(exception,
                    first, second);
            if (!featureChangeFromPolicy.isPresent())
            {
                throw new CoreException(
                        "Conflict merging level {}, failed to apply forgiveness policy:\n{}\nFeatureChanges:\n{}\nvs\n{}",
                        level, policy, first.prettify(), second.prettify(), exception);
            }
            logger.warn(
                    "Conflict merging level {}, successfully applied forgiveness policy:\n{}\nFeatureChanges:\n{}\nvs\n{}\nChose:\n{}",
                    level, policy, first.prettify(), second.prettify(),
                    featureChangeFromPolicy.get().prettify(), exception);
            return featureChangeFromPolicy.get();
        }
    }

    private static Time startTime(final AtlasMutationLevel level, final String message,
            final String... elements)
    {
        return startTime(level.toString(), message, elements);
    }

    private static Time startTime(final String level, final String message,
            final String... elements)
    {
        final Object[] objects = logObjectsStart(level, elements);
        logger.info(MessageFormatter.arrayFormat("{}: Starting: " + message, objects).getMessage());
        return Time.now();
    }

    private static Optional<Change> stripShallowFeatureChanges(final Change change)
    {
        return stripShallowFeatureChanges(change, FeatureChange::afterViewIsFull);
    }

    private static Optional<Change> stripShallowFeatureChanges(final Change change,
            final Predicate<FeatureChange> filter)
    {
        return createChange(change.changes().filter(filter).collect(Collectors.toList()));
    }

    private static FeatureChange withGeometry(final Atlas source, final FeatureChange origin)
    {
        if (origin.getItemType() == ItemType.RELATION)
        {
            return origin;
        }
        final AtlasEntity sourceEntity = source.entity(origin.getIdentifier(),
                origin.getItemType());
        if (sourceEntity == null)
        {
            return origin;
        }
        if (sourceEntity instanceof Area
                && ((CompleteArea) origin.getAfterView()).asPolygon() == null)
        {
            ((CompleteArea) origin.getBeforeView()).withPolygon(((Area) sourceEntity).asPolygon());
        }
        if (sourceEntity instanceof LineItem
                && ((CompleteLineItem) origin.getAfterView()).asPolyLine() == null)
        {
            ((CompleteLineItem) origin.getBeforeView())
                    .withPolyLine(((LineItem) sourceEntity).asPolyLine());
        }
        if (sourceEntity instanceof LocationItem
                && ((CompleteLocationItem) origin.getAfterView()).getLocation() == null)
        {
            ((CompleteLocationItem) origin.getBeforeView())
                    .withLocation(((LocationItem) sourceEntity).getLocation());
        }
        return origin;
    }

    private static FeatureChange withMetaData(final FeatureChange original,
            final String mutatorName, final Shard shard)
    {
        original.addMetaData(AtlasMutator.MUTATOR_META_DATA_KEY,
                mutatorName + AtlasMutator.MUTATOR_META_DATA_SPLIT + shard.getName());
        return original;
    }

    private static FeatureChange withTags(final AtlasMutationLevel level, final Atlas source,
            final FeatureChange origin, final String mutatorName, final String country)
    {
        if (origin.getChangeType() == ChangeType.REMOVE || !level.isAddMutationTags())
        {
            return origin;
        }
        Map<String, String> tags = origin.getAfterView().getTags();
        if (tags == null)
        {
            tags = origin.getBeforeView().getTags();
        }
        if (tags == null)
        {
            tags = source.entity(origin.getIdentifier(), origin.getItemType()).getTags();
        }
        tags = new HashMap<>(tags);
        if (origin.getBeforeView() == null)
        {
            tags.put(ISOCountryTag.KEY, country);
        }
        tags.put(AtlasMutator.MUTATOR_META_DATA_KEY + AtlasMutator.MUTATOR_META_DATA_SPLIT
                + mutatorName, String.valueOf(level.getLevelIndex()));
        final AtlasEntity newAfterView = (AtlasEntity) ((CompleteEntity) origin.getAfterView())
                .withTags(tags);
        final FeatureChange featureChangeWithTags = FeatureChange.add(newAfterView, source);
        origin.getMetaData().forEach(featureChangeWithTags::addMetaData);
        return featureChangeWithTags;
    }

    private AtlasMutatorHelper()
    {
    }
}
