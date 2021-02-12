package org.openstreetmap.atlas.mutator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.InputDependency;
import org.openstreetmap.atlas.utilities.time.Time;
import org.openstreetmap.atlas.utilities.tuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Helper class to {@link AtlasMutator} which handles RDD processing. Those methods are designed to
 * be run on the driver.
 * 
 * @author matthieun
 */
public final class AtlasMutatorDriver
{
    private static final Logger logger = LoggerFactory.getLogger(AtlasMutatorDriver.class);

    /**
     * Take an AtlasRDD and join it with all the shards needed for expansion for each shard, to get
     * a new RDD that contains all the needed Atlas files for each shard.
     *
     * @param name
     *            The name used to prefix the RDD names in the Spark UI
     * @param size
     *            The pre-computed RDD size
     * @param atlasRDD
     *            The original Atlas RDD. It needs to be cached
     * @param shardExplorer
     *            The function that determines what shards are needed for expansion, given a shard.
     * @param inputDependencyOption
     *            Optionally, an input dependency in case the expansion shards need to be filtered
     *            down
     * @return A new RDD that contains all the needed Atlas files for each shard.
     */
    protected static JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> getAtlasGroupsRDD(
            final String name, final int size,
            final JavaPairRDD<CountryShard, PackedAtlas> atlasRDD,
            final Function<CountryShard, Set<CountryShard>> shardExplorer,
            final Optional<InputDependency> inputDependencyOption)
    {
        // 1. Get the shard list from the AtlasRDD
        final JavaRDD<CountryShard> shardsRDD = atlasRDD.keys().coalesce(size);
        shardsRDD.setName(name + ": shardsRDD");
        // 2. FlatMap to all requested atlas shard to key shard (reversed)
        final JavaPairRDD<CountryShard, CountryShard> requestedAtlasShardToKeyShardRDD = shardsRDD
                .flatMapToPair(countryShard ->
                {
                    final var result = shardExplorer.apply(countryShard).stream()
                            .map(shardExplored -> new Tuple2<>(shardExplored, countryShard))
                            .collect(Collectors.toList());
                    if (logger.isInfoEnabled())
                    {
                        logger.info(
                                "{}: Exploring shards for preemptive expansion for {}. Found {}",
                                name, countryShard.getName(),
                                result.stream().map(tuple -> tuple._2().getName())
                                        .collect(Collectors.toList()));
                    }
                    return result.iterator();
                });
        requestedAtlasShardToKeyShardRDD.setName(name + ": requestedAtlasShardToKeyShardRDD");
        // 3. Join with AtlasRDD to get all Shard to needed Atlas in the Value Tuples. Remove all
        // the requested Atlases in the value tuples that don't exist and are null.
        final JavaPairRDD<CountryShard, Tuple2<CountryShard, PackedAtlas>> requestedAtlasShardToKeyShardAtlasRDD = requestedAtlasShardToKeyShardRDD
                .join(atlasRDD).filter(tuple -> tuple._2()._2() != null);
        requestedAtlasShardToKeyShardAtlasRDD
                .setName(name + ": requestedAtlasShardToKeyShardAtlasRDD");
        // 4. Swap shard Keys and shard Values to get back with key shards on the left
        final JavaPairRDD<CountryShard, Tuple<CountryShard, PackedAtlas>> keyShardToRequestedAtlasRDD = requestedAtlasShardToKeyShardAtlasRDD
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2()._1(),
                        new Tuple<>(tuple2._1(), tuple2._2()._2())));
        keyShardToRequestedAtlasRDD.setName(name + ": keyShardToRequestedAtlasRDD");
        // 5. Group by key shard to get all the Atlas needed by each shard. Also make the returned
        // iterable into a map. Coalesce to kill all the empty tasks from all the joins
        // (here open the Optional InputDependency now since Optional is not Serializable
        final InputDependency inputDependency = inputDependencyOption.orElse(null);
        final JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> atlasGroupsRDD = keyShardToRequestedAtlasRDD
                .groupByKey().coalesce(size).mapToPair(tuple -> new Tuple2<>(tuple._1(),
                        iterableToMap(name, tuple._2(), tuple._1(), inputDependency)));
        atlasGroupsRDD.setName(name + ": atlasGroupsRDD");
        return atlasGroupsRDD;
    }

    /**
     * Take a simple Atlas RDD and make it look like a group RDD with the identity shard explorer
     * 
     * @param name
     *            The name used to prefix the RDD names in the Spark UI
     * @param atlasRDD
     *            The original Atlas RDD. It needs to be cached
     * @return A new RDD that contains all the single Atlas files for each shard (in this case it is
     *         the same).
     */
    protected static JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> getAtlasGroupsRDDSimple(
            final String name, final JavaPairRDD<CountryShard, PackedAtlas> atlasRDD)
    {
        final JavaPairRDD<CountryShard, Map<CountryShard, PackedAtlas>> atlasGroupsRDDSimple = atlasRDD
                .mapToPair(tuple ->
                {
                    final CountryShard countryShard = tuple._1();
                    final Map<CountryShard, PackedAtlas> result = new HashMap<>();
                    result.put(countryShard, tuple._2());
                    return new Tuple2<>(countryShard, result);
                });
        atlasGroupsRDDSimple.setName(name + ": atlasGroupsRDDSimple");
        return atlasGroupsRDDSimple;
    }

    /**
     * Take a Shard RDD and make it into an AtlasRDD using the level's fetcher.
     *
     * @param level
     *            The level to use to generate the initial Atlas RDD.
     * @param shardsRDD
     *            The original Shard RDD
     * @return A new RDD that contains each Atlas file for each Shard.
     */
    protected static JavaPairRDD<CountryShard, PackedAtlas> getAtlasRDD(
            final AtlasMutationLevel level, final JavaRDD<CountryShard> shardsRDD)
    {
        return shardsRDD.flatMapToPair(countryShard ->
        {
            final List<Tuple2<CountryShard, PackedAtlas>> result = new ArrayList<>();
            level.getGenerationPolicy(countryShard).getAtlasFetcher().apply(countryShard).ifPresent(
                    atlas -> result.add(new Tuple2<>(countryShard, (PackedAtlas) atlas)));
            return result.iterator();
        });
    }

    protected static Map<CountryShard, PackedAtlas> iterableToMap(final String name,
            final Iterable<Tuple<CountryShard, PackedAtlas>> iterable,
            final CountryShard originShard, final InputDependency inputDependency)
    {
        final Map<CountryShard, PackedAtlas> result = new HashMap<>();
        iterable.forEach(tuple ->
        {
            final CountryShard expansionShard = tuple.getFirst();
            PackedAtlas expansionAtlas = tuple.getSecond();
            if (inputDependency != null
                    && !originShard.getShard().equals(expansionShard.getShard()))
            {
                final Time start = Time.now();
                expansionAtlas = (PackedAtlas) inputDependency.getSubAtlas().apply(expansionAtlas)
                        .orElse(null);
                if (logger.isInfoEnabled())
                {
                    logger.info("{}: For {}, filtered down expansion shard {} using {} in {}", name,
                            originShard.getName(), expansionShard.getName(),
                            inputDependency.getPathName(), start.elapsedSince());
                }
            }
            if (result.containsKey(expansionShard))
            {
                throw new CoreException(
                        "For origin shard {} there was a conflict with expansion shard {} "
                                + "being supplied more than one Atlas.\nFirst Atlas is {}, "
                                + "Second Atlas is {}",
                        originShard.getName(), expansionShard.getName(), result.get(expansionShard),
                        expansionAtlas);
            }
            if (expansionAtlas != null)
            {
                result.put(expansionShard, expansionAtlas);
            }
        });
        return result;
    }

    private AtlasMutatorDriver()
    {
    }
}
