package org.openstreetmap.atlas.generator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.raw.sectioning.WaySectionProcessor;
import org.openstreetmap.atlas.geography.atlas.raw.slicing.RawAtlasCountrySlicer;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.geography.atlas.statistics.Counter;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.runtime.system.memory.Memory;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Utility class for {@link AtlasGenerator}.
 *
 * @author matthieun
 * @author mgostintsev
 */
public final class AtlasGeneratorHelper implements Serializable
{
    /**
     * @author matthieun
     */
    protected static class NamedAtlasStatistics implements Serializable
    {
        private static final long serialVersionUID = 1593790111775268766L;
        private final String name;
        private final AtlasStatistics atlasStatistics;

        public NamedAtlasStatistics(final String name, final AtlasStatistics atlasStatistics)
        {
            this.name = name;
            this.atlasStatistics = atlasStatistics;
        }

        public AtlasStatistics getAtlasStatistics()
        {
            return this.atlasStatistics;
        }

        public String getName()
        {
            return this.name;
        }
    }

    private static final long serialVersionUID = 1300098384789754747L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasGeneratorHelper.class);

    private static final AtlasResourceLoader ATLAS_LOADER = new AtlasResourceLoader();

    /**
     * @param atlasCache
     *            The cache object for the Atlas files
     * @param country
     *            The country to look for
     * @param validShards
     *            All available shards for given country, to avoid fetching shards that do not exist
     * @return A function that returns an {@link Atlas} given a {@link Shard}
     */
    protected static Function<Shard, Optional<Atlas>> atlasFetcher(
            final HadoopAtlasFileCache atlasCache, final String country,
            final Set<Shard> validShards)
    {
        // & Serializable is very important as that function will be passed around by Spark, and
        // functions are not serializable by default.
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            if (!validShards.isEmpty() && !validShards.contains(shard))
            {
                logger.debug("Ignoring loading request for invalid shard {}", shard);
                return Optional.empty();
            }

            final Optional<Resource> cachedAtlasResource = atlasCache.get(country, shard);
            if (cachedAtlasResource.isPresent())
            {
                logger.debug("Cache hit, returning loaded atlas for shard {}", shard);
                return Optional.ofNullable(ATLAS_LOADER.load(cachedAtlasResource.get()));
            }
            logger.debug("No atlas file found for shard {}", shard);
            return Optional.empty();
        };
    }

    /**
     * @param sparkContext
     *            Spark context (or configuration) as a key-value map
     * @param previousOutputForDelta
     *            Previous Atlas generation delta output location
     * @return A Spark {@link PairFlatMapFunction} that takes a tuple of a country shard name and
     *         atlas file and returns all the {@link AtlasDelta} for the country
     */
    protected static PairFlatMapFunction<Tuple2<String, Atlas>, String, AtlasDelta> computeAtlasDelta(
            final Map<String, String> sparkContext, final String previousOutputForDelta)
    {
        return tuple ->
        {
            final String countryShardName = tuple._1();
            final Atlas current = tuple._2();
            logger.info("Starting computing deltas for Atlas {}", current.getName());
            final Time start = Time.now();
            final List<Tuple2<String, AtlasDelta>> result = new ArrayList<>();
            try
            {
                final Optional<Atlas> alter = new AtlasLocator(sparkContext).atlasForShard(
                        SparkFileHelper.combine(previousOutputForDelta,
                                StringList.split(countryShardName,
                                        CountryShard.COUNTRY_SHARD_SEPARATOR).get(0)),
                        countryShardName);
                if (alter.isPresent())
                {
                    logger.info("Printing memory after other Atlas loaded for Delta {}",
                            current.getName());
                    Memory.printCurrentMemory();
                    final AtlasDelta delta = new AtlasDelta(current, alter.get()).generate();
                    result.add(new Tuple2<>(countryShardName, delta));
                }
            }
            catch (final Exception e)
            {
                logger.error("Skipping! Could not generate deltas for {}", current.getName(), e);
            }
            logger.info("Finished computing deltas for Atlas {} in {}", current.getName(),
                    start.elapsedSince());
            return result;
        };
    }

    /**
     * @param sharding
     *            The sharding tree
     * @return a Spark {@link PairFunction} that processes a shard to Atlas tuple, and constructs a
     *         {@link AtlasStatistics} for each shard.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, AtlasStatistics> generateAtlasStatistics(
            final Broadcast<Sharding> sharding)
    {
        return tuple ->
        {
            final String shardName = tuple._1();
            logger.info("Starting generating Atlas statistics for {}", shardName);
            final Time start = Time.now();
            final Counter counter = new Counter().withSharding(sharding.getValue());
            counter.setCountsDefinition(Counter.POI_COUNTS_DEFINITION.getDefault());
            AtlasStatistics statistics = new AtlasStatistics();
            try
            {
                statistics = counter.processAtlas(tuple._2());
                logger.info("Finished generating Atlas statistics for {} in {}", shardName,
                        start.elapsedSince());
            }
            catch (final Exception e)
            {
                logger.error("Building Atlas Statistics for {} failed!", shardName, e);
            }
            return new Tuple2<>(shardName, statistics);
        };
    }

    /**
     * @param boundaries
     *            The {@link CountryBoundaryMap} to use for pbf to atlas generation
     * @param sparkContext
     *            Spark context (or configuration) as a key-value map
     * @param loadingOptions
     *            The basic required properties to create an {@link AtlasLoadingOption}
     * @param pbfContext
     *            The context explaining where to find the PBFs
     * @param atlasScheme
     *            The folder structure of the output atlas
     * @return a Spark {@link PairFunction} that processes an {@link AtlasGenerationTask}, loads the
     *         PBF for the task's shard, generates the raw atlas for the shard and outputs a shard
     *         name to raw atlas tuple.
     */
    protected static PairFunction<AtlasGenerationTask, String, Atlas> generateRawAtlas(
            final Broadcast<CountryBoundaryMap> boundaries, final Map<String, String> sparkContext,
            final Broadcast<Map<String, String>> loadingOptions, final PbfContext pbfContext,
            final SlippyTilePersistenceScheme atlasScheme)
    {
        return task ->
        {
            final String countryName = task.getCountry();
            final Shard shard = task.getShard();
            final String name = countryName + CountryShard.COUNTRY_SHARD_SEPARATOR
                    + shard.getName();
            logger.info("Starting creating raw Atlas {}", name);
            final Time start = Time.now();

            // Set the country code that is being processed!
            final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                    .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());
            atlasLoadingOption.setAdditionalCountryCodes(countryName);

            // Build the PbfLoader
            final PbfLoader loader = new PbfLoader(pbfContext, sparkContext, boundaries.getValue(),
                    atlasLoadingOption,
                    loadingOptions.getValue().get(AtlasGeneratorParameters.CODE_VERSION.getName()),
                    loadingOptions.getValue().get(AtlasGeneratorParameters.DATA_VERSION.getName()),
                    task.getAllShards());

            // Generate the raw Atlas for this shard
            final Atlas atlas;
            try
            {
                atlas = loader.generateRawAtlas(countryName, shard);
            }
            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException("Building raw Atlas {} failed!", name, e);
            }

            logger.info("Finished creating raw Atlas {} in {}", name, start.elapsedSince());

            // Report on memory usage
            logger.info("Printing memory after loading raw Atlas {}", name);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            return new Tuple2<>(
                    name + CountryShard.COUNTRY_SHARD_SEPARATOR + atlasScheme.getScheme(), atlas);
        };
    }

    protected static Function2<NamedAtlasStatistics, NamedAtlasStatistics, NamedAtlasStatistics> reduceAtlasStatistics()
    {
        return (left, right) ->
        {
            try
            {
                return new NamedAtlasStatistics(left.getName(), AtlasStatistics
                        .merge(left.getAtlasStatistics(), right.getAtlasStatistics()));
            }
            catch (final Throwable e) // NOSONAR
            {
                logger.error(
                        "Unable to merge AtlasStatistics for {}! Returning the first one only.\nLeft:\n{}\nRight:\n{}",
                        left.getName(), left.getAtlasStatistics(), right.getAtlasStatistics(), e);
                return left;
            }
        };
    }

    /**
     * @param boundaries
     *            The {@link CountryBoundaryMap} required to create an {@link AtlasLoadingOption}
     * @param sharding
     *            The {@link Sharding} strategy
     * @param sparkContext
     *            Spark context (or configuration) as a key-value map
     * @param loadingOptions
     *            The basic required properties to create an {@link AtlasLoadingOption}
     * @param slicedRawAtlasPath
     *            The path where the sliced raw atlas files were saved
     * @param atlasScheme
     *            The folder structure of the output atlas
     * @param tasks
     *            The list of {@link AtlasGenerationTask}s used to grab all possible {@link Shard}s
     *            for a country
     * @return a Spark {@link PairFunction} that processes a tuple of shard-name and sliced raw
     *         atlas, sections the sliced raw atlas and returns the final sectioned (and sliced) raw
     *         atlas for that shard name.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sectionRawAtlas(
            final Broadcast<CountryBoundaryMap> boundaries, final Broadcast<Sharding> sharding,
            final Map<String, String> sparkContext,
            final Broadcast<Map<String, String>> loadingOptions, final String slicedRawAtlasPath,
            final SlippyTilePersistenceScheme atlasScheme, final List<AtlasGenerationTask> tasks)
    {
        return tuple ->
        {
            final Atlas atlas;
            final Time start = Time.now();
            try
            {
                final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                        .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());

                // Calculate the shard, country name and possible shards
                final String countryShardString = tuple._1();
                final CountryShard countryShard = CountryShard.forName(countryShardString);
                final String country = countryShard.getCountry();
                final Set<Shard> possibleShards = getAllShardsForCountry(tasks, country);

                // Instantiate the cache
                final HadoopAtlasFileCache atlasCache = new HadoopAtlasFileCache(slicedRawAtlasPath,
                        atlasScheme, sparkContext);
                // Create the fetcher
                final Function<Shard, Optional<Atlas>> slicedRawAtlasFetcher = AtlasGeneratorHelper
                        .atlasFetcher(atlasCache, country, possibleShards);
                // Section the Atlas
                atlas = new WaySectionProcessor(countryShard.getShard(), atlasLoadingOption,
                        sharding.getValue(), slicedRawAtlasFetcher).run();
            }
            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException("Sectioning Raw Atlas for {} failed!", tuple._1(), e);
            }

            if (logger.isInfoEnabled())
            {
                logger.info("Finished sectioning raw Atlas for {} in {}", tuple._1(),
                        start.elapsedSince());

                // Report on memory usage
                logger.info("Printing memory after loading final Atlas for {}", tuple._1());
                Memory.printCurrentMemory();
            }

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), atlas);
        };
    }

    /**
     * @param boundaries
     *            The {@link CountryBoundaryMap} to use for slicing
     * @return a Spark {@link PairFunction} that processes a tuple of shard-name and raw atlas,
     *         slices the raw atlas and returns the sliced raw atlas for that shard name.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sliceRawAtlas(
            final Broadcast<CountryBoundaryMap> boundaries)
    {
        return tuple ->
        {
            final Atlas slicedAtlas;

            // Grab the tuple contents
            final String shardName = tuple._1();
            final Atlas rawAtlas = tuple._2();
            logger.info("Starting slicing raw Atlas {}", rawAtlas.getName());
            final Time start = Time.now();

            try
            {
                // Extract the country code
                final String countryName = shardName.split(CountryShard.COUNTRY_SHARD_SEPARATOR)[0];
                if (countryName != null)
                {
                    // Slice the Atlas
                    slicedAtlas = new RawAtlasCountrySlicer(countryName, boundaries.getValue())
                            .slice(rawAtlas);
                }
                else
                {
                    slicedAtlas = null;
                    logger.error("Unable to extract valid country code for {}", shardName);
                }
            }

            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException("Slicing raw Atlas failed for {}", shardName, e);
            }

            logger.info("Finished slicing raw Atlas for {} in {}", shardName, start.elapsedSince());

            // Report on memory usage
            logger.info("Printing memory after loading sliced raw Atlas for {}", shardName);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), slicedAtlas);
        };
    }

    private static Set<Shard> getAllShardsForCountry(final List<AtlasGenerationTask> tasks,
            final String country)
    {
        for (final AtlasGenerationTask task : tasks)
        {
            if (task.getCountry().equals(country))
            {
                // We found the target country, return its shards
                return task.getAllShards();
            }
        }
        logger.debug("Could not find shards for {}", country);
        return Collections.emptySet();
    }

    /**
     * Hide constructor for this utility class.
     */
    private AtlasGeneratorHelper()
    {
    }
}
