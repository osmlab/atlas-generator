package org.openstreetmap.atlas.generator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

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
import org.openstreetmap.atlas.geography.atlas.multi.MultiAtlas;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.atlas.raw.sectioning.WaySectionProcessor;
import org.openstreetmap.atlas.geography.atlas.raw.slicing.RawAtlasCountrySlicer;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.geography.atlas.statistics.Counter;
import org.openstreetmap.atlas.geography.atlas.sub.AtlasCutType;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.tags.Taggable;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;
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
 * @author samg
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

    public static final String STARTED_MESSAGE = "Starting task {} for shard {}";
    public static final String FINISHED_MESSAGE = "Finished task {} for shard {} in {}";
    public static final String ERROR_MESSAGE = "Error during task {} for shard {} :";
    public static final String MEMORY_MESSAGE = "Printing memory after task {} for shard{} :";

    private static final long serialVersionUID = 1300098384789754747L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasGeneratorHelper.class);
    private static final String LINE_SLICED_SUBATLAS_NAMESPACE = "lineSlicedSubAtlas";
    private static final String LINE_SLICED_ATLAS_NAMESPACE = "lineSlicedAtlas";

    private static final AtlasResourceLoader ATLAS_LOADER = new AtlasResourceLoader();

    @SuppressWarnings("unchecked")
    public static Function<Shard, Optional<Atlas>> atlasFetcher(
            final HadoopAtlasFileCache lineSlicedSubAtlasCache,
            final HadoopAtlasFileCache lineSlicedAtlasCache, final CountryBoundaryMap boundaries,
            final String countryBeingSliced, final Shard initialShard)
    {
        // & Serializable is very important as that function will be passed around by Spark, and
        // functions are not serializable by default.
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            final StringList countriesForShardList = boundaries
                    .countryCodesOverlappingWith(shard.bounds());
            final Set<String> countriesForShard = new HashSet<>();
            countriesForShardList.forEach(countriesForShard::add);

            final Set<Resource> atlasResources = new HashSet<>();
            // If this is the initial shard, load in all sliced lines not just water relation lines
            if (shard.equals(initialShard))
            {
                final Optional<Resource> cachedInitialShardResource = lineSlicedAtlasCache
                        .get(countryBeingSliced, shard);
                if (cachedInitialShardResource.isPresent())
                {
                    // Add the full line sliced data to the multi atlas, then remove this country
                    // from the list of countries with the shard-- the remaining countries will have
                    // their shards added before returning and we don't want to double add this
                    // initial shard
                    atlasResources.add(cachedInitialShardResource.get());
                    countriesForShard.remove(countryBeingSliced);
                }
                else
                {
                    logger.error("No Atlas file found for initial Shard {}!", shard);
                    return Optional.empty();
                }
            }

            // Multi-atlas all remaining sliced water relation data together and return that
            countriesForShard.forEach(country ->
            {
                final Optional<Resource> cachedAtlas = lineSlicedSubAtlasCache.get(country, shard);
                if (cachedAtlas.isPresent())
                {
                    logger.debug("Cache hit, loading sliced subAtlas for Shard {} and country {}",
                            shard, country);
                    atlasResources.add(cachedAtlas.get());
                }
            });
            return Optional.ofNullable(MultiAtlas.loadFromPackedAtlas(atlasResources));
        };
    }

    @SuppressWarnings("unchecked")
    public static Function<Shard, Optional<Atlas>> atlasFetcher(
            final HadoopAtlasFileCache subAtlasCache, final HadoopAtlasFileCache atlasCache,
            final String countryBeingSliced, final Shard initialShard)
    {
        // & Serializable is very important as that function will be passed around by Spark, and
        // functions are not serializable by default.
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            final Optional<Resource> cachedInitialShardResource;
            // If this is the initial shard, load from full Atlas cache, not subAtlas cache
            if (shard.equals(initialShard))
            {
                cachedInitialShardResource = atlasCache.get(countryBeingSliced, shard);
            }
            else
            {
                // Otherwise, load from subatlas cache
                cachedInitialShardResource = subAtlasCache.get(countryBeingSliced, shard);
            }
            if (!cachedInitialShardResource.isPresent())
            {
                logger.error("No Atlas file found for initial Shard {}!", shard);
                return Optional.empty();
            }
            return Optional.ofNullable(ATLAS_LOADER.load(cachedInitialShardResource.get()));
        };
    }

    /**
     * @param atlasCache
     *            The cache object for the Atlas files
     * @param country
     *            The country to look for
     * @param validShards
     *            All available shards for given country, to avoid fetching shards that do not exist
     * @return A function that returns an {@link Atlas} given a {@link Shard}
     */
    @SuppressWarnings("unchecked")
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
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.DELTAS.getDescription(),
                    countryShardName);
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
                    logger.info(MEMORY_MESSAGE, AtlasGeneratorJobGroup.DELTAS.getDescription(),
                            countryShardName);
                    Memory.printCurrentMemory();
                    final AtlasDelta delta = new AtlasDelta(current, alter.get()).generate();
                    result.add(new Tuple2<>(countryShardName, delta));
                }
            }
            catch (final Exception e)
            {
                logger.error(ERROR_MESSAGE, AtlasGeneratorJobGroup.DELTAS.getDescription(),
                        countryShardName, e);
            }
            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.DELTAS.getDescription(),
                    countryShardName, start.elapsedSince().asMilliseconds());
            return result.iterator();
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
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.SHARD_STATISTICS.getDescription(),
                    shardName);
            final Time start = Time.now();
            final Counter counter = new Counter().withSharding(sharding.getValue());
            counter.setCountsDefinition(Counter.POI_COUNTS_DEFINITION.getDefault());
            AtlasStatistics statistics = new AtlasStatistics();
            try
            {
                statistics = counter.processAtlas(tuple._2());
            }
            catch (final Exception e)
            {
                logger.error(ERROR_MESSAGE,
                        AtlasGeneratorJobGroup.SHARD_STATISTICS.getDescription(), shardName, e);
            }
            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.SHARD_STATISTICS.getDescription(),
                    shardName, start.elapsedSince().asMilliseconds());
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
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.RAW.getDescription(), name);
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
                throw new CoreException(ERROR_MESSAGE, AtlasGeneratorJobGroup.RAW.getDescription(),
                        name, e);
            }

            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.RAW.getDescription(), name,
                    start.elapsedSince().asMilliseconds());

            // Report on memory usage
            logger.info(MEMORY_MESSAGE, AtlasGeneratorJobGroup.RAW.getDescription(), name);
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
     * @param edgeSubAtlasPath
     *            The path where the edge-only sub atlas files were saved
     * @param slicedAtlasPath
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
    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sectionAtlas(
            final Broadcast<CountryBoundaryMap> boundaries, final Broadcast<Sharding> sharding,
            final Map<String, String> sparkContext,
            final Broadcast<Map<String, String>> loadingOptions, final String edgeSubAtlasPath,
            final String slicedAtlasPath, final SlippyTilePersistenceScheme atlasScheme,
            final List<AtlasGenerationTask> tasks)
    {
        return tuple ->
        {
            final String countryShardString = tuple._1();
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.FULLY_SLICED.getDescription(),
                    countryShardString);
            final Time start = Time.now();

            final CountryShard countryShard = CountryShard.forName(countryShardString);
            final String country = countryShard.getCountry();
            final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                    .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());
            // Instantiate the caches
            final HadoopAtlasFileCache atlasCache = new HadoopAtlasFileCache(slicedAtlasPath,
                    atlasScheme, sparkContext);
            final HadoopAtlasFileCache edgeSubCache = new HadoopAtlasFileCache(edgeSubAtlasPath,
                    atlasScheme, sparkContext);
            // Create the fetcher
            final Function<Shard, Optional<Atlas>> slicedRawAtlasFetcher = AtlasGeneratorHelper
                    .atlasFetcher(edgeSubCache, atlasCache, country, countryShard.getShard());

            final Atlas atlas;
            try
            {
                // Section the Atlas
                atlas = new WaySectionProcessor(countryShard.getShard(), atlasLoadingOption,
                        sharding.getValue(), slicedRawAtlasFetcher).run();
            }
            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException(ERROR_MESSAGE,
                        AtlasGeneratorJobGroup.WAY_SECTIONED_PBF.getDescription(),
                        countryShardString, e);
            }

            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.WAY_SECTIONED_PBF.getDescription(),
                    countryShardString, start.elapsedSince().asMilliseconds());
            // Report on memory usage
            logger.info(MEMORY_MESSAGE, AtlasGeneratorJobGroup.WAY_SECTIONED_PBF.getDescription(),
                    countryShardString);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), atlas);
        };
    }

    /**
     * @param boundaries
     *            The {@link CountryBoundaryMap} to use for slicing
     * @param loadingOptions
     *            The basic required properties to create an {@link AtlasLoadingOption}
     * @return a Spark {@link PairFunction} that processes a tuple of shard-name and raw atlas,
     *         slices the raw atlas and returns the sliced raw atlas for that shard name.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sliceLines(
            final Broadcast<CountryBoundaryMap> boundaries,
            final Broadcast<Map<String, String>> loadingOptions)
    {
        return tuple ->
        {
            // Grab the tuple contents
            final String shardName = tuple._1();
            final Atlas rawAtlas = tuple._2();
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.LINE_SLICED.getDescription(),
                    shardName);

            final Time start = Time.now();

            final Atlas slicedAtlas;
            try
            {
                // Extract the country code
                final String countryName = CountryShard.forName(shardName).getCountry();
                // Set the country code that is being processed!
                final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                        .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());
                atlasLoadingOption.setAdditionalCountryCodes(countryName);
                logger.error("Country codes during line slicing was: {}",
                        atlasLoadingOption.getCountryCodes());
                // Slice the Atlas
                slicedAtlas = new RawAtlasCountrySlicer(atlasLoadingOption).sliceLines(rawAtlas);
            }
            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException(ERROR_MESSAGE,
                        AtlasGeneratorJobGroup.LINE_SLICED.getDescription(), shardName, e);
            }

            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.LINE_SLICED.getDescription(),
                    shardName, start.elapsedSince().asMilliseconds());

            // Report on memory usage
            logger.info(MEMORY_MESSAGE, AtlasGeneratorJobGroup.LINE_SLICED.getDescription(),
                    shardName);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), slicedAtlas);
        };
    }

    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sliceRelations(
            final Broadcast<CountryBoundaryMap> boundaries,
            final Broadcast<Map<String, String>> loadingOptions, final Broadcast<Sharding> sharding,
            final String lineSlicedSubAtlasPath, final String lineSlicedAtlasPath,
            final SlippyTilePersistenceScheme atlasScheme, final Map<String, String> sparkContext)
    {
        return tuple ->
        {
            // Grab the tuple contents
            final String shardName = tuple._1();
            logger.info(STARTED_MESSAGE, AtlasGeneratorJobGroup.FULLY_SLICED.getDescription(),
                    shardName);
            final Time start = Time.now();

            final Atlas slicedAtlas;
            try
            {
                // Calculate the shard, country name and possible shards
                final String countryShardString = tuple._1();
                final CountryShard countryShard = CountryShard.forName(countryShardString);
                final String country = countryShard.getCountry();

                final HadoopAtlasFileCache lineSlicedSubAtlasCache = new HadoopAtlasFileCache(
                        lineSlicedSubAtlasPath, LINE_SLICED_SUBATLAS_NAMESPACE, atlasScheme,
                        sparkContext);

                final HadoopAtlasFileCache lineSlicedAtlasCache = new HadoopAtlasFileCache(
                        lineSlicedAtlasPath, LINE_SLICED_ATLAS_NAMESPACE, atlasScheme,
                        sparkContext);

                final Function<Shard, Optional<Atlas>> atlasFetcher = AtlasGeneratorHelper
                        .atlasFetcher(lineSlicedSubAtlasCache, lineSlicedAtlasCache,
                                boundaries.getValue(), country, countryShard.getShard());

                // Set the country code that is being processed!
                final AtlasLoadingOption atlasLoadingOption = AtlasGeneratorParameters
                        .buildAtlasLoadingOption(boundaries.getValue(), loadingOptions.getValue());
                atlasLoadingOption.setAdditionalCountryCodes(country);
                slicedAtlas = new RawAtlasCountrySlicer(atlasLoadingOption, sharding.getValue(),
                        atlasFetcher).sliceRelations(countryShard.getShard());
            }

            catch (final Throwable e) // NOSONAR
            {
                throw new CoreException(ERROR_MESSAGE,
                        AtlasGeneratorJobGroup.FULLY_SLICED.getDescription(), shardName, e);
            }

            logger.info(FINISHED_MESSAGE, AtlasGeneratorJobGroup.FULLY_SLICED.getDescription(),
                    shardName, start.elapsedSince().asMilliseconds());

            // Report on memory usage
            logger.info(MEMORY_MESSAGE, AtlasGeneratorJobGroup.FULLY_SLICED.getDescription(),
                    shardName);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), slicedAtlas);
        };
    }

    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> subatlas(
            final ConfiguredFilter filter, final AtlasCutType cutType)
    {
        return (Serializable & PairFunction<Tuple2<String, Atlas>, String, Atlas>) tuple ->
        {
            final Atlas subAtlas;
            // Grab the tuple contents
            final String shardName = tuple._1();
            final Atlas originalAtlas = tuple._2();
            logger.info("Starting sub Atlas for for Atlas {}", originalAtlas.getName());
            final Time start = Time.now();
            try
            {
                // Slice the Atlas
                final Optional<Atlas> subAtlasOptional = originalAtlas.subAtlas(filter::test,
                        cutType);
                if (subAtlasOptional.isPresent())
                {
                    subAtlas = subAtlasOptional.get();
                }
                else
                {
                    subAtlas = null;
                    logger.error("Unable to extract valid subAtlas code for {}", shardName);
                }
            }
            catch (final Exception e) // NOSONAR
            {
                throw new CoreException("Sub Atlas failed for {}", shardName, e);
            }
            logger.info("Finished sub Atlas for {} in {}", shardName, start.elapsedSince());
            // Report on memory usage
            logger.info("Printing memory after loading sub Atlas for {}", shardName);
            Memory.printCurrentMemory();
            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), subAtlas);
        };
    }

    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> subatlas(
            final Predicate<Taggable> filter, final AtlasCutType cutType)
    {
        return (Serializable & PairFunction<Tuple2<String, Atlas>, String, Atlas>) tuple ->
        {
            final Atlas subAtlas;

            // Grab the tuple contents
            final String shardName = tuple._1();
            final Atlas originalAtlas = tuple._2();
            logger.info("Starting sub Atlas for for Atlas {}", originalAtlas.getName());
            final Time start = Time.now();

            try
            {
                // Slice the Atlas
                final Optional<Atlas> subAtlasOptional = originalAtlas.subAtlas(filter::test,
                        cutType);
                if (subAtlasOptional.isPresent())
                {
                    subAtlas = subAtlasOptional.get();
                }
                else
                {
                    subAtlas = null;
                    logger.error("Unable to extract valid subAtlas code for {}", shardName);
                }

            }

            catch (final Exception e) // NOSONAR
            {
                throw new CoreException("Sub Atlas failed for {}", shardName, e);
            }

            logger.info("Finished sub Atlas for {} in {}", shardName, start.elapsedSince());

            // Report on memory usage
            logger.info("Printing memory after loading sub Atlas for {}", shardName);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            return new Tuple2<>(tuple._1(), subAtlas);
        };
    }

    /**
     * Hide constructor for this utility class.
     */
    private AtlasGeneratorHelper()
    {
    }
}
