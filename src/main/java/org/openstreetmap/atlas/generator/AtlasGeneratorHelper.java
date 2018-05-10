package org.openstreetmap.atlas.generator;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
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
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.tags.filters.ConfiguredTaggableFilter;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;
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
    private static final long serialVersionUID = 1300098384789754747L;
    private static final Logger logger = LoggerFactory.getLogger(AtlasGeneratorHelper.class);

    private static final String GZIPPED_ATLAS_EXTENSION = FileSuffix.ATLAS.toString()
            + FileSuffix.GZIP.toString();
    private static final String ATLAS_EXTENSION = FileSuffix.ATLAS.toString();
    private static final AtlasResourceLoader ATLAS_LOADER = new AtlasResourceLoader();

    /**
     * @param atlasDirectory
     *            The path of the folder containing the Atlas files, in format CTRY_z-x-y.atlas.gz
     * @param temporaryDirectory
     *            The path of the temporary folder to download Atlas files if they are not
     *            downloaded already
     * @param country
     *            The country to look for
     * @param sparkContext
     *            The Spark configuration as a map (to allow the creation of the proper FileSystem)
     * @param validShards
     *            All available shards for given country, to avoid fetching shards that do not exist
     * @return A function that returns an {@link Atlas} given a {@link Shard}
     */
    protected static Function<Shard, Optional<Atlas>> atlasFetcher(final String atlasDirectory,
            final String temporaryDirectory, final String country,
            final Map<String, String> sparkContext, final Set<Shard> validShards)
    {
        // & Serializable is very important as that function will be passed around by Spark, and
        // functions are not serializable by default.
        return (Function<Shard, Optional<Atlas>> & Serializable) shard ->
        {
            if (!validShards.isEmpty() && !validShards.contains(shard))
            {
                return Optional.empty();
            }

            // Check if non-gzipped file exists in final temporary directory
            final String pathFromTemporaryDirectory = SparkFileHelper.combine(temporaryDirectory,
                    String.format("%s%s", getAtlasName(country, shard), ATLAS_EXTENSION));
            final File fileFromTemporaryDirectory = new File(pathFromTemporaryDirectory);

            // Download file to disk if it is not cached already
            if (!fileFromTemporaryDirectory.exists())
            {
                try
                {
                    String path = SparkFileHelper.combine(atlasDirectory,
                            String.format("%s%s", getAtlasName(country, shard), ATLAS_EXTENSION));

                    if (!fileExists(path, sparkContext))
                    {
                        path = SparkFileHelper.combine(atlasDirectory, String.format("%s%s",
                                getAtlasName(country, shard), GZIPPED_ATLAS_EXTENSION));
                    }

                    final Resource fileFromNetwork = FileSystemHelper.resource(path, sparkContext);
                    final File temporaryLocalFile = File
                            .temporary(getAtlasName(country, shard) + "-", ATLAS_EXTENSION);

                    System.out.println("Downloaded atlas from " + path
                            + " and is found as temp file " + temporaryLocalFile.getAbsolutePath());

                    // FileSystemHelper.resource sets the Decompressor on the Resource for us, so
                    // this call will gunzip the file
                    fileFromNetwork.copyTo(temporaryLocalFile);

                    // Before making the move, check again if file is there or not
                    if (!fileFromTemporaryDirectory.exists())
                    {
                        try
                        {
                            Files.move(Paths.get(temporaryLocalFile.getPath()),
                                    Paths.get(fileFromTemporaryDirectory.getPath()),
                                    StandardCopyOption.ATOMIC_MOVE);
                        }
                        catch (final FileAlreadyExistsException e)
                        {
                            logger.warn("Failed to rename file, but file exists already.", e);
                        }
                        catch (final Exception e)
                        {
                            logger.warn("Failed to rename file on local disk.", e);
                        }
                    }
                }
                catch (final Exception e)
                {
                    logger.warn("Failed to cache file on local disk.", e);
                }
            }

            // If we were able to find the file on local disk, then load from there
            if (fileFromTemporaryDirectory.exists())
            {
                System.out.println("AtlasExisted - Cache Hit: "
                        + fileFromTemporaryDirectory.getAbsolutePath());
                return loadAtlas(fileFromTemporaryDirectory);
            }
            else
            {
                logger.warn("Falling back to Atlas file hosted on {} for shard {}.", atlasDirectory,
                        shard.getName());
                final String path = SparkFileHelper.combine(atlasDirectory,
                        String.format("%s%s", getAtlasName(country, shard), ATLAS_EXTENSION));
                final Resource fileFromNetwork = FileSystemHelper.resource(path, sparkContext);
                return loadAtlas(fileFromNetwork);
            }
        };
    }

    protected static AtlasLoadingOption buildAtlasLoadingOption(final CountryBoundaryMap boundaries,
            final Map<String, String> sparkContext, final Map<String, String> properties)
    {
        final AtlasLoadingOption atlasLoadingOption = AtlasLoadingOption
                .createOptionWithAllEnabled(boundaries);

        // Apply all configurations
        final String edgeConfiguration = properties
                .get(AtlasGenerator.EDGE_CONFIGURATION.getName());
        if (edgeConfiguration != null)
        {
            atlasLoadingOption
                    .setEdgeFilter(getTaggableFilterFrom(new StringResource(edgeConfiguration)));
        }

        final String waySectioningConfiguration = properties
                .get(AtlasGenerator.WAY_SECTIONING_CONFIGURATION.getName());
        if (waySectioningConfiguration != null)
        {
            atlasLoadingOption.setWaySectionFilter(
                    getTaggableFilterFrom(new StringResource(waySectioningConfiguration)));
        }

        final String pbfNodeConfiguration = properties
                .get(AtlasGenerator.PBF_NODE_CONFIGURATION.getName());
        if (pbfNodeConfiguration != null)
        {
            atlasLoadingOption.setOsmPbfNodeFilter(
                    getTaggableFilterFrom(new StringResource(pbfNodeConfiguration)));
        }

        final String pbfWayConfiguration = properties
                .get(AtlasGenerator.PBF_WAY_CONFIGURATION.getName());
        if (pbfWayConfiguration != null)
        {
            atlasLoadingOption.setOsmPbfWayFilter(
                    getTaggableFilterFrom(new StringResource(pbfWayConfiguration)));
        }

        final String pbfRelationConfiguration = properties
                .get(AtlasGenerator.PBF_RELATION_CONFIGURATION.getName());
        if (pbfRelationConfiguration != null)
        {
            atlasLoadingOption.setOsmPbfRelationFilter(
                    getTaggableFilterFrom(new StringResource(pbfRelationConfiguration)));
        }

        return atlasLoadingOption;
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
                        previousOutputForDelta + "/"
                                + StringList.split(countryShardName,
                                        CountryShard.COUNTRY_SHARD_SEPARATOR).get(0),
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
            final Sharding sharding)
    {
        return tuple ->
        {
            logger.info("Starting generating Atlas statistics for {}", tuple._1());
            final Time start = Time.now();
            final Counter counter = new Counter().withSharding(sharding);
            counter.setCountsDefinition(Counter.POI_COUNTS_DEFINITION.getDefault());
            final AtlasStatistics statistics;
            try
            {
                statistics = counter.processAtlas(tuple._2());
            }
            catch (final Exception e)
            {
                throw new CoreException("Building Atlas Statistics for {} failed!", tuple._1(), e);
            }
            logger.info("Finished generating Atlas statistics for {} in {}", tuple._1(),
                    start.elapsedSince());
            final Tuple2<String, AtlasStatistics> result = new Tuple2<>(tuple._1(), statistics);
            return result;
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
            final CountryBoundaryMap boundaries, final Map<String, String> sparkContext,
            final Map<String, String> loadingOptions, final PbfContext pbfContext,
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
            final AtlasLoadingOption atlasLoadingOption = buildAtlasLoadingOption(boundaries,
                    sparkContext, loadingOptions);
            atlasLoadingOption.setAdditionalCountryCodes(countryName);

            // Build the PbfLoader
            final PbfLoader loader = new PbfLoader(pbfContext, sparkContext, boundaries,
                    atlasLoadingOption, loadingOptions.get(AtlasGenerator.CODE_VERSION.getName()),
                    loadingOptions.get(AtlasGenerator.DATA_VERSION.getName()), task.getAllShards());

            // Generate the raw Atlas for this shard
            final Atlas atlas;
            try
            {
                atlas = loader.generateRawAtlas(countryName, shard);
            }
            catch (final Throwable e)
            {
                throw new CoreException("Building raw Atlas {} failed!", name, e);
            }

            logger.info("Finished creating raw Atlas {} in {}", name, start.elapsedSince());

            // Report on memory usage
            logger.info("Printing memory after loading raw Atlas {}", name);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            final Tuple2<String, Atlas> result = new Tuple2<>(
                    name + CountryShard.COUNTRY_SHARD_SEPARATOR + atlasScheme.getScheme(), atlas);
            return result;
        };
    }

    protected static StandardConfiguration getStandardConfigurationFrom(
            final Resource configurationResource)
    {
        return new StandardConfiguration(configurationResource);
    }

    protected static ConfiguredTaggableFilter getTaggableFilterFrom(
            final Resource configurationResource)
    {
        return new ConfiguredTaggableFilter(getStandardConfigurationFrom(configurationResource));
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
     * @param tasks
     *            The list of {@link AtlasGenerationTask}s used to grab all possible {@link Shard}s
     *            for a country
     * @return a Spark {@link PairFunction} that processes a tuple of shard-name and sliced raw
     *         atlas, sections the sliced raw atlas and returns the final sectioned (and sliced) raw
     *         atlas for that shard name.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sectionRawAtlas(
            final CountryBoundaryMap boundaries, final Sharding sharding,
            final Map<String, String> sparkContext, final Map<String, String> loadingOptions,
            final String slicedRawAtlasPath, final List<AtlasGenerationTask> tasks)
    {
        return tuple ->
        {
            final Atlas atlas;
            final Time start = Time.now();
            try
            {
                final AtlasLoadingOption atlasLoadingOption = buildAtlasLoadingOption(boundaries,
                        sparkContext, loadingOptions);

                // Calculate the shard, country name and possible shards
                final String countryShardString = tuple._1();
                final CountryShard countryShard = CountryShard.forName(countryShardString);
                final String country = countryShard.getCountry();
                final Set<Shard> possibleShards = getAllShardsForCountry(tasks, country);

                logger.info("Started sectioning raw Atlas for {}", countryShardString);

                // Create the fetcher
                final Function<Shard, Optional<Atlas>> slicedRawAtlasFetcher = AtlasGeneratorHelper
                        .atlasFetcher(SparkFileHelper.combine(slicedRawAtlasPath, country),
                                System.getProperty("java.io.tmpdir"), country, sparkContext,
                                possibleShards);

                // Section the Atlas
                atlas = new WaySectionProcessor(countryShard.getShard(), atlasLoadingOption,
                        sharding, slicedRawAtlasFetcher).run();
            }
            catch (final Throwable e)
            {
                throw new CoreException("Sectioning Raw Atlas for {} failed!", tuple._1(), e);
            }

            logger.info("Finished sectioning raw Atlas for {} in {}", tuple._1(),
                    start.elapsedSince());

            // Report on memory usage
            logger.info("Printing memory after loading final Atlas for {}", tuple._1());
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            final Tuple2<String, Atlas> result = new Tuple2<>(tuple._1(), atlas);
            return result;
        };
    }

    /**
     * @param country
     *            The country being processed
     * @param boundaries
     *            The {@link CountryBoundaryMap} to use for slicing
     * @return a Spark {@link PairFunction} that processes a tuple of shard-name and raw atlas,
     *         slices the raw atlas and returns the sliced raw atlas for that shard name.
     */
    protected static PairFunction<Tuple2<String, Atlas>, String, Atlas> sliceRawAtlas(
            final String country, final CountryBoundaryMap boundaries)
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
                slicedAtlas = new RawAtlasCountrySlicer(country, boundaries).slice(rawAtlas);
            }
            catch (final Throwable e)
            {
                throw new CoreException("Slicing raw Atlas failed for {}", shardName, e);
            }

            logger.info("Finished slicing raw Atlas for {} in {}", shardName, start.elapsedSince());

            // Report on memory usage
            logger.info("Printing memory after loading sliced raw Atlas for {}", shardName);
            Memory.printCurrentMemory();

            // Output the Name/Atlas couple
            final Tuple2<String, Atlas> result = new Tuple2<>(tuple._1(), slicedAtlas);
            return result;
        };
    }

    private static boolean fileExists(final String path, final Map<String, String> configuration)
    {
        final FileSystem fileSystem = new FileSystemCreator().get(path, configuration);
        try
        {
            return fileSystem.exists(new Path(path));
        }
        catch (IllegalArgumentException | IOException e)
        {
            logger.warn("can't determine if {} exists", path);
            return false;
        }
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

    private static String getAtlasName(final String country, final Shard shard)
    {
        return String.format("%s_%s", country, shard.getName());
    }

    private static Optional<Atlas> loadAtlas(final Resource file)
    {
        return Optional.ofNullable(ATLAS_LOADER.load(file));
    }

    /**
     * Hide constructor for this utility class.
     */
    private AtlasGeneratorHelper()
    {
    }
}
