package org.openstreetmap.atlas.generator.tools.spark.rdd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.boundary.CountryShardListing;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.maps.MultiMapWithSet;
import org.openstreetmap.atlas.utilities.scalars.Distance;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import scala.Tuple2;

/**
 * @author tian_du
 */
public class ShardedAtlasRDDLoader
{
    private static final AtlasResourceLoader ATLAS_LOADER = new AtlasResourceLoader();
    private static final Logger logger = LoggerFactory
            .getLogger(ShardedAtlasRDDLoader.class.getCanonicalName());

    /**
     * Generate JavaPairRDD with CountryShard as the key and the Atlas as the value. The Atlas is
     * inside a list to keep the API consistent.
     *
     * @param sparkContext
     *            - Spark context
     * @param countries
     *            - country list
     * @param boundaries
     *            - boundaries
     * @param atlasSharding
     *            - atlas sharding information
     * @param atlasDirectory
     *            - directory that contains the atlas files
     * @param configurationMap
     *            - file system configuration
     * @return - RDD with org.openstreetmap.atlas.geography.sharding.CountryShard as the key and its
     *         org.openstreetmap.atlas.geography.atlas.Atlas inside a list
     */
    public static JavaPairRDD<CountryShard, List<Atlas>> generateCountryShardedAtlasRDD(
            final JavaSparkContext sparkContext, final StringList countries,
            final CountryBoundaryMap boundaries, final Sharding atlasSharding,
            final String atlasDirectory, final Map<String, String> configurationMap)
    {
        final JavaRDD<CountryShard> countryToAtlasShardMapping = loadCountryToAtlasShardMapping(
                boundaries, countries, atlasSharding, sparkContext);

        return countryToAtlasShardMapping.mapToPair(countryAndShard ->
        {
            final String country = countryAndShard.getCountry();
            final Shard shard = countryAndShard.getShard();
            final Atlas atlas = loadOneAtlasShard(countryAndShard.getCountry(),
                    countryAndShard.getShard().getName(), atlasDirectory, configurationMap);
            if (atlas != null)
            {
                logger.info("Loaded Atlas atlas_name={} atlas_size={} number_of_edges={}",
                        atlas.getName(), atlas.size(), atlas.numberOfEdges());
                final List<Atlas> atlases = Collections.singletonList(atlas);
                return new Tuple2<>(new CountryShard(country, shard), atlases);
            }
            else
            {
                logger.error("Atlas is null for CountryShard: {}", countryAndShard);
                return new Tuple2<>(new CountryShard(country, shard), null);
            }
        }).filter(tuple2 -> Objects.nonNull(tuple2._2()));
    }

    /**
     * Generate JavaPairRDD with CountryShard as the key and the Atlas inside the expanded bounding
     * box as the value.
     *
     * @param sparkContext
     *            - Spark context
     * @param expandingDistance
     *            - the distance will be used to expand on the current shard
     * @param countries
     *            - country list
     * @param boundaries
     *            - boundaries
     * @param atlasSharding
     *            - atlas sharding information
     * @param atlasDirectory
     *            - directory that contains the atlas files
     * @param configurationMap
     *            - file system configuration
     * @return RDD with org.openstreetmap.atlas.geography.sharding.CountryShard as the key and list
     *         of org.openstreetmap.atlas.geography.atlas.Atlas within the expanded bounding box
     */
    public static JavaPairRDD<CountryShard, List<Atlas>> generateExpandedCountryShardedAtlasRDD(
            final JavaSparkContext sparkContext, final Distance expandingDistance,
            final StringList countries, final CountryBoundaryMap boundaries,
            final Sharding atlasSharding, final String atlasDirectory,
            final Map<String, String> configurationMap)
    {
        final JavaRDD<CountryShard> countryToAtlasShardMapping = loadCountryToAtlasShardMapping(
                boundaries, countries, atlasSharding, sparkContext);

        final JavaPairRDD<CountryShard, CountryShard> countryShardToExpandedShardsMapping = countryToAtlasShardMapping
                .flatMapToPair(countryShard ->
                {
                    final String country = countryShard.getCountry();
                    final Shard centralShard = countryShard.getShard();
                    final Rectangle expandedBoundingBox = centralShard.bounds()
                            .expand(expandingDistance);
                    final Set<Shard> expandedShards = Sets.newHashSet();
                    atlasSharding.shards(expandedBoundingBox.bounds()).forEach(expandedShards::add);
                    return expandedShards.stream().map(shardFromExtension ->
                    {
                        return new Tuple2<>(new CountryShard(country, shardFromExtension),
                                new CountryShard(country, centralShard));
                    }).collect(Collectors.toList()).iterator();
                });

        final JavaPairRDD<CountryShard, List<Atlas>> countryShardedAtlas = generateCountryShardedAtlasRDD(
                sparkContext, countries, boundaries, atlasSharding, atlasDirectory,
                configurationMap);

        return countryShardToExpandedShardsMapping.join(countryShardedAtlas)
                .mapToPair(countryShardWithExpandedAtlas ->
                {
                    final String country = countryShardWithExpandedAtlas._1().getCountry();
                    final Shard centralShard = countryShardWithExpandedAtlas._2()._1().getShard();
                    final List<Atlas> atlases = countryShardWithExpandedAtlas._2()._2();
                    return new Tuple2<>(new CountryShard(country, centralShard), atlases);
                }).reduceByKey(ListUtils::union).filter(tuple2 -> Iterables
                        .size(((Tuple2<CountryShard, List<Atlas>>) tuple2)._2()) > 0);
    }

    /**
     * Load sharded atlas file and turn it into spark RDD
     *
     * @param context
     *            - spark context for loading atlas files
     * @param country
     *            - country name
     * @param boundaries
     *            - boundaries
     * @param atlasDirectory
     *            - directory that contains atlas files
     * @param atlasSharding
     *            - atlas sharding information
     * @param configurationMap
     *            - configuration
     * @return JavaPairRDD - Shard and Atlas pair
     */
    public static JavaPairRDD<Shard, Atlas> generateShardedAtlasRDD(final JavaSparkContext context,
            final String country, final CountryBoundaryMap boundaries, final String atlasDirectory,
            final Sharding atlasSharding, final Map<String, String> configurationMap)
    {
        final Set<Shard> countryAtlasShards = getCountryShards(atlasSharding, country, boundaries);

        final JavaRDD<Shard> countryShardsRDD = context
                .parallelize(new ArrayList<>(countryAtlasShards));

        return countryShardsRDD.mapToPair(shard ->
        {
            final Atlas atlas = loadOneAtlasShard(country, shard.getName(), atlasDirectory,
                    configurationMap);
            if (atlas != null)
            {
                logger.info("Loaded Atlas atlas_name={} atlas_size={} number_of_edges={}",
                        atlas.getName(), atlas.size(), atlas.numberOfEdges());
            }
            else
            {
                logger.error("Atlas is null for shard: {}", shard.getName());
            }

            return new Tuple2<>(shard, atlas);
        });
    }

    /**
     * Convert country list to RDD of CountryShard to bootstrap the parallel processing
     *
     * @param boundaries
     *            - boundaries
     * @param countries
     *            - country list
     * @param atlasSharding
     *            - atlas sharding information
     * @param sparkContext
     *            - Spark context
     * @return - RDD of org.openstreetmap.atlas.geography.sharding.CountryShard
     */
    public static JavaRDD<CountryShard> loadCountryToAtlasShardMapping(
            final CountryBoundaryMap boundaries, final StringList countries,
            final Sharding atlasSharding, final JavaSparkContext sparkContext)
    {
        final List<CountryShard> countryToShards = countryToCountryShardList(countries, boundaries,
                atlasSharding);
        logger.info("generated CountryShard list: {}", countryToShards);
        return sparkContext.parallelize(countryToShards, countryToShards.size());
    }

    /**
     * @param country
     *            - country name
     * @param shardName
     *            - shard name
     * @param atlasDirectory
     *            - directory that contains atlas file
     * @param configurationMap
     *            - configuration
     * @return Atlas
     * @throws CoreException
     *             potential CoreException while reading files
     */
    public static Atlas loadOneAtlasShard(final String country, final String shardName,
            final String atlasDirectory, final Map<String, String> configurationMap)
    {
        final String atlasPath = atlasPath(atlasDirectory, country, shardName);
        logger.info("Start to load atlas from atlas directory: {} ", atlasPath);
        Atlas atlas = null;
        try
        {
            if (!new FileSystemCreator().get(atlasPath, configurationMap)
                    .exists(new Path(atlasPath)))
            {
                logger.warn("No atlas found for path {}", atlasPath);
                return atlas;
            }
        }
        catch (final Exception exception)
        {
            throw new CoreException("Can't check if path " + atlasPath
                    + " exists or not with exception: " + exception.getMessage());
        }

        atlas = ATLAS_LOADER.load(FileSystemHelper.resource(atlasPath, configurationMap));
        return atlas;
    }

    private static String atlasPath(final String atlasDirectory, final String country,
            final String shardName)
    {
        return SparkFileHelper.combine(atlasDirectory + "/" + country, String.format("%s%s",
                getAtlasName(country, shardName), FileSuffix.ATLAS.toString()));
    }

    /**
     * Convert country list to a list of org.openstreetmap.atlas.geography.sharding.CountryShard
     *
     * @param countries
     *            - country list
     * @param boundaries
     *            - boundaries
     * @param sharding
     *            - sharding information
     * @return - list of org.openstreetmap.atlas.geography.sharding.CountryShard
     */
    private static List<CountryShard> countryToCountryShardList(final StringList countries,
            final CountryBoundaryMap boundaries, final Sharding sharding)
    {
        final MultiMapWithSet<String, Shard> countryShardList = CountryShardListing
                .countryToShardList(Iterables.asList(countries), boundaries, sharding);
        final List<CountryShard> countryToShards = Lists.newArrayList();
        countryShardList.forEach((country, shardSet) -> shardSet
                .forEach(shard -> countryToShards.add(new CountryShard(country, shard))));
        return countryToShards;
    }

    private static String getAtlasName(final String country, final String shardName)
    {
        return String.format("%s_%s", country, shardName);
    }

    /**
     * Helper method to generate country shards
     *
     * @param sharding
     *            - sharding information
     * @param country
     *            - country name
     * @param boundary
     *            - boundary
     * @return Set - shards for the country
     */
    private static Set<Shard> getCountryShards(final Sharding sharding, final String country,
            final CountryBoundaryMap boundary)
    {
        // Generate country shards, could be atlas shards or probe shards
        final Time time = Time.now();
        final Set<Shard> countryShards = CountryShardListing
                .countryToShardList(new StringList(country), boundary, sharding).get(country);
        logger.info("Generating shards for country {} takes {}", country, time.elapsedSince());
        logger.info("Country {} has {} shards", country, countryShards.size());
        countryShards.forEach(atlasShard -> logger.info("Shard {}", atlasShard.getName()));
        return countryShards;
    }

    protected ShardedAtlasRDDLoader()
    {
    }
}
