package org.openstreetmap.atlas.generator.tools.spark.rdd;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemCreator;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.boundary.CountryShardListing;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * @author tian_du
 */
public class ShardedAtlasRDDLoader
{

    private static final Logger logger = LoggerFactory
            .getLogger(ShardedAtlasRDDLoader.class.getCanonicalName());

    private static final AtlasResourceLoader ATLAS_LOADER = new AtlasResourceLoader();

    protected ShardedAtlasRDDLoader()
    {
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
            throws CoreException
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

    private static String atlasPath(final String atlasDirectory, final String country,
            final String shardName)
    {
        return SparkFileHelper.combine(atlasDirectory + "/" + country, String.format("%s%s",
                getAtlasName(country, shardName), FileSuffix.ATLAS.toString()));
    }

    private static String getAtlasName(final String country, final String shardName)
    {
        return String.format("%s_%s", country, shardName);
    }
}
