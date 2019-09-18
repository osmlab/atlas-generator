package org.openstreetmap.atlas.generator.persistence;

import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.geography.sharding.converters.SlippyTileConverter;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link MultipleOutputFormat} for the Atlas jobs. This ensures all the output files of the
 * {@link AtlasGenerator} follow the same output format structure.
 *
 * @author matthieun
 * @param <T>
 *            The type to be saved
 */
public abstract class AbstractMultipleAtlasBasedOutputFormat<T>
        extends MultipleOutputFormat<String, T>
{
    // By default, save all the shards under each country folder
    public static final String DEFAULT_SCHEME = "";

    private static final Logger logger = LoggerFactory
            .getLogger(AbstractMultipleAtlasBasedOutputFormat.class);

    @Override
    protected String generateFileNameForKeyValue(final String key, final T value, final String name)
    {
        final StringList countrySplit = StringList.split(key, CountryShard.COUNTRY_SHARD_SEPARATOR);
        final String country = countrySplit.get(0);
        final String shard = countrySplit.get(1);
        final String schemeDefinition = countrySplit.size() > 2 ? countrySplit.get(2) : "";

        // TODO remove debug logs
        logger.info("key: {}", key);
        logger.info("name: {}", name);
        logger.info("countrySplit: {}", countrySplit);
        logger.info("country: {}", country);
        logger.info("shard: {}", shard);
        logger.info("schemeDefinition: {}", schemeDefinition);

        final SlippyTilePersistenceScheme scheme = SlippyTilePersistenceScheme
                .getSchemeInstanceFromString(schemeDefinition);
        /*
         * This is a temporary hack to handle geohash sharded tiles. If we catch a "Wrong format"
         * error from the slippy tile converter, then let's just assume it's a geohash and carry on.
         * Obviously this is not an ideal solution. A better solution may be to do some JSON
         * packing/parsing with the key parameter.
         */
        SlippyTile slippyTile = null;
        try
        {
            slippyTile = new SlippyTileConverter().backwardConvert(shard);
        }
        catch (final CoreException exception)
        {
            /*
             * Rethrow the exception if it did not match the allowed message.
             */
            if (!exception.getMessage().contains("Wrong format of input string"))
            {
                throw exception;
            }
            logger.warn(
                    "Could not parse {} as a SlippyTile, defaulting to GeoHash and ignoring the scheme",
                    shard);
        }

        if (slippyTile != null)
        {
            return SparkFileHelper.combine(country, scheme.compile(slippyTile),
                    country + CountryShard.COUNTRY_SHARD_SEPARATOR + shard);
        }
        else
        {
            return SparkFileHelper.combine(country,
                    country + CountryShard.COUNTRY_SHARD_SEPARATOR + shard);
        }
    }
}
