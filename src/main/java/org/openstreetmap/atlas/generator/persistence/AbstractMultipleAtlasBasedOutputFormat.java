package org.openstreetmap.atlas.generator.persistence;

import java.util.Optional;

import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.geography.sharding.converters.StringToShardConverter;
import org.openstreetmap.atlas.utilities.tuples.Tuple;

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

    @Override
    protected String generateFileNameForKeyValue(final String key, final T value, final String name)
    {
        final StringToShardConverter converter = new StringToShardConverter();
        final Tuple<Shard, Optional<String>> shardAndData = converter.convertWithMetadata(key);
        final Shard shard = shardAndData.getFirst();
        final Optional<String> schemeMetadata = shardAndData.getSecond();

        if (!(shard instanceof CountryShard))
        {
            throw new CoreException("{} must be an instance of {}, found {}", shard,
                    CountryShard.class.getName(), shard.getClass().getName());
        }

        final CountryShard countryShard = (CountryShard) shard;
        final String country = countryShard.getCountry();
        final Shard actualShard = countryShard.getShard();

        SlippyTilePersistenceScheme scheme = null;
        if (schemeMetadata.isPresent() && actualShard instanceof SlippyTile)
        {
            scheme = SlippyTilePersistenceScheme.getSchemeInstanceFromString(schemeMetadata.get());
        }

        if (scheme != null)
        {
            return SparkFileHelper.combine(country, scheme.compile((SlippyTile) actualShard),
                    countryShard.getName());
        }
        return SparkFileHelper.combine(country, countryShard.getName());
    }
}
