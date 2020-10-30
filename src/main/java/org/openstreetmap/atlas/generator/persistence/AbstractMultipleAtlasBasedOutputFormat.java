package org.openstreetmap.atlas.generator.persistence;

import java.util.Optional;

import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.json.PersistenceJsonParser;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.geography.sharding.converters.StringToShardConverter;

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
    @Override
    protected String generateFileNameForKeyValue(final String key, final T value, final String name)
    {
        final String countryString = PersistenceJsonParser.parseCountry(key);
        final String shardString = PersistenceJsonParser.parseShard(key);
        final Optional<String> schemeString = PersistenceJsonParser.parseScheme(key);

        final StringToShardConverter converter = new StringToShardConverter();
        final Shard shard = converter.convert(shardString);

        SlippyTilePersistenceScheme scheme = null;
        // We only support alternate schemes for SlippyTile shards
        if (schemeString.isPresent() && !schemeString.get().isEmpty()
                && shard instanceof SlippyTile)
        {
            scheme = SlippyTilePersistenceScheme.getSchemeInstanceFromString(schemeString.get());
        }

        final CountryShard countryShard = new CountryShard(countryString, shard);
        if (scheme != null)
        {
            return SparkFileHelper.combine(countryString, scheme.compile((SlippyTile) shard),
                    countryShard.getName());
        }
        return SparkFileHelper.combine(countryString, countryShard.getName());
    }
}
