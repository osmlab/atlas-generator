package org.openstreetmap.atlas.generator.persistence;

import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.geography.sharding.converters.SlippyTileConverter;
import org.openstreetmap.atlas.utilities.collections.StringList;

/**
 * Default {@link MultipleOutputFormat} for the Atlas jobs. This ensures all the output files of the
 * {@link AtlasGenerator} follow the same output format structure.
 *
 * @author matthieun
 * @param <Type>
 *            The type to be saved
 */
public abstract class AbstractMultipleAtlasBasedOutputFormat<Type>
        extends MultipleOutputFormat<String, Type>
{
    @Override
    protected String generateFileNameForKeyValue(final String key, final Type value,
            final String name)
    {
        final StringList countrySplit = StringList.split(key, CountryShard.COUNTRY_SHARD_SEPARATOR);
        final String country = countrySplit.get(0);
        final String shard = countrySplit.get(1);
        final SlippyTile slippyTile = new SlippyTileConverter().backwardConvert(shard);
        return country + "/" + slippyTile.getZoom() + "/" + slippyTile.getX() + "/"
                + slippyTile.getY() + "/" + key;
    }
}
