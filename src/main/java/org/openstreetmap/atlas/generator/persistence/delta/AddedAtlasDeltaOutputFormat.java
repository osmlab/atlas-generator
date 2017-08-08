package org.openstreetmap.atlas.generator.persistence.delta;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.delta.Diff;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;

/**
 * {@link FileOutputFormat} that writes an {@link AtlasStatistics}.
 *
 * @author matthieun
 */
public class AddedAtlasDeltaOutputFormat extends AtlasDeltaOutputFormat
{
    @Override
    protected void save(final AtlasDelta value, final AbstractWritableResource out)
    {
        out.writeAndClose(value.toGeoJson(Diff::isAdded));
    }
}
