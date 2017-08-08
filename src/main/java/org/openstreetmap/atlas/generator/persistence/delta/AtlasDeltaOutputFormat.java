package org.openstreetmap.atlas.generator.persistence.delta;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.openstreetmap.atlas.generator.tools.persistence.AbstractFileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;

/**
 * {@link FileOutputFormat} that writes an {@link AtlasStatistics}.
 *
 * @author matthieun
 */
public class AtlasDeltaOutputFormat extends AbstractFileOutputFormat<AtlasDelta>
{
    @Override
    protected String fileExtension()
    {
        return "geojson";
    }

    @Override
    protected void save(final AtlasDelta value, final AbstractWritableResource out)
    {
        out.writeAndClose(value.toGeoJson());
    }
}
