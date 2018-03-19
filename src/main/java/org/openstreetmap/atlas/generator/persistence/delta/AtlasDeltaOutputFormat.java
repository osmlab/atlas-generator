package org.openstreetmap.atlas.generator.persistence.delta;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.openstreetmap.atlas.generator.persistence.AbstractFileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

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
        return FileSuffix.GEO_JSON.toString();
    }

    @Override
    protected boolean isCompressed()
    {
        return true;
    }

    @Override
    protected void save(final AtlasDelta value, final AbstractWritableResource out)
    {
        out.writeAndClose(value.toGeoJson());
    }
}
