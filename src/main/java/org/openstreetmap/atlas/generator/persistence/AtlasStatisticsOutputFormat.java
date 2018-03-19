package org.openstreetmap.atlas.generator.persistence;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * {@link FileOutputFormat} that writes an {@link AtlasStatistics}.
 *
 * @author matthieun
 */
public class AtlasStatisticsOutputFormat extends AbstractFileOutputFormat<AtlasStatistics>
{
    @Override
    protected String fileExtension()
    {
        return FileSuffix.CSV.toString();
    }

    @Override
    protected boolean isCompressed()
    {
        return true;
    }

    @Override
    protected void save(final AtlasStatistics value, final AbstractWritableResource out)
    {
        value.save(out);
    }
}
