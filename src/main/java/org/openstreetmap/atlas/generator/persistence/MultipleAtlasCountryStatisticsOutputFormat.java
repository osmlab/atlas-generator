package org.openstreetmap.atlas.generator.persistence;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;

/**
 * {@link MultipleOutputFormat} for the country {@link AtlasStatistics}, based on the model of
 * {@link MultipleTextOutputFormat}
 *
 * @author matthieun
 */
public class MultipleAtlasCountryStatisticsOutputFormat
        extends MultipleOutputFormat<String, AtlasStatistics>
{
    private AtlasStatisticsOutputFormat format = null;

    @Override
    protected String generateFileNameForKeyValue(final String key, final AtlasStatistics value,
            final String name)
    {
        return key;
    }

    @Override
    protected RecordWriter<String, AtlasStatistics> getBaseRecordWriter(final FileSystem fileSystem,
            final JobConf job, final String name, final Progressable progress) throws IOException
    {
        if (this.format == null)
        {
            this.format = new AtlasStatisticsOutputFormat();
        }
        return this.format.getRecordWriter(fileSystem, job, name, progress);
    }
}
