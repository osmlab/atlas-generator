package org.openstreetmap.atlas.generator.persistence.delta;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.openstreetmap.atlas.generator.persistence.AbstractMultipleAtlasBasedOutputFormat;
import org.openstreetmap.atlas.geography.atlas.delta.AtlasDelta;
import org.openstreetmap.atlas.geography.atlas.statistics.AtlasStatistics;

/**
 * {@link MultipleOutputFormat} for the {@link AtlasStatistics}, based on the model of
 * {@link MultipleTextOutputFormat}
 *
 * @author matthieun
 */
public class MultipleAtlasDeltaOutputFormat
        extends AbstractMultipleAtlasBasedOutputFormat<AtlasDelta>
{
    @Override
    protected RecordWriter<String, AtlasDelta> getBaseRecordWriter(final FileSystem fileSystem,
            final JobConf job, final String name, final Progressable progress) throws IOException
    {
        return new AtlasDeltaOutputFormat().getRecordWriter(fileSystem, job, name, progress);
    }
}
