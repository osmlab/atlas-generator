package org.openstreetmap.atlas.generator.persistence;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.openstreetmap.atlas.geography.atlas.Atlas;

/**
 * @author lcram
 */
public class MultipleAtlasProtoOutputFormat extends AbstractMultipleAtlasBasedOutputFormat<Atlas>
{
    private AtlasProtoOutputFormat format = null;

    @Override
    protected RecordWriter<String, Atlas> getBaseRecordWriter(final FileSystem fileSystem,
            final JobConf job, final String name, final Progressable progress) throws IOException
    {
        if (this.format == null)
        {
            this.format = new AtlasProtoOutputFormat();
        }
        return this.format.getRecordWriter(fileSystem, job, name, progress);
    }
}
