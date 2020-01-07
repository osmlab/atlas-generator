package org.openstreetmap.atlas.generator.persistence;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;

/**
 * @author matthieun
 */
public class MultipleFeatureChangeOutputFormat
        extends AbstractMultipleAtlasBasedOutputFormat<List<FeatureChange>>
{
    private FeatureChangeOutputFormat format = null;

    @Override
    protected RecordWriter<String, List<FeatureChange>> getBaseRecordWriter(
            final FileSystem fileSystem, final JobConf job, final String name,
            final Progressable progress) throws IOException
    {
        if (this.format == null)
        {
            this.format = new FeatureChangeOutputFormat();
        }
        return this.format.getRecordWriter(fileSystem, job, name, progress);
    }
}
