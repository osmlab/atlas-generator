package org.openstreetmap.atlas.generator.persistence;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.openstreetmap.atlas.geography.atlas.Atlas;

/**
 * {@link MultipleOutputFormat} for the shard {@link Atlas} to line delimited geojson, based on the
 * model of {@link MultipleTextOutputFormat}
 *
 * @author matthieun
 */
public class MultipleLineDelimitedGeojsonOutputFormat
        extends AbstractMultipleAtlasBasedOutputFormat<Atlas>
{
    private LineDelimitedGeojsonOutputFormat format = null;

    @Override
    protected RecordWriter<String, Atlas> getBaseRecordWriter(final FileSystem fileSystem,
            final JobConf job, final String name, final Progressable progress) throws IOException
    {
        if (this.format == null)
        {
            this.format = new LineDelimitedGeojsonOutputFormat();
        }
        return this.format.getRecordWriter(fileSystem, job, name, progress);
    }
}
