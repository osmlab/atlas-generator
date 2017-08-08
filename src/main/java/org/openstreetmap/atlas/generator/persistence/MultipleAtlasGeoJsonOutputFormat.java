package org.openstreetmap.atlas.generator.persistence;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.openstreetmap.atlas.geography.atlas.Atlas;

/**
 * @author tony
 */
public class MultipleAtlasGeoJsonOutputFormat extends MultipleOutputFormat<String, Atlas>
{
    private AtlasGeoJsonOutputFormat format = null;

    @Override
    protected String generateFileNameForKeyValue(final String key, final Atlas value,
            final String name)
    {
        return key;
    }

    @Override
    protected RecordWriter<String, Atlas> getBaseRecordWriter(final FileSystem fileSystem,
            final JobConf job, final String name, final Progressable progress) throws IOException
    {
        if (this.format == null)
        {
            this.format = new AtlasGeoJsonOutputFormat();
        }
        return this.format.getRecordWriter(fileSystem, job, name, progress);
    }
}
