package org.openstreetmap.atlas.generator.persistence;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * {@link FileOutputFormat} that writes an {@link Atlas}.
 *
 * @author matthieun
 */
public class AtlasOutputFormat extends AbstractFileOutputFormat<Atlas>
{
    @Override
    protected String fileExtension()
    {
        return FileSuffix.ATLAS.toString();
    }

    @Override
    protected boolean isCompressed()
    {
        return false;
    }

    @Override
    protected void save(final Atlas value, final AbstractWritableResource out)
    {
        value.save(out);
    }
}
