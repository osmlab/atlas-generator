package org.openstreetmap.atlas.generator.persistence;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.openstreetmap.atlas.generator.tools.persistence.AbstractFileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;

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
        return "atlas";
    }

    @Override
    protected void save(final Atlas value, final AbstractWritableResource out)
    {
        value.save(out);
    }
}
