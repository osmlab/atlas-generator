package org.openstreetmap.atlas.generator.persistence;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * @author tony
 */
public class AtlasGeoJsonOutputFormat extends AbstractFileOutputFormat<Atlas>
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
    protected void save(final Atlas value, final AbstractWritableResource out)
    {
        value.saveAsGeoJson(out);
    }
}
