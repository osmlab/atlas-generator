package org.openstreetmap.atlas.generator.persistence;

import org.openstreetmap.atlas.generator.tools.persistence.AbstractFileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;

/**
 * @author tony
 */
public class AtlasGeoJsonOutputFormat extends AbstractFileOutputFormat<Atlas>
{
    @Override
    protected String fileExtension()
    {
        return "geojson";
    }

    @Override
    protected void save(final Atlas value, final AbstractWritableResource out)
    {
        value.saveAsGeoJson(out);
    }
}
