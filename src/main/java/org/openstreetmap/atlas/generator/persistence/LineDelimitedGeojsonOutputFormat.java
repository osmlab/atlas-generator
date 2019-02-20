package org.openstreetmap.atlas.generator.persistence;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;

/**
 * {@link FileOutputFormat} that writes an {@link Atlas} as line delimited GeoJson.
 *
 * @author matthieun
 */
public class LineDelimitedGeojsonOutputFormat extends AbstractFileOutputFormat<Atlas>
{
    @Override
    protected String fileExtension()
    {
        return ".ldgeojson";
    }

    @Override
    protected boolean isCompressed()
    {
        return true;
    }

    @Override
    protected void save(final Atlas value, final AbstractWritableResource out)
    {
        value.saveAsLineDelimitedGeoJsonFeatures(out, (atlasEntity, jsonMutator) ->
        {
        });
    }
}
