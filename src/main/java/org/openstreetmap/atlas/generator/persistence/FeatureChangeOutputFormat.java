package org.openstreetmap.atlas.generator.persistence;

import java.io.Writer;
import java.util.List;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;

/**
 * @author matthieun
 */
public class FeatureChangeOutputFormat extends AbstractFileOutputFormat<List<FeatureChange>>
{
    @Override
    protected String fileExtension()
    {
        return ".geojson";
    }

    @Override
    protected boolean isCompressed()
    {
        return true;
    }

    @Override
    protected void save(final List<FeatureChange> value, final AbstractWritableResource out)
    {
        try (Writer writer = out.writer())
        {
            for (final FeatureChange featureChange : value)
            {
                writer.write(featureChange.toGeoJson());
                writer.write(System.lineSeparator());
            }
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to save {}!", out.getName(), e);
        }
    }
}
