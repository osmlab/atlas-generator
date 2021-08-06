package org.openstreetmap.atlas.generator.persistence;

import java.io.Writer;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * @author samg
 */
public class RelationFeatureChangeOutputFormat extends AbstractFileOutputFormat<FeatureChange>
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
    protected void save(final FeatureChange value, final AbstractWritableResource out)
    {
        try (Writer writer = out.writer())
        {
            writer.write(value.toGeoJson());
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to save {}!", out.getName(), e);
        }
    }
}
