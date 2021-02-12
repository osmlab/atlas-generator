package org.openstreetmap.atlas.mutator.persistence;

import java.io.Writer;
import java.util.List;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.AbstractFileOutputFormat;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.streaming.resource.AbstractWritableResource;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * @author matthieun
 */
public class FeatureChangeOutputFormat extends AbstractFileOutputFormat<List<FeatureChange>>
{
    public static String getTotalExtension()
    {
        final FeatureChangeOutputFormat temporary = new FeatureChangeOutputFormat();
        return temporary.fileExtension()
                + (temporary.isCompressed() ? FileSuffix.GZIP.toString() : "");
    }

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
