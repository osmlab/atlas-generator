package org.openstreetmap.atlas.generator.world;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * Filter that only accepts .atlas files.
 *
 * @author jwpgage
 */
public class AtlasFilePathFilter implements PathFilter
{
    @Override
    public boolean accept(final Path path)
    {
        return path.getName().endsWith(FileSuffix.ATLAS.toString());
    }
}
