package org.openstreetmap.atlas.generator.sharding;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * Filters out non pbf files and directories.
 *
 * @author jamesgage
 */
public class PbfFilePathFilter implements PathFilter
{
    @Override
    public boolean accept(final Path path)
    {
        return path.getName().endsWith(FileSuffix.PBF.toString());
    }
}
