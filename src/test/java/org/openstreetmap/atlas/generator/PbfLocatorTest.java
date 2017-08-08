package org.openstreetmap.atlas.generator;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.PbfLocator.LocatedPbf;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.sharding.SlippyTileSharding;
import org.openstreetmap.atlas.streaming.Streams;
import org.openstreetmap.atlas.utilities.collections.Iterables;

/**
 * @author matthieun
 */
public class PbfLocatorTest
{
    private static final String version = "07082015";
    private static final String fileName = "BHS_" + version + ".pbf";

    @Before
    public void init()
    {
        uploadFile(10 + "-" + 291 + "-" + 438 + ".pbf");
        uploadFile(10 + "-" + 292 + "-" + 438 + ".pbf");
        System.out.println(ResourceFileSystem.files());
    }

    @Test
    public void testTileFinder()
    {
        final PbfContext pbfContext = new PbfContext("resource://" + version,
                new SlippyTileSharding(10));
        final PbfLocator locator = new PbfLocator(pbfContext,
                ResourceFileSystem.simpleconfiguration());
        final Polygon outer = Rectangle.forCorners(Location.forString("24.953302,-77.608195"),
                Location.forString("25.131896,-77.207033"));
        final MultiPolygon multiPolygon = MultiPolygon.forPolygon(outer);
        final List<LocatedPbf> tiles = Iterables.asList(locator.pbfsCovering(multiPolygon));
        Assert.assertEquals(2, tiles.size());
    }

    private void uploadFile(final String name)
    {
        try
        {
            @SuppressWarnings("resource")
            final OutputStream output = new ResourceFileSystem()
                    .create(new Path("resource://" + version + "/" + name));
            final InputStream input = PbfLocatorTest.class.getResourceAsStream(fileName);
            int index = -1;
            while ((index = input.read()) >= 0)
            {
                output.write(index);
            }
            Streams.close(input);
            Streams.close(output);
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not setup test environment.", e);
        }
    }
}
