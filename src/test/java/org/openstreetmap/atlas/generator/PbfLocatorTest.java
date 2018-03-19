package org.openstreetmap.atlas.generator;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
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
import org.openstreetmap.atlas.utilities.collections.Iterables;

import com.amazonaws.util.StringInputStream;

/**
 * @author matthieun
 */
public class PbfLocatorTest
{
    private static final String version = "07082015";

    @Before
    public void init()
    {
        uploadFile(10 + "/" + 291 + "-" + 438 + ".osm.pbf");
        uploadFile(10 + "/" + 292 + "-" + 438 + ".osm.pbf");
        uploadFile(10 + "-" + 291 + "-" + 438 + ".pbf");
        uploadFile(10 + "-" + 292 + "-" + 438 + ".pbf");
        System.out.println(ResourceFileSystem.files());
    }

    @Test
    public void testDefaultTileFinder()
    {
        // Test a default scheme
        final String scheme = PbfLocator.DEFAULT_SCHEME;
        final PbfLocator locator = new PbfLocator("resource://" + version, scheme,
                new SlippyTileSharding(10), ResourceFileSystem.simpleconfiguration());
        final Polygon outer = Rectangle.forCorners(Location.forString("24.953302,-77.608195"),
                Location.forString("25.131896,-77.207033"));
        final MultiPolygon multiPolygon = MultiPolygon.forPolygon(outer);
        final List<LocatedPbf> tiles = Iterables.asList(locator.pbfsCovering(multiPolygon));
        Assert.assertEquals(2, tiles.size());
    }

    @Test
    public void testDifferentTileFinder()
    {
        // Test a non-default scheme
        final String scheme = PbfLocator.ZOOM + "/" + PbfLocator.X_INDEX + "-" + PbfLocator.Y_INDEX
                + ".osm.pbf";
        final PbfLocator locator = new PbfLocator("resource://" + version, scheme,
                new SlippyTileSharding(10), ResourceFileSystem.simpleconfiguration());
        final Polygon outer = Rectangle.forCorners(Location.forString("24.953302,-77.608195"),
                Location.forString("25.131896,-77.207033"));
        final MultiPolygon multiPolygon = MultiPolygon.forPolygon(outer);
        final List<LocatedPbf> tiles = Iterables.asList(locator.pbfsCovering(multiPolygon));
        Assert.assertEquals(2, tiles.size());
    }

    private void uploadFile(final String name)
    {
        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem();
                OutputStream output = resourceFileSystem
                        .create(new Path("resource://" + version + "/" + name));
                InputStream input = new StringInputStream("test"))
        {
            IOUtils.copy(input, output);
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not setup test environment.", e);
        }
    }
}
