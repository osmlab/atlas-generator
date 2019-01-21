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
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceSchemeType;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.Polygon;
import org.openstreetmap.atlas.geography.Rectangle;
import org.openstreetmap.atlas.geography.sharding.SlippyTileSharding;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matthieun
 */
public class PbfLocatorTest
{
    private static final String version = "07082015";
    private static final Logger logger = LoggerFactory.getLogger(PbfLocatorTest.class);

    @Before
    public void init()
    {
        uploadFile(10 + "/" + 291 + "-" + 438 + FileSuffix.PBF.toString());
        uploadFile(10 + "/" + 292 + "-" + 438 + FileSuffix.PBF.toString());
        uploadFile(10 + "-" + 291 + "-" + 438 + FileSuffix.PBF.toString());
        uploadFile(10 + "-" + 292 + "-" + 438 + FileSuffix.PBF.toString());
        System.out.println(ResourceFileSystem.files());
    }

    @Test
    public void testDefaultTileFinder()
    {
        // Test a default scheme
        final SlippyTilePersistenceScheme scheme = new SlippyTilePersistenceScheme(
                SlippyTilePersistenceSchemeType.ZZ_XX_YY_PBF);
        logger.warn("{}", scheme.getScheme());
        final PbfContext pbfContext = new PbfContext("resource://" + version,
                new SlippyTileSharding(10), scheme);
        final PbfLocator locator = new PbfLocator(pbfContext,
                ResourceFileSystem.simpleconfiguration());
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
        final SlippyTilePersistenceScheme scheme = new SlippyTilePersistenceScheme(
                SlippyTilePersistenceSchemeType.ZZ_SLASH_XX_YY_PBF);
        logger.warn("{}", scheme.getScheme());
        final PbfContext pbfContext = new PbfContext("resource://" + version,
                new SlippyTileSharding(10), scheme);
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
        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem();
                OutputStream output = resourceFileSystem
                        .create(new Path("resource://" + version + "/" + name));
                InputStream input = new StringResource("test").read())
        {
            IOUtils.copy(input, output);
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not setup test environment.", e);
        }
    }
}
