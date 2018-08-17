package org.openstreetmap.atlas.generator.tools.caching;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopAtlasFileCacheTest
{
    private static final Logger logger = LoggerFactory.getLogger(HadoopAtlasFileCacheTest.class);

    @Test
    public void testCache()
    {
        final File parent = File.temporaryFolder();
        final File parentAtlas = new File(parent + "/atlas");
        final File parentAtlasCountry = new File(parentAtlas + "/AAA");
        parentAtlasCountry.mkdirs();
        try
        {
            final File atlas1 = parentAtlasCountry.child("AAA_1-1-1.atlas");
            atlas1.writeAndClose("1");
            final File atlas2 = parentAtlasCountry.child("AAA_2-2-2.atlas");
            atlas2.writeAndClose("2");

            final String path = "file://" + parentAtlas.toString();
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(path, new HashMap<>());

            final Resource resource1 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource2 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();

            Assert.assertEquals("1", resource1.firstLine());
            Assert.assertEquals("2", resource2.firstLine());

            final Resource resource3 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource4 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();

            Assert.assertEquals("1", resource3.firstLine());
            Assert.assertEquals("2", resource4.firstLine());
        }
        finally
        {
            parentAtlas.deleteRecursively();
        }
    }
}
