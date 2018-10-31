package org.openstreetmap.atlas.generator.tools.caching;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.Resource;

/**
 * @author lcram
 */
public class HadoopAtlasFileCacheTest
{
    @Test
    public void testCache()
    {
        final File parent = File.temporaryFolder();
        final File parentAtlas = new File(parent + "/atlas");
        final File parentAtlasCountry = new File(parentAtlas + "/AAA");
        parentAtlasCountry.mkdirs();
        try
        {
            final File atlas1 = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
            atlas1.writeAndClose("1");
            final File atlas2 = parentAtlasCountry.child("2/AAA_2-2-2.atlas");
            atlas2.writeAndClose("2");

            final String path = "file://" + parentAtlas.toString();
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(path,
                    AtlasGeneratorParameters.ATLAS_SCHEME.get("zz/"), new HashMap<>());

            // cache miss, this will create the cached copy
            final Resource resource1 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource2 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();

            Assert.assertEquals("1", resource1.firstLine());
            Assert.assertEquals("2", resource2.firstLine());

            // cache hit, using cached copy
            final Resource resource3 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource4 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();

            Assert.assertEquals("1", resource3.firstLine());
            Assert.assertEquals("2", resource4.firstLine());
        }
        finally
        {
            parent.deleteRecursively();
        }
    }
}
