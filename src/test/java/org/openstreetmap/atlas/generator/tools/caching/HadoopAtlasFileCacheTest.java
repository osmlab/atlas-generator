package org.openstreetmap.atlas.generator.tools.caching;

import java.util.HashMap;
import java.util.Optional;

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
        final String fullParentPathURI = "file://" + parentAtlas.toString();
        final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI,
                AtlasGeneratorParameters.ATLAS_SCHEME.get("zz/"), new HashMap<>());
        parentAtlasCountry.mkdirs();
        try
        {
            final File atlas1 = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
            atlas1.writeAndClose("1");
            final File atlas2 = parentAtlasCountry.child("2/AAA_2-2-2.atlas");
            atlas2.writeAndClose("2");

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
            cache.invalidate();
            parent.deleteRecursively();
        }
    }

    @Test
    public void testCachesWithDifferentNamespaces()
    {
        final File parent = File.temporaryFolder();
        final File parentAtlas = new File(parent + "/atlas");
        final File parentAtlasCountry = new File(parentAtlas + "/AAA");
        final String fullParentPathURI = "file://" + parentAtlas.toString();
        final HadoopAtlasFileCache cache1 = new HadoopAtlasFileCache(fullParentPathURI,
                "namespace1", AtlasGeneratorParameters.ATLAS_SCHEME.get("zz/"), new HashMap<>());
        final HadoopAtlasFileCache cache2 = new HadoopAtlasFileCache(fullParentPathURI,
                "namespace2", AtlasGeneratorParameters.ATLAS_SCHEME.get("zz/"), new HashMap<>());
        parentAtlasCountry.mkdirs();
        try
        {
            // set up file for cache1
            File atlasFile = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
            atlasFile.writeAndClose("version1");

            // cache file in cache1 under namespace "namespace1"
            final Resource resource1 = cache1.get("AAA", new SlippyTile(1, 1, 1)).get();

            // delete and recreate the same file (with same URI) but with new contents for cache2
            atlasFile.delete();
            atlasFile = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
            atlasFile.writeAndClose("version2");

            // cache the file in cache2 under namespace "namespace2"
            final Resource resource2 = cache2.get("AAA", new SlippyTile(1, 1, 1)).get();

            // the files should be unequal, even though the URIs are the same
            Assert.assertNotEquals(resource1.all(), resource2.all());

            // now we are getting the cached versions of the file
            final Resource resource3 = cache1.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource4 = cache2.get("AAA", new SlippyTile(1, 1, 1)).get();

            // the files should still be unequal, even though the URIs are the same
            Assert.assertNotEquals(resource3.all(), resource4.all());

            // delete cache1's cached version of the file
            cache1.invalidate("AAA", new SlippyTile(1, 1, 1));

            // recreate version 2 of the file
            atlasFile.delete();
            atlasFile = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
            atlasFile.writeAndClose("version2");

            // get version 2 of the file into cache1
            final Resource resource5 = cache1.get("AAA", new SlippyTile(1, 1, 1)).get();

            // now the resources should be identical
            Assert.assertEquals(resource4.all(), resource5.all());
        }
        finally
        {
            cache1.invalidate();
            cache2.invalidate();
            parent.deleteRecursively();
        }
    }

    @Test
    public void testNonexistentResource()
    {
        final File parent = File.temporaryFolder();
        final File parentAtlas = new File(parent + "/atlas");
        final File parentAtlasCountry = new File(parentAtlas + "/AAA");
        final String fullParentPathURI = "file://" + parentAtlas.toString();
        final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI,
                AtlasGeneratorParameters.ATLAS_SCHEME.get("zz/"), new HashMap<>());
        parentAtlasCountry.mkdirs();
        try
        {
            final File atlas1 = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
            atlas1.writeAndClose("1");

            // this resource does not exist!
            final Optional<Resource> resourceOptional = cache.get("AAA", new SlippyTile(5, 5, 5));
            Assert.assertFalse(resourceOptional.isPresent());
        }
        finally
        {
            cache.invalidate();
            parent.deleteRecursively();
        }
    }
}
