package org.openstreetmap.atlas.generator.tools.caching;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceSchemeType;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlasBuilder;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.utilities.caching.strategies.NamespaceCachingStrategy;
import org.openstreetmap.atlas.utilities.collections.Maps;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

/**
 * @author lcram
 */
public class HadoopAtlasFileCacheTest
{
    @Test
    public void testBasicCache()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystem1(filesystem);
            final int[] fetcherCount = new int[1];
            final Function<URI, Optional<Resource>> fetcher = uri ->
            {
                fetcherCount[0]++;
                final String path = uri.getPath();
                return Optional.of(new File(path, filesystem));
            };
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache("/Users/foo/atlas",
                    new SlippyTilePersistenceScheme(SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER),
                    new NamespaceCachingStrategy("HadoopAtlasFileCache_Test", filesystem), fetcher);
            Assert.assertEquals(0, fetcherCount[0]);
            final Optional<Resource> resource = cache.get("AAA", new SlippyTile(1, 1, 1));
            Assert.assertTrue(resource.isPresent());
            Assert.assertEquals(1, fetcherCount[0]);

            final Optional<Resource> resourceAgain = cache.get("AAA", new SlippyTile(1, 1, 1));
            Assert.assertTrue(resourceAgain.isPresent());
            Assert.assertEquals(1, fetcherCount[0]);
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    @Test
    public void testCacheUsingHadoopFileSystem()
    {
        /*
         * Here we have to rely on the default FileSystem since the Hadoop filesystem code does not
         * support alternate implementations like jimfs.
         */
        final File parent = File.temporaryFolder(FileSystems.getDefault());
        final File parentAtlas = parent.child("atlas");
        final File parentAtlasCountry = parentAtlas.child("AAA");
        final String fullParentPathURI = "file://" + parentAtlas.toString();
        final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI,
                new SlippyTilePersistenceScheme(SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER),
                new HashMap<>());
        parentAtlasCountry.mkdirs();
        try
        {
            final PackedAtlasBuilder builder1 = new PackedAtlasBuilder();
            builder1.addPoint(1L, Location.CENTER, Maps.hashMap());
            final PackedAtlas atlas1 = (PackedAtlas) builder1.get();

            final PackedAtlasBuilder builder2 = new PackedAtlasBuilder();
            builder2.addPoint(2L, Location.CENTER, Maps.hashMap());
            final PackedAtlas atlas2 = (PackedAtlas) builder2.get();

            final File atlasFile1 = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
            atlas1.save(atlasFile1);
            final File atlasFile2 = parentAtlasCountry.child("2/AAA_2-2-2.atlas");
            atlas2.save(atlasFile2);

            // cache miss, this will create the cached copy
            final Resource resource1 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource2 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();

            Assert.assertEquals(atlas1, PackedAtlas.load(resource1));
            Assert.assertEquals(atlas2, PackedAtlas.load(resource2));

            // cache hit, using cached copy
            final Resource resource3 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource4 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();

            Assert.assertEquals(atlas1, PackedAtlas.load(resource3));
            Assert.assertEquals(atlas2, PackedAtlas.load(resource4));
        }
        finally
        {
            cache.invalidate();
            parent.deleteRecursively();
        }
    }

    @Test
    public void testInvalidate()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystem1(filesystem);
            final int[] fetcherCount = new int[1];
            final Function<URI, Optional<Resource>> fetcher = uri ->
            {
                fetcherCount[0]++;
                final String path = uri.getPath();
                return Optional.of(new File(path, filesystem));
            };
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache("/Users/foo/atlas",
                    new SlippyTilePersistenceScheme(SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER),
                    new NamespaceCachingStrategy("HadoopAtlasFileCache_Test", filesystem), fetcher);
            Assert.assertEquals(0, fetcherCount[0]);
            final Optional<Resource> resource = cache.get("AAA", new SlippyTile(1, 1, 1));
            Assert.assertTrue(resource.isPresent());
            Assert.assertEquals(1, fetcherCount[0]);

            cache.invalidate("AAA", new SlippyTile(1, 1, 1));

            final Optional<Resource> resourceAgain = cache.get("AAA", new SlippyTile(1, 1, 1));
            Assert.assertTrue(resourceAgain.isPresent());
            // fetcher run count should now be 2 since we invalidated the cache
            Assert.assertEquals(2, fetcherCount[0]);
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    private void setupFilesystem1(final FileSystem filesystem)
    {
        final PackedAtlasBuilder builder = new PackedAtlasBuilder();

        builder.addPoint(1000000L, Location.forWkt("POINT(1 1)"), Maps.hashMap("foo", "bar"));

        final Atlas atlas = builder.get();
        final File atlasFile = new File("/Users/foo/atlas/AAA/1/AAA_1-1-1.atlas", filesystem);
        assert atlas != null;
        atlas.saveAsText(atlasFile);
    }
}
