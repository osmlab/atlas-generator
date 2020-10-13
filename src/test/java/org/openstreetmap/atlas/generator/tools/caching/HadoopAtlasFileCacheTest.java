package org.openstreetmap.atlas.generator.tools.caching;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceSchemeType;
import org.openstreetmap.atlas.geography.Location;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlasBuilder;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.TemporaryFile;
import org.openstreetmap.atlas.utilities.caching.strategies.NamespaceCachingStrategy;
import org.openstreetmap.atlas.utilities.collections.Maps;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

/**
 * @author lcram
 */
public class HadoopAtlasFileCacheTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBasicCache()
    {
        // We will test a couple different atlas schemes.
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystemZZSubfolderScheme(filesystem);
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

        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystemDefaultScheme(filesystem);
            final int[] fetcherCount = new int[1];
            final Function<URI, Optional<Resource>> fetcher = uri ->
            {
                fetcherCount[0]++;
                final String path = uri.getPath();
                return Optional.of(new File(path, filesystem));
            };
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache("/Users/foo/atlas",
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
         * support alternate implementations like jimfs. We will test each cache constructor in its
         * own try-with-resources block.
         */

        try (TemporaryFile parent = File.temporaryFolder(FileSystems.getDefault()))
        {
            final File parentAtlas = parent.child("atlas");
            final File parentAtlasCountry = parentAtlas.child("AAA");
            final String fullParentPathURI = "file://" + parentAtlas.toString();
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI,
                    new SlippyTilePersistenceScheme(SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER),
                    new HashMap<>());
            parentAtlasCountry.mkdirs();
            testCacheWithHadoopFileSystemHelper(cache, parentAtlasCountry,
                    SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER);
        }

        try (TemporaryFile parent = File.temporaryFolder(FileSystems.getDefault()))
        {
            final File parentAtlas = parent.child("atlas");
            final File parentAtlasCountry = parentAtlas.child("AAA");
            final String fullParentPathURI = "file://" + parentAtlas.toString();
            final Function<URI, Optional<Resource>> fetcher = uri ->
            {
                final File file = new File(uri.getPath(), parent.toPath().getFileSystem());
                if (!file.exists())
                {
                    return Optional.empty();
                }
                return Optional.of(file);
            };
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI,
                    "test-namespace",
                    new SlippyTilePersistenceScheme(SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER),
                    fetcher);
            parentAtlasCountry.mkdirs();
            testCacheWithHadoopFileSystemHelper(cache, parentAtlasCountry,
                    SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER);
        }

        try (TemporaryFile parent = File.temporaryFolder(FileSystems.getDefault()))
        {
            final File parentAtlas = parent.child("atlas");
            final File parentAtlasCountry = parentAtlas.child("AAA");
            final String fullParentPathURI = "file://" + parentAtlas.toString();
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI,
                    new HashMap<>());
            parentAtlasCountry.mkdirs();
            testCacheWithHadoopFileSystemHelper(cache, parentAtlasCountry,
                    AtlasGeneratorParameters.ATLAS_SCHEME.getDefault().getType());
        }

        try (TemporaryFile parent = File.temporaryFolder(FileSystems.getDefault()))
        {
            final File parentAtlas = parent.child("atlas");
            final File parentAtlasCountry = parentAtlas.child("AAA");
            final String fullParentPathURI = "file://" + parentAtlas.toString();
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI,
                    "test-namepsace", new HashMap<>());
            parentAtlasCountry.mkdirs();
            testCacheWithHadoopFileSystemHelper(cache, parentAtlasCountry,
                    AtlasGeneratorParameters.ATLAS_SCHEME.getDefault().getType());
        }

        try (TemporaryFile parent = File.temporaryFolder(FileSystems.getDefault()))
        {
            final File parentAtlas = parent.child("atlas");
            final File parentAtlasCountry = parentAtlas.child("AAA");
            final String fullParentPathURI = "file://" + parentAtlas.toString();
            final Function<URI, Optional<Resource>> fetcher = uri ->
            {
                final File file = new File(uri.getPath(), parent.toPath().getFileSystem());
                if (!file.exists())
                {
                    return Optional.empty();
                }
                return Optional.of(file);
            };
            final HadoopAtlasFileCache cache = new HadoopAtlasFileCache(fullParentPathURI, fetcher);
            parentAtlasCountry.mkdirs();
            testCacheWithHadoopFileSystemHelper(cache, parentAtlasCountry,
                    AtlasGeneratorParameters.ATLAS_SCHEME.getDefault().getType());
        }
    }

    @Test
    public void testInvalidate()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystemZZSubfolderScheme(filesystem);
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

    @Test
    public void testURIException()
    {
        try (FileSystem filesystem = Jimfs.newFileSystem(Configuration.osX()))
        {
            setupFilesystemZZSubfolderScheme(filesystem);
            final Function<URI, Optional<Resource>> fetcher = uri ->
            {
                final String path = uri.getPath();
                return Optional.of(new File(path, filesystem));
            };

            this.expectedException.expect(CoreException.class);
            this.expectedException.expectMessage("Bad URI syntax");
            new HadoopAtlasFileCache("scheme:|\\|;//foo/bar",
                    new SlippyTilePersistenceScheme(SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER),
                    new NamespaceCachingStrategy("HadoopAtlasFileCache_Test", filesystem), fetcher)
                            .get("AAA", new SlippyTile(1, 1, 1));
        }
        catch (final IOException exception)
        {
            throw new CoreException("FileSystem operation failed", exception);
        }
    }

    private void setupFilesystemDefaultScheme(final FileSystem filesystem)
    {
        final PackedAtlasBuilder builder = new PackedAtlasBuilder();

        builder.addPoint(1000000L, Location.forWkt("POINT(1 1)"), Maps.hashMap("foo", "bar"));

        final Atlas atlas = builder.get();
        final File atlasFile = new File("/Users/foo/atlas/AAA/AAA_1-1-1.atlas", filesystem);
        assert atlas != null;
        atlas.saveAsText(atlasFile);
    }

    private void setupFilesystemZZSubfolderScheme(final FileSystem filesystem)
    {
        final PackedAtlasBuilder builder = new PackedAtlasBuilder();

        builder.addPoint(1000000L, Location.forWkt("POINT(1 1)"), Maps.hashMap("foo", "bar"));

        final Atlas atlas = builder.get();
        final File atlasFile = new File("/Users/foo/atlas/AAA/1/AAA_1-1-1.atlas", filesystem);
        assert atlas != null;
        atlas.saveAsText(atlasFile);
    }

    private void testCacheWithHadoopFileSystemHelper(final HadoopAtlasFileCache cache,
            final File parentAtlasCountry, final SlippyTilePersistenceSchemeType schemeType)
    {
        try
        {
            final PackedAtlasBuilder builder1 = new PackedAtlasBuilder();
            builder1.addPoint(1L, Location.CENTER, Maps.hashMap());
            final PackedAtlas atlas1 = (PackedAtlas) builder1.get();

            final PackedAtlasBuilder builder2 = new PackedAtlasBuilder();
            builder2.addPoint(2L, Location.CENTER, Maps.hashMap());
            final PackedAtlas atlas2 = (PackedAtlas) builder2.get();

            if (SlippyTilePersistenceSchemeType.ZZ_SUBFOLDER.equals(schemeType))
            {
                final File atlasFile1 = parentAtlasCountry.child("1/AAA_1-1-1.atlas");
                atlas1.save(atlasFile1);
                final File atlasFile2 = parentAtlasCountry.child("2/AAA_2-2-2.atlas");
                atlas2.save(atlasFile2);
            }
            else if (SlippyTilePersistenceSchemeType.EMPTY.equals(schemeType))
            {
                final File atlasFile1 = parentAtlasCountry.child("AAA_1-1-1.atlas");
                atlas1.save(atlasFile1);
                final File atlasFile2 = parentAtlasCountry.child("AAA_2-2-2.atlas");
                atlas2.save(atlasFile2);
            }
            else
            {
                throw new CoreException("Unsupported scheme type: {}", schemeType);
            }

            // cache miss, this will create the cached copy
            final Resource resource1 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource2 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();
            final Optional<Resource> resourceDoesNotExist = cache.get("AAA",
                    new SlippyTile(3, 3, 3));

            Assert.assertEquals(atlas1, PackedAtlas.load(resource1));
            Assert.assertEquals(atlas2, PackedAtlas.load(resource2));
            Assert.assertTrue(resourceDoesNotExist.isEmpty());

            // cache hit, using cached copy
            final Resource resource3 = cache.get("AAA", new SlippyTile(1, 1, 1)).get();
            final Resource resource4 = cache.get("AAA", new SlippyTile(2, 2, 2)).get();

            Assert.assertEquals(atlas1, PackedAtlas.load(resource3));
            Assert.assertEquals(atlas2, PackedAtlas.load(resource4));
        }
        finally
        {
            cache.invalidate();
        }
    }
}
