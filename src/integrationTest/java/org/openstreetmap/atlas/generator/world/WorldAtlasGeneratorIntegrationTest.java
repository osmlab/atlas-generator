package org.openstreetmap.atlas.generator.world;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorIntegrationTest;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;

public class WorldAtlasGeneratorIntegrationTest
{
    static
    {
        addResource("resource://test/pbf/9/9-168-233.osm.pbf", "DMA_cutout.osm.pbf");
    }

    private static void addResource(final String path, final String name)
    {
        ResourceFileSystem.addResource(path, new InputStreamResource(
                () -> AtlasGeneratorIntegrationTest.class.getResourceAsStream(name)));
    }

    @Test
    public void testWorldAtlasGenerator()
    {
        final String[] args = { "-pbf=resource://test/pbf/9/9-168-233.osm.pbf",
                "-atlas=resource://test/output/atlas/9-168-233.atlas" };
        final WorldAtlasGenerator worldAtlasGenerator = new WorldAtlasGenerator();
        final Map<String, String> configuration = new HashMap<>();
        configuration.put("fs.resource.impl", ResourceFileSystem.class.getCanonicalName());
        worldAtlasGenerator.setHadoopFileSystemConfiguration(configuration);
        ResourceFileSystem.printContents();
        worldAtlasGenerator.runWithoutQuitting(args);
        ResourceFileSystem.printContents();
        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem())
        {
            Assert.assertTrue(resourceFileSystem
                    .exists(new Path("resource://test/output/atlas/9-168-233.atlas")));
        }
        catch (IllegalArgumentException | IOException e)
        {
            throw new CoreException("Unable to find output Atlas files.", e);
        }
    }
}
