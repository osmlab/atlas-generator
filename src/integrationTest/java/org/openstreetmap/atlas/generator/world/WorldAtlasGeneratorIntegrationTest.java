package org.openstreetmap.atlas.generator.world;

import java.util.Optional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.generator.AtlasGeneratorIntegrationTest;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.packed.PackedAtlas;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;

/**
 * @author jwpgage
 * @author matthieun
 */
public class WorldAtlasGeneratorIntegrationTest
{
    private static final String PBF_RESOURCE = "resource://test/pbf/9/9-168-233.osm.pbf";
    private static final String ATLAS_RESOURCE = "resource://test/output/atlas/9-168-233.atlas";

    @Before
    @After
    public void cleanup()
    {
        ResourceFileSystem.clear();
    }

    @Test
    public void testWorldAtlasGenerator()
    {
        ResourceFileSystem.addResource(PBF_RESOURCE,
                new InputStreamResource(() -> AtlasGeneratorIntegrationTest.class
                        .getResourceAsStream("DMA_cutout.osm.pbf")));

        final String[] args = { "-pbf=" + PBF_RESOURCE, "-atlas=" + ATLAS_RESOURCE };
        final WorldAtlasGenerator worldAtlasGenerator = new WorldAtlasGenerator();
        WorldAtlasGenerator
                .setHadoopFileSystemConfiguration(ResourceFileSystem.simpleconfiguration());
        ResourceFileSystem.printContents();
        worldAtlasGenerator.runWithoutQuitting(args);
        ResourceFileSystem.printContents();
        final Optional<PackedAtlas> atlasOptional = ResourceFileSystem.getAtlas(ATLAS_RESOURCE);
        Assert.assertTrue(atlasOptional.isPresent());
        final Atlas atlas = atlasOptional.get(); // NOSONAR
        Assert.assertEquals(112, atlas.numberOfNodes());
        Assert.assertEquals(240, atlas.numberOfEdges());
        Assert.assertEquals(327, atlas.numberOfAreas());
        Assert.assertEquals(10, atlas.numberOfLines());
        Assert.assertEquals(12, atlas.numberOfPoints());
        Assert.assertEquals(2, atlas.numberOfRelations());
    }
}
