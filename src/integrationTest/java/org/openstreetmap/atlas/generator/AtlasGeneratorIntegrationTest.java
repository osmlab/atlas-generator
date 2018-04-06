package org.openstreetmap.atlas.generator;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.collections.StringList;

/**
 * @author matthieun
 */
public class AtlasGeneratorIntegrationTest
{
    public static final String BOUNDARY = "resource://test/boundaries/DMA.txt";
    public static final String TREE_13 = "resource://test/sharding/tree-6-13-100000.txt";
    public static final String TREE_14 = "resource://test/sharding/tree-6-14-100000.txt";
    public static final String PBF = "resource://test/pbf";
    public static final String PBF_233 = PBF + "/9/9-168-233.osm.pbf";
    public static final String PBF_234 = PBF + "/9/9-168-234.osm.pbf";
    public static final String OUTPUT = "resource://test/output";
    public static final String ATLAS_OUTPUT = "resource://test/atlas/DMA";

    static
    {
        addResource(PBF_233, "DMA_cutout.osm.pbf");
        addResource(PBF_234, "DMA_cutout.osm.pbf");
        addResource(TREE_13, "tree-6-13-100000.txt");
        addResource(TREE_14, "tree-6-14-100000.txt");
        addResource(BOUNDARY, "DMA.txt");
    }

    private static void addResource(final String path, final String name)
    {
        ResourceFileSystem.addResource(path, new InputStreamResource(
                () -> AtlasGeneratorIntegrationTest.class.getResourceAsStream(name)));
    }

    @Test
    public void testAtlasGeneration()
    {
        final StringList arguments = new StringList();
        arguments.add("-master=local");
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-countries=DMA");
        arguments.add("-countryShapes=" + BOUNDARY);
        arguments.add("-pbfs=" + PBF);
        arguments.add("-pbfSharding=dynamic@" + TREE_14);
        arguments.add("-pbfScheme=zz/zz-xx-yy.osm.pbf");
        arguments.add("-atlasScheme=zz/");
        arguments.add("-sharding=dynamic@" + TREE_13);
        arguments.add(
                "-sparkOptions=fs.resource.impl=" + ResourceFileSystem.class.getCanonicalName());

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }

        ResourceFileSystem.printContents();
        new AtlasGenerator().runWithoutQuitting(args);
        ResourceFileSystem.printContents();

        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem())
        {
            Assert.assertTrue(
                    resourceFileSystem.exists(new Path(ATLAS_OUTPUT + "/9/DMA_9-168-233.atlas")));
            Assert.assertTrue(
                    resourceFileSystem.exists(new Path(ATLAS_OUTPUT + "/9/DMA_9-168-234.atlas")));

        }
        catch (IllegalArgumentException | IOException e)
        {
            throw new CoreException("Unable to find output Atlas files.", e);
        }
    }
}
