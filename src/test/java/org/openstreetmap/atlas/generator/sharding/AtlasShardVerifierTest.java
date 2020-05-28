package org.openstreetmap.atlas.generator.sharding;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.filesystem.FileSystemHelper;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.openstreetmap.atlas.utilities.collections.StringList;

/**
 * @author matthieun
 */
public class AtlasShardVerifierTest
{
    private static final String INPUT = "resource://test/input";
    private static final String CHECK = "resource://test/check";
    private static final String OUTPUT = "resource://test/output/missing.txt";

    @Before
    @After
    public void clean()
    {
        ResourceFileSystem.clear();
    }

    public String[] getArguments()
    {
        final StringList sparkConfiguration = new StringList();
        ResourceFileSystem.simpleconfiguration().entrySet().stream()
                .forEach(entry -> sparkConfiguration.add(entry.getKey() + "=" + entry.getValue()));

        final List<String> arguments = new ArrayList<>();
        arguments.add("-" + AtlasShardVerifier.ATLAS_FOLDER.getName() + "=" + INPUT);
        arguments.add("-" + AtlasShardVerifier.OUTPUT.getName() + "=" + OUTPUT);
        arguments.add("-" + AtlasShardVerifier.COUNTRIES.getName() + "=ABC");
        arguments.add("-" + AtlasShardVerifier.EXPECTED_SHARDS.getName() + "=" + CHECK);
        arguments.add("-" + AtlasShardVerifier.SPARK_OPTIONS.getName() + "="
                + sparkConfiguration.join(","));

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }
        return args;
    }

    @Test
    public void testCommandFromDirectory()
    {
        final WritableResource atlas = new StringResource("blah");

        ResourceFileSystem.addResource(CHECK + "/ABC/ABC_10-11-12.atlas", atlas);
        ResourceFileSystem.addResource(CHECK + "/ABC/ABC_10-11-13.atlas", atlas);
        ResourceFileSystem.addResource(CHECK + "/ABC/ABC_10-11-14.atlas", atlas);
        ResourceFileSystem.addResource(CHECK + "/ABC/ABC_10-11-15.atlas", atlas);

        ResourceFileSystem.addResource(INPUT + "/ABC/ABC_10-11-12.atlas", atlas);
        ResourceFileSystem.addResource(INPUT + "/ABC/ABC_10-11-13.atlas", atlas);
        ResourceFileSystem.addResource(INPUT + "/ABC/ABC_10-11-14.atlas", atlas);

        new AtlasShardVerifier().runWithoutQuitting(getArguments());

        Assert.assertEquals("ABC_10-11-15",
                FileSystemHelper.resource(OUTPUT, ResourceFileSystem.simpleconfiguration()).all());
    }

    @Test
    public void testCommandFromFile()
    {
        final WritableResource atlas = new StringResource("blah");
        final WritableResource check = new StringResource(
                "ABC_10-11-12\nABC_10-11-13\nABC_10-11-14\nABC_10-11-15");

        ResourceFileSystem.addResource(CHECK, check);

        ResourceFileSystem.addResource(INPUT + "/ABC/ABC_10-11-12.atlas", atlas);
        ResourceFileSystem.addResource(INPUT + "/ABC/ABC_10-11-13.atlas", atlas);
        ResourceFileSystem.addResource(INPUT + "/ABC/ABC_10-11-14.atlas", atlas);

        new AtlasShardVerifier().runWithoutQuitting(getArguments());

        Assert.assertEquals("ABC_10-11-15",
                FileSystemHelper.resource(OUTPUT, ResourceFileSystem.simpleconfiguration()).all());
    }

    @Test
    public void testCountryFiltering()
    {
        final WritableResource atlas = new StringResource("blah");

        ResourceFileSystem.addResource(CHECK + "/ABC/ABC_10-11-12.atlas", atlas);
        ResourceFileSystem.addResource(CHECK + "/ABC/ABC_10-11-13.atlas", atlas);
        ResourceFileSystem.addResource(CHECK + "/DEF/DEF_10-11-14.atlas", atlas);
        ResourceFileSystem.addResource(CHECK + "/DEF/DEF_10-11-15.atlas", atlas);

        ResourceFileSystem.addResource(INPUT + "/ABC/ABC_10-11-12.atlas", atlas);
        ResourceFileSystem.addResource(INPUT + "/XYZ/XYZ_10-11-13.atlas", atlas);
        ResourceFileSystem.addResource(INPUT + "/DEF/DEF_10-11-14.atlas", atlas);

        new AtlasShardVerifier().runWithoutQuitting(getArguments());

        Assert.assertEquals("ABC_10-11-13",
                FileSystemHelper.resource(OUTPUT, ResourceFileSystem.simpleconfiguration()).all());
    }
}
