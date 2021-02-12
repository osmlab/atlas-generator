package org.openstreetmap.atlas.mutator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * @author matthieun
 */
public class AtlasMutatorParametersTest
{
    private static final String BOUNDARIES = "resource://boundaries/boundaries.txt";
    private static final String SHARDING = "resource://sharding/tree.txt";

    @After
    public void cleanup()
    {
        ResourceFileSystem.clear();
    }

    @Before
    public void setup()
    {
        ResourceFileSystem.addResource(SHARDING,
                new InputStreamResource(() -> AtlasMutatorParametersTest.class
                        .getResourceAsStream("tree-6-14-100000.txt")));
        ResourceFileSystem.addResource(BOUNDARIES, new InputStreamResource(
                () -> AtlasMutatorParametersTest.class.getResourceAsStream("XYZ.txt")));
    }

    @Test
    public void testBoundaries()
    {
        final CommandMap commandMap = new CommandMap();
        commandMap.put(AtlasMutatorParameters.BOUNDARIES_PROVIDED.getName(), BOUNDARIES);
        final CountryBoundaryMap boundaries = AtlasMutatorParameters.boundaries(commandMap,
                ResourceFileSystem.simpleconfiguration());
        Assert.assertEquals(1, boundaries.size());
    }

    @Test
    public void testSharding()
    {
        final CommandMap commandMap = new CommandMap();
        commandMap.put(AtlasMutatorParameters.SHARDING_PROVIDED.getName(), "dynamic@" + SHARDING);
        final Sharding sharding = AtlasMutatorParameters.sharding(commandMap,
                ResourceFileSystem.simpleconfiguration());
        Assert.assertEquals(14290, Iterables.size(sharding.shards()));
    }
}
