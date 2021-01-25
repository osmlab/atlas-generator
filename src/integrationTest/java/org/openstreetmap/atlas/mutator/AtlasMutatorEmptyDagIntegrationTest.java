package org.openstreetmap.atlas.mutator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;

/**
 * Test for good behavior when the configuration leads to nothing to run
 * 
 * @author matthieun
 */
public class AtlasMutatorEmptyDagIntegrationTest extends AbstractAtlasMutatorIntegrationTest
{
    @Override
    public List<String> arguments()
    {
        final List<String> arguments = new ArrayList<>();
        // Here use a country that does not exist in the JSON configuration, to result in an empty
        // dag
        arguments.add("-countries=AAA");
        return arguments;
    }

    @Before
    public void before()
    {
        setup("AtlasMutatorEmptyDagIntegrationTest.json", "tree-6-14-100000.txt", "XYZ.txt");
        addResource(BOUNDARY, "AAA.txt", true);
    }

    @Test(expected = CoreException.class)
    public void mutate()
    {
        runWithoutQuitting();
    }
}
