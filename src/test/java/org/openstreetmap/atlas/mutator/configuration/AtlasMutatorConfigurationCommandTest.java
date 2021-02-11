package org.openstreetmap.atlas.mutator.configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.mutator.AtlasMutatorParameters;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;

/**
 * @author matthieun
 */
public class AtlasMutatorConfigurationCommandTest
{
    private static final String RESOURCE_PATH = "resource://test/conf.json";
    private static final String OUTPUT_PATH = "resource://test/output.log";

    @Before
    @After
    public void clean()
    {
        ResourceFileSystem.clear();
    }

    @Test
    public void testCommand()
    {
        final Resource input = new InputStreamResource(
                () -> AtlasMutatorConfigurationCommandTest.class
                        .getResourceAsStream("testLoadConfiguration.json"));
        ResourceFileSystem.addResource(RESOURCE_PATH, input);
        new AtlasMutatorConfigurationCommand().runWithoutQuitting(getArguments());
        final String output = new InputStreamResource(() ->
        {
            try
            {
                return new ResourceFileSystem().open(new Path(OUTPUT_PATH));
            }
            catch (final IOException e)
            {
                throw new CoreException("Could not open {}", OUTPUT_PATH, e);
            }
        }).all();
        final String reference = new InputStreamResource(
                () -> AtlasMutatorConfigurationCommandTest.class
                        .getResourceAsStream("AtlasMutatorConfigurationCommandTest.txt")).all();
        Assert.assertEquals(reference, output);
    }

    private String[] getArguments()
    {
        final List<String> arguments = new ArrayList<>();
        arguments.add("-" + AtlasGeneratorParameters.COUNTRIES.getName() + "=ABC,DEF,XYZ");
        arguments.add("-" + AtlasMutatorParameters.MUTATOR_CONFIGURATION_RESOURCE.getName() + "="
                + RESOURCE_PATH);
        arguments.add("-" + AtlasMutatorParameters.GROUP_COUNTRIES.getName() + "=true");
        arguments.add("-" + AtlasMutatorConfigurationCommand.OUTPUT.getName() + "=" + OUTPUT_PATH);

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }
        return args;
    }
}
