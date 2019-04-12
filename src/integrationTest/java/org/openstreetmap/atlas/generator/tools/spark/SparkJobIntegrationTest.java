package org.openstreetmap.atlas.generator.tools.spark;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * @author matthieun
 */
public class SparkJobIntegrationTest
{
    @Test
    public void test() throws IOException
    {
        final SparkJob sparkJob = new SparkJob()
        {
            private static final long serialVersionUID = -6657905234445292742L;

            @Override
            public String getName()
            {
                return "SparkJobIntegrationTest";
            }

            @Override
            public void start(final CommandMap command)
            {
                // Do nothing.
            }
        };

        sparkJob.runWithoutQuitting(getArguments());

        try (ResourceFileSystem resourceFileSystem = new ResourceFileSystem())
        {
            Assert.assertTrue(resourceFileSystem.exists(new Path("resource://blah/_SUCCESS")));
        }
    }

    private String[] getArguments()
    {
        final StringList sparkConfiguration = new StringList();
        ResourceFileSystem.simpleconfiguration().entrySet().stream()
                .forEach(entry -> sparkConfiguration.add(entry.getKey() + "=" + entry.getValue()));

        final StringList arguments = new StringList();
        arguments.add("-master=local");
        arguments.add("-output=resource://blah");
        arguments.add("-sparkOptions=" + sparkConfiguration.join(","));

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }
        return args;
    }
}
