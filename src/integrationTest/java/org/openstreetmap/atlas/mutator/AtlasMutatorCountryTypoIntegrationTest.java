package org.openstreetmap.atlas.mutator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.streaming.resource.StringResource;

/**
 * Test for good behavior when the configuration leads to nothing to run
 *
 * @author matthieun
 */
public class AtlasMutatorCountryTypoIntegrationTest extends AbstractAtlasMutatorIntegrationTest
{
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Override
    public List<String> arguments()
    {
        final List<String> arguments = new ArrayList<>();
        // Here use a country that does not have a boundary
        arguments.add("-" + AtlasGeneratorParameters.COUNTRIES.getName() + "=AAA");
        return arguments;
    }

    @Before
    public void before()
    {
        setup("AtlasMutatorIntegrationTest.json", "tree-6-14-100000.txt", "XYZ.txt");
    }

    @Test
    public void mutate()
    {
        this.exceptionRule.expect(new BaseMatcher<Object>()
        {
            @Override
            public void describeTo(final Description description)
            {
                // N/A
            }

            @Override
            public boolean matches(final Object item)
            {
                if (item instanceof Throwable)
                {
                    final Throwable throwable = (Throwable) item;
                    final StringResource stack = new StringResource();
                    try (OutputStream out = stack.write())
                    {
                        throwable.printStackTrace(new PrintStream(out));
                    }
                    catch (final IOException e)
                    {
                        throw new CoreException("Unable to parse exception", e);
                    }
                    final String stackString = stack.writtenString();
                    return stackString != null && stackString.contains("not have a boundary");
                }
                return false;
            }
        });
        runWithoutQuitting();
    }
}
