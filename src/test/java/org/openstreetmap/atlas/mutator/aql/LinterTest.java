package org.openstreetmap.atlas.mutator.aql;

import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.lint.Linter;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.lint.domain.LintException;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.lint.domain.Source;
import org.openstreetmap.atlas.geography.atlas.dsl.engine.lint.lintlet.impl.InsecureWellFormednessLintlet;

/**
 * @author Yazad Khambata
 */
public class LinterTest
{
    @Test
    public void testFromClasspath()
    {
        try
        {
            Linter.instance.lint(Source.CLASSPATH, "aql-files",
                    new InsecureWellFormednessLintlet());
        }
        catch (final LintException e)
        {
            throw new AssertionError(e);
        }
    }
}
