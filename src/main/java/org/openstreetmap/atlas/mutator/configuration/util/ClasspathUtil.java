package org.openstreetmap.atlas.mutator.configuration.util;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang.Validate;

/**
 * Access files from the classpath.
 *
 * @author Yazad Khambata
 */
public final class ClasspathUtil
{
    public static String fromClasspath(final String classpathLocation)
    {
        Validate.notEmpty(classpathLocation, "The classpath is EMPTY.");

        try
        {
            final byte[] aqlBytes = Files.readAllBytes(
                    Paths.get(ClasspathUtil.class.getResource(classpathLocation).toURI()));
            return new String(aqlBytes, Charset.defaultCharset());
        }
        catch (final Exception exception)
        {
            throw new IllegalArgumentException("Couldn't load: " + classpathLocation, exception);
        }
    }

    private ClasspathUtil()
    {
    }
}
