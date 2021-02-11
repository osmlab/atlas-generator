package org.openstreetmap.atlas.mutator.configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;

import com.google.common.reflect.ClassPath;

/**
 * @author matthieun
 */
public abstract class ConfiguredObjectBuilder
{
    private static final ConcurrentHashMap<String, List<ClassPath.ClassInfo>> SCAN_URL_TO_CLASS_INFOS = new ConcurrentHashMap<>();

    public static final String CONFIGURATION_SCAN_URLS = "global.scanUrls";
    public static final String CONFIGURATION_CLASS_NAME = "className";

    private final Configuration configuration;
    private final Map<String, String> sparkConfiguration;

    public ConfiguredObjectBuilder(final Configuration configuration,
            final Map<String, String> sparkConfiguration)
    {
        this.configuration = configuration;
        this.sparkConfiguration = sparkConfiguration;
        try
        {
            final List<String> scanUrls = configuration.get(CONFIGURATION_SCAN_URLS,
                    Collections.singletonList("org.openstreetmap.atlas")).value();
            if (scanUrls.stream()
                    .anyMatch(scanUrl -> !SCAN_URL_TO_CLASS_INFOS.containsKey(scanUrl)))
            {
                final ClassLoader loader = Thread.currentThread().getContextClassLoader();
                final ClassPath classPath = ClassPath.from(loader);
                for (final String scanUrl : scanUrls)
                {
                    if (!SCAN_URL_TO_CLASS_INFOS.containsKey(scanUrl))
                    {
                        SCAN_URL_TO_CLASS_INFOS.put(scanUrl, new ArrayList<>());
                        for (final ClassPath.ClassInfo classInfo : classPath
                                .getTopLevelClassesRecursive(scanUrl))
                        {
                            SCAN_URL_TO_CLASS_INFOS.get(scanUrl).add(classInfo);
                        }
                    }
                }
            }
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to build objects with {}.",
                    this.getClass().getSimpleName(), e);
        }
    }

    public Configuration getConfiguration()
    {
        return this.configuration;
    }

    protected Class<?> getClass(final String configurationRoot)
    {
        final String className = getClassName(configurationRoot);
        Class<?> clazz = null;
        for (final ClassPath.ClassInfo classInfo : SCAN_URL_TO_CLASS_INFOS.values().stream()
                .flatMap(List::stream).collect(Collectors.toList()))
        {
            if (classInfo.getSimpleName().equals(className))
            {
                try
                {
                    clazz = Thread.currentThread().getContextClassLoader()
                            .loadClass(classInfo.getName());
                }
                catch (final Exception e)
                {
                    throw new CoreException("Cannot build {}", className, e);
                }
            }
        }
        return clazz;
    }

    protected String getClassName(final String configurationRoot)
    {
        return new ConfigurationReader(configurationRoot).configurationValue(this.configuration,
                ConfiguredObjectBuilder.CONFIGURATION_CLASS_NAME, configurationRoot);
    }

    protected Map<String, String> getSparkConfiguration()
    {
        return this.sparkConfiguration;
    }
}
