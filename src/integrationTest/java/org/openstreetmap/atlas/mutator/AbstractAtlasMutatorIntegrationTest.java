package org.openstreetmap.atlas.mutator;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.tools.caching.HadoopAtlasFileCache;
import org.openstreetmap.atlas.generator.tools.spark.SparkJob;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.AtlasResourceLoader;
import org.openstreetmap.atlas.mutator.configuration.parsing.provider.file.HadoopPbfFileCache;
import org.openstreetmap.atlas.streaming.compression.Compressor;
import org.openstreetmap.atlas.streaming.resource.ByteArrayResource;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.streaming.resource.Resource;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.collections.StringList;

/**
 * @author matthieun
 */
public abstract class AbstractAtlasMutatorIntegrationTest
{
    public static final String KEY = "mutated";
    public static final String VALUE = "yes";

    public static final String INPUT = "resource://test/input";
    public static final String BOUNDARY_FILE_NAME = "boundaries.txt.gz";
    public static final String SHARDING_FILE_NAME = "sharding.txt";
    public static final String BOUNDARY_META_NAME = "boundaries.meta";
    public static final String SHARDING_META_NAME = "sharding.meta";
    public static final String BOUNDARY_META_CONTENTS = "I am the boundaries meta-data";
    public static final String SHARDING_META_CONTENTS = "I am the sharding meta-data";
    public static final String BOUNDARY = INPUT + "/" + BOUNDARY_FILE_NAME;
    public static final String SHARDING = INPUT + "/" + SHARDING_FILE_NAME;
    public static final String BOUNDARY_META = INPUT + "/" + BOUNDARY_META_NAME;
    public static final String SHARDING_META = INPUT + "/" + SHARDING_META_NAME;
    public static final String MUTATIONS = "resource://test/mutations/mutations.json";
    public static final String OUTPUT_MINUS_1 = "resource://test/output";
    public static final String OUTPUT = OUTPUT_MINUS_1 + "/atlas";
    public static final String OUTPUT_SHARDING = OUTPUT + "/" + SHARDING_FILE_NAME;
    public static final String OUTPUT_SHARDING_NAME = "dynamic@" + OUTPUT_SHARDING;
    public static final String OUTPUT_BOUNDARIES = OUTPUT + "/" + BOUNDARY_FILE_NAME;
    public static final String OUTPUT_SHARDING_META = OUTPUT + "/" + SHARDING_META_NAME;
    public static final String OUTPUT_BOUNDARIES_META = OUTPUT + "/" + BOUNDARY_META_NAME;
    public static final String OUTPUT_COUNTRY_LIST = OUTPUT + "/" + AtlasMutator.COUNTRY_AND_LEVELS;

    public static void addAtlasResource(final String path, final Atlas atlas)
    {
        final long size = 1024 * 8;
        final ByteArrayResource resource = new ByteArrayResource(size);
        atlas.save(resource);
        addResource(path, resource);
    }

    public static void addResource(final String path, final Resource resource)
    {
        ResourceFileSystem.addResource(path, resource);
    }

    public static void addResource(final String path, final String name, final boolean gzipIt)
    {
        Resource input = new InputStreamResource(
                () -> AtlasMutatorIntegrationTest.class.getResourceAsStream(name));
        if (gzipIt)
        {
            final ByteArrayResource newInput = new ByteArrayResource();
            newInput.setCompressor(Compressor.GZIP);
            input.copyTo(newInput);
            input = newInput;
        }
        addResource(path, input);
    }

    public static void addResource(final String path, final String name)
    {
        addResource(path, name, false);
    }

    public static List<String> listPaths(final String prefix)
    {
        try
        {
            try (FileSystem fileSystem = new ResourceFileSystem())
            {
                return Iterables.stream(Iterables.asList(fileSystem.listStatus(new Path(prefix))))
                        .map(fileStatus -> fileStatus.getPath().toString()).collectToList();
            }
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not list resource {}", prefix, e);
        }
    }

    public abstract List<String> arguments();

    public Atlas atlasForPath(final String path)
    {
        try
        {
            return new AtlasResourceLoader().load(forPath(path));
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not open {}", path, e);
        }
    }

    public Resource forPath(final String path)
    {
        try
        {
            final Resource result = SparkJob.resource(path, sparkConfiguration());
            if (result == null)
            {
                throw new CoreException("Could not find {}", path);
            }
            return result;
        }
        catch (final Exception e)
        {
            throw new CoreException("Could not open {}", path, e);
        }
    }

    public String[] getArguments()
    {
        final StringList sparkConfiguration = new StringList();
        sparkConfiguration().entrySet().stream()
                .forEach(entry -> sparkConfiguration.add(entry.getKey() + "=" + entry.getValue()));

        final List<String> arguments = arguments();
        arguments.add("-cluster=local");
        arguments.add("-atlas=" + INPUT);
        arguments.add("-output=" + OUTPUT);
        arguments.add("-startedFolder=resource://test/started");
        arguments.add("-mutatorConfigurationResource=" + MUTATIONS);
        arguments.add("-mutatorConfigurationJson={}");
        arguments.add("-copyShardingAndBoundaries=true");
        arguments.add("-debugFeatureChangeOutput="
                + AtlasMutatorParameters.DebugFeatureChangeOutput.ALL.name().toLowerCase());
        arguments.add("-sparkOptions=" + sparkConfiguration.join(","));

        final String[] args = new String[arguments.size()];
        for (int i = 0; i < arguments.size(); i++)
        {
            args[i] = arguments.get(i);
        }
        return args;
    }

    public boolean resourceExits(final String path)
    {
        final Resource result = SparkJob.resource(path, sparkConfiguration());
        return result != null;
    }

    public void runWithoutQuitting()
    {
        // Run the mutation job
        new AtlasMutator().runWithoutQuitting(getArguments());
    }

    public void setup(final String jsonConfiguration, final String treeFile,
            final String boundaryFile)
    {
        new HadoopAtlasFileCache("", Maps.hashMap()).invalidate();
        new HadoopPbfFileCache("", SlippyTilePersistenceScheme.getSchemeInstanceFromString(""),
                Maps.hashMap()).invalidate();
        ResourceFileSystem.clear();

        addResource(SHARDING, treeFile);
        addResource(BOUNDARY, boundaryFile, true);
        addResource(MUTATIONS, jsonConfiguration);
        addResource(SHARDING_META, new StringResource(SHARDING_META_CONTENTS));
        addResource(BOUNDARY_META, new StringResource(BOUNDARY_META_CONTENTS));
    }

    public Map<String, String> sparkConfiguration()
    {
        final Map<String, String> result = ResourceFileSystem.simpleconfiguration();
        result.put("spark.driver.allowMultipleContexts", "true");
        result.put("spark.executor.cores", "3");
        result.put("spark.driver.cores", "3");
        result.put("spark.speculation", "false");
        return result;
    }

    void dump()
    {
        // This is used for local testing. If a developer adds a local file system path in this
        // environment variable, the result of the job will be entirely copied to the specified
        // path.
        final String resourceFileSystemDump = System.getenv("RESOURCE_FILE_SYSTEM_DUMP");
        if (resourceFileSystemDump != null && !resourceFileSystemDump.isEmpty())
        {
            final File folder = new File(resourceFileSystemDump);
            if (folder.exists())
            {
                folder.deleteRecursively();
            }
            folder.mkdirs();
            ResourceFileSystem.dumpToDisk(folder);
        }
    }
}
