package org.openstreetmap.atlas.generator.sharding;

import java.util.Set;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.writers.SafeBufferedWriter;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * @author matthieun
 */
public class AtlasShardVerifier extends Command
{
    private static final Switch<File> ATLAS_FOLDER = new Switch<>("atlasFolder",
            "Folder containing Atlas Shards named by the AtlasGenerator", File::new);
    private static final Switch<Set<CountryShard>> EXPECTED_SHARDS = new Switch<>("expectedShards",
            "Text file containing all the expected shards", value -> new File(value).linesList()
                    .stream().map(CountryShard::forName).collect(Collectors.toSet()));
    private static final Switch<File> OUTPUT = new Switch<>("output",
            "The file to list all the missing shards", File::new);

    public static void main(final String[] args)
    {
        new AtlasShardVerifier().run(args);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final File atlasFolder = (File) command.get(ATLAS_FOLDER);
        final File output = (File) command.get(OUTPUT);
        @SuppressWarnings("unchecked")
        final Set<CountryShard> expectedShards = (Set<CountryShard>) command.get(EXPECTED_SHARDS);
        final Set<CountryShard> existingShards = atlasFolder.listFilesRecursively().stream()
                .filter(Atlas::isAtlas).map(File::getName)
                .map(name -> StringList.split(name, ".").get(0)).map(CountryShard::forName)
                .collect(Collectors.toSet());
        expectedShards.removeAll(existingShards);
        try (SafeBufferedWriter writer = output.writer())
        {
            expectedShards.stream().map(CountryShard::toString).forEach(writer::writeLine);
        }
        catch (final Exception e)
        {
            throw new CoreException("Verification failed", e);
        }
        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(ATLAS_FOLDER, EXPECTED_SHARDS, OUTPUT);
    }
}
