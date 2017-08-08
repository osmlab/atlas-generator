package org.openstreetmap.atlas.generator.tools.streaming.resource;

import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * Argument handling for the HDFSCommand example application
 *
 * @author cstaylor
 */
class HDFSArgumentsCommand extends Command
{
    private static final Switch<String> SERVER_PARAMETER = new Switch<>("server", "HDFS host",
            str -> str, Optionality.REQUIRED);

    private static final Switch<Integer> PORT_PARAMETER = new Switch<>("port", "HDFS port",
            value -> Integer.valueOf(value), Optionality.OPTIONAL, "9000");

    private static final Switch<String> PATH_PARAMETER = new Switch<>("path", "HDFS path",
            str -> str, Optionality.REQUIRED);

    private Path root;

    protected Path getRoot()
    {
        return this.root;
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final String server = (String) command.get(SERVER_PARAMETER);
        final Integer port = (Integer) command.get(PORT_PARAMETER);
        final String path = (String) command.get(PATH_PARAMETER);
        setRoot(new Path(String.format("hdfs://%s:%d/%s", server, port, path)));
        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(SERVER_PARAMETER, PORT_PARAMETER, PATH_PARAMETER);
    }

    private void setRoot(final Path root)
    {
        this.root = root;
    }
}
