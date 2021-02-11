package org.openstreetmap.atlas.mutator.configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.AtlasGeneratorParameters;
import org.openstreetmap.atlas.generator.tools.streaming.ResourceFileSystem;
import org.openstreetmap.atlas.mutator.AtlasMutatorParameters;
import org.openstreetmap.atlas.streaming.resource.OutputStreamWritableResource;
import org.openstreetmap.atlas.streaming.resource.WritableResource;
import org.openstreetmap.atlas.utilities.collections.StringList;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to test locally whether a configuration works
 * 
 * @author matthieun
 */
public class AtlasMutatorConfigurationCommand extends Command
{
    public static final Switch<WritableResource> OUTPUT = new Switch<>("output",
            "Resource to save output", value ->
            {
                try
                {
                    return new OutputStreamWritableResource(
                            new ResourceFileSystem().create(new Path(value)));
                }
                catch (final IOException e)
                {
                    throw new CoreException("Could not save to {}", value, e);
                }
            }, Optionality.OPTIONAL);

    private static final Logger logger = LoggerFactory
            .getLogger(AtlasMutatorConfigurationCommand.class);

    public static void main(final String[] args)
    {
        new AtlasMutatorConfigurationCommand().run(args);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final Map<String, String> configurationMap = ResourceFileSystem.simpleconfiguration();
        final Set<String> countries = ((StringList) command.get(AtlasGeneratorParameters.COUNTRIES))
                .stream().collect(Collectors.toSet());
        final Configuration configuration = AtlasMutatorParameters.mutatorsConfiguration(command,
                configurationMap);
        final boolean groupCountries = (boolean) command
                .get(AtlasMutatorParameters.GROUP_COUNTRIES);
        final AtlasMutatorConfiguration atlasMutatorConfiguration = new AtlasMutatorConfiguration(
                countries, null, null, "input", "output", configurationMap, configuration,
                groupCountries, true, true);
        final String result = atlasMutatorConfiguration.detailsString();
        if (logger.isInfoEnabled())
        {
            logger.info("Parsed configuration:\n{}", result);
        }
        final WritableResource output = (WritableResource) command.get(OUTPUT);
        if (output != null)
        {
            output.writeAndClose(result);
        }
        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(AtlasMutatorParameters.MUTATOR_CONFIGURATION_RESOURCE,
                AtlasMutatorParameters.MUTATOR_CONFIGURATION_JSON,
                AtlasGeneratorParameters.COUNTRIES, AtlasMutatorParameters.GROUP_COUNTRIES, OUTPUT);
    }
}
