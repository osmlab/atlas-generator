package org.openstreetmap.atlas.mutator.configuration.parsing.provider;

import java.io.Serializable;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.mutator.configuration.parsing.AtlasMutatorConfigurationParser;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;

/**
 * Configured wrapper around an {@link AtlasProvider}, the class of which can be swapped underneath
 * with any proper implementation. The associated provider configuration will be passed to the
 * default constructor of the associated {@link AtlasProvider} as a clean configuration.
 * 
 * @author matthieun
 */
public class ConfiguredAtlasProvider implements Serializable
{
    private static final long serialVersionUID = 5631940672482541575L;

    public static final String CONFIGURATION_ROOT = AtlasMutatorConfigurationParser.CONFIGURATION_GLOBAL
            + ".atlasProviders";

    private static final String CONFIGURATION_CLASS_NAME = "className";
    private static final String CONFIGURATION_PROVIDER_CONFIGURATION = "providerConfiguration";

    private final AtlasProvider atlasProvider;

    public ConfiguredAtlasProvider(final String name, final Configuration configuration)
    {
        final String configurationRoot = CONFIGURATION_ROOT + "." + name;
        final ConfigurationReader reader = new ConfigurationReader(configurationRoot);
        final String atlasProviderClassName = reader.configurationValue(configuration,
                CONFIGURATION_CLASS_NAME, "");
        final Configuration providerConfiguration = configuration
                .subConfiguration(configurationRoot + "." + CONFIGURATION_PROVIDER_CONFIGURATION)
                .orElseThrow(() -> new CoreException("Unable to find \"{}\" for atlasProvider {}",
                        CONFIGURATION_PROVIDER_CONFIGURATION, atlasProviderClassName));

        this.atlasProvider = AtlasProvider.from(atlasProviderClassName, providerConfiguration);
    }

    public AtlasProvider getAtlasProvider()
    {
        return this.atlasProvider;
    }
}
