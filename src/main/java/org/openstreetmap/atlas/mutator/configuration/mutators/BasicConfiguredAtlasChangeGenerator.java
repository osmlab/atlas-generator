package org.openstreetmap.atlas.mutator.configuration.mutators;

import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.AtlasChangeGenerator;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * Class allowing a non-configured {@link AtlasChangeGenerator} to be treated as a
 * {@link ConfiguredAtlasChangeGenerator} with default configuration.
 *
 * @author matthieun
 */
public class BasicConfiguredAtlasChangeGenerator extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = 3191012712865364099L;

    private final AtlasChangeGenerator source;

    /**
     * @param name
     *            The name of the {@link AtlasChangeGenerator}
     * @param configuration
     *            The configuration where the name can be found
     * @return A no-operation {@link BasicConfiguredAtlasChangeGenerator} that still parsed the
     *         configuration and adopted the given name, so it can be used to determine if a
     *         not-found {@link AtlasChangeGenerator} was configured as disabled for example.
     */
    public static BasicConfiguredAtlasChangeGenerator noOp(final String name,
            final Configuration configuration)
    {
        return new BasicConfiguredAtlasChangeGenerator(name, atlas -> new HashSet<>(),
                configuration);
    }

    public BasicConfiguredAtlasChangeGenerator(final String name, final AtlasChangeGenerator source,
            final Configuration configuration)
    {
        super(name, configuration);
        this.source = source;
    }

    @Override
    public boolean equals(final Object other)
    {
        return super.equals(other);
    }

    @Override
    public Set<FeatureChange> generate(final Atlas atlas)
    {
        try
        {
            return super.generate(atlas);
        }
        catch (final Exception e)
        {
            throw new CoreException("Unable to generate feature changes for {}", getName(), e);
        }
    }

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        return this.source.generateWithoutValidation(atlas);
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
}
