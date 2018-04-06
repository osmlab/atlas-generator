package org.openstreetmap.atlas.generator;

import java.io.Serializable;

import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.geography.sharding.Sharding;

/**
 * @author matthieun
 */
public class PbfContext implements Serializable
{
    private static final long serialVersionUID = -369231755462486466L;

    private final String pbfPath;
    private final Sharding sharding;
    private final SlippyTilePersistenceScheme scheme;

    public PbfContext(final String pbfPath, final Sharding sharding,
            final SlippyTilePersistenceScheme scheme)
    {
        this.pbfPath = pbfPath;
        this.sharding = sharding;
        this.scheme = scheme;
    }

    public String getPbfPath()
    {
        return this.pbfPath;
    }

    public SlippyTilePersistenceScheme getScheme()
    {
        return this.scheme;
    }

    public Sharding getSharding()
    {
        return this.sharding;
    }
}
