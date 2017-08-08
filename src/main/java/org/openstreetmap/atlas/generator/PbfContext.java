package org.openstreetmap.atlas.generator;

import java.io.Serializable;

import org.openstreetmap.atlas.geography.sharding.Sharding;

/**
 * @author matthieun
 */
public class PbfContext implements Serializable
{
    private static final long serialVersionUID = -369231755462486466L;

    private final String pbfPath;
    private final Sharding sharding;

    public PbfContext(final String pbfPath, final Sharding sharding)
    {
        this.pbfPath = pbfPath;
        this.sharding = sharding;
    }

    public String getPbfPath()
    {
        return this.pbfPath;
    }

    public Sharding getSharding()
    {
        return this.sharding;
    }
}
