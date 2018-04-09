package org.openstreetmap.atlas.generator.persistence.scheme;

import java.io.Serializable;

import org.openstreetmap.atlas.geography.sharding.SlippyTile;

/**
 * @author matthieun
 */
public class SlippyTilePersistenceScheme implements Serializable
{
    private static final long serialVersionUID = -7098822765716165700L;

    public static final String ZOOM = "zz";
    public static final String X_INDEX = "xx";
    public static final String Y_INDEX = "yy";

    private final String scheme;

    public SlippyTilePersistenceScheme(final String scheme)
    {
        this.scheme = scheme;
    }

    public String compile(final SlippyTile tile)
    {
        return compile(String.valueOf(tile.getZoom()), String.valueOf(tile.getX()),
                String.valueOf(tile.getY()));
    }

    public String compile(final String zoom, final String xIndex, final String yIndex)
    {
        return this.scheme.replaceAll(ZOOM, String.valueOf(zoom))
                .replaceAll(X_INDEX, String.valueOf(xIndex))
                .replaceAll(Y_INDEX, String.valueOf(yIndex));
    }

    public String getScheme()
    {
        return this.scheme;
    }
}
