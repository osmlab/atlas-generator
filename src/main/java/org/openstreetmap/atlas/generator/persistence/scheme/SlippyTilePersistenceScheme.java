package org.openstreetmap.atlas.generator.persistence.scheme;

import java.io.Serializable;

import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * This class helps users construct storage paths dynamically based on the name of the target
 * resource to be saved. The various allowed storage schemes are defined in
 * {@link SlippyTilePersistenceSchemeType}.<br>
 * <br>
 * For example, one PBF storage scheme is 'ZZ_XX_YY_SUBFOLDERS_PBF', which is defined as
 * 'zz/xx/yy/zz-xx-yy.pbf'. This means that when saving the PBF '12-34-48.pbf' to a remote store,
 * users can easily construct the proper storage path for that PBF (i.e. 12/34/48/12-34-48.pbf) and
 * use that to save the file. An example usage:<br>
 * <br>
 * <code>
 * // Pbf is tile 12-34-48<br>
 * // Assume we have some kind of PBF object<br>
 * Pbf myPbfObject;<br>
 * SlippyTilePersistenceScheme scheme = new SlippyTilePersistenceScheme(SlippyTilePersistenceSchemeType.ZZ_XX_YY_SUBFOLDERS_PBF);<br>
 * // OR<br>
 * SlippyTilePersistenceScheme scheme = SlippyTilePersistenceScheme.getSchemeInstanceFromString("ZZ_XX_YY_SUBFOLDERS_PBF");<br>
 * WritableResource resource = new File(SparkFileHelper.combine("hdfs://myPBFs", scheme.compile(myPbfObject.getShard())));<br>
 * myPbfObject.save(resource);<br>
 * // we now have a PBF saved at "hdfs://myPBFs/12/34/48/12-34-48.pbf"<br>
 * </code>
 *
 * @author matthieun
 */
public class SlippyTilePersistenceScheme implements Serializable
{
    private static final long serialVersionUID = -7098822765716165700L;

    public static final String ZOOM = "zz";
    public static final String X_INDEX = "xx";
    public static final String Y_INDEX = "yy";

    static final String HYPHEN = "-";

    private final String scheme;
    private final FileSuffix suffix;

    public static SlippyTilePersistenceScheme getSchemeInstanceFromString(final String string)
    {
        SlippyTilePersistenceSchemeType type;

        /*
         * Backwards compatibility hack: for now, allow this class to parse any old scheme strings
         * still found in the wild. Ideally, downstream users should migrate calling code to use the
         * scheme enum names instead of arbitrary scheme strings. E.G. users should use the scheme
         * enum name 'ZZ_XX_YY_PBF' as opposed to the actual scheme string, which is 'zz-xx-yy.pbf'.
         */
        type = SlippyTilePersistenceSchemeType.legacyArbitrarySchemeToSchemeType(string);

        if (type == null)
        {
            type = SlippyTilePersistenceSchemeType.enumNameToSchemeType(string);
        }
        return new SlippyTilePersistenceScheme(type);
    }

    public SlippyTilePersistenceScheme(final SlippyTilePersistenceSchemeType type)
    {
        this.scheme = type.getValue();
        this.suffix = type.getSuffix();
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
                .replaceAll(Y_INDEX, String.valueOf(yIndex)) + this.suffix;
    }

    public String getScheme()
    {
        return this.scheme + this.suffix;
    }
}
