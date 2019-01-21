package org.openstreetmap.atlas.generator.persistence.scheme;

import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;

/**
 * Definitions for various types of {@link SlippyTilePersistenceScheme}s. This enum forces users of
 * {@link SlippyTilePersistenceScheme} into some predefined schemes, instead of allowing totally
 * custom schemes. This should help mitigate gotchas surrounding path construction and other
 * implementation details. Schemes may or may not contain a terminating file extension. In the case
 * that the file extension is {@link FileSuffix} NONE, then the scheme can be thought of as a
 * directory scheme. If the {@link FileSuffix} is provided, then the scheme will represent a
 * terminated path (i.e.) a path containing a terminating file.
 *
 * @author lcram
 */
public enum SlippyTilePersistenceSchemeType
{
    // zz-xx-yy.pbf
    ZZ_XX_YY_PBF(
            SlippyTilePersistenceScheme.ZOOM + SlippyTilePersistenceScheme.HYPHEN
                    + SlippyTilePersistenceScheme.X_INDEX + SlippyTilePersistenceScheme.HYPHEN
                    + SlippyTilePersistenceScheme.Y_INDEX,
            FileSuffix.PBF),

    // zz/xx-yy.pbf
    ZZ_SLASH_XX_YY_PBF(
            SparkFileHelper.combine(SlippyTilePersistenceScheme.ZOOM,
                    SlippyTilePersistenceScheme.X_INDEX + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.Y_INDEX),
            FileSuffix.PBF),

    // zz/xx/yy/zz-xx-yy.pf
    ZZ_XX_YY_SUBFOLDERS_PBF(
            SparkFileHelper.combine(SlippyTilePersistenceScheme.ZOOM,
                    SlippyTilePersistenceScheme.X_INDEX, SlippyTilePersistenceScheme.Y_INDEX,
                    SlippyTilePersistenceScheme.ZOOM + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.X_INDEX
                            + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.Y_INDEX),
            FileSuffix.PBF),

    // zz-xx-yy.osm.pbf
    ZZ_XX_YY_OSMPBF(
            SlippyTilePersistenceScheme.ZOOM + SlippyTilePersistenceScheme.HYPHEN
                    + SlippyTilePersistenceScheme.X_INDEX + SlippyTilePersistenceScheme.HYPHEN
                    + SlippyTilePersistenceScheme.Y_INDEX,
            FileSuffix.OSMPBF),

    // zz/xx-yy.osm.pbf
    ZZ_SLASH_XX_YY_OSMPBF(
            SparkFileHelper.combine(SlippyTilePersistenceScheme.ZOOM,
                    SlippyTilePersistenceScheme.X_INDEX + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.Y_INDEX),
            FileSuffix.OSMPBF),

    // zz/zz-xx-yy.osm.pbf
    ZZ_SLASH_ZZ_XX_YY_OSMPBF(
            SparkFileHelper.combine(SlippyTilePersistenceScheme.ZOOM,
                    SlippyTilePersistenceScheme.ZOOM + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.X_INDEX
                            + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.Y_INDEX),
            FileSuffix.OSMPBF),

    // zz/xx/yy/zz-xx-yy.osm.pbf
    ZZ_XX_YY_SUBFOLDERS_OSMPBF(
            SparkFileHelper.combine(SlippyTilePersistenceScheme.ZOOM,
                    SlippyTilePersistenceScheme.X_INDEX, SlippyTilePersistenceScheme.Y_INDEX,
                    SlippyTilePersistenceScheme.ZOOM + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.X_INDEX
                            + SlippyTilePersistenceScheme.HYPHEN
                            + SlippyTilePersistenceScheme.Y_INDEX),
            FileSuffix.OSMPBF),

    // zz/
    // NOTE: this is a hack, it does NOT use SparkFileHelper.combine(), since the combine() method
    // filters empty path components. However, there are lots of "zz/" in the wild, so we must
    // support it here using a hardcoded "/"
    ZZ_SUBFOLDER(SlippyTilePersistenceScheme.ZOOM + "/", FileSuffix.NONE),

    EMPTY("", FileSuffix.NONE);

    private String value;

    private FileSuffix suffix;

    static SlippyTilePersistenceSchemeType enumNameToSchemeType(final String string)
    {
        for (final SlippyTilePersistenceSchemeType type : SlippyTilePersistenceSchemeType.values())
        {
            // Try matching by enum name
            if (type.name().equalsIgnoreCase(string))
            {
                return type;
            }
        }
        throw new CoreException("Invalid SlippyTilePersistenceSchemeType name {}", string);
    }

    static SlippyTilePersistenceSchemeType legacyArbitrarySchemeToSchemeType(final String string)
    {
        for (final SlippyTilePersistenceSchemeType type : SlippyTilePersistenceSchemeType.values())
        {
            // Try matching by the scheme string
            final String schemeString = type.getValue() + type.getSuffix().toString();
            if (schemeString.equals(string))
            {
                return type;
            }
        }
        return null;
    }

    SlippyTilePersistenceSchemeType(final String value, final FileSuffix suffix)
    {
        this.value = value;
        this.suffix = suffix;
    }

    public FileSuffix getSuffix()
    {
        return this.suffix;
    }

    public String getValue()
    {
        return this.value;
    }
}
