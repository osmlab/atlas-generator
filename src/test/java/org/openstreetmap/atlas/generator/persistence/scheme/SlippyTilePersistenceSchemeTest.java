package org.openstreetmap.atlas.generator.persistence.scheme;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.exception.CoreException;
import org.openstreetmap.atlas.generator.tools.spark.utilities.SparkFileHelper;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lcram
 */
public class SlippyTilePersistenceSchemeTest
{
    private static final Logger logger = LoggerFactory.getLogger(SlippyTilePersistenceScheme.class);

    @Test(expected = CoreException.class)
    public void testInvalidSchemeString()
    {
        final SlippyTilePersistenceScheme invalidScheme = SlippyTilePersistenceScheme
                .getSchemeInstanceFromString("zz/xx/");
    }

    @Test
    public void testVariousSchemes()
    {
        final SlippyTilePersistenceScheme scheme1 = SlippyTilePersistenceScheme
                .getSchemeInstanceFromString("zz/");
        final String path1 = SparkFileHelper.combine("resource://parentPath",
                scheme1.compile("12", "12", "12"), "morePath");
        Assert.assertEquals("resource://parentPath/12/morePath", path1);

        final SlippyTilePersistenceScheme scheme2 = new SlippyTilePersistenceScheme(
                SlippyTilePersistenceSchemeType.ZZ_XX_YY_SUBFOLDERS_PBF);
        final String path2 = SparkFileHelper.combine("resource://parentPath",
                scheme2.compile(new SlippyTile(12, 12, 12)));
        Assert.assertEquals("resource://parentPath/12/12/12/12-12-12.pbf", path2);

        final SlippyTilePersistenceScheme scheme3 = new SlippyTilePersistenceScheme(
                SlippyTilePersistenceSchemeType.ZZ_SLASH_XX_YY_OSMPBF);
        final String path3 = SparkFileHelper.combine("resource://parentPath",
                scheme3.compile(new SlippyTile(12, 12, 12)));
        Assert.assertEquals("resource://parentPath/12/12-12.osm.pbf", path3);

        final SlippyTilePersistenceScheme scheme4 = new SlippyTilePersistenceScheme(
                SlippyTilePersistenceSchemeType.EMPTY);
        final String path4 = SparkFileHelper.combine("resource://parentPath",
                scheme4.compile(new SlippyTile(12, 12, 12)));
        Assert.assertEquals("resource://parentPath", path4);

        final SlippyTilePersistenceScheme scheme5 = new SlippyTilePersistenceScheme(
                SlippyTilePersistenceSchemeType.EMPTY);
        final String path5 = SparkFileHelper.combine("resource://parentPath",
                scheme5.compile(new SlippyTile(12, 12, 12)), "morePath");
        Assert.assertEquals("resource://parentPath/morePath", path5);

        final SlippyTilePersistenceScheme scheme6 = SlippyTilePersistenceScheme
                .getSchemeInstanceFromString("zz-xx-yy.pbf");
        final String path6 = SparkFileHelper.combine("resource://parentPath",
                scheme6.compile("12", "12", "12"));
        Assert.assertEquals("resource://parentPath/12-12-12.pbf", path6);

        final SlippyTilePersistenceScheme scheme7 = SlippyTilePersistenceScheme
                .getSchemeInstanceFromString("ZZ_XX_YY_PBF");
        final String path7 = SparkFileHelper.combine("resource://parentPath",
                scheme7.compile("12", "12", "12"));
        Assert.assertEquals("resource://parentPath/12-12-12.pbf", path7);
    }
}
