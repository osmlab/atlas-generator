package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.streaming.resource.InputStreamResource;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;

/**
 * @author matthieun
 */
public class AtlasMutatorIndexTest
{
    @Test
    public void configurationOverrideTest()
    {
        final InputStreamResource resource = new InputStreamResource(
                () -> AtlasMutatorIndexTest.class
                        .getResourceAsStream("ConfigurationOverrides.json"));
        System.out.println(resource.all());
        final Configuration source = new StandardConfiguration(resource);
        final Configuration updatedXYZ = new AtlasMutatorIndex(null).updatedConfiguration(source,
                "XYZ");
        Assert.assertEquals("123value", updatedXYZ.get("key1.key12.key123").value());
        final List<String> key2XYZ = updatedXYZ.get("key2").value();
        Assert.assertEquals(0, key2XYZ.size());
        final Configuration updatedABC = new AtlasMutatorIndex(null).updatedConfiguration(source,
                "ABC");
        Assert.assertEquals("value123", updatedABC.get("key1.key12.key123").value());
        final List<String> key2ABC = updatedABC.get("key2").value();
        Assert.assertEquals(2, key2ABC.size());
        Assert.assertEquals("value2_1", key2ABC.get(0));
        Assert.assertEquals("value2_2", key2ABC.get(1));
    }
}
