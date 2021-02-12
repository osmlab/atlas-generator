package org.openstreetmap.atlas.mutator.configuration.parsing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutationLevel;
import org.openstreetmap.atlas.mutator.configuration.AtlasMutatorConfiguration;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.streaming.resource.StringResource;
import org.openstreetmap.atlas.utilities.collections.Sets;
import org.openstreetmap.atlas.utilities.configuration.StandardConfiguration;

/**
 * @author matthieun
 */
public class AtlasMutatorConfigurationParserTest
{
    @Test
    public void testCoalesce()
    {
        final Map<String, List<AtlasMutationLevel>> countryToMutationLevels = new HashMap<>();
        final List<AtlasMutationLevel> abcList = new ArrayList<>();
        abcList.add(newLevel("ABC", "myMutation0", 0, 0));
        final List<AtlasMutationLevel> defList = new ArrayList<>();
        defList.add(newLevel("DEF", "myMutation0", 0, 0));
        final List<AtlasMutationLevel> ghiList = new ArrayList<>();
        ghiList.add(newLevel("GHI", "myMutation0", 0, 1));
        ghiList.add(newLevel("GHI", "myMutation1", 1, 1));

        countryToMutationLevels.put("ABC", abcList);
        countryToMutationLevels.put("DEF", defList);
        countryToMutationLevels.put("GHI", ghiList);

        final Map<String, List<AtlasMutationLevel>> coalesced = AtlasMutatorConfigurationParser
                .coalesce(countryToMutationLevels);
        System.out.println(coalesced);
        Assert.assertEquals(2, coalesced.size());
        Assert.assertNotNull(coalesced.get(AtlasMutatorConfiguration.GROUP_PREFIX + "0"));
        Assert.assertNotNull(coalesced.get("GHI"));
        Assert.assertEquals(1, coalesced.get(AtlasMutatorConfiguration.GROUP_PREFIX + "0").size());
        Assert.assertEquals(2, coalesced.get("GHI").size());
    }

    private AtlasMutationLevel newLevel(final String country, final String mutatorName,
            final int levelIndex, final int maximumLevelIndex)
    {
        return new AtlasMutationLevel(null, country, Sets.hashSet(country),
                Sets.hashSet(
                        new ConfiguredAtlasChangeGenerator(mutatorName, new StandardConfiguration(
                                new StringResource("{\"" + mutatorName + "\":{}}")))
                        {
                            private static final long serialVersionUID = 7294462604291608386L;

                            @Override
                            public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
                            {
                                return new HashSet<>();
                            }
                        }),
                levelIndex, maximumLevelIndex);
    }
}
