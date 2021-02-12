package org.openstreetmap.atlas.mutator;

import java.util.Set;
import java.util.stream.Collectors;

import org.openstreetmap.atlas.geography.MultiPolygon;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEntity;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.geography.atlas.items.complex.RelationOrAreaToMultiPolygonConverter;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.tags.RelationTypeTag;
import org.openstreetmap.atlas.utilities.collections.Iterables;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.scalars.Surface;

/**
 * Test mutation that re-constructs multipolygons and add area as a tag to all members.
 *
 * @author matthieun
 */
public class AtlasMutatorCountryvoreIntegrationTestAddMultiPolygonArea
        extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = 1145018005499574527L;

    public static final String AREA_KEY = "area";

    public AtlasMutatorCountryvoreIntegrationTestAddMultiPolygonArea(final String name,
            final Configuration configuration)
    {
        super(name, configuration);
    }

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        return Iterables
                .asList(atlas.relations(relation -> RelationTypeTag.MULTIPOLYGON.name()
                        .equalsIgnoreCase(relation.tag(RelationTypeTag.KEY))))
                .stream().flatMap(relation ->
                {
                    final RelationOrAreaToMultiPolygonConverter converter = new RelationOrAreaToMultiPolygonConverter();
                    final MultiPolygon result = converter.convert(relation);
                    final Surface area = result.surface();
                    return relation.members().stream()
                            .map(member -> FeatureChange
                                    .add((AtlasEntity) ((CompleteEntity) CompleteEntity
                                            .shallowFrom(member.getEntity())).withAddedTag(AREA_KEY,
                                                    String.format("%.1f", area.asMeterSquared())
                                                            + " m^2")));
                }).collect(Collectors.toSet());
    }
}
