package org.openstreetmap.atlas.mutator.testing;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.change.FeatureChange;
import org.openstreetmap.atlas.geography.atlas.complete.CompleteEntity;
import org.openstreetmap.atlas.geography.atlas.items.AtlasEntity;
import org.openstreetmap.atlas.mutator.configuration.mutators.ConfiguredAtlasChangeGenerator;
import org.openstreetmap.atlas.utilities.configuration.Configuration;
import org.openstreetmap.atlas.utilities.configuration.ConfigurationReader;
import org.openstreetmap.atlas.utilities.configuration.ConfiguredFilter;

/**
 * @author matthieun
 */
public class AtlasChangeGeneratorAddTag extends ConfiguredAtlasChangeGenerator
{
    private static final long serialVersionUID = -5027030876937712437L;

    private final String key;
    private final String value;
    private final ConfiguredFilter filter;
    private final boolean shallow;

    public AtlasChangeGeneratorAddTag(final String name, final Configuration configuration)
    {
        super(name, configuration);
        final ConfigurationReader reader = new ConfigurationReader(name);
        this.key = reader.configurationValue(configuration, "tag.key", "unknownKey");
        this.value = reader.configurationValue(configuration, "tag.value", "unknownValue");
        this.filter = ConfiguredFilter.from(
                reader.configurationValue(configuration, "predicate", ConfiguredFilter.DEFAULT),
                configuration);
        this.shallow = reader.configurationValue(configuration, "shallow", false);
    }

    @Override
    public boolean equals(final Object other)
    {
        if (other instanceof AtlasChangeGeneratorAddTag)
        {
            return super.equals(other) && this.key.equals(((AtlasChangeGeneratorAddTag) other).key)
                    && this.value.equals(((AtlasChangeGeneratorAddTag) other).value)
                    && this.filter.equals(((AtlasChangeGeneratorAddTag) other).filter);
        }
        return false;
    }

    @Override
    public Set<FeatureChange> generateWithoutValidation(final Atlas atlas)
    {
        final Set<FeatureChange> result = new HashSet<>();
        for (final AtlasEntity entity : atlas.entities(this.filter))
        {
            final String keyInternal = this.getKey(entity);
            final String valueInternal = this.getValue(entity);
            final CompleteEntity completeEntity = createFrom(entity).withAddedTag(keyInternal,
                    "sparkConfiguration".equals(keyInternal)
                            ? this.getSparkConfiguration().toString()
                            : valueInternal);
            result.add(FeatureChange.add((AtlasEntity) completeEntity));
        }
        return result;
    }

    public String getKey(final AtlasEntity entity) // NOSONAR
    {
        return this.key;
    }

    public String getValue(final AtlasEntity entity) // NOSONAR
    {
        return this.value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), this.key, this.value, this.filter);
    }

    private CompleteEntity createFrom(final AtlasEntity entity)
    {
        if (this.shallow)
        {
            return ((CompleteEntity) CompleteEntity.shallowFrom(entity)).withTags(entity.getTags());
        }
        else
        {
            return (CompleteEntity) CompleteEntity.from(entity);
        }
    }
}
