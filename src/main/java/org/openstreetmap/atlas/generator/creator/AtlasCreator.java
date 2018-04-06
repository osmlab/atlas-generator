package org.openstreetmap.atlas.generator.creator;

import java.util.Collections;
import java.util.HashMap;

import org.openstreetmap.atlas.generator.AtlasGenerator;
import org.openstreetmap.atlas.generator.PbfContext;
import org.openstreetmap.atlas.generator.PbfLoader;
import org.openstreetmap.atlas.generator.PbfLocator;
import org.openstreetmap.atlas.generator.persistence.scheme.SlippyTilePersistenceScheme;
import org.openstreetmap.atlas.generator.sharding.AtlasSharding;
import org.openstreetmap.atlas.geography.atlas.Atlas;
import org.openstreetmap.atlas.geography.atlas.pbf.AtlasLoadingOption;
import org.openstreetmap.atlas.geography.boundary.CountryBoundaryMap;
import org.openstreetmap.atlas.geography.sharding.CountryShard;
import org.openstreetmap.atlas.geography.sharding.Shard;
import org.openstreetmap.atlas.geography.sharding.Sharding;
import org.openstreetmap.atlas.geography.sharding.SlippyTile;
import org.openstreetmap.atlas.streaming.resource.File;
import org.openstreetmap.atlas.streaming.resource.FileSuffix;
import org.openstreetmap.atlas.utilities.collections.Maps;
import org.openstreetmap.atlas.utilities.conversion.StringConverter;
import org.openstreetmap.atlas.utilities.runtime.Command;
import org.openstreetmap.atlas.utilities.runtime.CommandMap;

/**
 * This is mostly a debugging tool trying to replicate the conditions of the {@link AtlasGenerator}
 * in a specific area
 *
 * @author matthieun
 */
public class AtlasCreator extends Command
{
    public static final Switch<CountryBoundaryMap> BOUNDARIES = new Switch<>("boundaries",
            "The boundary map to use", value -> CountryBoundaryMap.fromPlainText(new File(value)),
            Optionality.REQUIRED);
    public static final Switch<String> COUNTRY = new Switch<>("country", "The country code",
            StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<File> OUTPUT = new Switch<>("output",
            "The path where the output will be saved", value ->
            {
                final File result = new File(value);
                result.mkdirs();
                return result;
            }, Optionality.REQUIRED);
    public static final Switch<String> PBF_PATH = new Switch<>("pbfs", "The path to PBFs",
            StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<SlippyTilePersistenceScheme> PBF_SCHEME = new Switch<>("pbfScheme",
            "The folder structure of the PBF", SlippyTilePersistenceScheme::new,
            Optionality.OPTIONAL, PbfLocator.DEFAULT_SCHEME);
    public static final Switch<String> PBF_SHARDING = new Switch<>("pbfSharding",
            "The sharding tree of the pbf files. If not specified, this will default to the general Atlas sharding.",
            StringConverter.IDENTITY, Optionality.OPTIONAL);
    public static final Switch<String> SHARDING_TYPE = new Switch<>("sharding",
            "The sharding definition.", StringConverter.IDENTITY, Optionality.REQUIRED);
    public static final Switch<Shard> TILE = new Switch<>("tile", "The SlippyTile name to use",
            SlippyTile::forName, Optionality.REQUIRED);

    public static void main(final String[] args)
    {
        new AtlasCreator().run(args);
    }

    public Atlas generateAtlas(final CountryBoundaryMap map, final Shard tile, final String pbfPath,
            final SlippyTilePersistenceScheme pbfScheme, final Sharding pbfSharding,
            final Sharding sharding, final String countryName)
    {
        final PbfContext pbfContext = new PbfContext(pbfPath, pbfSharding, pbfScheme);
        final PbfLoader loader = new PbfLoader(pbfContext, new HashMap<>(), map,
                AtlasLoadingOption.createOptionWithAllEnabled(map)
                        .setAdditionalCountryCodes(countryName),
                "dummyCodeVersion", "dummyDataVersion", Collections.emptySet());
        return loader.load(countryName, tile);
    }

    @Override
    protected int onRun(final CommandMap command)
    {
        final CountryBoundaryMap map = (CountryBoundaryMap) command.get(BOUNDARIES);
        final Shard tile = (Shard) command.get(TILE);
        final String pbfPath = (String) command.get(PBF_PATH);
        final SlippyTilePersistenceScheme pbfScheme = (SlippyTilePersistenceScheme) command
                .get(PBF_SCHEME);
        final String pbfShardingName = (String) command.get(PBF_SHARDING);
        final String shardingName = (String) command.get(SHARDING_TYPE);
        final Sharding sharding = AtlasSharding.forString(shardingName, Maps.stringMap());
        final Sharding pbfSharding = pbfShardingName != null
                ? AtlasSharding.forString(shardingName, Maps.stringMap())
                : sharding;
        final String countryName = (String) command.get(COUNTRY);
        final File output = (File) command.get(OUTPUT);
        PbfLoader.setAtlasSaveFolder(output);
        final Atlas atlas = generateAtlas(map, tile, pbfPath, pbfScheme, pbfSharding, sharding,
                countryName);
        atlas.save(output.child(countryName + CountryShard.COUNTRY_SHARD_SEPARATOR + tile.getName()
                + FileSuffix.ATLAS));
        atlas.saveAsGeoJson(output.child(countryName + "_" + tile.getName() + ".geojson"));
        return 0;
    }

    @Override
    protected SwitchList switches()
    {
        return new SwitchList().with(BOUNDARIES, TILE, SHARDING_TYPE, PBF_PATH, PBF_SCHEME,
                PBF_SHARDING, COUNTRY, OUTPUT);
    }
}
