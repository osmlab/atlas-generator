# Atlas Mutator

[AtlasMutator](AtlasMutator.java) is a Spark-based platform to apply changes (mutations/`AtlasChangeGenerator`) to Atlas at scale.

## Configuration

Atlas Mutator is very versatile. It can be run for one to many countries at the same time, and be configured by command line switches, and by a `json` file containing the definitions of all the mutations to apply.

### Switches

All switches are defined in [`AtlasMutatorParameters`](AtlasMutatorParameters.java)

Only few switches really matter to run Atlas Mutator:

- `-countries=ABC,XYZ,...`
  - The list of countries to run with
- `-mutatorConfigurationResource=url://to/configuration.json`
  - The path to a json resource that defines all the mutations to apply
- `-atlas=url://to/input/atlas`
  - The input folder containing sharded atlas files to load and modify. It also needs to contain a `sharding.txt` file that can be loaded by `DynamicTileSharding` and a `boundaries.txt.gz` which can be loaded by `CountryBoundaryMap`.
- `-output=url://to/output/atlas`
  - The output folder to save the modified Atlas files to.

Every Atlas Mutator job can be run locally in single-node mode by adding the following switch: `-cluster=local`

In the simplest form:

```bash
java -classpath "/path/to/classpath" org.openstreetmap.atlas.mutator.AtlasMutator \
    -cluster=local \
    -countries=ABC,XYZ \
    -atlas=url://to/input/atlas \
    -output=url://to/output/atlas
```

### Json configuration

A sample json configuration can be found in the `integrationTest` folder of AtlasMutator, [here](../../../../../../integrationTest/resources/org/openstreetmap/atlas/mutator/AtlasMutatorIntegrationTest.json).

It contains two types of sections. A global section, and one section per mutation.

#### Global Section

The global section contains multiple re-usable objects (expansion policies, filters, etc.)

- `scanUrls`
  - The packages that the classpath scanner will have to scan to find the classes that implement the mutations defined below.
- `broadcast`
  - Definition of broadcast variables. See section below.
- `filters`
  - Definitions to create `ConfiguredFilter` objects that allow for custom filtering of Atlas objects
- `subAtlases`
  - Options to build filtered down Atlases using one of the above filters
- `inputDependencies`
  - Sometimes, to be able to expand further, some mutations expect neighboring atlases to be loaded using a filtered down version. The input dependency section defines what sub atlas to use.
- `fetchers`
  - A fetcher is configured using an input dependency, to tell it what type of filtered neighboring atlases to load, or using an atlas provider below in case the input is not Atlas files.
- `atlasProviders`
  - This is uncommon; this lets the first mutations load Atlases on the fly from resources that are not Atlases. It has support for OSM PBF files for example.
- `dynamicAtlasPolicies`
  - By far the most complex, dynamic atlas policies govern how neighboring Atlases are loaded when the spark job is processing one shard.

#### Mutation Section

Each mutation is defined by a name, and a few parameters:

- `className` (optional, default name)
  - The full path to the class name. If this is not present, it will use the global classpath scanner to find a proper class with the same name as the section.
- `enabled` (Optional, default true)
  - True or false.
- `broadcast`
  - Array of names of all broadcast variables needed
- `countries` (Optional, default all)
  - All the countries include-listed for this mutation
- `excludedCountries` (Optional, default none)
  - All the countries exclude-listed for this mutation
- `dynamicAtlas` (Optional, default full expansion (not recommended))
  - `generation`
    - The dynamic atlas policy to use when running the mutation to generate all the `FeatureChange` objects
  - `application`
    - The dynamic atlas policy to use when applying all the `FeatureChange` objects
- `dependsOn` (Optional, default none)
  - All the other mutations that might conflict with this one and have to run before.

Within each mutation section, custom configuration parameters can also be added. See [`AtlasChangeGeneratorAddTag`](../../../../../../main/java/org/openstreetmap/atlas/mutator/testing/AtlasChangeGeneratorAddTag.java#L31-L32) and the [configuration](../../../../../../integrationTest/resources/org/openstreetmap/atlas/mutator/AtlasMutatorIntegrationTest.json#L66-L70) for it.

### Levels

Once all the mutations are properly defined, the job puts them all in a Directed Acyclic Graph (DAG) depending on their relative dependencies.

The mutations are organized by level, with each level grouping all the mutations that do not conflict and can run at the same time. The levels are further split to make sure all the mutations in each level have the exact same dynamic atlas expansion policy.

#### Level actions

Each level is a group of mutations which do not conflict with each other, and which share the same dynamic atlas expansion policy. The actions performed by the spark job at each level are for each shard in each country:

- Load the initial shard and the neighboring shards needed to have a locally complete view of the map, according to the `generation` dynamic atlas policy
- Invoke all the mutations for this level in parallel, and collect all the `FeatureChange`s they produce
- Distribute all the `FeatureChange` objects to all the shards they might apply to, and merge them together (when many apply to the same feature)
- Load the initial shard and the neighboring shards needed to have a locally complete view of the map, according to the `application` dynamic atlas policy
- Apply all the merged `FeatureChange` objects to that `DynamicAtlas` using `ChangeAtlas`
- Trim the `ChangeAtlas` to the shard boundaries, and save to an intermediate folder
- Optionally, if the next level's expansion policy needs it, save a filtered down version of the atlas
- Save the line delimited geojson log files

#### Level output folder

Each level saves its output in an intermediate folder that is used by the next level.

For example, for country `XYZ` and two levels:

```bash
./output
./output/intermediate
./output/intermediate/XYZ_0
./output/intermediate/XYZ_0/XYZ/XYZ_1-2-3.atlas
./output/intermediate/XYZ_0/XYZ/XYZ_4-5-6.atlas
```

Only the output of the first level "0" will be stored in the intermediate folder, as the output of level "1" is the official output.

### Broadcast Variables

Broadcast variables are useful in Spark for immutable data that is needed on all executors.

Define a broadcast variable in the global section:

```json
{
    "global":
    {
        ...
        "broadcast":
        {
            "myBroadcastVariableName":
            {
                "className": "MyClassName",
                "definition": "This is a definition that MyClassName understands"
            },
            ...
        }
        ...
    }
    ...
}
```

Make sure that `MyClassName` can understand the definition and use it to produce an object that is `Serializable`.

Finally, within a mutator:

```json
{
    ...
    "AddGeoHashName":
    {
        "className": "MyMutator",
        ...
        "broadcast": [
            "myBroadcastVariableName",
            "myOtherBroadcastVariableName",
            ...
        ],
        ...
    },
    ...
}
```

Those broadcast variables are fed into the `ConfiguredAtlasChangeGenerator` on the executor right before execution. When executing, the mutator can call this to retrieve the broadcast object;

```java
final Object broadcasted = this.getBroadcastVariable(broadcastName);
```

and cast it accordingly.

For local unit testing, the broadcast variables can be substituted by using the following method:

```java
public void addBroadcastVariable(final String name, final Object broadcastVariable)
```

### Atlas Provider

In case the input folder contains something that is not an Atlas object but can become one, like a OSM PBF file for example, the user can define a specific Atlas provider here. An Atlas provider class has to implement [`AtlasProvider`](../../../../../../main/java/org/openstreetmap/atlas/mutator/configuration/parsing/provider/AtlasProvider.java) and be on the classpath. (See [`PbfRawAtlasProvider`](../../../../../../main/java/org/openstreetmap/atlas/mutator/configuration/parsing/provider/file/PbfRawAtlasProvider.java) as an example).

```json
{
    "global":
    {
        ...
        "atlasProviders":
        {
            "myAtlasProvider":
            {
                "className": "fully.qualified.class.name.MyAtlasProvider",
                "providerConfiguration":
                {
                    "some": "configuration",
                    "specific to": "my provider"
                }
            },
            ...
        },
        ...
        "fetchers":
        {
            "mySpecificFetcher":
            {
                "atlasProvider": "myAtlasProvider"
            }
        },
        ...
        "dynamicAtlasPolicies":
        {
            "mySimpleDynamicAtlasPolicy":
            {
                ...
                "directFetcher": "mySpecificFetcher",
                "fetcher": "mySpecificFetcher",
                ...
            }
        },
    },
    ...
    "MyMutation":
    {
        ...
        "dynamicAtlas":
        {
            "generation": "mySimpleDynamicAtlasPolicy",
            ...
        },
        ...
    }
}
```

## Results

### Output folder

The outptut folder contains one sub-folder per country that was included in the `-countries` switch. Each of those sub-folders contains one Atlas file per shard that the related country intersects.

For example, for country `XYZ`:

```bash
./output
./output/XYZ
./output/XYZ/XYZ_1-2-3.atlas
./output/XYZ/XYZ_4-5-6.atlas
```

### Logs

The `logs` folder is a sub-folder of the output folder. It contains as many folders as there are countries and levels.

For example, for country `XYZ` and two levels:

```bash
./output
./output/logs
./output/logs/XYZ-0-applied/XYZ-0-applied_1-2-3.geojson.gz
./output/logs/XYZ-0-applied/XYZ-0-applied_4-5-6.geojson.gz
./output/logs/XYZ-1-applied/XYZ-1-applied_1-2-3.geojson.gz
./output/logs/XYZ-1-applied/XYZ-1-applied_4-5-6.geojson.gz
```

Each geojson file is line delimited. It contains all the `FeatureChange` objects that were applied to that shard at that level, one per line.

One line taken off those geojson files might look like this once prettyfied:

```json
{
    "type": "Feature",
    "bbox": [-62.0473182, -16.1411809, -62.0472877, -16.1411145],
    "geometry":
    {
        "type": "LineString",
        "coordinates": [
            [-62.0472938, -16.1411809],
            [-62.0473182, -16.1411653],
            [-62.0472877, -16.1411145]
        ]
    },
    "properties":
    {
        "featureChangeType": "ADD",
        "metadata":
        {
            "mutator": "MyMutation|7-41-69"
        },
        "description":
        {
            "type": "ADD",
            "descriptors": [
            {
                "name": "TAG",
                "type": "ADD",
                "key": "myTagKey",
                "value": "myTagValue"
            },
            {
                "name": "GEOMETRY",
                "type": "ADD",
                "position": "0/0",
                "afterView": "LINESTRING (-62.0472938 -16.1411809, -62.0473182 -16.1411653, -62.0472877 -16.1411145)"
            }]
        },
        "entityType": "LINE",
        "completeEntityClass": "org.openstreetmap.atlas.geography.atlas.complete.CompleteLine",
        "identifier": 123,
        "tags":
        {
            "myTagKey": "myTagValue"
        },
        "relations": [],
        "WKT": "LINESTRING (-62.0472938 -16.1411809, -62.0473182 -16.1411653, -62.0472877 -16.1411145)",
        "bboxWKT": "POLYGON ((-62.0473182 -16.1411809, -62.0473182 -16.1411145, -62.0472877 -16.1411145, -62.0472877 -16.1411809, -62.0473182 -16.1411809))"
    }
}
```

In this example, a new Atlas `Line` with identifier `123` is created with the geometry WKT `LINESTRING (-62.0472938 -16.1411809, -62.0473182 -16.1411653, -62.0472877 -16.1411145)` and the tag `myTagKey=myTagValue` by the mutation `MyMutation` which ran on the shard name `7-41-69`.
