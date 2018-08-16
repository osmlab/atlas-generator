# AtlasGenerator

[![Build Status](https://travis-ci.org/osmlab/atlas-generator.svg?branch=master)](https://travis-ci.org/osmlab/atlas-generator)
[![quality gate](https://sonarcloud.io/api/project_badges/measure?project=org.openstreetmap.atlas%3Aatlas-generator&metric=alert_status)](https://sonarcloud.io/dashboard?id=org.openstreetmap.atlas%3Aatlas-generator)

AtlasGenerator is a Spark Job that generates [Atlas](https://github.com/osmlab/atlas) shards from OSM pbf shards (built from an OSM database with osmosis).

## Getting started

It is recommended to use Oracle's latest JDK 1.8 with that project.

### Build

This command will create the necessary fat (contains all the dependencies) and shaded (contains all the dependencies, except spark and hadoop) jars.

```
gradle clean shaded fat
```

### Setup example: BLZ (Belize)

Download the country boundaries and the sharding tree files from the respective sub-folders available [here](https://apple.box.com/s/3k3wcc0lq1fhqgozxr4mdi0llf95byo3). We will then call `file:///path/to/boundaries/world_boundaries_osm_20170424.txt` and `file:///path/to/sharding/tree-6-14-100000.txt` the boundaries and sharding files just downloaded. Finally download the latest `.osm.pbf` from geofabrik [here](http://download.geofabrik.de/central-america/belize-latest.osm.pbf). We will call the folder that contains it `file:///path/to/pbf`.

We will also trick the program into thinking that the 3 shards Belize intersects with in this sharding tree each have a corresponding `.osm.pbf` file, which will each just be the same copy of the file that was downloaded from geofabrik:

```
cp /path/to/pbf/belize-latest.osm.pbf /path/to/pbf/7-32-57.pbf
cp /path/to/pbf/belize-latest.osm.pbf /path/to/pbf/8-64-117.pbf
cp /path/to/pbf/belize-latest.osm.pbf /path/to/pbf/8-65-117.pbf
```

### Run

The `AtlasGenerator` is a `SparkJob` that can be run directly from a simple java command. Assuming `/path/to/jars` contains a fat jar version built from this project here is a sample command that can run locally.

```
java -classpath "/path/to/jars/*" org.openstreetmap.atlas.generator.AtlasGenerator \
    -output=file:///path/to/BLZ/output \
    -master=local \
    -startedFolder=file:///path/to/BLZ/started \
    -countries=BLZ \
    -countryShapes=file:///path/to/boundaries/world_boundaries_osm_20170424.txt \
    -pbfs=file:///path/to/pbf \
    -sharding=dynamic@file:///path/to/sharding/tree-6-14-100000.txt \
    -sparkOptions=spark.executor.memory->10g,spark.driver.memory->10g
```

### Result

The above command should do the following:

* Create a _STARTED empty file in the started folder to indicate that it started
* Create 3 atlas shards in /path/to/BLZ/atlas

Those 3 files can be loaded together at once, in the JOSM Atlas plugin for example, or programmatically loaded with a `MultiAtlas` using the [`AtlasResourceLoader` from the atlas project](https://github.com/osmlab/atlas#using-atlas).

## Contributing

Please see the [contributing guidelines](https://github.com/osmlab/atlas/blob/dev/CONTRIBUTING.md)!
