# Regenerate Sharding Trees
This script is designed to split shards in the sharding tree
when the shard size reaches 5 MiB.
## Install
```shell
$ pip install --upgrade 'git+https://github.com/osmlab/atlas-generator.git@dev#subdirectory=scripts/regenerateShardingTree'
# Or if you need s3 support (notice the egg parameter)
$ pip install --upgrade 'git+https://github.com/osmlab/atlas-generator.git@dev#egg=regenerate_sharding_tree[s3]&subdirectory=scripts/regenerateShardingTree'
```

## Usage
```shell
$ python -m regenerate_sharding_tree --input ${INPUT_FILE} --output ${OUTPUT_FILE} --shards ${SHARD_DIRECTORY} --geojson
```

## Testing
This requires you to clone the repository and have a
terminal open to the script root directory.
You should see `test` and `regenerate_sharding_tree` as
subdirectories.
```shell
$ python -m unittest discover
```