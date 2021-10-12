#!/usr/bin/env python3
import argparse
import gzip
import os
import re
from datetime import datetime
from typing import Union

from geojson import FeatureCollection, dump, dumps

try:
    import boto3

    _S3_SUPPORT = None
except ModuleNotFoundError as e:
    _S3_SUPPORT = e

from .ShardingTree import ShardingTree


def main():
    if _S3_SUPPORT is None:
        s3_support = """
        All file/directory options can take s3://[bucket]/[key] formatted urls,
        along with local files.
        """
    else:
        s3_support = """
        All file/directory options take local files as s3 packages are not
        installed.
        """
    parser = argparse.ArgumentParser(
        description="Update sharding files such that each shard is a "
        + "reasonable size",
        usage=s3_support
        + """
        For --output arguments, there is a placeholder for dates. Example:
            --output "sharding_quadtree_{date(%%Y-%%m-%%d)}.txt"
            In this example, the output file will be named
            `sharding_quadtree_{datetime.utcnow().strftime("%%Y-%%m-%%d)}.txt`
            which will become something like sharding_quadtree_2021-01-01.txt
        """,
    )
    parser.add_argument(
        "-i", "--input", type=str, help="Input sharding", required=True
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Output sharding", required=False
    )
    parser.add_argument(
        "-s",
        "--shards",
        type=str,
        help=(
            "Shard directory -- while this can take s3 paths,"
            + " this should be avoided (it is slow)."
        ),
        required=True,
    )
    parser.add_argument(
        "--geojson",
        action="store_true",
        help=(
            "Also write a geojson of the shards (same filename as --output,"
            + "but with .geojson as the extension)."
        ),
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=5,
        help="Set the maximum size of each shard in MiB.",
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=14,
        help="Set the maximum zoom for shards. "
        + "This takes precedence over --max-size.",
    )
    args = parser.parse_args()

    if _S3_SUPPORT and (
        args.input.startswith("s3:")
        or args.output.startswith("s3:")
        or args.shards.startswith("s3:")
    ):
        parser.print_help()
        parser.print_usage()
        print("S3 support not installed")
        raise _S3_SUPPORT

    input_tree = ShardingTree(args.input)
    output_tree = input_tree.update_sharding_tree(
        args.shards, size=args.max_size, max_zoom=args.max_zoom
    )
    if not args.output:
        args.output = "sharding_quadtree_{date(%Y-%m-%d)}.txt"
    date_pattern = re.compile(r"{date\((.*?)\)}")
    if date_pattern.search(args.output):
        match = date_pattern.search(args.output)
        args.output = date_pattern.sub(
            datetime.utcnow().strftime(match.group(1)), args.output
        )

    feature_collection: Union[None, FeatureCollection] = None
    if args.geojson:
        feature_collection = FeatureCollection(
            features=[
                tile.to_feature() for tile in output_tree.tiles if tile.final
            ]
        )
    if args.output.startswith("s3://"):
        path_str = args.output[len("s3://") :]
        bucket = path_str[: path_str.index("/")]
        path_str = path_str[len(bucket) + 1 :]
        # Assume the user has authenticated using any of the methods shown at
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        s3_resource = boto3.resource("s3")
        body = os.linesep.join(output_tree.to_iterable_str()).encode("ascii")
        s3_resource.Bucket(bucket).put_object(Key=path_str, Body=body)
        if feature_collection:
            path_str = os.path.splitext(path_str)[0] + ".geojson.gz"
            body = dumps(feature_collection).encode("utf-8")
            s3_resource.Bucket(bucket).put_object(
                Key=path_str, Body=gzip.compress(body)
            )
    else:
        with open(args.output, "w") as output_file:
            output_file.writelines(
                [line + os.linesep for line in output_tree.to_iterable_str()]
            )
        if feature_collection:
            with open(
                os.path.splitext(args.output)[0] + ".geojson", "w"
            ) as geojson:
                dump(feature_collection, geojson)

    print("Changed tiles:")
    count: int = 0
    for tile in output_tree.tiles:
        if tile.new:
            print(tile)
            count += 1
    print(f"Total {count} tiles changed")


if __name__ == "__main__":
    main()
