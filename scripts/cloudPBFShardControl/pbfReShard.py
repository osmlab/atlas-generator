#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
this script will search pbf release folder for shards that above certain size (default 5mb) and re-shards them locally.
"""
import argparse
import json
import logging
import math
import os
import shutil
import subprocess
from typing import List, Tuple


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=default_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("PBFReShard")


def finish(error_message=None, status=0):
    """
    Method to exit the Python script. It will log the given message and then exit().
    :param status:
    :param error_message:
    """
    if error_message:
        logger.error(error_message)
    else:
        logger.info("Done")
    exit(status)


def parse_args() -> argparse.Namespace:
    """
    Parse user parameters
    return: args
    """
    parser = argparse.ArgumentParser(
        description="This script reads local shard generation to look at the size of shards. "
                    "If shards are too big then it will split up large shards in the sharding.txt"
    )
    parser.add_argument(
        '--input',
        required=True,
        type=str,
        help="The shard file directory to look at for shard sizes and sharding.txt file",
    )
    parser.add_argument(
        '--shardingFileName',
        default="sharding.txt",
        type=str,
        help="The shard file directory to look at for shard sizes and sharding.txt file",
    )
    parser.add_argument(
        '--max',
        default=5,
        type=int,
        help="Maximum size allowed for a shard.",
    )
    return parser.parse_args()


class PBFReShardCtl:
    def __init__(
            self,
            release_pbf_dir="",
            sharding="sharding.txt",
            shard_max_size=5
    ):
        self.releasePbfDir = release_pbf_dir
        self.shardingFileName = sharding
        self.shardMaxSize = shard_max_size
        self.tmpDir = "/tmp/osmium/"

    @property
    def shardingFileName(self):
        return os.path.join(self.releasePbfDir, self.__shardingFileName)

    @shardingFileName.setter
    def shardingFileName(self, sharding_file_name):
        if os.path.exists(os.path.join(self.releasePbfDir, sharding_file_name)):
            self.__shardingFileName = sharding_file_name
        else:
            finish(
                "Sharding File '{}' doesn't exist.".format(
                    os.path.join(self.releasePbfDir, sharding_file_name)
                )
            )

    @property
    def shardMaxSizeKB(self):
        return self.shardMaxSize * 1024 * 1024

    @property
    def sharding_content(self):
        logger.info("reading sharding tree %s.", self.shardingFileName)
        with open(self.shardingFileName, 'r') as input_file:
            sharding_content = input_file.readlines()
        return sharding_content

    def get_oversize_shard_list(self) -> List[str]:
        oversize_shard_list = []
        for path, subdirs, files in os.walk(self.releasePbfDir):
            for file in files:
                if file.endswith(".pbf") and os.path.getsize(os.path.join(path, file)) > self.shardMaxSizeKB:
                    oversize_shard_list.append(os.path.join(path, file))
        return oversize_shard_list

    def gen_osmium_extract_config(self, shard: "SlippyTileQuadTreeNode") -> str:
        config = {
            # a hardcoded default value; can be overwritten when running osmium
            "directory": self.tmpDir
        }
        extracts = []
        osmium_config_file_name = ''
        for child_shard in shard.children:
            minlat, minlon, maxlat, maxlon = child_shard.to_bbox()
            extracts.append(
                {
                    "output": child_shard.pbf_file_name(),
                    "bbox": [minlon, minlat, maxlon, maxlat],
                }
            )
            config["extracts"] = extracts

            osmium_config_file_name = f"{shard.pbf_file_name()}.extracts.json"
            osmium_config_path = os.path.join(self.tmpDir, osmium_config_file_name)

            with open(osmium_config_path, "w") as f:
                json.dump(config, f, indent=2)
        logger.info("osmium extract config %s generated.", osmium_config_file_name)
        return osmium_config_file_name

    def gen_osmium_extract_cmd(self, config_file_name, shard_path):
        osmium_extract_cmd = ["osmium", "extract", "-c", self.tmpDir + config_file_name, shard_path]
        subprocess.run(osmium_extract_cmd, stdout=subprocess.PIPE, text=True)

    def update_release_folder(self, shard: "SlippyTileQuadTreeNode"):
        for child in shard.children:
            final_release_dir = os.path.join(self.releasePbfDir,
                                             str(child.tile_x), str(child.tile_y), str(child.tile_z))
            if not os.path.exists(final_release_dir):
                os.makedirs(final_release_dir)
            pbf_copy_cmd = ["cp", self.tmpDir + child.pbf_file_name(), final_release_dir]
            logger.info("copy %s to %s.", child.pbf_file_name(), final_release_dir)
            subprocess.run(pbf_copy_cmd, stdout=subprocess.PIPE, text=True)

    def update_sharding_tree(self, shard: "SlippyTileQuadTreeNode"):
        # Handle last line to prevent IndexError
        if shard.name() in self.sharding_content[-1] and "+" not in self.sharding_content[-1]:
            print(f"Found {shard.name} in last line: {self.sharding_content[-1]}")
            self.sharding_content[-1] = shard.name() + "+\n"
            for child in shard.children:
                self.sharding_content.append(f"{child.tile_z}-{child.tile_x}-{shard.tile_y}\n")
            changes = True
        else:
            for index, line in enumerate(self.sharding_content):
                if shard.name() in line and "+" not in line:
                    print(f"Found {shard.name()} in line {index}: {line}.  Breaking shard up...")
                    self.sharding_content[index] = shard.name() + "+\n"
                    for child in shard.children:
                        self.sharding_content.append(f"{child.tile_z}-{child.tile_x}-{shard.tile_y}\n")
                    changes = True
                    break

    def execute(self):
        oversize_shard_list = self.get_oversize_shard_list()
        for oversizeShard in oversize_shard_list:
            shard_obj = SlippyTileQuadTreeNode.read(oversizeShard)
            logger.info("shard %s too big.", shard_obj.pbf_file_name())
            shard_extract_config = self.gen_osmium_extract_config(shard_obj)
            self.gen_osmium_extract_cmd(shard_extract_config, oversizeShard)
            self.update_release_folder(shard_obj)
            self.update_sharding_tree(shard_obj)


class SlippyTileQuadTreeNode:
    MAXIMUM_CHILDREN = 4

    def __init__(
            self,
            tile_x: int,
            tile_y: int,
            tile_z: int,
            children: List["SlippyTileQuadTreeNode"],
    ) -> None:
        self.tile_x = tile_x
        self.tile_y = tile_y
        self.tile_z = tile_z
        self.children = children

    @staticmethod
    def tile_xyz_from_line(line: str) -> Tuple[int, int, int]:
        shard = line.split("/")[-1].split(".")[0]
        zxy = shard.split("-")
        return int(zxy[1]), int(zxy[2]), int(zxy[0])

    @staticmethod
    def read(line) -> "SlippyTileQuadTreeNode":
        x, y, z = SlippyTileQuadTreeNode.tile_xyz_from_line(line)
        children = [SlippyTileQuadTreeNode(x * 2, y * 2, z + 1, []),
                    SlippyTileQuadTreeNode(x * 2, y * 2 + 1, z + 1, []),
                    SlippyTileQuadTreeNode(x * 2 + 1, y * 2, z + 1, []),
                    SlippyTileQuadTreeNode(x * 2 + 1, y * 2 + 1, z + 1, [])]
        return SlippyTileQuadTreeNode(x, y, z, children)

    # Return the (latitude, longitude) for the NW corner point of a tile
    # Logic copied from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    @staticmethod
    def tile_nw_latlon(x, y, z) -> Tuple[float, float]:
        n = 1 << z
        minlon_deg = x / n * 360.0 - 180.0
        maxlat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        maxlat_deg = math.degrees(maxlat_rad)
        return maxlat_deg, minlon_deg

    # Return the bbox in (minlat, minlon, maxlat, maxlon) for the tile.
    def to_bbox(self) -> Tuple[float, float, float, float]:
        maxlat, minlon = SlippyTileQuadTreeNode.tile_nw_latlon(
            self.tile_x, self.tile_y, self.tile_z
        )
        minlat, maxlon = SlippyTileQuadTreeNode.tile_nw_latlon(
            self.tile_x + 1, self.tile_y + 1, self.tile_z
        )
        return minlat, minlon, maxlat, maxlon

    def name(self) -> str:
        return f"{self.tile_z}-{self.tile_x}-{self.tile_y}"

    def pbf_file_name(self) -> str:
        return f"{self.name()}.pbf"


logger = setup_logging()

if __name__ == "__main__":
    args = parse_args()
    re_sharder = PBFReShardCtl(args.input, args.shardingFileName, args.max)
    re_sharder.execute()
