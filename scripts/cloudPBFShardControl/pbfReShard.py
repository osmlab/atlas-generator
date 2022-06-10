#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
this script will search pbf release folder for shards that above certain size (default 5mb) and re-shards them locally.
@author: Vladimir Lemberg
@history: 06/10/2022 Created
"""
import argparse
import json
import logging
import math
import os
import shutil
import subprocess
from datetime import date
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


def copy_command(input_file: str, output_file: str):
    """
    execute unix copy command
    :param input_file:
    :param output_file:
    """
    pbf_copy_cmd = ["cp", input_file, output_file]
    logger.info("copy %s to %s.", input_file, output_file)
    try:
        subprocess.run(pbf_copy_cmd, stdout=subprocess.PIPE, text=True)
    except subprocess.CalledProcessError as e:
        finish(e.output, -1)


class PBFReShardCtl:
    def __init__(
            self,
            release_pbf_dir="",
            sharding_file="sharding.txt",
            shard_max_size=5
    ):
        self.releasePbfDir = release_pbf_dir
        self.shardingFile = sharding_file
        self.shardMaxSize = shard_max_size
        self.shardingContent = self.shardingFileName
        self.tmpDir = "/tmp/osmium/"
        self.change = False

    @property
    def shardingFileName(self):
        return os.path.join(self.releasePbfDir, self.shardingFile)

    @property
    def shardingContent(self):
        return self._shardingContent

    @shardingContent.setter
    def shardingContent(self, sharding_file):
        logger.info("reading sharding tree %s.", sharding_file)
        try:
            with open(sharding_file, 'r') as input_file:
                self._shardingContent = input_file.readlines()
        except (IOError, OSError) as e:
            finish(e.output, -1)

    @property
    def shardMaxSizeKB(self):
        return self.shardMaxSize * 1024 * 1024

    def get_oversize_shard_list(self) -> List[str]:
        """
        Collect shards that are larger than max size.
        """
        oversize_shard_list = []
        for path, subdirs, files in os.walk(self.releasePbfDir):
            for file in files:
                if file.endswith(".pbf") and os.path.getsize(os.path.join(path, file)) > self.shardMaxSizeKB:
                    oversize_shard_list.append(os.path.join(path, file))
        return oversize_shard_list

    def gen_osmium_extract_config(self, shard: "SlippyTileQuadTreeNode") -> str:
        """
        Create osmium pbf extract config strategy.
        @:param shard: SlippyTileQuadTreeNode object
        @:return str: path to extract config json file
        """

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
        """
        execute osmium extract commend
        :param config_file_name: osmium extract strategy json file
        :param shard_path: output directory
        """
        osmium_extract_cmd = ["osmium", "extract", "-c", self.tmpDir + config_file_name, shard_path]
        subprocess.run(osmium_extract_cmd, stdout=subprocess.PIPE, text=True)

    def update_release_folder(self, shard: "SlippyTileQuadTreeNode"):
        """
        copy generated shards to pbf release folder
        :param shard: SlippyTileQuadTreeNode object
        """
        for child_shard in shard.children:
            final_release_dir = os.path.join(self.releasePbfDir,
                                             str(child_shard.tile_z), str(child_shard.tile_x), str(child_shard.tile_y))
            if not os.path.exists(final_release_dir):
                os.makedirs(final_release_dir)
            # copy generated shards to pbf release folder
            pbf_copy_cmd = ["cp", self.tmpDir + child_shard.pbf_file_name(), final_release_dir]
            logger.info("copy %s to %s.", child_shard.pbf_file_name(), final_release_dir)
            subprocess.run(pbf_copy_cmd, stdout=subprocess.PIPE, text=True)
        # delete oversize shard form pbf release folder
        logger.info("delete shard %s from %s.", shard.pbf_file_name(),
                    os.path.join(self.releasePbfDir, str(shard.tile_z),
                                 str(shard.tile_x), str(shard.tile_y)))
        shutil.rmtree(os.path.join(self.releasePbfDir, str(shard.tile_z), str(shard.tile_x), str(shard.tile_y)))

    def update_sharding_tree(self, shard: "SlippyTileQuadTreeNode"):
        """
        update sharding quad tree content according to sharding extract
        :param shard: SlippyTileQuadTreeNode object
        """
        # Handle last line to prevent IndexError
        if shard.name() + "\n" in self.shardingContent[-1] and "+" not in self.shardingContent[-1]:
            print(f"Found {shard.name()} in last line: {self.shardingContent[-1]}")
            self.shardingContent[-1] = shard.name() + "+\n"
            for child in shard.children:
                self.shardingContent.append(f"{child.tile_z}-{child.tile_x}-{shard.tile_y}\n")
            self.change = True
        else:
            for index, line in enumerate(self.shardingContent):
                if shard.name() + "\n" in line and not shard.is_leaf():
                    logger.info("found %s in line %s. updating sharding tree", shard.name(), index)
                    self.shardingContent[index] = shard.name() + "+\n"
                    for child in shard.children:
                        self.shardingContent.append(f"{child.tile_z}-{child.tile_x}-{shard.tile_y}\n")
                    self.change = True
                    break

    def apply_sharding_changes(self):
        """
        create new sharding.txt file
        """
        if self.change:
            logger.info("creating new sharding tree file")
            shard_date = date.today().strftime("%Y%m%d")
            new_sharding_quadtree_file = f"sharding_quadtree_{shard_date}.txt"
            with open(os.path.join(self.tmpDir, new_sharding_quadtree_file), 'w') as output_file:
                output_file.writelines(self.shardingContent)
            # copy original sharding file
            copy_command(self.shardingFileName, os.path.join(self.releasePbfDir, f"sharding_original_{shard_date}.txt"))
            # copy generated sharding file to pbf release folder
            copy_command(self.tmpDir + new_sharding_quadtree_file, self.shardingFileName)
        else:
            logger.info("all shards are less then %smb", self.shardMaxSize)

    def execute(self):
        """
        execute re-sharding steps.
        """
        oversize_shard_list = self.get_oversize_shard_list()
        for oversizeShard in oversize_shard_list:
            shard_obj = SlippyTileQuadTreeNode.read(oversizeShard)
            logger.info("shard %s too big.", shard_obj.pbf_file_name())
            shard_extract_config = self.gen_osmium_extract_config(shard_obj)
            self.gen_osmium_extract_cmd(shard_extract_config, oversizeShard)
            self.update_release_folder(shard_obj)
            self.update_sharding_tree(shard_obj)
        self.apply_sharding_changes()
        finish()


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

    def is_leaf(self) -> bool:
        return len(self.children) == 0


logger = setup_logging()

if __name__ == "__main__":
    args = parse_args()
    re_sharder = PBFReShardCtl(args.input, args.shardingFileName, args.max)
    re_sharder.execute()
