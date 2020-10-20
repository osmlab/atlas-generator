#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
execute the pbf sharding process locally
"""
import argparse
import glob
import json
import logging
import math
import os
import random
import shutil
import subprocess
import time
from typing import List, TextIO, Tuple


VERSION = "0.1.0"


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=default_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("PBFShardGenerator")


def finish(error_message=None, status=0):
    """
    Method to exit the Python script. It will log the given message and then exit().
    :param error_message:
    """
    if error_message:
        logger.error(error_message)
    else:
        logger.critical("Done")
    exit(status)


"""
pbf Shard Control Class
"""


class PBFShardCtl:
    def __init__(
        self,
        pbfURL="",
        processes=5,
        s3Folder=None,
        quadtree="sharding_quadtree.txt",
        maxShardPerStep=512,
        maxShardPerConfig=32,
    ):
        self.awsCliUrl = "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
        self.awsCliDir = "/home/ubuntu/awscli/"
        self.awsCliZip = "awscliv2.zip"
        self.scriptDir = "/home/ubuntu/pbfSharding/"
        self.tmpDir = "/tmp/osmium/"
        self.global_step_count = 0
        self.max_sharding_leaves = 0
        self.max_sharding_leaves_subtree = ""
        self.min_sharding_leaves = 20000
        self.min_sharding_leaves_subtree = ""
        self.max_sharding_intermediate = 0
        self.max_sharding_intermediate_subtree = ""
        self.min_sharding_intermediate = 20000
        self.min_sharding_intermediate_subtree = ""
        self.sharded_subtrees_by_zoom = {}

        self.pbfURL = pbfURL
        self.maxOsmiumProcesses = processes
        self.s3Folder = s3Folder
        self.quadtreeFileName = quadtree
        self.maxShardPerStep = maxShardPerStep
        self.maxShardPerConfig = maxShardPerConfig

    @property
    def pbfDir(self):
        return os.path.join(self.scriptDir, "pbf/")

    @property
    def logDir(self):
        return os.path.join(self.scriptDir, "log/")

    @property
    def osmiumCfgDir(self):
        return os.path.join(self.scriptDir, "osmium_config/")

    @property
    def quadtreeFilePath(self):
        return os.path.join(self.scriptDir, self.__quadtreeFileName)

    @property
    def quadtreeFileName(self):
        return os.path.join(self.scriptDir, self.__quadtreeFileName)

    @quadtreeFileName.setter
    def quadtreeFileName(self, quadtreeFileName):
        if os.path.exists(os.path.join(self.scriptDir, quadtreeFileName)):
            self.__quadtreeFileName = quadtreeFileName
        else:
            finish(
                "Sharding Quadtree File '{}' doesn't exist.".format(
                    os.path.join(self.scriptDir, quadtreeFileName)
                )
            )

    @property
    def pbfFinalDir(self):
        return os.path.join(self.pbfDir, "final")

    def prep(self) -> None:
        """
        Prepare server for Sharding process
        """
        logger.info("Prepare system for Sharding Process...")
        # install aws cli on instance
        logger.info("Installing osmium and AWS CLI.")
        if (
            subprocess.run(["sudo", "apt", "update", "-y"]).returncode
            or subprocess.run(
                ["sudo", "apt", "-y", "install", "unzip", "osmium-tool"]
            ).returncode
        ):
            finish("ERROR: Unable to install unzip", -1)
        if subprocess.run(["osmium", "--version"]).returncode:
            finish("ERROR: Failed to install osmium, aborting", -1)

        if subprocess.run(
            [
                "curl",
                "--create-dirs",
                self.awsCliUrl,
                "-o",
                self.awsCliDir + self.awsCliZip,
            ]
        ).returncode:
            finish("ERROR: Unable to curl {}".format(self.awsCliUrl), -1)
        if subprocess.run(
            ["unzip", "-o", self.awsCliDir + self.awsCliZip, "-d", self.awsCliDir]
        ).returncode:
            finish("ERROR: Unable to unzip {}".format(self.awsCliZip), -1)
        if subprocess.run(["sudo", self.awsCliDir + "aws/install", "-u"]).returncode:
            finish("ERROR: Unable to install awscli", -1)
        if subprocess.run(["aws", "--version"]).returncode:
            finish("ERROR: Failed to install AWS CLI, aborting", -1)
        shutil.rmtree(self.awsCliDir)

    def verify_leaves_in_tree(self, tree_root, tree_file) -> None:
        leaves = []
        tree_root.leaves(leaves)

        logger.info(
            "Comparing leaf nodes in %s against deserialized quadtree built...",
            args.quadtree_txt_file,
        )
        with open(tree_file) as f:
            lines = f.readlines()
            leaf_lines = {l[:-1] for l in lines if len(l) > 1 and not l.endswith("+\n")}
            logger.info("Number of leaves in file: %d", len(leaf_lines))

            leaf_strs = {l.to_str() for l in leaves}
            logger.info("Number of leaves in deserialized quadtree: %d", len(leaf_strs))

            for l in leaf_lines:
                if l not in leaf_strs:
                    logger.info("line from file not in quadtree: %s", l)

            for s in leaf_strs:
                if s not in leaf_lines:
                    logger.info("line from quadtree not in file: %s", s)

            logger.info("Done with comparison")

    def gen_shard_configs(self, tree: "SlippyTileQuadTreeNode") -> None:

        if tree.is_leaf():
            logger.info(">>>>>>No sharding on leaf node %s<<<<<<", tree.to_str())
            return

        self.global_step_count += 1
        logger.info("======Step %d======", self.global_step_count)
        logger.info(
            "Subtree %s : max_leaf_level %d, min_leaf_level %d, num_leaves %d",
            tree.to_str(),
            tree.max_leaf_level,
            tree.min_leaf_level,
            tree.num_leaves,
        )

        # osmium suffers from big number of shards when the input pbf is large, so we limit
        # it to 16 for big tiles with zoom <= 4
        # shard_limit = 16 if tree.tile_z <= 4 else max_shard_per_config
        # shard_limit = max_shard_per_config

        if tree.num_leaves <= self.maxShardPerStep:
            logger.info(
                "Shard decision: num_leaves within range, directly shard on leaves"
            )
            leaves = []
            tree.leaves(leaves)
            for l in leaves:
                logger.info("  -> %s", l.to_str())
            osmium_config_file_names = self.gen_osmium_extract_config(
                tree.pbf_file_name(), leaves
            )

            num_shards = tree.num_leaves
            if num_shards < self.min_sharding_leaves:
                self.min_sharding_leaves = num_shards
                self.min_sharding_leaves_subtree = tree.to_str()
            if num_shards > self.max_sharding_leaves:
                self.max_sharding_leaves = num_shards
                self.max_sharding_leaves_subtree = tree.to_str()
        else:
            intermediate_level = tree.min_leaf_level
            # for a quad tree, number of subtrees on level L is 4 ^ L
            while (1 << (2 * intermediate_level)) > self.maxShardPerStep:
                intermediate_level = intermediate_level - 1
            logger.info(
                "Shard decision: num_leaves too large, shard on intermediate level %d",
                intermediate_level,
            )
            subtrees = []
            tree.collect_subtrees_on_level(subtrees, intermediate_level)
            for s in subtrees:
                logger.info("  -> %s", s.to_str())
            osmium_config_file_names = self.gen_osmium_extract_config(
                tree.pbf_file_name(), subtrees
            )

            num_shards = len(subtrees)
            if num_shards < self.min_sharding_intermediate:
                self.min_sharding_intermediate = num_shards
                self.min_sharding_intermediate_subtree = tree.to_str()
            if num_shards > self.max_sharding_intermediate:
                self.max_sharding_intermediate = num_shards
                self.max_sharding_intermediate_subtree = tree.to_str()

            for s in subtrees:
                self.gen_shard_configs(s)

        if tree.tile_z not in self.sharded_subtrees_by_zoom:
            self.sharded_subtrees_by_zoom[tree.tile_z] = []
        self.sharded_subtrees_by_zoom[tree.tile_z].append(
            (tree, osmium_config_file_names)
        )

    def gen_osmium_extract_config(self, pbf_file_name: str, shard_nodes) -> List[str]:
        num_config_files = math.ceil(len(shard_nodes) / self.maxShardPerConfig)
        batches = [[] for i in range(num_config_files)]
        for i, n in enumerate(shard_nodes):
            batches[i % num_config_files].append(n)

        config_file_names = []
        for i, batch in enumerate(batches, start=1):
            config = {
                # a hardcoded default value; can be overwritten when running osmium
                "directory": "/tmp/"
            }
            extracts = []
            for n in batch:
                minlat, minlon, maxlat, maxlon = n.to_bbox()
                extracts.append(
                    {
                        "output": n.pbf_file_name(),
                        "bbox": [minlon, minlat, maxlon, maxlat],
                    }
                )
            config["extracts"] = extracts

            osmium_config_file_name = f"{pbf_file_name}.extracts-{i}.json"
            osmium_config_path = os.path.join(
                self.osmiumCfgDir, osmium_config_file_name
            )
            with open(osmium_config_path, "w") as f:
                json.dump(config, f, indent=2)
            logger.info("osmium extract config %s generated.", osmium_config_file_name)
            config_file_names.append(osmium_config_file_name)

        return config_file_names

    # Each osmium batch file contains a list of pbf file names for which multiple
    # osmium processes can run in parallel
    def genOsmiumBatchFiles(self) -> None:
        for z in sorted(self.sharded_subtrees_by_zoom.keys()):
            batch_file_name = f"osmium_batch_{z:03d}.txt"
            batch_file_path = os.path.join(self.osmiumCfgDir, batch_file_name)
            with open(batch_file_path, "w") as f:
                subtree_tuples = self.sharded_subtrees_by_zoom[z]
                name_pairs = []
                for subtree, config_file_names in subtree_tuples:
                    for n in config_file_names:
                        name_pairs.append((subtree.pbf_file_name(), n))

                random.shuffle(name_pairs)
                for pbf_file_name, config_file_name in name_pairs:
                    f.write(f"{pbf_file_name} {config_file_name}\n")
            logger.info("osmium batch file %s generated.", batch_file_name)

    def processOsmiumBatch(self, batchFilePath: str) -> int:
        """
        Execute osmium extract pass based on a batch file given as input parameter
        """
        batchFile = open(batchFilePath, "r")
        procList = []
        for line in batchFile.readlines():
            # extract file names from batch file line
            pbfFile = line.strip().split(" ", 1)[0]
            configFile = line.strip().split(" ", 1)[1]

            pbfSize = os.path.getsize(os.path.join(self.tmpDir, pbfFile))
            # only start a maximum number of processes in parallel
            while len(procList) >= self.maxOsmiumProcesses:
                logger.debug(
                    "waiting to spawn more. processes: {} ....".format(len(procList))
                )
                time.sleep(1)
                for p in procList:
                    r = p.poll()
                    if r == 0:
                        logger.debug(
                            "removing completed process {} ....".format(p.args)
                        )
                        procList.remove(p)
                    elif r != None:
                        finish(
                            "ERROR: osmium process {} completed: {}".format(p.args, r),
                            r,
                        )

            # create a log file from the config file
            logFile = open(
                os.path.join(self.logDir, os.path.splitext(configFile)[0] + ".log"),
                "wb",
            )
            logger.info(
                "osmium extract processing {} using cfg file: {} ....".format(
                    pbfFile, configFile
                )
            )
            p = subprocess.Popen(
                [
                    "osmium",
                    "extract",
                    "-vO",
                    "-c{}".format(os.path.join(self.osmiumCfgDir, configFile)),
                    "-d{}".format(self.tmpDir),
                    "-scomplete_ways",
                    os.path.join(self.tmpDir, pbfFile),
                ],
                stdout=logFile,
                stderr=logFile,
            )
            logger.debug("adding process {} ....".format(p.args))
            procList.append(p)

        for p in procList:
            r = p.wait()
            if r:
                finish("ERROR: osmium process {} completed: {}".format(p.args, r), r)

        # Move final pbfs to the final pbf directory structure
        for tmpFilePath in glob.iglob(self.tmpDir + "*final.osm.pbf"):
            tmpFileName = os.path.basename(tmpFilePath)
            [x, y, z, _] = tmpFileName.split("-", 3)
            finalDir = os.path.join(self.pbfFinalDir, x, y, z)
            newFileName = "{}-{}-{}.pbf".format(x, y, z)
            if not os.path.exists(finalDir):
                os.makedirs(finalDir)
            finalFilePath = os.path.join(finalDir, newFileName)
            if os.path.exists(finalFilePath):
                os.remove(finalFilePath)
            os.rename(tmpFilePath, finalFilePath)
        return 0

    def shard(self):
        """
        Main Sharding process
        """
        logger.info("Start Sharding Process...")

        if not os.path.exists(self.osmiumCfgDir):
            os.makedirs(self.osmiumCfgDir)
        if not os.path.exists(self.logDir):
            os.makedirs(self.logDir)
        if os.path.exists(self.pbfDir):
            shutil.rmtree(self.pbfDir)
        os.makedirs(self.pbfFinalDir)
        if os.path.exists(self.tmpDir):
            shutil.rmtree(self.tmpDir)
        os.makedirs(self.tmpDir)

        logger.info("Reading %s...", self.quadtreeFilePath)
        tree = None
        with open(self.quadtreeFilePath, "r+") as f:
            tree = SlippyTileQuadTreeNode.read(f)

        max_leaf_level, min_leaf_level, num_leaves = tree.scan_meta_data()
        logger.debug(
            "Tree stats: max_leaf_level %d, min_leaf_level %d, num_leaves %d.",
            max_leaf_level,
            min_leaf_level,
            num_leaves,
        )

        logger.info(
            "Generating sharding configs with max_shard_per_config %d...",
            self.maxShardPerConfig,
        )
        self.gen_shard_configs(tree)

        logger.debug(
            "Final sharding stats (max_shard_per_config = %d):", self.maxShardPerConfig
        )
        logger.debug(
            "max_sharding_leaves %d from subtree %s; self.min_sharding_leaves %d from subtree %s",
            self.max_sharding_leaves,
            self.max_sharding_leaves_subtree,
            self.min_sharding_leaves,
            self.min_sharding_leaves_subtree,
        )
        logger.debug(
            "max_sharding_intermediate %d from subtree %s; self.min_sharding_intermediate %d from subtree %s",
            self.max_sharding_intermediate,
            self.max_sharding_intermediate_subtree,
            self.min_sharding_intermediate,
            self.min_sharding_intermediate_subtree,
        )

        logger.info("Generate osmium batch files...")
        self.genOsmiumBatchFiles()

        # use wget to fetch the pbf URL because it will figure out if the latest needs to be downloaded
        if subprocess.run(["wget", "-NP", self.scriptDir, self.pbfURL]).returncode:
            finish("Error Downloading PBF...", -1)

        # create a link to the main pbf as the first intermediate pbf in the temp folder
        primarypbf = os.path.join(self.tmpDir, "0-0-0-intermediate.osm.pbf")
        if os.path.exists(primarypbf):
            os.remove(primarypbf)
        os.symlink(self.scriptDir + self.pbfURL.rsplit("/", 1)[1], primarypbf)

        for filepath in sorted(glob.iglob(self.osmiumCfgDir + "osmium_batch_*.txt")):
            if self.processOsmiumBatch(filepath):
                finish("Unable to run osmium batch script...", -1)

        shutil.copy(
            self.quadtreeFilePath, os.path.join(self.pbfFinalDir, "sharding.txt")
        )

    def sync(self) -> None:
        """
        Sync an existing instance with pbf shards with s3
        Dependencies:
            self.s3Folder
        """
        if self.s3Folder is None:
            logger.info(
                "No S3 output folder specified, skipping s3 sync. Use -o 's3folder/path' to sync to s3"
            )
            return
        if not os.path.exists(self.pbfFinalDir):
            finish("local PBF directory does not exist. Try running 'shard' first", -1)
        logger.info("Syncing final pbf files with S3.")
        # push sharded pbfs to s3
        logger.info("Syncing final shards to S3 bucket {}.".format(self.s3Folder))
        aws_cmd = [
            "aws",
            "s3",
            "sync",
            "--quiet",
            self.pbfFinalDir,
            "s3://{}".format(self.s3Folder),
        ]
        if subprocess.run(aws_cmd).returncode:
            finish("ERROR: Unable to sync with S3", -1)

    def clean(self) -> None:
        """
        Clean a running Instance
        """
        logger.info("Cleaning up.")
        if subprocess.run(
            ["rm", "-rf", self.osmiumCfgDir, self.pbfDir, self.tmpDir, self.logDir]
        ).returncode:
            finish("unable to clean up instance", -1)


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
    def tile_xyz_from_line(line: str) -> Tuple[int]:
        if line.endswith("+"):
            line = line[:-1]
        zxy = line.split("-")
        return (int(zxy[1]), int(zxy[2]), int(zxy[0]))

    @staticmethod
    def read(tree_dump_file: TextIO) -> "SlippyTileQuadTreeNode":
        line = next(tree_dump_file)[:-1]  # remove '\n' at the end
        x, y, z = SlippyTileQuadTreeNode.tile_xyz_from_line(line)
        children = []
        if line.endswith("+"):
            for i in range(SlippyTileQuadTreeNode.MAXIMUM_CHILDREN):
                children.append(SlippyTileQuadTreeNode.read(tree_dump_file))

        return SlippyTileQuadTreeNode(x, y, z, children)

    # Return the (latitude, longitude) for the NW corner point of a tile
    # Logic copied from https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    @staticmethod
    def tile_nw_latlon(x, y, z) -> Tuple[float, float]:
        n = 1 << z
        minlon_deg = x / n * 360.0 - 180.0
        maxlat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        maxlat_deg = math.degrees(maxlat_rad)
        return (maxlat_deg, minlon_deg)

    # Return the bbox in (minlat, minlon, maxlat, maxlon) for the tile.
    def to_bbox(self) -> None:
        maxlat, minlon = SlippyTileQuadTreeNode.tile_nw_latlon(
            self.tile_x, self.tile_y, self.tile_z
        )
        minlat, maxlon = SlippyTileQuadTreeNode.tile_nw_latlon(
            self.tile_x + 1, self.tile_y + 1, self.tile_z
        )
        return (minlat, minlon, maxlat, maxlon)

    def is_leaf(self) -> bool:
        return len(self.children) == 0

    def to_str(self) -> str:
        zxy = f"{self.tile_z}-{self.tile_x}-{self.tile_y}"
        return zxy if self.is_leaf() else f"{zxy}+"

    def leaves(self, nodes: List["SlippyTileQuadTreeNode"]) -> None:
        if self.is_leaf():
            nodes.append(self)
            return
        else:
            for c in self.children:
                c.leaves(nodes)

    # Count 3 stats about the sub tree starting from this node ("self"):
    # (1) max level for leaf nodes, i.e., depth of the subtree
    # (2) min level with any leaf nodes
    # (3) total number of leaf nodes
    # For a subtree, the level starts with 0 from the root, and increases as one goes
    # deeper along the tree.
    def scan_meta_data(self) -> Tuple[int]:
        if self.is_leaf():
            self.max_leaf_level = 0
            self.min_leaf_level = 0
            self.num_leaves = 1
        else:
            children_stats = [c.scan_meta_data() for c in self.children]
            self.max_leaf_level = 1 + max([t[0] for t in children_stats])
            self.min_leaf_level = 1 + min([t[1] for t in children_stats])
            self.num_leaves = sum([t[2] for t in children_stats])

        return (self.max_leaf_level, self.min_leaf_level, self.num_leaves)

    def collect_subtrees_on_level(
        self, subtrees: List["SlippyTileQuadTreeNode"], level: int
    ) -> None:
        if level == 0:
            subtrees.append(self)
        else:
            for c in self.children:
                c.collect_subtrees_on_level(subtrees, level - 1)

    def pbf_file_name(self) -> str:
        zxy = f"{self.tile_z}-{self.tile_x}-{self.tile_y}"
        return (
            f"{zxy}-final.osm.pbf" if self.is_leaf() else f"{zxy}-intermediate.osm.pbf"
        )


def parse_args(sharder: PBFShardCtl) -> argparse.ArgumentParser:
    """
    Parse user parameters
    return: args

    """

    parser = argparse.ArgumentParser(
        description="This script controls the sharding process on a local system."
    )
    parser.add_argument(
        "-v", "--version", help="Display the current version", action="store_true"
    )

    subparsers = parser.add_subparsers(
        title="commands",
        description="One of the following commands must be specified when executed. "
        + "To see more information about each command and the parameters that are used for each command then specify the command and "
        + 'the --help parameter. (e.g. "./PBFShardCtl.py prep --help")',
    )

    parser_prep = subparsers.add_parser(
        "prep", help="Prepare the local system to execute the sharding process."
    )
    parser_prep.set_defaults(func=sharder.prep)

    parser_shard = subparsers.add_parser("shard", help="Start the sharding process.")
    parser_shard.add_argument(
        "-p",
        "--pbf",
        required=True,
        help="pbf URL - pointer to the pbf to shard. (e.g. '--pbf=https://download.geofabrik.de/central-america-latest.osm.pbf')",
    )
    parser_shard.add_argument(
        "-P",
        "--processes",
        type=int,
        help="Number of parallel osmium processes to use. (Default: {})".format(
            sharder.maxOsmiumProcesses
        ),
    )
    parser_shard.add_argument(
        "--quadtree_txt_file",
        help="Text file dump of a quadtree. (Default: {})".format(
            sharder.quadtreeFileName
        ),
    )
    parser_shard.add_argument(
        "--max_shard_per_step",
        type=int,
        help="Max number of shards in one sharding step (i.e. how many shards a single intermediate pbf will be split into. Default: {})".format(
            sharder.maxShardPerStep
        ),
    )
    parser_shard.add_argument(
        "--max_shard_per_config",
        type=int,
        help="Max number of shards in one sharding config (e.g. one osmium 'extract' run. Default: {})".format(
            sharder.maxShardPerConfig
        ),
    )
    parser_shard.set_defaults(func=sharder.shard)

    parser_sync = subparsers.add_parser(
        "sync", help="Sync pbf files from this system to an S3 bucket and folder"
    )
    parser_sync.add_argument(
        "-o", "--out", required=True, help="Out - The S3 Output directory."
    )
    parser_sync.set_defaults(func=sharder.sync)

    parser_clean = subparsers.add_parser(
        "clean", help="Clean up sharding instance accoutrements"
    )
    parser_clean.set_defaults(func=sharder.clean)

    args = parser.parse_args()
    return args


def execute(args, sharder: PBFShardCtl):
    """
    Evaluate the given arguments.
    :param args: The user's input.
    """

    if hasattr(args, "quadtree_txt_file") and args.quadtree_txt_file:
        sharder.quadtreeFileName = args.quadtree_txt_file
    if hasattr(args, "max_shard_per_step") and args.max_shard_per_step:
        sharder.maxShardPerStep = args.max_shard_per_step
    if hasattr(args, "max_shard_per_config") and args.max_shard_per_config:
        sharder.maxShardPerConfig = args.max_shard_per_config
    if hasattr(args, "out") and args.out:
        sharder.s3Folder = args.out
    if hasattr(args, "pbf") and args.pbf:
        sharder.pbfURL = args.pbf
    if hasattr(args, "processes") and args.processes:
        sharder.maxOsmiumProcesses = args.processes
    if args.version:
        logger.critical("This is version {0}.".format(VERSION))
        finish()

    if hasattr(args, "func") and args.func:
        args.func()
    else:
        finish("A command must be specified. Try '-h' for help.")


logger = setup_logging()

if __name__ == "__main__":
    sharder = PBFShardCtl()
    args = parse_args(sharder)
    execute(args, sharder)
    finish()
