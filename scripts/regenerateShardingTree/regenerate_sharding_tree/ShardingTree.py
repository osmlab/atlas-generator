import os
import re
from typing import Iterable, List, Optional, Union

from tqdm import tqdm

from .file_information import FileInformation, FileInformationBase
from .Tile import Tile


class ShardingTree(object):
    """A sharding tree (tiles only)"""

    shard_pattern: re.Pattern = re.compile(r"^(\d+)-(\d+)-(\d+)(\+?)$")
    """
    The shard pattern in the format of "zoom-x-y" with a trailing `+`
    if the shard is not the last shard
    """
    tile_pattern: re.Pattern = re.compile(r"(\d+)-(\d+)-(\d+)")
    """This pattern is expected to appear in filenames"""
    file_extensions = [".atlas", ".pbf", ".osm"]
    """Valid file extensions for sharded files"""
    tiles: List[Tile]
    """The tiles from the sharding tree"""

    def __init__(
        self, sharding: Union[str, Iterable[str], Iterable[Tile]], **kwargs
    ):
        """
        Initialize a sharding tree
        :param sharding: The sharding information, either a file name or an
                         iterable of strings
        :param kwargs: Any additional information needed to read the file
        """
        self.tiles = list(
            ShardingTree.read_tiles_from_sharding_tree(sharding, **kwargs)
        )

    def update_sharding_tree(
        self, shard_directory: str, size: int = 5, max_zoom: int = 14
    ) -> "ShardingTree":
        """
        Update the sharding tree using shards in the specified directory
        :param shard_directory: The directory to use
        :param size: The maximum size of the file in MiB
        :param max_zoom: The maximum zoom (defaults to 14)
        :return: A new ShardingTree with the split tiles
        """
        return ShardingTree(
            ShardingTree.update_sharding_tiles(
                self, shard_directory, size=size, max_zoom=max_zoom
            )
        )

    def to_iterable_str(self) -> Iterable[str]:
        """
        Return an iterable of strings for writing
        :return: An iterable with strings
        """
        for tile in self.tiles:
            yield str(tile)

    @staticmethod
    def update_sharding_tiles(
        sharding_tree: "ShardingTree",
        shard_directory: str,
        size: int = 5,
        max_zoom: int = 14,
    ) -> Iterable[Tile]:
        """
        Update the sharding tree using shards in the specified directory

        :param sharding_tree: The original sharding tree
        :param shard_directory: The directory to use
        :param size: The maximum size of the file in MiB
        :param max_zoom: The maximum zoom (defaults to 14)
        :return: A new sharding tree as an iterable of tiles
        """
        # Account for z/x/y and z-x-y naming schemes
        # Examples:
        #   10/1002/648/10-10002-648.pbf
        #   USA/USA_12-1209-1539.atlas

        # Convert the size to bytes
        size: int = size << 20
        for tile in sharding_tree.tiles:
            # Short-circuit if we have already reached max zoom or the
            # tile already has children
            if tile.zoom >= max_zoom or not tile.final:
                yield tile
                continue
            filename = ShardingTree._find_file(
                tile, shard_directory=shard_directory
            )
            if filename is not None and filename.getsize() > size:
                for new_tile in tile.split():
                    yield new_tile
            else:
                yield tile

    @staticmethod
    def _find_file(
        tile: Tile, shard_directory: str = None
    ) -> Optional[FileInformationBase]:
        """
        Find the file for a tile
        :param tile: The tile to search for
        :param shard_directory: The directory to search in
        :return: The filename for the shard
        """
        directory_name = os.path.join(str(tile.zoom), str(tile.x), str(tile.y))
        file_information = FileInformation.get_file_information(
            shard_directory, directory_name
        )
        if file_information.isdir():
            files = list(file_information.scandir())
            if len(files) == 1:
                return files[0]
        else:
            files = FileInformation.get_file_information(
                os.path.dirname(shard_directory),
                os.path.basename(shard_directory),
            ).scandir()
        for file in files:
            path = file.path
            if hasattr(path, "path"):
                path = path.path
            file_ext = os.path.splitext(path)
            if file_ext[1] not in ShardingTree.file_extensions:
                continue
            if ShardingTree.tile_pattern.match(path):
                return file

    @staticmethod
    def read_tiles_from_sharding_tree(
        sharding: Union[str, Iterable[Union[str, Tile]]], **kwargs
    ) -> Iterable[Tile]:
        """
        Read tiles from a sharding tree

        :param sharding: The sharding information, either a file name or a
                         iterable of strings
        :param kwargs: The arguments necessary to read the file
        :return: An iterable of tiles
        """
        generator: Iterable[Union[str, Tile]]
        if isinstance(sharding, str):
            generator = ShardingTree._file_line_generator(sharding, **kwargs)
        else:
            generator = sharding
        for line in tqdm(
            generator,
            desc="Reading tiles from sharding tree"
            + ((" from " + sharding) if isinstance(sharding, str) else ""),
        ):
            if isinstance(line, Tile):
                yield line
                continue
            match = ShardingTree.shard_pattern.match(line)
            if match:
                zoom: int = int(match.group(1))
                x: int = int(match.group(2))
                y: int = int(match.group(3))
                final: bool = True if not match.group(4) else False
                yield Tile(zoom, x, y, final=final)

    @staticmethod
    def _file_line_generator(sharding: str, **kwargs) -> Iterable[str]:
        """
        Read a file line-by-line as a generator
        :param sharding: The name of the sharding file
        :param kwargs: The additional arguments necessary to read the file
        :return: A string iterable
        """
        for line in FileInformation.get_file_information(
            os.path.dirname(sharding), os.path.basename(sharding), **kwargs
        ).file_line_generator():
            yield line
