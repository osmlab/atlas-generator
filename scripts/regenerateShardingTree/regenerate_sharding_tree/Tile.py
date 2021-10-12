import math
from typing import Iterable, Tuple

from geojson import Feature, Polygon


class Tile(object):
    """A tile class"""

    zoom: int
    """The zoom level of the tile"""
    x: int
    """The x coordinate of the tile"""
    y: int
    """The y coordinate of the tile"""
    final: bool
    """True if the tile is the "final" tile."""
    new: bool
    """New if this is from a split shard"""

    def __init__(
        self, zoom: int, x: int, y: int, final: bool = True, new: bool = False
    ):
        """
        Create a new tile
        :param zoom: The zoom level to use
        :param x: The x coordinate
        :param y: The y coordinate
        :param final: If there are no further tiles in this branch of the tree
        :param new: If this is a new shard
        """
        self.zoom = zoom
        self.x = x
        self.y = y
        self.final = final
        self.new = new

    def split(self) -> Iterable["Tile"]:
        """
        Split the tile (aka zoom in). Returns a copy of this tile marked as
        non-final.
        :return A Tile iterable, with the first tile being this tile marked as
        non-final
        """
        start_x: int = 2 * self.x
        start_y: int = 2 * self.y
        zoom: int = self.zoom + 1
        yield Tile(self.zoom, self.x, self.y, final=False)
        for x in range(0, 2):
            for y in range(0, 2):
                yield Tile(zoom, start_x + x, start_y + y, new=True)

    def __eq__(self, other):
        if isinstance(other, Tile):
            return (
                self.zoom == other.zoom
                and self.x == other.x
                and self.y == other.y
                and self.final == other.final
            )
        return False

    def __str__(self):
        return f"{self.zoom}-{self.x}-{self.y}" + (
            "+" if not self.final else ""
        )

    def to_feature(self) -> Feature:
        """Convert the tile into a geojson Feature object"""
        bounds = Tile.tile_latlon_bounds(self)
        # Typing is wrong here -- expects [[(coord), (coord), (coord)...]]
        polygon = Polygon(
            coordinates=[
                [
                    (bounds[1], bounds[0]),
                    (bounds[1], bounds[2]),
                    (bounds[3], bounds[2]),
                    (bounds[3], bounds[0]),
                    (bounds[1], bounds[0]),
                ]
            ]
        )
        properties = {"shard": str(self)}
        if self.new:
            properties["new"] = self.new
        return Feature(geometry=polygon, properties=properties)

    @staticmethod
    def tile_latlon_bounds(tile: "Tile") -> Tuple[float, float, float, float]:
        """
        Get the tile bounds
        :param tile: The tile to get the bounds for
        :return: a tuple in the format of (lat, lon, lat, lon) ordered so
                 it could be grouped like ([upper left], [lower right])
        """
        lon1 = Tile.x_to_lon(tile.x, tile.zoom)
        lon2 = Tile.x_to_lon(tile.x + 1, tile.zoom)
        lat1 = Tile.y_to_lat(tile.y, tile.zoom)
        lat2 = Tile.y_to_lat(tile.y + 1, tile.zoom)
        return lat1, lon1, lat2, lon2

    @staticmethod
    def x_to_lon(x: int, zoom: int) -> float:
        """
        Get the upper-left longitude for the x coordinate
        :param x: The x coordinate
        :param zoom: The zoom level
        :return: The longitude
        """
        return (x / 2 ** zoom) * 360.0 - 180.0

    @staticmethod
    def y_to_lat(y: int, zoom: int) -> float:
        """
        Get the upper-left latitude for the y coordiante
        :param y: The y coordinate
        :param zoom: The zoom level
        :return: The latitude
        """
        step_one = math.pi - 2 * math.pi * y / 2 ** zoom
        return (
            180
            / math.pi
            * math.atan((math.exp(step_one) - math.exp(-step_one)) / 2)
        )
