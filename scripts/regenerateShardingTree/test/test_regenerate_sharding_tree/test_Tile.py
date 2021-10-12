from typing import List
from unittest import TestCase, main

from regenerate_sharding_tree import Tile


class TestTile(TestCase):
    def test_split(self):
        tile: Tile = Tile(9, 101, 195, final=True)
        new_tiles: List[Tile] = list(tile.split())
        self.assertEqual(5, len(new_tiles))

        # Check the first tile -- must always be the originating tile,
        # but not final
        self.assertEqual(Tile(9, 101, 195, final=False), new_tiles[0])
        # Check that x increases before y
        self.assertEqual(Tile(10, 202, 390), new_tiles[1])
        self.assertEqual(Tile(10, 202, 391), new_tiles[2])
        self.assertEqual(Tile(10, 203, 390), new_tiles[3])
        self.assertEqual(Tile(10, 203, 391), new_tiles[4])

    def test_str(self):
        tile1: Tile = Tile(9, 101, 195, final=True)
        tile2: Tile = Tile(9, 101, 195, final=False)
        self.assertEqual("9-101-195", str(tile1))
        self.assertEqual("9-101-195+", str(tile2))

    def test_eq(self):
        tile1: Tile = Tile(9, 101, 195, final=True)
        tile2: Tile = Tile(9, 101, 195, final=False)
        self.assertNotEqual(tile1, tile2)
        self.assertEqual(Tile(9, 101, 195, final=True), tile1)
        self.assertEqual(Tile(9, 101, 195, final=False), tile2)
        self.assertNotEqual(Tile(9, 100, 195, final=True), tile1)
        self.assertNotEqual(Tile(9, 101, 194, final=True), tile1)
        self.assertNotEqual(Tile(9, 101, 196, final=True), tile1)
        self.assertNotEqual(Tile(9, 102, 195, final=True), tile1)
        self.assertNotEqual(Tile(8, 101, 195, final=True), tile1)
        self.assertNotEqual(Tile(10, 101, 195, final=True), tile1)
        self.assertNotEqual(1, tile1)

    def test_to_feature(self):
        tile1: Tile = Tile(9, 101, 195, new=True, final=False)
        tile2: Tile = Tile(9, 101, 195, new=False, final=True)
        dict1 = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-108.984375, 39.368279],
                        [-108.984375, 38.822591],
                        [-108.28125, 38.822591],
                        [-108.28125, 39.368279],
                        [-108.984375, 39.368279],
                    ]
                ],
            },
            "properties": {"shard": "9-101-195+", "new": True},
        }
        dict2 = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-108.984375, 39.368279],
                        [-108.984375, 38.822591],
                        [-108.28125, 38.822591],
                        [-108.28125, 39.368279],
                        [-108.984375, 39.368279],
                    ]
                ],
            },
            "properties": {"shard": "9-101-195"},
        }
        self.assertDictEqual(dict1, tile1.to_feature())
        self.assertDictEqual(dict2, tile2.to_feature())


if __name__ == "__main__":
    main()
