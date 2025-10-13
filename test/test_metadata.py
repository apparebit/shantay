from pathlib import Path
import unittest

from shantay.metadata import _FILE_TYPE, Metadata
from shantay.model import MetadataConflict
from shantay.schema import normalize_category, StatementCategoryProtectionOfMinors

ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"
METADATA_2000 = FIXTURE / "metadata" / "2000" / "meta.json"

CATEGORY = "protection_of_minors"


class TestMetadata(unittest.TestCase):

    def test_category(self) -> None:
        self.assertEqual(
            normalize_category(CATEGORY),
            StatementCategoryProtectionOfMinors
        )

    def check_metadata_2000(self, metadata: Metadata) -> None:
        self.assertEqual(metadata.stem, "doom")
        assert metadata.filter is not None
        self.assertEqual(metadata.filter.kind, "platform")
        self.assertEqual(metadata.filter.criterion, ("End", "of", "THE WORLD"))
        self.assertListEqual([*metadata.records], [
            {"release": "1999-12-31", "batch_count": 665},
            {"release": "2000-01-01", "batch_count":   1},
        ])
        self.assertEqual(metadata.tag(), "PLATFORM_END_OF_THE_WORLD")

    def test_new_metadata(self) -> None:
        # Instantiate metadata
        metadata = Metadata.for_category(normalize_category(CATEGORY))
        self.assertEqual(metadata.stem, "protection-of-minors")
        assert metadata.filter is not None
        self.assertEqual(metadata.filter.kind, "category")
        self.assertEqual(metadata.filter.criterion, StatementCategoryProtectionOfMinors)
        self.assertListEqual([*metadata.records], [])
        self.assertEqual(metadata.tag(), StatementCategoryProtectionOfMinors)

    def test_merge_same_metadata(self) -> None:
        # Merge with identical data
        metadata = Metadata.merge(
            METADATA_2000.parent / "meta.json",
            METADATA_2000.parent / "meta.json"
        )
        self.check_metadata_2000(metadata)

    def test_find_metadata(self) -> None:
        # Let's do some glassbox testing first...
        bytes = METADATA_2000.read_bytes()
        prefix = bytes[:bytes.find(b'\n', 3)]
        self.assertTrue(prefix.endswith(b'",'))
        self.assertTrue(_FILE_TYPE.match(prefix))

        # Then some helper method testing...
        self.assertTrue(Metadata.is_file(METADATA_2000))

        # Before hitting the real method
        self.assertEqual(
            Metadata.find_file(METADATA_2000.parent).name,
            METADATA_2000.name
        )

    def test_merge_release(self) -> None:
        metadata = Metadata.read_json(METADATA_2000)
        self.check_metadata_2000(metadata)
        metadata.merge_release("2042-01-01", {"batch_count": 42})
        self.assertEqual(len(metadata), 3)
        for record in metadata.records:
            self.assertEqual(len(record), 2)

    def test_merge_with_extra_data(self) -> None:
        # When one record has more data, that becomes authoritative
        metadata = Metadata.read_json(METADATA_2000)
        self.check_metadata_2000(metadata)

        metadata_too = Metadata.read_json(METADATA_2000)
        metadata_too["1999-12-31"]["sha256"] = "test-digest"

        metadata = metadata.merge_with(metadata_too)
        self.assertDictEqual(metadata["1999-12-31"], {
            "batch_count": 665,
            "sha256": "test-digest",
        })

    def test_merge_with_inconsistent_data(self) -> None:
        # Fields must not diverge when they are part of the core schema
        metadata = Metadata.read_json(METADATA_2000)
        metadata_too = Metadata.read_json(METADATA_2000)
        metadata_too["1999-12-31"]["batch_count"] = 666
        self.assertRaises(MetadataConflict, metadata.merge_with, metadata_too)
