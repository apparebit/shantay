from pathlib import Path
import unittest

from shantay.metadata import Metadata
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
        self.assertEqual(metadata.category, "END_OF_THE_WORLD")
        self.assertListEqual([*metadata.records], [
            {"release": "1999-12-31", "batch_count": 665},
            {"release": "2000-01-01", "batch_count":   1},
        ])

    def test_new_metadata(self) -> None:
        # Instantiate metadata
        metadata = Metadata(normalize_category(CATEGORY))
        self.assertEqual(metadata.category, StatementCategoryProtectionOfMinors)
        self.assertListEqual([*metadata.records], [])

    def test_merge_same_metadata(self) -> None:
        # Merge with identical data
        metadata = Metadata.merge(
            METADATA_2000.parent / "meta.json",
            METADATA_2000.parent / "meta.json"
        )
        self.check_metadata_2000(metadata)

    def test_merge_with_extra_data(self) -> None:
        # When one record has more data, that becomes authoritative
        metadata = Metadata.read_json(METADATA_2000)
        self.check_metadata_2000(metadata)

        metadata_too = Metadata.read_json(METADATA_2000)
        self.assertDictEqual(metadata_too["2000-01-01"], {"batch_count": 1})
        self.assertEqual(metadata.batch_count("1999-12-31"), 665)
        self.assertEqual(metadata.batch_count("2000-01-01"), 1)

        metadata_too["2000-01-01"]["batch_memory"] = 1_234_567
        metadata = metadata.merge_with(metadata_too)
        self.assertDictEqual(metadata["2000-01-01"], {
            "batch_count": 1,
            "batch_memory": 1_234_567,
        })

    def test_merge_with_ignored_data(self) -> None:
        # Fields may diverge if they are not part of the core schema
        metadata = Metadata.read_json(METADATA_2000)
        metadata["2000-01-01"]["batch_memory"] = 1_234_567
        metadata_too = Metadata.read_json(METADATA_2000)
        metadata_too["2000-01-01"]["batch_memory"] = 666

        metadata.merge_with(metadata_too)
        self.assertEqual(metadata["2000-01-01"].get("batch_memory"), 1_234_567)

    def test_merge_with_inconsistent_data(self) -> None:
        # Fields must not diverge when they are part of the core schema
        metadata = Metadata.read_json(METADATA_2000)
        metadata_too = Metadata.read_json(METADATA_2000)
        metadata_too["1999-12-31"]["batch_count"] = 666
        self.assertRaises(MetadataConflict, metadata.merge_with, metadata_too)
