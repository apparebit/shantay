from collections import Counter
import datetime as dt
import io
from pathlib import Path
import random
import shutil
import zipfile

from shantay.metadata import Metadata
from shantay.model import Config, DateRange, Release, Storage
from shantay.dsa_sor import StatementsOfReasons
from shantay.processor import Processor


def main() -> None:
    random.seed(665)

    dataset = StatementsOfReasons()
    release_range = DateRange(dt.date(2025,8,11), dt.date(2025,8, 14)).dailies()
    storage = Storage(
        archive_root=Path("/Volumes/dsa/archive"),
        extract_root=None,
        staging_root=Path("dsa-db-staging"),
    )

    fix = storage.staging_root / "fixture"
    fix.mkdir(exist_ok=True)

    processor = Processor(
        dataset=dataset,
        storage=storage,
        coverage=release_range,
        config=Config(),
        metadata=Metadata("db"),
    )

    for release in release_range:
        release_dir = storage.staging_root / release.directory
        release_dir.mkdir(parents=True, exist_ok=True)

        input_archive_path = processor.stage_archive(release)
        output_archive_path = fix / input_archive_path.name

        with zipfile.ZipFile(input_archive_path) as input_archive:
            release_dir = storage.staging_root / release.directory
            release_dir.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(output_archive_path, mode="x") as output_archive:

                print(f"creating {output_archive_path.name}")

                count = random.randrange(3, 7)
                for index, name in enumerate(sorted(input_archive.namelist())):
                    with input_archive.open(name) as nested_zipfile:
                        with zipfile.ZipFile(nested_zipfile) as input_archive2:

                            buffer = io.BytesIO()
                            with zipfile.ZipFile(
                                buffer, mode="x", compression=zipfile.ZIP_DEFLATED
                            ) as output_archive2:

                                print(f"    {name}")

                                count2 = random.randrange(3, 7)
                                for index2, name2 in enumerate(
                                    sorted(input_archive2.namelist())
                                ):
                                    buffer2 = io.BytesIO()
                                    with input_archive2.open(name2) as input:
                                        for _ in range(random.randrange(11, 23)):
                                            line = input.readline()
                                            buffer2.write(line)

                                    print(f"        {name2}")

                                    output_archive2.writestr(name2, buffer2.getvalue())
                                    if count2 <= index2:
                                        break

                            output_archive.writestr(name, buffer.getvalue())

                    if count <= index:
                        break

        shutil.rmtree(release_dir)


if __name__ == "__main__":
    main()

