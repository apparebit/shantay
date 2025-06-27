import hashlib
from pathlib import Path
from typing import Literal


def compute_digest(path: Path, algo: Literal["sha1", "sha256"] = "sha256") -> str:
    """Compute a file's digest."""
    with open(path, mode="rb") as file:
        return hashlib.file_digest(file, algo).hexdigest()


def compute_all_digests(directory: Path, glob: str) -> dict[str, str]:
    digests = {}
    for entry in directory.glob(glob):
        digests[entry.name] = compute_digest(entry)
    return digests


def read_digest_file(path: Path) -> dict[str, str]:
    """Read a text file listing file names and their digests."""
    digests = {}
    with open(path, mode="r", encoding="utf8") as file:
        for line in file.readlines():
            digest, batchfile = line.strip().split(" ")
            digests[batchfile] = digest
    return digests


def write_digest_file(path: Path, digests: dict[str, str]) -> None:
    """Write the text file with a list of file names and their digests."""
    tmp = path.with_suffix(".tmp.txt")

    with open(tmp, mode="w", encoding="utf8") as file:
        for batchfile, digest in digests.items():
            file.write(f"{digest} {batchfile}\n")

    tmp.replace(path)


def diff_digests(actual: dict[str, str], expected: dict[str, str]) -> None | str:
    if actual == expected:
        return None

    actual_files = set(actual.keys())
    expected_files = set(expected.keys())
    if actual_files != expected_files:
        unique_actual = actual_files - expected_files
        unique_expected = expected_files - actual_files

        msg = "directory "
        if unique_actual:
            msg += f"unexpectedly contains {', '.join(unique_actual)}"
        if unique_expected:
            if 20 < len(msg):
                msg += ", but "
            msg += f"does not contain {', '.join(unique_expected)}"
        return msg

    bad_digests = []
    for file in actual.keys():
        actual_digest = actual[file]
        expected_digest = expected[file]
        if actual_digest != expected_digest:
            bad_digests.append((file, actual_digest, expected_digest))

    if len(bad_digests) == 1:
        return (
            f"digest for {bad_digests[0][0]} is "
            f"{bad_digests[0][1]} instead of {bad_digests[0][2]}"
        )

    return f"digests for {', '.join(d[0] for d in bad_digests)} differ"


def validate_digests(
    directory: Path,
    glob: str,
    digest_path: Path,
    digest_of_digests: None | str = None,
    restore: bool = False,
) -> str:
    actual_digests = compute_all_digests(directory, glob)

    try:
        expected_digests = read_digest_file(digest_path)
    except FileNotFoundError:
        if not restore:
            raise
        expected_digests = actual_digests
        write_digest_file(digest_path, expected_digests)

    actual_digest_of_digests = compute_digest(digest_path)
    if digest_of_digests is not None and actual_digest_of_digests != digest_of_digests:
        raise ValueError("digest of digests diverges")

    diff = diff_digests(actual_digests, expected_digests)
    if diff is not None:
        raise ValueError(diff)

    return digest_of_digests or actual_digest_of_digests
