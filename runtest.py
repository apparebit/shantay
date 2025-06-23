#!./.venv/bin/python

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
import unittest

from shantay.__main__ import configure_logging
from test.runtime import ResultAdapter, StyledStream

if __name__ == "__main__":
    successful = False
    stream = sys.stdout
    styled = StyledStream(stream)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-types",
        action="store_true",
        help="skip type checking",
    )
    options = parser.parse_args(sys.argv[1:])

    if not options.skip_types and os.name != "nt":
        print(styled.h0("Type Checking…"))
        try:
            subprocess.run(["npm", "run", "pyright"], check=True)
        except subprocess.CalledProcessError:
            print(styled.failure("shantay failed to type check!"))
            sys.exit(1)

    print(styled.h0("Tests Are Running…"))
    print()

    # Recreate staging directory and configure logging
    STAGING = Path(__file__).parent / "test" / "tmp"
    shutil.rmtree(STAGING)
    STAGING.mkdir(exist_ok=True)
    configure_logging(str(STAGING / "log.log"), verbose=True)

    try:
        runner = unittest.main(
            module="test",
            exit=False,
            testRunner=unittest.TextTestRunner(
                stream=stream, resultclass=ResultAdapter # type: ignore
            ),
        )
        successful = runner.result.wasSuccessful()
    except Exception as x:
        trace = traceback.format_exception(x)
        print("".join(trace[:-1]))
        print(styled.err(trace[-1]))

    sys.exit(not successful)
