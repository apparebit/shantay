#!.venv/bin/python

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
import unittest

from test.runtime import get_ticker, load_tests, ResultAdapter, StyledStream

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
    del sys.argv[1:]

    if not options.skip_types and os.name != "nt":
        print(styled.h0("Type Checking…"))
        try:
            subprocess.run(["npm", "run", "pyright"], check=True)
        except subprocess.CalledProcessError:
            print(styled.failure("shantay failed to type check!"))
            sys.exit(1)

    print(styled.h0("Testing…"))
    print()

    # Prepare staging, version number, logging, and test classes.
    ticker = get_ticker(stream)

    STAGING = Path(__file__).parent / "test" / "tmp"
    shutil.rmtree(STAGING, ignore_errors=True)
    STAGING.mkdir(parents=True)
    ticker()

    import shantay
    shantay.__version__ = "665.0"
    ticker()

    from shantay.__main__ import configure_logging
    configure_logging(str(STAGING / "log.log"), verbose=True)
    ticker()

    load_tests()
    ticker()

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
