#!.venv/bin/python

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback
import unittest

from test.runtime import ResultAdapter, setup, StyledStream


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

    # ==================================================================================
    if not options.skip_types and os.name != "nt":
        print(styled.h0("Type Checking…"), flush=True)
        try:
            subprocess.run(["npm", "run", "pyright"], check=True)
        except subprocess.CalledProcessError:
            print(styled.failure("shantay failed to type check!"), flush=True)
            sys.exit(1)

    # ==================================================================================
    print(styled.h0("Testing…"))
    print("", flush=True)
    setup(stream)

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
        print(styled.err(trace[-1]), flush=True)

    sys.exit(not successful)
