#!./.venv/bin/python

import os
import subprocess
import sys
import traceback
import unittest

from test.runtime import ResultAdapter, StyledStream

if __name__ == "__main__":
    successful = False
    stream = sys.stdout
    styled = StyledStream(stream)

    skip_types = False
    if "--skip-types" in sys.argv:
        del sys.argv[sys.argv.index("--skip-types")]
        skip_types = True

    if not skip_types and os.name != "nt":
        print(styled.h0("Type Checking…"))
        try:
            subprocess.run(["npm", "run", "pyright"], check=True)
        except subprocess.CalledProcessError:
            print(styled.failure("shantay failed to type check!"))
            sys.exit(1)

    print(styled.h0("Tests Are Running…"))
    print()

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
