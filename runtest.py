#!./.venv/bin/python

import sys
import traceback
import unittest

from test.runtime import ResultAdapter, StyledStream

if __name__ == "__main__":
    successful = False
    stream = sys.stdout
    styled = StyledStream(stream)

    print(styled.h0("Tests Are Running…"))
    print()

    try:
        runner = unittest.main(
            module="test",
            exit=False,
            testRunner=unittest.TextTestRunner(
                stream=stream, resultclass=ResultAdapter
            ),
        )
        successful = runner.result.wasSuccessful()
    except Exception as x:
        trace = traceback.format_exception(x)
        print("".join(trace[:-1]))
        print(styled.err(trace[-1]))

    sys.exit(not successful)
