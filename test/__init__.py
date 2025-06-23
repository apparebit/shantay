import importlib
import pathlib
import unittest


# It would have been easier to add necessary imports manually,
# but this can serve as recipe for future projects, too!

for direntry in sorted(pathlib.Path(__file__).parent.glob("test_*.py")):
    # For each submodule named test.test_something, ...
    if not direntry.is_file():
        continue

    # ... import the module, ...
    module_name = f"test.{direntry.stem}"
    module = importlib.import_module(module_name)

    # ... then iterate over its public symbols, ...
    for name in dir(module):
        if name.startswith("_"):
            continue

        # ... and, if the value is a subclass of unittest.TestCase, ...
        value = getattr(module, name)
        if not isinstance(value, type) or not issubclass(value, unittest.TestCase):
            continue

        # ... make the value available in this module, too.
        globals()[name] = value
