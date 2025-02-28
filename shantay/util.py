import functools
import inspect
from typing import Callable


def annotate_error[**P, R, F: Callable[P, R]](
    filename_arg: None | str = None
) -> Callable[[F], F]:
    """
    Annotate errors with missing information.

    Notably, if the error is an OSError without filename attribute, this wrapper
    determines the value of the named argument and updates the error's filename
    attribute with the stringified value of that argument.

    This decorator is motivated by shutil.copyfileobj() not setting the filename
    attribute upon OS error number 28, no space left on device, even though the
    file path is critical for determining the impacted device. Hence the wrapper
    updates the error's filename attribute with the stringified value of the
    named argument. That is, unless the filename is already set, in which case
    the wrapper does nothing.
    """
    def wrapper(fn: F) -> F:
        # No argument, nothing to annotate with
        if filename_arg is None:
            return fn

        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def inner(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                return fn(*args, **kwargs)
            except OSError as x:
                if x.filename is None:
                    value = sig.bind(*args, **kwargs).arguments[filename_arg]
                    x.filename = str(value)
        return inner
    return wrapper
