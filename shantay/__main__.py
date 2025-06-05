import sys

if __name__ == "__main__":
    # To be fully effective, this function must be invoked before the model,
    # schema, or stats modules have been loaded. That is clearly the case only
    # right here.
    from ._platform import sync_web_platforms
    action = sync_web_platforms()
    if action == "disk":
        raise AssertionError(
            "Syncing the platform names only updated the on-disk shantay._platform\n"
            "module, but somehow couldn't update the in-memory representation.\n"
            "Please file a bug report at\n"
            "    https://github.com/apparebit/shantay/issues/new/choose\n\n"
        )

    from .tool import run
    sys.exit(run(sys.argv[1:]))

