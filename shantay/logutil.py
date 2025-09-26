import enum
import logging
from types import MappingProxyType


_logger = logging.getLogger(__package__)


def configure_logging(logfile: str, *, level: int = logging.NOTSET) -> None:
    logging.Formatter.default_msec_format = "%s.%03d"
    logging.basicConfig(
        format='%(asctime)s︙%(process)d︙%(name)s︙%(levelname)s︙%(message)s',
        filename=logfile,
        encoding="utf8",
        level=level,
    )


_RULES = MappingProxyType({
    1: '────────────────────────────────────────────────────────────'
    '────────────────────────────────────────────────────────────',
    2: '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
    3: '════════════════════════════════════════════════════════════'
    '════════════════════════════════════════════════════════════',
})


class Size(enum.IntEnum):
    """An enumeration of rule thickness sizes."""
    S = 1
    M = 2
    L = 3


def log_rule(size: Size = Size.M) -> None:
    _logger.info(_RULES[size])


def log_max_rss(release: str) -> None:
    from .util import get_max_rss

    size = get_max_rss()
    if size is not None:
        _logger.info(
            'maximum resident-set-size=%s, unit="byte", release="%s"',
            f'{size:_}', release
        )
