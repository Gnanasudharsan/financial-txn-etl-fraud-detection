"""Structured-ish logging shared by all jobs."""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager

_FORMAT = "%(asctime)s %(levelname)-5s %(name)s | %(message)s"
_configured = False


def get_logger(name: str) -> logging.Logger:
    global _configured  # noqa: PLW0603
    if not _configured:
        logging.basicConfig(level=logging.INFO, format=_FORMAT, stream=sys.stdout)
        for noisy in ("py4j", "botocore", "urllib3", "great_expectations", "alembic"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        _configured = True
    return logging.getLogger(name)


@contextmanager
def timed(log: logging.Logger, label: str) -> Iterator[dict[str, float]]:
    """Log wall-clock duration of a block; yields a dict that receives ``seconds``."""
    out: dict[str, float] = {}
    start = time.perf_counter()
    log.info("start: %s", label)
    try:
        yield out
    finally:
        out["seconds"] = round(time.perf_counter() - start, 2)
        log.info("done:  %s in %.1fs", label, out["seconds"])
